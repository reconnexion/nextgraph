/*
 * Copyright (c) 2022-2025 Niko Bonnieure, Par le Peuple, NextGraph.org developers
 * All rights reserved.
 * Licensed under the Apache License, Version 2.0
 * <LICENSE-APACHE2 or http://www.apache.org/licenses/LICENSE-2.0>
 * or the MIT license <LICENSE-MIT or http://opensource.org/licenses/MIT>,
 * at your option. All files in the project carrying such
 * notice may not be copied, modified, or distributed except
 * according to those terms.
 */

//! Implementation of the Server Broker

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_std::sync::{Mutex, RwLock};
use either::Either;
use futures::{channel::mpsc, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

use ng_repo::{
    block_storage::BlockStorage,
    errors::{NgError, ProtocolError, ServerError},
    log::*,
    types::*,
};

use ng_net::{
    app_protocol::*,
    actor::SoS,
    broker::{ClientPeerId, BROKER},
    connection::NoiseFSM,
    server_broker::IServerBroker,
    types::*,
    utils::{spawn_and_log_error, Receiver, ResultSend, Sender},
};

use ng_verifier::{
    site::SiteV0,
    types::{BrokerPeerId, VerifierConfig, VerifierConfigType},
    verifier::Verifier,
};

use crate::rocksdb_server_storage::RocksDbServerStorage;

pub struct TopicInfo {
    pub repo: RepoHash,

    pub publisher_advert: Option<PublisherAdvert>,

    pub current_heads: HashSet<ObjectId>,

    pub root_commit: Option<ObjectId>,

    /// indicates which users have opened the topic (boolean says if as publisher or not)
    pub users: HashMap<UserId, bool>,
}

pub struct RepoInfo {
    /// set of users that requested the repo to be exposed on the outer overlay
    /// only possible if the user is a publisher
    pub expose_outer: HashSet<UserId>,

    /// set of topics of this repo
    pub topics: HashSet<TopicId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventInfo {
    pub event: Event,
    pub blocks: Vec<BlockId>,
}

pub struct CommitInfo {
    pub event: Either<EventInfo, TopicId>,
    pub home_pinned: bool,
    pub acks: HashSet<ObjectId>,
    pub deps: HashSet<ObjectId>,
    pub futures: HashSet<ObjectId>,
    pub files: HashSet<ObjectId>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum OverlayType {
    OuterOnly,
    Outer(OverlayId), // the ID of the inner overlay corresponding to this outer.
    Inner(OverlayId), // the ID of the outer overlay corresponding to the inner
    InnerOnly,
}

impl OverlayType {
    pub fn is_inner_get_outer(&self) -> Option<&OverlayId> {
        match self {
            Self::Inner(outer) => Some(outer),
            _ => None,
        }
    }
    pub fn is_outer_to_inner(&self) -> bool {
        match self {
            Self::Outer(_) => true,
            _ => false,
        }
    }
    pub fn is_outer_only(&self) -> bool {
        match self {
            Self::OuterOnly => true,
            _ => false,
        }
    }
}

impl From<OverlayAccess> for OverlayType {
    fn from(oa: OverlayAccess) -> OverlayType {
        match oa {
            OverlayAccess::ReadOnly(_) => {
                panic!("cannot create an OverlayType from a ReadOnly OverlayAccess")
            }
            OverlayAccess::ReadWrite((_inner, outer)) => OverlayType::Inner(outer),
            OverlayAccess::WriteOnly(_inner) => OverlayType::InnerOnly,
        }
    }
}

#[allow(dead_code)]
pub(crate) struct OverlayInfo {
    pub overlay_type: OverlayType,
    pub overlay_topic: Option<TopicId>,
    pub topics: HashMap<TopicId, TopicInfo>,
    pub repos: HashMap<RepoHash, RepoInfo>,
}

struct DetachableVerifier {
    detach: bool,
    attached: Option<(DirectPeerId, u64)>,
    verifier: Verifier,
}

/// A plugin served by a connected (headless) app.
pub struct ActivePlugin {
    peer: DirectPeerId, // peer-id of the app serving this plugin
}

pub struct ServerBrokerState {
    #[allow(dead_code)]
    overlays: HashMap<OverlayId, OverlayInfo>,
    #[allow(dead_code)]
    inner_overlays: HashMap<OverlayId, Option<OverlayId>>,

    local_subscriptions: HashMap<(OverlayId, TopicId), HashMap<PubKey, Option<UserId>>>,

    verifiers: HashMap<UserId, Arc<RwLock<DetachableVerifier>>>,
    remote_apps: HashMap<(DirectPeerId, u64), UserId>,

    wallet_rendezvous: HashMap<SymKey, Sender<ExportedWallet>>,
    wallet_exports: HashMap<SymKey, ExportedWallet>,
    wallet_exports_timestamp: BTreeMap<SystemTime, SymKey>,

    // Accumulated stats per user while they are connected:
    #[cfg(feature = "usage-stats")]
    usage_stats_storage_bytes: HashMap<UserId, u64>,

    /// Plugins that are connected and enabled, by plugin name.
    /// Plugins are added when they register while enabled, or when the admin
    /// enables them while connected (see app_plugin_activate). They are removed
    /// when the admin moves them away from Enabled (see app_plugin_deactivate),
    /// and lazily when ngd fails to forward them a message because they are no
    /// longer connected.
    /// There is at most one plugin in charge of any given service.
    active_plugins: HashMap<String, ActivePlugin>,
    /// Index of plugin names by peer. Updated in the same circumstances as the
    /// above active_plugins.
    /// In theory, a single plugin (peer-id) can be in charge of several services
    /// (identified by name). This is not supported by this simple HashMap (TODO).
    service_names: HashMap<DirectPeerId, String>,
}

pub struct ServerBroker {
    storage: RocksDbServerStorage,

    state: RwLock<ServerBrokerState>,

    path_users: PathBuf,

    master_key: Option<SymKey>,
}

impl ServerBroker {
    pub(crate) fn new(
        storage: RocksDbServerStorage,
        path_users: PathBuf,
        master_key: Option<SymKey>,
    ) -> Self {
        ServerBroker {
            storage: storage,
            state: RwLock::new(ServerBrokerState {
                overlays: HashMap::new(),
                inner_overlays: HashMap::new(),
                local_subscriptions: HashMap::new(),
                verifiers: HashMap::new(),
                remote_apps: HashMap::new(),
                wallet_rendezvous: HashMap::new(),
                wallet_exports: HashMap::new(),
                wallet_exports_timestamp: BTreeMap::new(),
                #[cfg(feature = "usage-stats")]
                usage_stats_storage_bytes: HashMap::new(),
                active_plugins: HashMap::new(),
                service_names: HashMap::new(),
            }),
            master_key,
            path_users,
        }
    }

    pub fn load(&mut self) -> Result<(), NgError> {
        Ok(())
    }

    async fn add_subscription(
        &self,
        overlay: OverlayId,
        topic: TopicId,
        peer: ClientPeerId,
    ) -> Result<(), ServerError> {
        let mut lock = self.state.write().await;
        let peers_map = lock
            .local_subscriptions
            .entry((overlay, topic))
            .or_insert(HashMap::with_capacity(1));

        log_debug!(
            "SUBSCRIBING PEER {:?} TOPIC {} OVERLAY {}",
            peer,
            topic,
            overlay
        );

        if peers_map.insert(*peer.key(), peer.value()).is_some() {
            //return Err(ServerError::PeerAlreadySubscribed);
        }
        Ok(())
    }

    #[allow(dead_code)]
    async fn remove_subscription(
        &self,
        overlay: &OverlayId,
        topic: &TopicId,
        peer: &PubKey,
    ) -> Result<(), ServerError> {
        let mut lock = self.state.write().await;
        let peers_set = lock
            .local_subscriptions
            .get_mut(&(*overlay, *topic))
            .ok_or(ServerError::SubscriptionNotFound)?;

        if peers_set.remove(peer).is_none() {
            return Err(ServerError::SubscriptionNotFound);
        }
        Ok(())
    }

    async fn new_verifier_from_credentials(
        &self,
        user_id: &UserId,
        credentials: Credentials,
        local_peer_id: DirectPeerId,
        partial_credentials: bool,
    ) -> Result<Verifier, NgError> {
        let block_storage = self.get_block_storage();
        let mut path = self.get_path_users();
        let user_hash: Digest = user_id.into();
        path.push(user_hash.to_string());
        std::fs::create_dir_all(path.clone()).unwrap();
        let peer_id_dh = credentials.peer_priv_key.to_pub().to_dh_from_ed();
        let mut verifier = Verifier::new(
            VerifierConfig {
                config_type: VerifierConfigType::RocksDb(path),
                user_master_key: *credentials.user_master_key.slice(),
                peer_priv_key: credentials.peer_priv_key,
                user_priv_key: credentials.user_key,
                private_store_read_cap: if partial_credentials {
                    None
                } else {
                    Some(credentials.read_cap)
                },
                private_store_id: if partial_credentials {
                    None
                } else {
                    Some(credentials.private_store)
                },
                protected_store_id: if partial_credentials {
                    None
                } else {
                    Some(credentials.protected_store)
                },
                public_store_id: if partial_credentials {
                    None
                } else {
                    Some(credentials.public_store)
                },
                locator: Locator::empty(),
            },
            block_storage,
        )
        .await?;
        if !partial_credentials {
            verifier.connected_broker = BrokerPeerId::Local(local_peer_id);
            // start the local transport connection
            let mut lock = BROKER.write().await;
            lock.connect_local(peer_id_dh, *user_id)?;
        }
        Ok(verifier)
    }

    // Return the current write counter for that user and stop tracking it
    #[cfg(feature = "usage-stats")]
    pub async fn usage_stats_storage_flush(&self, user_id: &UserId) -> Option<u64> {
        //log_debug!("usage_stats_storage_flush: flushing storage stats for {}", &user_id);
        let mut lock = self.state.write().await;
        lock.usage_stats_storage_bytes.remove(user_id)
    }
}

use async_std::future::timeout;

async fn wait_for_wallet(
    mut internal_receiver: Receiver<ExportedWallet>,
    mut sender: Sender<Result<ExportedWallet, ServerError>>,
    rendezvous: SymKey,
) -> ResultSend<()> {
    let wallet_future = internal_receiver.next();
    let _ = sender
        .send(
            match timeout(Duration::from_millis(5 * 60_000), wallet_future).await {
                Err(_) => Err(ServerError::ExportWalletTimeOut),
                Ok(Some(w)) => Ok(w),
                Ok(None) => Err(ServerError::BrokerError),
            },
        )
        .await;
    BROKER
        .read()
        .await
        .get_server_broker()?
        .read()
        .await
        .remove_rendezvous(&rendezvous)
        .await;

    Ok(())
}

//TODO: the purpose of this trait is to have a level of indirection so we can keep some data in memory (cache) and avoid hitting the storage backend (rocksdb) at every call.
//for now this cache is not implemented, but the structs are ready (see above), and it would just require to change slightly the implementation of the trait functions here below.
#[async_trait::async_trait]
impl IServerBroker for ServerBroker {
    fn take_master_key(&mut self) -> Result<SymKey, ProtocolError> {
        match self.master_key.take() {
            None => Err(ProtocolError::AccessDenied),
            Some(key) => Ok(key),
        }
    }
    async fn remove_rendezvous(&self, rendezvous: &SymKey) {
        let mut lock = self.state.write().await;
        let _ = lock.wallet_rendezvous.remove(&rendezvous);
    }
    async fn wait_for_wallet_at_rendezvous(
        &self,
        rendezvous: SymKey,
    ) -> Receiver<Result<ExportedWallet, ServerError>> {
        let (internal_sender, internal_receiver) = mpsc::unbounded();
        let (mut sender, receiver) = mpsc::unbounded();
        {
            let mut state = self.state.write().await;
            if state.wallet_rendezvous.contains_key(&rendezvous) {
                let _ = sender.send(Err(ServerError::BrokerError)).await;
                sender.close_channel();
                return receiver;
            } else {
                let _ = state
                    .wallet_rendezvous
                    .insert(rendezvous.clone(), internal_sender);
            }
        }
        spawn_and_log_error(wait_for_wallet(internal_receiver, sender, rendezvous));
        receiver
    }

    async fn get_wallet_export(&self, rendezvous: SymKey) -> Result<ExportedWallet, ServerError> {
        let mut state = self.state.write().await;
        match state.wallet_exports.remove(&rendezvous) {
            Some(wallet) => Ok(wallet),
            None => Err(ServerError::NotFound),
        }
    }

    async fn put_wallet_export(&self, rendezvous: SymKey, export: ExportedWallet) {
        let mut state = self.state.write().await;
        let _ = state.wallet_exports.insert(rendezvous.clone(), export);
        let _ = state
            .wallet_exports_timestamp
            .insert(SystemTime::now(), rendezvous);
    }

    // TODO:  periodically (every 5 min) remove entries in wallet_exports_timestamp and wallet_exports

    async fn put_wallet_at_rendezvous(
        &self,
        rendezvous: SymKey,
        export: ExportedWallet,
    ) -> Result<(), ServerError> {
        let mut state = self.state.write().await;
        match state.wallet_rendezvous.remove(&rendezvous) {
            None => Err(ServerError::NotFound),
            Some(mut sender) => {
                let _ = sender.send(export).await;
                Ok(())
            }
        }
    }

    fn get_block_storage(
        &self,
    ) -> std::sync::Arc<std::sync::RwLock<dyn BlockStorage + Send + Sync>> {
        self.storage.get_block_storage()
    }

    fn get_path_users(&self) -> PathBuf {
        self.path_users.clone()
    }

    fn has_block(&self, overlay_id: &OverlayId, block_id: &BlockId) -> Result<(), ServerError> {
        self.storage.has_block(overlay_id, block_id)
    }

    fn get_block(&self, overlay_id: &OverlayId, block_id: &BlockId) -> Result<Block, ServerError> {
        self.storage.get_block(overlay_id, block_id)
    }

    fn next_seq_for_peer(&self, peer: &PeerId, seq: u64) -> Result<(), ServerError> {
        self.storage.next_seq_for_peer(peer, seq)
    }

    fn put_block(&self, overlay_id: &OverlayId, block: Block) -> Result<usize, ServerError> {
        let (_block_id, sz) = self.storage.add_block(overlay_id, block)?;
        Ok(sz)
    }
    async fn create_user(&self, broker_id: &DirectPeerId) -> Result<UserId, ProtocolError> {
        let user_privkey = PrivKey::random_ed();
        let user_id = user_privkey.to_pub();
        let mut creds = Credentials::new_partial(&user_privkey);
        let mut verifier = self
            .new_verifier_from_credentials(&user_id, creds.clone(), *broker_id, true)
            .await?;
        let _site = SiteV0::create_personal(user_privkey.clone(), &mut verifier)
            .await
            .map_err(|e| {
                log_err!("create_personal failed with {e}");
                ProtocolError::BrokerError
            })?;

        // update credentials from config of verifier.
        verifier.complement_credentials(&mut creds);
        //verifier.close().await;
        // save credentials and user
        self.add_user_credentials(&user_id, &creds)?;

        verifier.connected_broker = BrokerPeerId::Local(*broker_id);

        // start the local transport connection
        {
            let mut lock = BROKER.write().await;
            let peer_id_dh = creds.peer_priv_key.to_pub().to_dh_from_ed();
            lock.connect_local(peer_id_dh, user_id)?;
        }
        let _res = verifier.send_outbox().await;
        if _res.is_err() {
            log_err!("{:?}", _res);
        }

        Ok(user_id)
    }

    fn get_user(&self, user_id: PubKey) -> Result<bool, ProtocolError> {
        self.storage.get_user(user_id)
    }
    fn has_no_user(&self) -> Result<bool, ProtocolError> {
        self.storage.has_no_user()
    }
    fn add_user_credentials(
        &self,
        user_id: &PubKey,
        credentials: &Credentials,
    ) -> Result<(), ProtocolError> {
        self.storage.add_user_credentials(user_id, credentials)
    }
    fn get_user_credentials(&self, user_id: &PubKey) -> Result<Credentials, ProtocolError> {
        self.storage.get_user_credentials(user_id)
    }
    fn add_user(&self, user_id: PubKey, is_admin: bool) -> Result<(), ProtocolError> {
        self.storage.add_user(user_id, is_admin)
    }

    fn del_user(&self, user_id: PubKey) -> Result<(), ProtocolError> {
        self.storage.del_user(user_id)
    }
    fn modify_user(&self, user_id: PubKey, set_admin: bool) -> Result<(), ProtocolError> {
        self.storage.modify_user(user_id, set_admin)
    }
    fn list_users(&self, admins: Option<bool>, hydrated: bool, after_user: Option<UserId>, limit: Option<u32>) -> Result<Vec<ListedUser>, ProtocolError> {
        self.storage.list_users(admins, hydrated, after_user, limit)
    }
    fn list_invitations(
        &self,
        admin: bool,
        unique: bool,
        multi: bool,
    ) -> Result<Vec<(InvitationCode, u32, Option<String>)>, ProtocolError> {
        self.storage.list_invitations(admin, unique, multi)
    }
    fn add_invitation(
        &self,
        invite_code: &InvitationCode,
        expiry: u32,
        memo: &Option<String>,
    ) -> Result<(), ProtocolError> {
        self.storage.add_invitation(invite_code, expiry, memo)
    }
    fn get_invitation_type(&self, invite_code: [u8; 32]) -> Result<u8, ProtocolError> {
        self.storage.get_invitation_type(invite_code)
    }
    fn remove_invitation(&self, invite_code: [u8; 32]) -> Result<(), ProtocolError> {
        self.storage.remove_invitation(invite_code)
    }
    fn usage_stats(&self, user: &UserId) -> Result<UsageStats, ProtocolError> {
        // Call RocksDbServerStorage::usage_stats which will call Account::usage_stats:
        self.storage.usage_stats(user)
    }

    async fn app_process_request(
        &self,
        req: AppRequest,
        request_id: i64,
        fsm: &Mutex<NoiseFSM>,
    ) -> Result<(), ServerError> {
        // get the session
        let remote = {
            fsm.lock()
                .await
                .remote_peer()
                .ok_or(ServerError::SessionNotFound)?
        };

        let session_id = (remote, req.session_id());
        let session_lock = {
            let lock = self.state.read().await;
            let user_id = lock
                .remote_apps
                .get(&session_id)
                .ok_or(ServerError::SessionNotFound)?
                .to_owned();

            Arc::clone(
                lock.verifiers
                    .get(&user_id)
                    .ok_or(ServerError::SessionNotFound)?,
            )
        };

        let mut session = session_lock.write().await;

        if session.attached.is_none() || session.attached.unwrap() != session_id {
            return Err(ServerError::SessionDetached);
        }

        if req.command().is_stream() {
            let res = session.verifier.app_request_stream(req).await;

            match res {
                Err(e) => {
                    let error: ServerError = e.into();
                    let error_res: AppMessage = error.into();
                    fsm.lock()
                        .await
                        .send_in_reply_to(error_res.into(), request_id)
                        .await?;
                }
                Ok((mut receiver, _cancel)) => {
                    //TODO: implement cancel
                    let mut some_sent = false;
                    while let Some(response) = receiver.next().await {
                        some_sent = true;
                        let mut msg: AppMessage = response.into();
                        msg.set_result(ServerError::PartialContent.into());
                        fsm.lock()
                            .await
                            .send_in_reply_to(msg.into(), request_id)
                            .await?;
                    }
                    let end: Result<EmptyAppResponse, ServerError> = if some_sent {
                        Err(ServerError::EndOfStream)
                    } else {
                        Err(ServerError::EmptyStream)
                    };
                    fsm.lock()
                        .await
                        .send_in_reply_to(end.into(), request_id)
                        .await?;
                }
            }
        } else {
            let res = session.verifier.app_request(req).await;
            //log_debug!("GOT RES {:?}", res);
            let app_message: AppMessage = match res {
                Err(e) => {
                    log_debug!("AppRequest error NgError {e}");
                    let server_err: ServerError = e.into();
                    server_err.into()
                }
                Ok(app_res) => app_res.into(),
            };
            fsm.lock()
                .await
                .send_in_reply_to(app_message.into(), request_id)
                .await?;
        }

        Ok(())
    }

    async fn app_session_start(
        &self,
        req: AppSessionStart,
        remote: DirectPeerId,
        local_peer_id: DirectPeerId,
    ) -> Result<AppSessionStartResponse, ServerError> {
        let user_id = req.user_id();
        let id = (remote, req.session_id());
        let verifier_lock_res = {
            let lock = self.state.read().await;
            lock.verifiers.get(user_id).map(|l| Arc::clone(l))
        };
        let verifier_lock = match verifier_lock_res {
            Some(session_lock) => {
                let mut session = session_lock.write().await;
                if let Some((peer_id, session_id)) = session.attached {
                    if peer_id != remote || session_id == req.session_id() {
                        // remove the previous session
                        let mut write_lock = self.state.write().await;
                        let _ = write_lock.remote_apps.remove(&(peer_id, session_id));
                    }
                }
                session.attached = Some(id);
                Arc::clone(&session_lock)
            }
            None => {
                // we create and load a new verifier

                let credentials = if req.credentials().is_none() {
                    // headless do not have credentials. we fetch them from server_storage
                    self.storage.get_user_credentials(user_id)?
                } else {
                    req.credentials().clone().unwrap()
                };

                if *user_id != credentials.user_key.to_pub() {
                    log_debug!("InvalidRequest");
                    return Err(ServerError::InvalidRequest);
                }

                let verifier = self
                    .new_verifier_from_credentials(user_id, credentials, local_peer_id, false)
                    .await;
                if verifier.is_err() {
                    log_err!(
                        "new_verifier failed with: {:?}",
                        verifier.as_ref().unwrap_err()
                    );
                }
                let mut verifier = verifier?;

                // TODO : key.zeroize();

                //load verifier from local_storage
                let _ = verifier.load();
                //TODO: save opened_branches in user_storage, so that when we open again the verifier, the syncing can work
                verifier.sync().await;

                let session = DetachableVerifier {
                    detach: true,
                    attached: Some(id),
                    verifier,
                };
                let mut write_lock = self.state.write().await;
                Arc::clone(
                    write_lock
                        .verifiers
                        .entry(*user_id)
                        .or_insert(Arc::new(RwLock::new(session))),
                )
            }
        };
        let verifier = &verifier_lock.read().await.verifier;
        let res = AppSessionStartResponse::V0(AppSessionStartResponseV0 {
            private_store: *verifier.private_store_id(),
            protected_store: *verifier.protected_store_id(),
            public_store: *verifier.public_store_id(),
        });
        let mut write_lock = self.state.write().await;
        if let Some(previous_user) = write_lock.remote_apps.insert(id, *user_id) {
            // weird. another session was opened for this id.
            // we have to stop it otherwise it would be dangling.
            if previous_user != *user_id {
                if let Some(previous_session_lock) = write_lock
                    .verifiers
                    .get(&previous_user)
                    .map(|v| Arc::clone(v))
                {
                    let mut previous_session = previous_session_lock.write().await;
                    if previous_session.detach {
                        previous_session.attached = None;
                    } else {
                        // we stop it and drop it
                        let verifier = write_lock.verifiers.remove(&previous_user);
                        verifier.unwrap().read().await.verifier.close().await;
                    }
                }
            }
        }
        Ok(res)
    }

    async fn app_session_stop(
        &self,
        req: AppSessionStop,
        remote_peer_id: &DirectPeerId,
    ) -> Result<EmptyAppResponse, ServerError> {
        let id = (*remote_peer_id, req.session_id());

        let mut write_lock = self.state.write().await;
        let must_be_destroyed = {
            let session_user = write_lock
                .remote_apps
                .remove(&id)
                .ok_or(ServerError::SessionNotFound)?;
            let session = Arc::clone(
                write_lock
                    .verifiers
                    .get(&session_user)
                    .ok_or(ServerError::SessionNotFound)?,
            );
            let mut verifier_lock = session.write().await;
            if !req.is_force_close() && verifier_lock.detach {
                verifier_lock.attached = None;
                None
            } else {
                Some(session_user)
            }
        };
        if let Some(user) = must_be_destroyed {
            let verifier = write_lock.verifiers.remove(&user);
            verifier.unwrap().read().await.verifier.close().await;
        }
        Ok(EmptyAppResponse(()))
    }

    // Check that this peer-id is allowed to act as this plugin.
    async fn app_plugin_register(
        &self,
        req: AppPluginRegister,
        peer_id: DirectPeerId,
    ) -> Result<AppPluginRegisterResp, ServerError> {
        log_debug!("app_plugin_register: peer_id={:?}", &peer_id);
        /* Is this peer enabled for req.service_name? */
        let serv =
            match self.service_plugin_get(&req.service_name, &peer_id) {
                Ok(serv) => {
                    assert!(serv.peer_id == peer_id); // Or service_plugin_get can't be trusted
                    serv
                },
                Err(ProtocolError::NotFound) => {
                    /* That's the first connection: register the plugin with a
                     * Waiting status: */
                    let serv = ServicePlugin {
                        name: req.service_name.clone(),
                        peer_id: peer_id,
                        status: PluginStatus::Waiting,
                    };
                    self.service_plugin_put(&serv, true)?;
                    serv
                },
                Err(e) => return Err(e.into()),
            };
        log_debug!("Service {} (peer {:?}) is {:?}", req.service_name, peer_id, serv.status);
        if serv.status == PluginStatus::Enabled {
            self.app_plugin_activate(&peer_id, &serv.name).await;
        }
        Ok(AppPluginRegisterResp::V0 { status: serv.status })
    }

    async fn app_plugin_activate(&self, peer_id: &DirectPeerId, service_name: &String) {
        let mut state = self.state.write().await;
        // Replace any previous registration, of this name or by this peer, mercilessly:
        if let Some(old_name) = state.service_names.remove(peer_id) {
            state.active_plugins.remove(&old_name);
        }
        if let Some(old_plugin) = state.active_plugins.remove(service_name) {
            state.service_names.remove(&old_plugin.peer);
        }
        log_info!("Activating plugin {:?} for service {}", peer_id, service_name);
        state.service_names.insert(*peer_id, service_name.clone());
        state
            .active_plugins
            .insert(service_name.clone(), ActivePlugin { peer: *peer_id });
    }

    async fn app_plugin_deactivate(&self, peer_id: &DirectPeerId, service_name: &String) {
        // Note: RwLock can't upgrade from read to write to take the write
        // upfront, although it's rarely needed:
        let mut state = self.state.write().await;
        if let Some(name) = state.service_names.get(peer_id) {
            // Waiting for 2024 edition for let-chaining:
            if name == service_name {
                log_info!("Deactivating plugin {:?} for service {}", peer_id, service_name);
                state.service_names.remove(peer_id);
                state.active_plugins.remove(service_name);
            }
        }
    }

    async fn service_name_of_peer(&self, peer: &DirectPeerId) -> Option<String> {
        self.state.read().await.service_names.get(peer).cloned()
    }

    /* Forward a PluginRequest message to its final recipient, setting the
     * caller field to the passed value. */
    async fn plugin_request_forward(
        &self,
        mut req: PluginRequest,
        caller: PluginCaller,
    ) -> Result<PluginResponse, ServerError> {
        // Only the broker can tell who the caller is:
        if req.caller.is_some() {
            return Err(ServerError::InvalidRequest);
        }
        req.caller = Some(caller);
        let peer = self
            .state
            .read()
            .await
            .active_plugins
            .get(&req.service_name)
            .ok_or(ServerError::PluginNotFound)?
            .peer;
        let service_name = req.service_name.clone();
        // Neither BROKER nor our state must stay locked while the plugin works on it:
        let response = BROKER
            .read()
            .await
            .prepare_request_to_app::<PluginRequest, PluginResponse>(
                peer.slice(),
                req,
                PLUGIN_REQUEST_TIMEOUT,
            )
            .map_err(|e| match e {
                NgError::ConnectionNotFound => ServerError::PluginNotFound,
                e => e.into(),
            });
        let response = match response {
            Ok(res) => res.await,
            Err(e) => {
                if e == ServerError::PluginNotFound {
                    // The plugin is registered but not connected anymore
                    let mut state = self.state.write().await;
                    if state.service_names.get(&peer) == Some(&service_name) {
                        state.service_names.remove(&peer);
                        state.active_plugins.remove(&service_name);
                    }
                }
                Err(e.into())
            },
        };
        match response {
            Ok(SoS::Single(res)) => Ok(res),
            Ok(SoS::Stream(_)) => Err(ServerError::BrokerError),
            Err(NgError::ServerError(e)) => Err(e),
            Err(NgError::ProtocolError(ProtocolError::Timeout)) => Err(ServerError::PluginTimeout),
            Err(e) => Err(e.into()),
        }
    }

    fn get_repo_pin_status(
        &self,
        overlay: &OverlayId,
        repo: &RepoHash,
        user: &UserId,
    ) -> Result<RepoPinStatus, ServerError> {
        self.storage.get_repo_pin_status(overlay, repo, user)
    }

    async fn pin_repo_write(
        &self,
        overlay: &OverlayAccess,
        repo: &RepoHash,
        user_id: &UserId,
        ro_topics: &Vec<TopicId>,
        rw_topics: &Vec<PublisherAdvert>,
        overlay_root_topic: &Option<TopicId>,
        expose_outer: bool,
        peer: &ClientPeerId,
    ) -> Result<RepoOpened, ServerError> {
        let res = self.storage.pin_repo_write(
            overlay,
            repo,
            user_id,
            ro_topics,
            rw_topics,
            overlay_root_topic,
            expose_outer,
        )?;

        for topic in res.iter() {
            self.add_subscription(
                *overlay.overlay_id_for_client_protocol_purpose(),
                *topic.topic_id(),
                peer.clone(),
            )
            .await?;
        }
        Ok(res)
    }

    async fn pin_repo_read(
        &self,
        overlay: &OverlayId,
        repo: &RepoHash,
        user_id: &UserId,
        ro_topics: &Vec<TopicId>,
        peer: &ClientPeerId,
    ) -> Result<RepoOpened, ServerError> {
        let res = self
            .storage
            .pin_repo_read(overlay, repo, user_id, ro_topics)?;

        for topic in res.iter() {
            // TODO: those outer subscriptions are not handled yet. they will not emit events.
            self.add_subscription(*overlay, *topic.topic_id(), peer.clone())
                .await?;
        }
        Ok(res)
    }

    async fn topic_sub(
        &self,
        overlay: &OverlayId,
        repo: &RepoHash,
        topic: &TopicId,
        user: &UserId,
        publisher: Option<&PublisherAdvert>,
        peer: &ClientPeerId,
    ) -> Result<TopicSubRes, ServerError> {
        let res = self
            .storage
            .topic_sub(overlay, repo, topic, user, publisher)?;
        self.add_subscription(*overlay, *topic, peer.clone())
            .await?;
        Ok(res)
    }

    fn get_commit(&self, overlay: &OverlayId, id: &ObjectId) -> Result<Vec<Block>, ServerError> {
        self.storage.get_commit(overlay, id)
    }

    async fn remove_all_subscriptions_of_client(&self, client: &ClientPeerId) {
        let remote_peer = client.key();
        let mut lock = self.state.write().await;
        for ((overlay, topic), peers) in lock.local_subscriptions.iter_mut() {
            if peers.remove(remote_peer).is_some() {
                log_debug!(
                    "subscription of peer {} to topic {} in overlay {} removed",
                    remote_peer,
                    topic,
                    overlay
                );
            }
        }
    }

    async fn inbox_post(&self, post: InboxPost) -> Result<(), ServerError> {
        // TODO: deal with Inbox that is not local to the broker (use Core protocol to dispatch it)

        let users = self
            .storage
            .get_readers_for_inbox(&post.msg.body.to_inbox, &post.msg.body.to_overlay)?;
        if users.is_empty() {
            self.storage.enqueue_inbox_msg(&post.msg)?;
            return Ok(());
        }

        let broker = BROKER.read().await;
        let not_dispatched = broker.dispatch_inbox_msg(&users, post.msg).await?;
        if let Some(msg) = not_dispatched {
            self.storage.enqueue_inbox_msg(&msg)?;
        }
        Ok(())
    }

    fn inbox_register(
        &self,
        user_id: UserId,
        registration: InboxRegister,
    ) -> Result<(), ServerError> {
        self.storage
            .register_inbox_reader(user_id, registration.inbox_id, registration.overlay)?;
        Ok(())
    }

    async fn inbox_pop_for_user(&self, user: UserId) -> Result<InboxMsg, ServerError> {
        let inboxes = self.storage.get_inboxes_for_readers(&user)?;

        for (inbox, overlay) in inboxes {
            match self.storage.take_first_msg_from_inbox(&inbox, &overlay) {
                Ok(msg) => return Ok(msg),
                Err(_) => {}
            }
        }
        Err(ServerError::NotFound)
    }

    async fn dispatch_event(
        &self,
        overlay: &OverlayId,
        event: Event,
        user_id: &UserId,
        remote_peer: &PubKey,
    ) -> Result<(Vec<ClientPeerId>, usize), ServerError> {
        let (topic, storage_size) = self.storage.save_event(overlay, event, user_id)?;

        let lock = self.state.read().await;
        let mut map = lock
            .local_subscriptions
            .get(&(*overlay, topic))
            .map(|map| map.iter().collect())
            .unwrap_or(HashMap::new());

        map.remove(remote_peer);

        // log_info!("DISPATCH EVENT {} {} {:?}", overlay, topic, map);

        let peer_ids = map
            .iter()
            .map(|(k, v)| ClientPeerId::new_from(k, v))
            .collect();
        Ok((peer_ids, storage_size))
    }

    fn topic_sync_req(
        &self,
        overlay: &OverlayId,
        topic: &TopicId,
        known_heads: &Vec<ObjectId>,
        target_heads: &Vec<ObjectId>,
        known_commits: &Option<BloomFilter>,
    ) -> Result<Vec<TopicSyncRes>, ServerError> {
        self.storage
            .topic_sync_req(overlay, topic, known_heads, target_heads, known_commits)
    }

    // Add this many bytes to the write counter of that user
    #[cfg(feature = "usage-stats")]
    async fn usage_stats_storage_bytes_add(&self, user_id: &UserId, bytes: usize) {
        let mut lock = self.state.write().await;
        let cur = lock.usage_stats_storage_bytes.entry(*user_id).or_insert(0u64);
        *cur += bytes as u64
    }

    #[cfg(feature = "usage-stats")]
    async fn usage_stats_update_for_user(
        &self,
        user_id: &UserId,
        net_stats: &NetStats,
    ) -> Result<(), ServerError> {
        log_debug!("Updating stats for user {}", user_id);
        let storage_bytes =
            self.usage_stats_storage_flush(user_id).await.unwrap_or(0u64);
        self.storage
            .usage_stats_update_for_user(user_id, net_stats, storage_bytes)
            .map_err(Into::into)
    }

    fn service_plugin_list(&self) -> Result<Vec<ServicePlugin>, ProtocolError> {
        self.storage.service_plugin_list()
            .map_err(Into::into)
    }
    fn service_plugin_put(&self, serv: &ServicePlugin, for_create: bool) -> Result<(), ProtocolError> {
        self.storage
            .service_plugin_put(serv, for_create)
            .map_err(Into::into)
    }
    fn service_plugin_get(&self, name: &String, peer_id: &DirectPeerId) -> Result<ng_net::types::ServicePlugin, ProtocolError> {
        self.storage
            .service_plugin_get(name, peer_id)
            .map_err(Into::into)
    }
}
