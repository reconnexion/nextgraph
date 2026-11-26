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

//! Trait for ServerBroker

use std::path::PathBuf;
use std::sync::Arc;

use async_std::sync::Mutex;

use ng_repo::block_storage::BlockStorage;
use ng_repo::errors::*;
use ng_repo::types::*;

use crate::app_protocol::{
    AppPluginRegister, AppPluginRegisterResp, AppRequest, AppSessionStart, AppSessionStartResponse, AppSessionStop,
};
use crate::broker::ClientPeerId;
use crate::connection::NoiseFSM;
use crate::types::*;
use crate::utils::Receiver;

#[async_trait::async_trait]
pub trait IServerBroker: Send + Sync {
    async fn remove_rendezvous(&self, rendezvous: &SymKey);
    async fn put_wallet_export(&self, rendezvous: SymKey, export: ExportedWallet);
    async fn get_wallet_export(&self, rendezvous: SymKey) -> Result<ExportedWallet, ServerError>;
    async fn put_wallet_at_rendezvous(
        &self,
        rendezvous: SymKey,
        export: ExportedWallet,
    ) -> Result<(), ServerError>;
    async fn wait_for_wallet_at_rendezvous(
        &self,
        rendezvous: SymKey,
    ) -> Receiver<Result<ExportedWallet, ServerError>>;
    async fn inbox_post(&self, post: InboxPost) -> Result<(), ServerError>;
    fn inbox_register(
        &self,
        user_id: UserId,
        registration: InboxRegister,
    ) -> Result<(), ServerError>;
    async fn inbox_pop_for_user(&self, user: UserId) -> Result<InboxMsg, ServerError>;
    fn get_path_users(&self) -> PathBuf;
    fn get_block_storage(&self) -> Arc<std::sync::RwLock<dyn BlockStorage + Send + Sync>>;
    // Return the size written (0 for a duplicate block)
    fn put_block(&self, overlay_id: &OverlayId, block: Block) -> Result<usize, ServerError>;
    fn has_block(&self, overlay_id: &OverlayId, block_id: &BlockId) -> Result<(), ServerError>;
    fn get_block(&self, overlay_id: &OverlayId, block_id: &BlockId) -> Result<Block, ServerError>;
    async fn create_user(&self, broker_id: &DirectPeerId) -> Result<UserId, ProtocolError>;
    fn get_user(&self, user_id: PubKey) -> Result<bool, ProtocolError>;
    fn has_no_user(&self) -> Result<bool, ProtocolError>;
    fn get_user_credentials(&self, user_id: &PubKey) -> Result<Credentials, ProtocolError>;
    fn add_user_credentials(
        &self,
        user_id: &PubKey,
        credentials: &Credentials,
    ) -> Result<(), ProtocolError>;
    fn add_user(&self, user_id: PubKey, is_admin: bool) -> Result<(), ProtocolError>;
    fn del_user(&self, user_id: PubKey) -> Result<(), ProtocolError>;
    fn modify_user(&self, user_id: PubKey, admin: bool) -> Result<(), ProtocolError>;
    fn list_users(&self, admins: Option<bool>, hydrated: bool, after_user: Option<UserId>, limit: Option<u32>) -> Result<Vec<ListedUser>, ProtocolError>;
    fn list_invitations(
        &self,
        admin: bool,
        unique: bool,
        multi: bool,
    ) -> Result<Vec<(InvitationCode, u32, Option<String>)>, ProtocolError>;
    fn add_invitation(
        &self,
        invite_code: &InvitationCode,
        expiry: u32,
        memo: &Option<String>,
    ) -> Result<(), ProtocolError>;
    fn get_invitation_type(&self, invite: [u8; 32]) -> Result<u8, ProtocolError>;
    fn remove_invitation(&self, invite: [u8; 32]) -> Result<(), ProtocolError>;
    fn take_master_key(&mut self) -> Result<SymKey, ProtocolError>;
    async fn app_process_request(
        &self,
        req: AppRequest,
        request_id: i64,
        fsm: &Mutex<NoiseFSM>,
    ) -> Result<(), ServerError>;

    async fn app_session_start(
        &self,
        req: AppSessionStart,
        remote_peer_id: DirectPeerId,
        local_peer_id: DirectPeerId,
    ) -> Result<AppSessionStartResponse, ServerError>;
    async fn app_session_stop(
        &self,
        req: AppSessionStop,
        remote_peer_id: &DirectPeerId,
    ) -> Result<EmptyAppResponse, ServerError>;

    async fn app_plugin_register(
        &self,
        req: AppPluginRegister,
        remote_peer_id: DirectPeerId,
    ) -> Result<AppPluginRegisterResp, ServerError>;

    async fn app_plugin_activate(
        &self,
        peer_id: &DirectPeerId,
        service_name: &String,
    );
    async fn app_plugin_deactivate(
        &self,
        peer_id: &DirectPeerId,
        service_name: &String,
    );

    /// The name of the plugin registered by the app connected as `peer`, if any.
    async fn service_name_of_peer(&self, peer: &DirectPeerId) -> Option<String>;

    /// Sends `notification` to the plugin connected as `peer`, without waiting for its answer.
    /// Fails with PluginNotFound if that peer is not connected.
    async fn plugin_notify(
        &self,
        peer: &DirectPeerId,
        service_name: &String,
        notification: PluginNotification,
    ) -> Result<(), ServerError>;

    /// Forwards `req` to the named plugin, on behalf of `caller`, and returns its response.
    async fn plugin_request_forward(
        &self,
        req: PluginReq,
        caller: PluginCaller,
    ) -> Result<PluginResp, ServerError>;

    fn next_seq_for_peer(&self, peer: &PeerId, seq: u64) -> Result<(), ServerError>;

    fn get_repo_pin_status(
        &self,
        overlay: &OverlayId,
        repo: &RepoHash,
        user_id: &UserId,
    ) -> Result<RepoPinStatus, ServerError>;

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
    ) -> Result<RepoOpened, ServerError>;

    async fn pin_repo_read(
        &self,
        overlay: &OverlayId,
        repo: &RepoHash,
        user_id: &UserId,
        ro_topics: &Vec<TopicId>,
        peer: &ClientPeerId,
    ) -> Result<RepoOpened, ServerError>;

    async fn topic_sub(
        &self,
        overlay: &OverlayId,
        repo: &RepoHash,
        topic: &TopicId,
        user_id: &UserId,
        publisher: Option<&PublisherAdvert>,
        peer: &ClientPeerId,
    ) -> Result<TopicSubRes, ServerError>;

    fn get_commit(&self, overlay: &OverlayId, id: &ObjectId) -> Result<Vec<Block>, ServerError>;

    async fn dispatch_event(
        &self,
        overlay: &OverlayId,
        event: Event,
        user_id: &UserId,
        remote_peer: &PubKey,
    ) -> Result<(Vec<ClientPeerId>, usize), ServerError>;

    async fn remove_all_subscriptions_of_client(&self, client: &ClientPeerId);

    fn topic_sync_req(
        &self,
        overlay: &OverlayId,
        topic: &TopicId,
        known_heads: &Vec<ObjectId>,
        target_heads: &Vec<ObjectId>,
        known_commits: &Option<BloomFilter>,
    ) -> Result<Vec<TopicSyncRes>, ServerError>;

    #[cfg(feature = "usage-stats")]
    async fn usage_stats_storage_bytes_add(&self, user_id: &UserId, bytes: usize);

    fn usage_stats(&self, user_id: &UserId) -> Result<UsageStats, ProtocolError>;

    #[cfg(feature = "usage-stats")]
    async fn usage_stats_update_for_user(
        &self,
        user_id: &UserId,
        net_stats: &NetStats,
    ) -> Result<(), ServerError>;

    fn service_plugin_list(&self) -> Result<Vec<ServicePlugin>, ProtocolError>;
    fn service_plugin_put(&self, serv: &ServicePlugin, for_create: bool) -> Result<(), ProtocolError>;
    fn service_plugin_get(&self, name: &String, peer_id: &DirectPeerId) -> Result<ServicePlugin, ProtocolError>;
    async fn service_plugin_list_for_user(&self, user_id: &UserId) -> Result<Vec<AvailableService>, ServerError>;
    async fn service_plugin_enable_for_user(
        &self,
        service_name: &String,
        user_id: &UserId,
        credentials: &Credentials,
    ) -> Result<(), ServerError>;
}
