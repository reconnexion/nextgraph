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
/* Plugin services are nextgraph clients that perform some additional services, mainly related to
 * communicating to the external world (email relays, pubsub relays...).  Since they are external
 * piece of software acting on behalf of many nextgraph users, they have no user wallet, no local
 * broker, no credentials. They have some storage on the broker where they can save some global
 * configuration; they also can create users but then the broker will manage (and store) their
 * credentials itself.
 *
 * Normal web applications, with a local broker connected to the broker in client mode, can talk to
 * these plugins using a very lax protocol: `service_plugin_call` will take a named plugin name and a
 * string value (likely in JSON) and the broker will then forward that value to the plugin
 * registered under that name (if any), alongside the (authenticated) identity of the caller (in
 * this case: the nextgraph user public key). The broker will then wait for the plugin to answer,
 * and forward that answer (also carrying a string value) back to the caller.
 *
 * Plugins can also call other plugins with `service_plugin_call`, this time as an AppMessage. The request
 * will also be forwarded by the broker to the final recipient, but this time the caller will be
 * just the originator's plugin name.
 *
 * See the two implementations of `service_plugin_call` in sdk/lib-wasm/src/lib.rs.
 *
 * Plugin messages:
 *
 * - PluginRegister, as a AppMessage, from a headless (app) client to the broker, to let the broker
 *   know that this client can respond to requests; To prevent a plugin to hijack messages of
 *   another, the association between plugin names and clients' peer-ids is recorded and vetted by
 *   the broker's admin;
 * - PluginReq, as either a ClientMessage or an AppMessage, from the originator to the broker
 *   or forwarded from the broker to the final recipient;
 *   A single actor (this one) manages those two cases and adapt its behavior depending on
 *   circumstances, to avoid code duplication;
 * - PluginResp, as either a ClientMessage or an AppMessage, carrying the response from the
 *   headless plugin in the other direction;
 * - PluginNotify, as an AppMessage, from the broker to a plugin (answered with an EmptyResponse
 *   once handed to the local handler).
 */

use std::sync::Arc;

use async_std::sync::Mutex;
use futures::channel::oneshot;

use ng_repo::errors::*;
use ng_repo::log::*;

use crate::app_protocol::*;
use crate::broker::{PluginIncoming, BROKER};
use crate::connection::NoiseFSM;
use crate::types::*;
use crate::utils::spawn_and_log_error;
use crate::{actor::*, types::ProtocolMessage};

/// PluginRegister
/// Send in an AppMessage (aka headless mode) by a plugin who wish to receive plugin requests.

impl AppPluginRegister {
    pub fn get_actor(&self, id: i64) -> Box<dyn EActor> {
        Actor::<AppPluginRegister, AppPluginRegisterResp>::new_responder(id)
    }
}

impl TryFrom<ProtocolMessage> for AppPluginRegister {
    type Error = ProtocolError;
    fn try_from(msg: ProtocolMessage) -> Result<Self, Self::Error> {
        if let AppMessageContentV0::PluginRegister(req) = msg.try_into()? {
            Ok(req)
        } else {
            log_debug!("INVALID AppMessageContentV0::PluginRegister");
            Err(ProtocolError::InvalidValue)
        }
    }
}

impl From<AppPluginRegister> for ProtocolMessage {
    fn from(req: AppPluginRegister) -> ProtocolMessage {
        AppMessageContentV0::PluginRegister(req).into()
    }
}

impl TryFrom<ProtocolMessage> for AppPluginRegisterResp {
    type Error = ProtocolError;
    fn try_from(msg: ProtocolMessage) -> Result<Self, Self::Error> {
        if let AppMessageContentV0::PluginRegisterResp(res) = msg.try_into()? {
            Ok(res)
        } else {
            log_debug!("INVALID AppMessageContentV0::PluginRegisterResp");
            Err(ProtocolError::InvalidValue)
        }
    }
}

impl From<AppPluginRegisterResp> for AppMessage {
    fn from(res: AppPluginRegisterResp) -> AppMessage {
        AppMessage::V0(AppMessageV0 {
            id: 0,
            result: 0,
            content: AppMessageContentV0::PluginRegisterResp(res),
        })
    }
}

impl From<AppPluginRegisterResp> for ProtocolMessage {
    fn from(res: AppPluginRegisterResp) -> ProtocolMessage {
        let msg: AppMessage = res.into();
        msg.into()
    }
}

impl Actor<'_, AppPluginRegister, AppPluginRegisterResp> {}

#[async_trait::async_trait]
impl EActor for Actor<'_, AppPluginRegister, AppPluginRegisterResp> {
    async fn respond(
        &mut self,
        msg: ProtocolMessage,
        fsm: Arc<Mutex<NoiseFSM>>,
    ) -> Result<(), ProtocolError> {
        let req = AppPluginRegister::try_from(msg)?;
        let remote = {
            let lock = fsm.lock().await;
            *lock.remote_peer()
        };
        let res = {
            if remote.is_none() {
                Err(ServerError::BrokerError)
            } else {
                let sb = BROKER.read().await.get_server_broker()?;
                let lock = sb.read().await;
                lock.app_plugin_register(req, remote.unwrap()).await
            }
        };
        let app_message: AppMessage = match res {
            Err(e) => e.into(),
            Ok(o) => o.into(),
        };
        fsm.lock()
            .await
            .send_in_reply_to(app_message.into(), self.id())
            .await?;
        Ok(())
    }
}

/// PluginReq, ether from a client (as a ClientRequest) or an (headless) app
/// (as an AppRequest). The same actor (in actors/app/) is used in both cases.
/// Contrary to every other non-stream requests, this one can also originates from
/// the broker, when forwarding to the final recipient (a headless plugin); And
/// same is true of the PluginResp too.

impl PluginReq {
    pub fn get_actor(&self, id: i64) -> Box<dyn EActor> {
        Actor::<PluginReq, PluginResp>::new_responder(id)
    }
}

impl TryFrom<ProtocolMessage> for PluginReq {
    type Error = ProtocolError;
    fn try_from(msg: ProtocolMessage) -> Result<Self, Self::Error> {
        if let ProtocolMessage::ClientMessage(_) = msg {
            if let ClientRequestContentV0::PluginReq(req) = msg.try_into()? {
                return Ok(req);
            }
        } else if let AppMessageContentV0::PluginReq(req) = msg.try_into()? {
            return Ok(req);
        }
        log_debug!("INVALID PluginReq");
        Err(ProtocolError::InvalidValue)
    }
}

impl From<PluginReq> for ProtocolMessage {
    fn from(req: PluginReq) -> ProtocolMessage {
        AppMessageContentV0::PluginReq(req).into()
    }
}

impl TryFrom<ProtocolMessage> for PluginResp {
    type Error = ProtocolError;
    fn try_from(msg: ProtocolMessage) -> Result<Self, Self::Error> {
        if let ProtocolMessage::ClientMessage(_) = msg {
            if let ClientResponseContentV0::PluginResp(res) = msg.try_into()? {
                return Ok(res);
            }
        } else if let AppMessageContentV0::PluginResp(res) = msg.try_into()? {
            return Ok(res);
        }
        log_debug!("INVALID PluginResp");
        Err(ProtocolError::InvalidValue)
    }
}

impl From<PluginResp> for ProtocolMessage {
    fn from(res: PluginResp) -> ProtocolMessage {
        let msg: AppMessage = res.into();
        msg.into()
    }
}

impl From<PluginResp> for AppMessage {
    fn from(res: PluginResp) -> AppMessage {
        AppMessage::V0(AppMessageV0 {
            id: 0,
            result: 0,
            content: AppMessageContentV0::PluginResp(res),
        })
    }
}

impl From<PluginResp> for ClientResponse {
    fn from(res: PluginResp) -> ClientResponse {
        ClientResponse::V0(ClientResponseV0 {
            id: 0,
            result: 0,
            content: ClientResponseContentV0::PluginResp(res),
        })
    }
}

/// Broker side of a client connection: forwards the request to the named plugin.
async fn serve_client_plugin_request(
    req: PluginReq,
    fsm: &Mutex<NoiseFSM>,
) -> Result<PluginResp, ServerError> {
    let user = {
        let lock = fsm.lock().await;
        if !lock.dir().is_server() {
            return Err(ServerError::InvalidRequest);
        }
        lock.user_id()?
    };
    let sb = BROKER.read().await.get_server_broker()?;
    let sb = sb.read().await;
    sb.plugin_request_forward(req, PluginCaller::User(user)).await
}

/// Either broker or plugin side of a client connection: either forwards the request to the named
/// plugin or to the JS handler.
/// Both can take a while, don't hold any lock!
async fn serve_app_plugin_request(
    req: PluginReq,
    fsm: &Mutex<NoiseFSM>,
) -> Result<PluginResp, ServerError> {
    let (is_server, remote) = {
        let lock = fsm.lock().await;
        (lock.dir().is_server(), *lock.remote_peer())
    };
    if is_server {
        /* Broker side: the request comes from a plugin (or a client) and is
         * forwarded to another. */
        let remote = remote.ok_or(ServerError::BrokerError)?;
        let sb = BROKER.read().await.get_server_broker()?;
        let sb = sb.read().await;
        let caller = sb
            .service_name_of_peer(&remote)
            .await
            .ok_or(ServerError::PluginNotFound)?;
        sb.plugin_request_forward(req, PluginCaller::Plugin(caller)).await
    } else {
        /* Plugin side: the request was forwarded by the broker and goes to
         * the local handler. */
        if req.caller.is_none() {  // Caller must have been set by now.
            return Err(ServerError::InvalidRequest);
        }
        let handler = BROKER.read().await.get_plugin_handler().ok_or(ServerError::NotFound)?;
        // A handler is sent the request and a tx to return the response:
        let (tx, rx) = oneshot::channel();
        handler
            .unbounded_send(PluginIncoming::Request(req, tx))
            .map_err(|_| ServerError::BrokerError)?;
        rx.await.map_err(|_| ServerError::BrokerError)?
    }
}

impl Actor<'_, PluginReq, PluginResp> {}

#[async_trait::async_trait]
impl EActor for Actor<'_, PluginReq, PluginResp> {
    async fn respond(
        &mut self,
        msg: ProtocolMessage,
        fsm: Arc<Mutex<NoiseFSM>>,
    ) -> Result<(), ProtocolError> {
        let from_client = matches!(msg, ProtocolMessage::ClientMessage(_));
        let req = PluginReq::try_from(msg)?;
        let id = self.id();
        /* Whether the request is forwarded to the plugin or to some JS handler,
         * do not wait for the answer. Return now, send_in_reply_to in another
         * task. */
        spawn_and_log_error(async move {
            let reply: ProtocolMessage = if from_client {
                let reply: ClientResponse = match serve_client_plugin_request(req, &fsm).await {
                    Ok(res) => res.into(),
                    Err(e) => e.into(),
                };
                reply.into()
            } else {
                let reply: AppMessage = match serve_app_plugin_request(req, &fsm).await {
                    Ok(res) => res.into(),
                    Err(e) => e.into(),
                };
                reply.into()
            };
            fsm.lock().await.send_in_reply_to(reply, id).await?;
            Ok(())
        });
        Ok(())
    }
}

/// PluginNotify, from the broker to a plugin (as an AppMessage).

impl PluginNotify {
    pub fn get_actor(&self, id: i64) -> Box<dyn EActor> {
        Actor::<PluginNotify, EmptyAppResponse>::new_responder(id)
    }
}

impl TryFrom<ProtocolMessage> for PluginNotify {
    type Error = ProtocolError;
    fn try_from(msg: ProtocolMessage) -> Result<Self, Self::Error> {
        if let AppMessageContentV0::PluginNotify(req) = msg.try_into()? {
            Ok(req)
        } else {
            log_debug!("INVALID AppMessageContentV0::PluginNotify");
            Err(ProtocolError::InvalidValue)
        }
    }
}

impl From<PluginNotify> for ProtocolMessage {
    fn from(req: PluginNotify) -> ProtocolMessage {
        AppMessageContentV0::PluginNotify(req).into()
    }
}

impl Actor<'_, PluginNotify, EmptyAppResponse> {}

#[async_trait::async_trait]
impl EActor for Actor<'_, PluginNotify, EmptyAppResponse> {
    async fn respond(
        &mut self,
        msg: ProtocolMessage,
        fsm: Arc<Mutex<NoiseFSM>>,
    ) -> Result<(), ProtocolError> {
        let req = PluginNotify::try_from(msg)?;
        let res = {
            // Only the broker sends notifications, to plugins:
            if fsm.lock().await.dir().is_server() {
                Err(ServerError::InvalidRequest)
            } else {
                match BROKER.read().await.get_plugin_handler() {
                    None => Err(ServerError::NotFound),
                    Some(handler) => handler
                        .unbounded_send(PluginIncoming::Notify(req))
                        .map(|_| EmptyAppResponse(()))
                        .map_err(|_| ServerError::BrokerError),
                }
            }
        };
        fsm.lock()
            .await
            .send_in_reply_to(res.into(), self.id())
            .await?;
        Ok(())
    }
}
