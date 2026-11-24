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
use std::sync::Arc;

use async_std::sync::Mutex;
use serde::{Deserialize, Serialize};

use ng_repo::errors::{ProtocolError, ServerError};
use ng_repo::log::*;

use super::super::StartProtocol;
use crate::broker::BROKER;
use crate::connection::NoiseFSM;
use crate::types::*;
use crate::{actor::*, types::ProtocolMessage};

/// Request update (of the status only) of an existing ServicePlugin
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestServicePluginSetStatusV0 {
    /// The whole service description has all the needed info
    pub service: ServicePlugin,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RequestServicePluginSetStatus {
    V0(RequestServicePluginSetStatusV0),
}

impl RequestServicePluginSetStatus {
    pub fn service(&self) -> &ServicePlugin {
        match self {
            RequestServicePluginSetStatus::V0(o) => &o.service,
        }
    }
    pub fn get_actor(&self) -> Box<dyn EActor> {
        Actor::<RequestServicePluginSetStatus, AdminResponse>::new_responder(0)
    }
}

impl TryFrom<ProtocolMessage> for RequestServicePluginSetStatus {
    type Error = ProtocolError;
    fn try_from(msg: ProtocolMessage) -> Result<Self, Self::Error> {
        if let ProtocolMessage::Start(StartProtocol::Admin(AdminRequest::V0(AdminRequestV0 {
            content: AdminRequestContentV0::RequestServicePluginSetStatus(a),
            ..
        }))) = msg
        {
            Ok(a)
        } else {
            Err(ProtocolError::InvalidValue)
        }
    }
}

impl From<RequestServicePluginSetStatus> for ProtocolMessage {
    fn from(_msg: RequestServicePluginSetStatus) -> ProtocolMessage {
        unimplemented!();
    }
}

impl From<RequestServicePluginSetStatus> for AdminRequestContentV0 {
    fn from(msg: RequestServicePluginSetStatus) -> AdminRequestContentV0 {
        AdminRequestContentV0::RequestServicePluginSetStatus(msg)
    }
}

impl Actor<'_, RequestServicePluginSetStatus, AdminResponse> {}

#[async_trait::async_trait]
impl EActor for Actor<'_, RequestServicePluginSetStatus, AdminResponse> {
    async fn respond(
        &mut self,
        msg: ProtocolMessage,
        fsm: Arc<Mutex<NoiseFSM>>,
    ) -> Result<(), ProtocolError> {
        let req = RequestServicePluginSetStatus::try_from(msg)?;
        let serv = req.service();
        let (response, updated, sb) = {
            let broker = BROKER.read().await;
            let sb = broker.get_server_broker()?;
            let res = {
                let sb = sb.read().await;
                let res = sb.service_plugin_put(serv, false);
                /* Storage is updated first, so that a plugin connecting after the
                 * is_connected check registers with the new status. */
                if res.is_ok() {
                    if serv.status != PluginStatus::Enabled {
                        sb.app_plugin_deactivate(&serv.peer_id, &serv.service_name).await;
                    } else if broker.is_connected(serv.peer_id.slice()) {
                        sb.app_plugin_activate(&serv.peer_id, &serv.service_name).await;
                    }
                }
                res
            };
            let updated = res.is_ok();
            let response: AdminResponseV0 = res.into();
            (response, updated, sb)
        };
        fsm.lock().await.send(response.into()).await?;
        // BROKER must not be locked anymore, as plugin_notify locks it:
        if updated {
            let notification = PluginNotification::StatusChange(serv.status.clone());
            match sb
                .read()
                .await
                .plugin_notify(&serv.peer_id, &serv.service_name, notification)
                .await
            {
                Ok(()) | Err(ServerError::PluginNotFound) => (),
                Err(e) => log_err!(
                    "Cannot notify plugin {:?} for service {}: {:?}",
                    serv.peer_id, serv.service_name, e
                ),
            }
        }
        Ok(())
    }
}
