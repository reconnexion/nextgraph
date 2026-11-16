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

use ng_repo::errors::*;
use ng_repo::types::OverlayId;
use ng_repo::log::*;

use crate::broker::BROKER;
use crate::connection::NoiseFSM;
use crate::types::*;
use crate::{actor::*, types::ProtocolMessage};

impl ServiceListRequest {
    pub fn get_actor(&self, id: i64) -> Box<dyn EActor> {
        Actor::<ServiceListRequest, Vec<MyService>>::new_responder(id)
    }
}

impl TryFrom<ProtocolMessage> for ServiceListRequest {
    type Error = ProtocolError;
    fn try_from(msg: ProtocolMessage) -> Result<Self, Self::Error> {
        let req: ClientRequestContentV0 = msg.try_into()?;
        if let ClientRequestContentV0::ServiceListRequest(a) = req {
            Ok(a)
        } else {
            log_debug!("INVALID {:?}", req);
            Err(ProtocolError::InvalidValue)
        }
    }
}

impl From<ServiceListRequest> for ProtocolMessage {
    fn from(msg: ServiceListRequest) -> ProtocolMessage {
        ProtocolMessage::from_client_request_v0(
            ClientRequestContentV0::ServiceListRequest(msg),
            OverlayId::nil(),
        )
    }
}

impl TryFrom<ProtocolMessage> for Vec<MyService> {
    type Error = ProtocolError;
    fn try_from(msg: ProtocolMessage) -> Result<Self, Self::Error> {
        let res: ClientResponseContentV0 = msg.try_into()?;
        if let ClientResponseContentV0::ServiceList(a) = res {
            Ok(a)
        } else {
            log_debug!("INVALID {:?}", res);
            Err(ProtocolError::InvalidValue)
        }
    }
}

impl From<Vec<MyService>> for ProtocolMessage {
    fn from(l: Vec<MyService>) -> ProtocolMessage {
        ClientResponseContentV0::ServiceList(l).into()
    }
}

impl Actor<'_, ServiceListRequest, Vec<MyService>> {}

#[async_trait::async_trait]
impl EActor for Actor<'_, ServiceListRequest, Vec<MyService>> {
    async fn respond(
        &mut self,
        _msg: ProtocolMessage,
        fsm: Arc<Mutex<NoiseFSM>>,
    ) -> Result<(), ProtocolError> {
        let user = fsm.lock().await.user_id()?;
        let sb = { BROKER.read().await.get_server_broker()? };
        let res = sb.read().await.service_plugin_list_for_user(&user);
        fsm.lock().await.send_in_reply_to(res.into(), self.id()).await?;
        Ok(())
    }
}
