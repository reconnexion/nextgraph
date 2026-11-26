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

impl EnableServiceReq {
    pub fn get_actor(&self, id: i64) -> Box<dyn EActor> {
        Actor::<EnableServiceReq, Vec<AvailableService>>::new_responder(id)
    }
}

impl TryFrom<ProtocolMessage> for EnableServiceReq {
    type Error = ProtocolError;
    fn try_from(msg: ProtocolMessage) -> Result<Self, Self::Error> {
        let req: ClientRequestContentV0 = msg.try_into()?;
        if let ClientRequestContentV0::EnableServiceReq(a) = req {
            Ok(a)
        } else {
            log_debug!("INVALID {:?}", req);
            Err(ProtocolError::InvalidValue)
        }
    }
}

impl From<EnableServiceReq> for ProtocolMessage {
    fn from(msg: EnableServiceReq) -> ProtocolMessage {
        ProtocolMessage::from_client_request_v0(
            ClientRequestContentV0::EnableServiceReq(msg),
            OverlayId::nil(),
        )
    }
}

impl TryFrom<ProtocolMessage> for () {
    type Error = ProtocolError;
    fn try_from(msg: ProtocolMessage) -> Result<Self, Self::Error> {
        let res: ClientResponseContentV0 = msg.try_into()?;
        if res == ClientResponseContentV0::EmptyResponse {
            Ok(())
        } else {
            log_debug!("INVALID {:?}", res);
            Err(ProtocolError::InvalidValue)
        }
    }
}

impl Actor<'_, EnableServiceReq, ()> {}

#[async_trait::async_trait]
impl EActor for Actor<'_, EnableServiceReq, ()> {
    async fn respond(
        &mut self,
        _msg: ProtocolMessage,
        fsm: Arc<Mutex<NoiseFSM>>,
    ) -> Result<(), ProtocolError> {
        let user = fsm.lock().await.user_id()?;
        let sb = { BROKER.read().await.get_server_broker()? };
        let res = sb.read().await.service_plugin_set_enabled(&user).await;
        fsm.lock().await.send_in_reply_to(res.into(), self.id()).await?;
        Ok(())
    }
}
