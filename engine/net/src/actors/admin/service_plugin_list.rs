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

use ng_repo::errors::ProtocolError;

use super::super::StartProtocol;
use crate::broker::BROKER;
use crate::connection::NoiseFSM;
use crate::types::*;
use crate::{actor::*, types::ProtocolMessage};

/// Request the list of all ServicePlugin objects
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RequestServicePluginList {
    V0,
}

impl RequestServicePluginList {
    pub fn get_actor(&self) -> Box<dyn EActor> {
        Actor::<RequestServicePluginList, AdminResponse>::new_responder(0)
    }
}

impl TryFrom<ProtocolMessage> for RequestServicePluginList {
    type Error = ProtocolError;
    fn try_from(msg: ProtocolMessage) -> Result<Self, Self::Error> {
        if let ProtocolMessage::Start(StartProtocol::Admin(AdminRequest::V0(AdminRequestV0 {
            content: AdminRequestContentV0::RequestServicePluginList(a),
            ..
        }))) = msg
        {
            Ok(a)
        } else {
            Err(ProtocolError::InvalidValue)
        }
    }
}

impl From<RequestServicePluginList> for ProtocolMessage {
    fn from(_msg: RequestServicePluginList) -> ProtocolMessage {
        unimplemented!();
    }
}

impl From<RequestServicePluginList> for AdminRequestContentV0 {
    fn from(msg: RequestServicePluginList) -> AdminRequestContentV0 {
        AdminRequestContentV0::RequestServicePluginList(msg)
    }
}

impl Actor<'_, RequestServicePluginList, AdminResponse> {}

#[async_trait::async_trait]
impl EActor for Actor<'_, RequestServicePluginList, AdminResponse> {
    async fn respond(
        &mut self,
        _msg: ProtocolMessage,
        fsm: Arc<Mutex<NoiseFSM>>,
    ) -> Result<(), ProtocolError> {
        let sb = { BROKER.read().await.get_server_broker()? };
        {
            let mut fsm = fsm.lock().await;
            let res = sb.read().await.service_plugin_list();
            let response: AdminResponseV0 = res.into();
            fsm.send(response.into()).await?;
        }
        Ok(())
    }
}

impl From<Result<Vec<ServicePlugin>, ProtocolError>> for AdminResponseV0 {
    fn from(
        res: Result<Vec<ServicePlugin>, ProtocolError>,
    ) -> AdminResponseV0 {
        match res {
            Err(e) => AdminResponseV0 {
                id: 0,
                result: e.into(),
                content: AdminResponseContentV0::EmptyResponse,
                padding: vec![],
            },
            Ok(vec) => AdminResponseV0 {
                id: 0,
                result: 0,
                content: AdminResponseContentV0::ServicePluginList(vec),
                padding: vec![],
            },
        }
    }
}
