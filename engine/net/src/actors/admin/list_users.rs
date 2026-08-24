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

use ng_repo::errors::*;
use ng_repo::types::UserId;

use super::super::StartProtocol;

use crate::broker::BROKER;
use crate::connection::NoiseFSM;
use crate::types::*;
use crate::{actor::*, types::ProtocolMessage};

/// List users registered on this broker
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ListUsersV0 {
    /// Filter: List only the admins, non-admins, or everyone
    pub admins: Option<bool>,
    /// Include user extra information
    pub hydrated: bool,
    /// Users are always returned ordered by key, which makes for an easy cursor
    /// made of last received UserId (if any) and limit:
    pub after_user: Option<UserId>,
    pub limit: Option<u32>,
}

/// List users registered on this broker
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum ListUsers {
    V0(ListUsersV0),
}

impl ListUsers {
    pub fn admins(&self) -> Option<bool> {
        match self {
            Self::V0(o) => o.admins,
        }
    }
    pub fn hydrated(&self) -> bool {
        match self {
            Self::V0(o) => o.hydrated,
        }
    }
    pub fn after_user(&self) -> Option<UserId> {
        match self {
            Self::V0(o) => o.after_user,
        }
    }
    pub fn limit(&self) -> Option<u32> {
        match self {
            Self::V0(o) => o.limit,
        }
    }
    pub fn get_actor(&self) -> Box<dyn EActor> {
        Actor::<ListUsers, AdminResponse>::new_responder(0)
    }
}

impl TryFrom<ProtocolMessage> for ListUsers {
    type Error = ProtocolError;
    fn try_from(msg: ProtocolMessage) -> Result<Self, Self::Error> {
        if let ProtocolMessage::Start(StartProtocol::Admin(AdminRequest::V0(AdminRequestV0 {
            content: AdminRequestContentV0::ListUsers(a),
            ..
        }))) = msg
        {
            Ok(a)
        } else {
            //log_debug!("INVALID {:?}", msg);
            Err(ProtocolError::InvalidValue)
        }
    }
}

impl From<ListUsers> for ProtocolMessage {
    fn from(_msg: ListUsers) -> ProtocolMessage {
        unimplemented!();
    }
}

impl From<ListUsers> for AdminRequestContentV0 {
    fn from(msg: ListUsers) -> AdminRequestContentV0 {
        AdminRequestContentV0::ListUsers(msg)
    }
}

impl Actor<'_, ListUsers, AdminResponse> {}

#[async_trait::async_trait]
impl EActor for Actor<'_, ListUsers, AdminResponse> {
    async fn respond(
        &mut self,
        msg: ProtocolMessage,
        fsm: Arc<Mutex<NoiseFSM>>,
    ) -> Result<(), ProtocolError> {
        let req = ListUsers::try_from(msg)?;
        let sb = { BROKER.read().await.get_server_broker()? };
        let res = {
            sb.read().await.list_users(
                req.admins(),
                req.hydrated(),
                req.after_user(),
                req.limit(),
            ) };

        let response: AdminResponseV0 = res.into();
        fsm.lock().await.send(response.into()).await?;
        Ok(())
    }
}
