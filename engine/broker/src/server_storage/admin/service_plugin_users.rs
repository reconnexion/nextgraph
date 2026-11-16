/* Service plugins store information about each users: the user-state (ServicePluginUserState),
 * which is a pair holding:
 * - the nuri of the settings document for that user;
 * - some optional opaque value that makes sense only to the plugin and that ngd makes on attempt
 *   to interpret.
 * The key for that user-state value is composed of:
 * - 'S' (uppercase, distinct from the lowercase 's' used for the service plugin themselves);
 * - blake3 of the service name;
 * - user-enabled flag 'E' (enabled) or 'D' (disabled);
 * - user-state readiness flag 'F' (functional) or 'D' (dirty);
 * - the user_id.
 * The readiness flag makes it possible to quickly scoop up all dirty states for a given service
 * (comes handy during plugin startup). */
use serde::{Deserialize, Serialize};
use serde_bare::{from_slice, to_vec};
use ng_repo::errors::StorageError;
use ng_repo::kcv_storage::KCVStorage;
use ng_repo::log::{log, log_debug};
use ng_repo::object::{USER_ID_SIZE, BLAKE3_HASH_SIZE};
use ng_repo::types::UserId;

/// The content of the UserState key:
#[derive(Debug, Serialize, Deserialize)]
pub struct UserState {
    // The nuri of the settings document for that service and user:
    pub settings_nuri: String,
    // Serialized however the plugin wants. Empty if unset:
    pub plugin_state: Vec<u8>,
}

pub struct HydratedUserState {
    pub is_user_enabled: bool,
    pub is_dirty: bool,
    pub content: UserState,
}

impl HydratedUserState {
    const PREFIX: u8 = b'S';
    const KEY_LEN: usize = BLAKE3_HASH_SIZE + 1 + 1 + USER_ID_SIZE; // Plus the prefix
    const ENABLED: u8 = b'E';
    const DISABLED: u8 = b'D';
    const FUNCTIONAL: u8 = b'F';
    const DIRTY: u8 = b'D';

    fn user_enabled_to_byte(user_enabled: bool) -> u8 {
        if user_enabled { Self::ENABLED } else { Self::DISABLED }
    }

    fn dirty_to_byte(dirty: bool) -> u8 {
        if dirty { Self::DIRTY } else { Self::FUNCTIONAL }
    }

    // Returns the key without the PREFIX
    fn key(service_name: &String, user_id: &UserId, user_enabled: bool, dirty: bool) -> Vec<u8> {
        let mut v = Vec::with_capacity(Self::KEY_LEN);
        v.extend_from_slice(blake3::hash(service_name.as_bytes()).as_bytes());
        v.push(Self::user_enabled_to_byte(user_enabled));
        v.push(Self::dirty_to_byte(dirty));
        v.extend_from_slice(&to_vec(user_id).unwrap());
        v
    }

    // Retrieve the UserState for a given service_name and UserId:
    pub fn find<'a>(
        storage: &'a dyn KCVStorage,
        service_name: &String,
        user_id: &UserId,
    ) -> Result<HydratedUserState, StorageError> {
        let mut found = None;
        storage.write_transaction(&mut |tx| {
            for is_user_enabled in [false, true] {
                for is_dirty in [false, true] {
                    let k = Self::key(service_name, user_id, is_user_enabled, is_dirty);
                    match tx.get(Self::PREFIX, &k, None, &None) {
                        Ok(v) => {
                            found = Some((is_user_enabled, is_dirty, v));
                            return Ok(());
                        }
                        Err(StorageError::NotFound) => (),
                        Err(e) => return Err(e),
                    }
                }
            }
            log_debug!("HydratedUserState.find: no key for service {}, user {}", service_name, user_id);
            Ok(())
        })?;
        let (is_user_enabled, is_dirty, v) = found.ok_or(StorageError::NotFound)?;
        Ok(Self { is_user_enabled, is_dirty, content: from_slice(&v)? })
    }

    /** Given a service name and a UserId, tells if that service is enabled for that used.
     * Used to populate the list of services for a given user. */
    pub fn is_user_enabled<'a>(
        storage: &'a dyn KCVStorage,
        service_name: &String,
        user_id: &UserId,
    ) -> Result<bool, StorageError> {
        match Self::find(storage, service_name, user_id) {
            Ok(user_state) => Ok(user_state.is_user_enabled),
            Err(StorageError::NotFound) => Ok(false), // never enabled
            Err(e) => Err(e),
        }
    }
}
