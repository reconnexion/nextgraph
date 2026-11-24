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
use ng_repo::kcv_storage::{KCVStorage, ReadTransaction, WriteTransaction};
use ng_repo::log::*;
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
    pub user_enabled: bool,
    pub dirty: bool,
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

    /// Retrieve the UserState for a given service_name and UserId.
    /// Works on the storage itself as well as within a transaction.
    pub fn find<T: ReadTransaction + ?Sized>(
        tx: &T,
        service_name: &String,
        user_id: &UserId,
    ) -> Result<HydratedUserState, StorageError> {
        for user_enabled in [false, true] {
            for dirty in [false, true] {
                let k = Self::key(service_name, user_id, user_enabled, dirty);
                match tx.get(Self::PREFIX, &k, None, &None) {
                    Ok(v) => {
                        return Ok(Self { user_enabled, dirty, content: from_slice(&v)? });
                    }
                    Err(StorageError::NotFound) => (),
                    Err(e) => return Err(e),
                }
            }
        }
        log_debug!("HydratedUserState.find: no key for service {}, user {}", service_name, user_id);
        Err(StorageError::NotFound)
    }

    /** Change the key of a value */
    fn change_key(
        tx: &dyn WriteTransaction,
        old_key: &Vec<u8>,
        new_key: &Vec<u8>,
    ) -> Result<(), StorageError> {
        let v = tx.get(Self::PREFIX, old_key, None, &None)?;
        tx.put(Self::PREFIX, new_key, None, &v, &None)?;
        tx.del(Self::PREFIX, old_key, None, &None)
    }

    /** Set the user_enabled bit, returning the previous user state.
     * Err out if the key does not exist. */
    pub fn set_user_enabled(
        storage: &dyn KCVStorage,
        service_name: &String,
        user_id: &UserId,
        user_enabled: bool,
    ) -> Result<HydratedUserState, StorageError> {
        let mut previous = None;
        storage.write_transaction(&mut |tx| {
            let user_state = Self::find(tx, service_name, user_id)?;
            if user_state.user_enabled == user_enabled {
                log_warn!("service_plugin_users: set_user_enabled: \
                    user-state for service {}, user {}, is already {}",
                    service_name, user_id,
                    if user_enabled { "enabled" } else { "disabled" });
            } else {
                let old_key =
                    Self::key(service_name, user_id, user_state.user_enabled, user_state.dirty);
                let new_key = Self::key(service_name, user_id, user_enabled, true);
                Self::change_key(tx, &old_key, &new_key)?;
            }
            previous = Some(user_state);
            Ok(())
        })?;
        previous.ok_or(StorageError::NotFound)
    }

    /** Create a new user state (user-enabled, dirty).
     * Fails if there is already one, which should not happen unless the same
     * user enable the same service very quickly. */
    pub fn create(
        storage: &dyn KCVStorage,
        service_name: &String,
        user_id: &UserId,
        settings_nuri: &String,
    ) -> Result<Self, StorageError> {
        let user_state = UserState {
            settings_nuri: settings_nuri.clone(),
            plugin_state: Vec::<u8>::new(),
        };
        let v = to_vec(&user_state)?;
        storage.write_transaction(&mut |tx| {
            if Self::find(tx, service_name, user_id).is_ok() {
                log_err!("HydratedUserState: create: user state already exist for service {service_name} and user {user_id}");
                return Err(StorageError::AlreadyExists);
            }
            let k = Self::key(service_name, user_id, true, true);
            tx.put(Self::PREFIX, &k, None, &v, &None)
        })?;
        Ok(HydratedUserState {
            user_enabled: true,
            dirty: true,
            content: user_state,
        })
    }
}
