/* ServicePlugin information are stored in rocksDb for convenience.
 * After the prefix, the key just has the service name (thus ensuring unicity)
 * and the value is the whole serialized ServicePlugin object. */
use serde_bare::{from_slice, to_vec};
use ng_repo::kcv_storage::KCVStorage;
use ng_repo::log::{log, log_err};
use ng_repo::errors::StorageError;
use ng_repo::types::DirectPeerId;
use ng_repo::object::{DIRECT_PEER_ID_SIZE, BLAKE3_HASH_SIZE};

pub struct ServicePlugin<'a> {
    name: String,
    peer_id: DirectPeerId,
    storage: &'a dyn KCVStorage,
}

impl<'a> ServicePlugin<'a> {
    pub const PREFIX: u8 = b's';
    pub const KEY_LEN: usize = BLAKE3_HASH_SIZE + DIRECT_PEER_ID_SIZE; // Plus the prefix

    fn key(name: &String, peer_id: &DirectPeerId) -> Vec<u8> {
        let mut v = blake3::hash(name.as_bytes()).as_bytes().to_vec();
        v.extend(to_vec(peer_id).unwrap());
        debug_assert!(v.len() == Self::KEY_LEN, "bad hash length: {}+{}: {}", &name, peer_id, v.len());
        v
    }
    pub fn open(name: &String, peer_id: &DirectPeerId, storage: &'a dyn KCVStorage) -> Result<ServicePlugin<'a>, StorageError> {
        let opening = ServicePlugin { name: name.clone(), peer_id: peer_id.clone(), storage };
        if !opening.exists() {
            log_err!("ServicePlugin::open: no plugin service {}", &name);
            return Err(StorageError::NotFound);
        }
        Ok(opening)
    }
    pub fn create(
        serv: &ng_net::types::ServicePlugin,
        storage: &'a dyn KCVStorage,
    ) -> Result<ServicePlugin<'a>, StorageError> {
        let serv_ =
            ServicePlugin { name: serv.name.clone(), peer_id: serv.peer_id.clone(), storage };
        if serv_.exists() {
            return Err(StorageError::AlreadyExists);
        }
        storage.put(
            Self::PREFIX,
            &Self::key(&serv.name, &serv.peer_id),
            None,
            &to_vec(&serv)?,
            &None,
        )?;
        Ok(serv_)
    }
    pub fn update(
        serv: &ng_net::types::ServicePlugin,
        storage: &'a dyn KCVStorage,
    ) -> Result<(), StorageError> {
        let serv_ = ServicePlugin { name: serv.name.clone(), peer_id: serv.peer_id.clone(), storage };
        if !serv_.exists() {
            return Err(StorageError::NotFound);
        }
        storage.put(
            Self::PREFIX,
            &Self::key(&serv.name, &serv.peer_id),
            None,
            &to_vec(&serv)?,
            &None,
        )?;
        Ok(())
    }
    pub fn get(storage: &'a dyn KCVStorage, name: &String, peer_id: &DirectPeerId) -> Result<ng_net::types::ServicePlugin, StorageError> {
        let v = storage.get(
            Self::PREFIX,
            &Self::key(name, peer_id),
            None,
            &None,
        )?;
        Ok(from_slice(&v)?)
    }

    pub fn exists(&self) -> bool {
        self.storage
            .get(
                Self::PREFIX,
                &Self::key(&self.name, &self.peer_id),
                None,
                &None,
            )
            .is_ok()
    }
    pub fn list(storage: &'a dyn KCVStorage) -> Result<Vec<ng_net::types::ServicePlugin>, StorageError> {
        let mut res = Vec::new();
        for kv in
            storage.get_all_keys_and_values(
                Self::PREFIX, Self::KEY_LEN, vec![], None, &None)?
        {
            let serv: ng_net::types::ServicePlugin = from_slice(&kv.1)?;
            res.push(serv);
        }
        Ok(res)
    }
}
