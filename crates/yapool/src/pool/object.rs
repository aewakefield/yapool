use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::Manager;
use crate::pool::PoolInner;
use crate::slots::{OccupiedSlot, Slot};

const POOL_OBJECT_SHOULD_HAVE_OCCUPIED_SLOT: &str = "Pool object should have occupied slot";

/// An object managed by a [`Pool`]. Will be reset and released back to the
/// pool when dropped. Can be dereferenced to the inner object type.
pub struct PoolObject<M: Manager> {
    occupied_slot: Option<OccupiedSlot<M::Object>>,
    pool: Arc<PoolInner<M>>,
}

impl<M: Manager> PoolObject<M> {
    pub(crate) fn new(occupied_slot: OccupiedSlot<M::Object>, pool: Arc<PoolInner<M>>) -> Self {
        Self {
            occupied_slot: Some(occupied_slot),
            pool,
        }
    }
}

impl<M: Manager> Drop for PoolObject<M> {
    fn drop(&mut self) {
        if let Some(occupied_slot) = self.occupied_slot.take() {
            self.pool.clone().release(occupied_slot);
        }
    }
}

impl<M: Manager> Deref for PoolObject<M> {
    type Target = M::Object;

    fn deref(&self) -> &Self::Target {
        self.occupied_slot
            .as_ref()
            .expect(POOL_OBJECT_SHOULD_HAVE_OCCUPIED_SLOT)
    }
}

impl<M: Manager> DerefMut for PoolObject<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.occupied_slot
            .as_mut()
            .expect(POOL_OBJECT_SHOULD_HAVE_OCCUPIED_SLOT)
    }
}

impl<M: Manager> From<PoolObject<M>> for Slot<M::Object> {
    fn from(mut pool_object: PoolObject<M>) -> Self {
        Slot::Occupied(
            pool_object
                .occupied_slot
                .take()
                .expect(POOL_OBJECT_SHOULD_HAVE_OCCUPIED_SLOT),
        )
    }
}

impl<M: Manager> std::fmt::Debug for PoolObject<M>
where
    M::Object: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PoolObject {{ occupied_slot: {:?} }}",
            self.occupied_slot
        )
    }
}
