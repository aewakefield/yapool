use std::marker::PhantomData;

use tokio::sync::OwnedSemaphorePermit;

#[derive(Debug)]
pub enum Slot<Object> {
    Vacant(VacantSlot<Object>),
    Occupied(OccupiedSlot<Object>),
}

#[derive(Debug)]
pub struct VacantSlot<Object> {
    slot_permit: OwnedSemaphorePermit,
    phantom: PhantomData<Object>,
}

impl<Object> VacantSlot<Object> {
    pub fn new(slot_permit: OwnedSemaphorePermit) -> Self {
        Self {
            slot_permit,
            phantom: PhantomData,
        }
    }

    pub fn occupy(self, object: Object) -> OccupiedSlot<Object> {
        OccupiedSlot {
            slot_permit: self.slot_permit,
            object,
        }
    }
}

#[derive(Debug)]
pub struct OccupiedSlot<Object> {
    slot_permit: OwnedSemaphorePermit,
    object: Object,
}

impl<Object> OccupiedSlot<Object> {
    pub fn vacate(self) -> (Object, VacantSlot<Object>) {
        (
            self.object,
            VacantSlot {
                slot_permit: self.slot_permit,
                phantom: PhantomData,
            },
        )
    }
}

impl<Object> std::ops::Deref for OccupiedSlot<Object> {
    type Target = Object;

    fn deref(&self) -> &Self::Target {
        &self.object
    }
}

impl<Object> std::ops::DerefMut for OccupiedSlot<Object> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.object
    }
}
