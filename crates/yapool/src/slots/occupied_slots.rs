use std::collections::VecDeque;

use super::{OccupiedSlot, SlotsError, error::SlotsResult};

pub struct OccupiedSlots<Object> {
    slots: Option<VecDeque<OccupiedSlot<Object>>>,
}

impl<Object> OccupiedSlots<Object> {
    pub fn new(total_slots: usize) -> Self {
        Self {
            slots: Some(VecDeque::with_capacity(total_slots)),
        }
    }

    pub fn try_acquire(&mut self) -> SlotsResult<Option<OccupiedSlot<Object>>> {
        self.slots
            .as_mut()
            .map(|slots| slots.pop_back())
            .ok_or(SlotsError::Closed)
    }

    pub fn is_empty(&self) -> SlotsResult<bool> {
        self.slots
            .as_ref()
            .map(|slots| slots.is_empty())
            .ok_or(SlotsError::Closed)
    }

    #[must_use]
    pub fn try_release(&mut self, slot: OccupiedSlot<Object>) -> Option<OccupiedSlot<Object>> {
        if let Some(slots) = self.slots.as_mut() {
            slots.push_back(slot);
            None
        } else {
            Some(slot)
        }
    }

    #[must_use]
    pub fn shutdown(&mut self) -> Option<VecDeque<OccupiedSlot<Object>>> {
        self.slots.take()
    }
}
