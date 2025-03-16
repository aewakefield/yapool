mod error;
mod occupied_slots;
mod slot;

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use error::SlotsResult;
use occupied_slots::OccupiedSlots;
use tokio::sync::{Notify, Semaphore, TryAcquireError};

pub use error::SlotsError;
pub use slot::{OccupiedSlot, Slot, VacantSlot};

const POISONED_MUTEX_ERROR: &str = "Pool occupied slots mutex should not be poisoned";

pub struct Slots<Object> {
    total_slots_semaphore: Arc<Semaphore>,
    occupied_slots: Mutex<OccupiedSlots<Object>>,
    slot_available_notifier: Notify,
}

impl<Object> Slots<Object> {
    pub fn new(total_slots: usize) -> Self {
        Self {
            total_slots_semaphore: Arc::new(Semaphore::new(total_slots)),
            occupied_slots: Mutex::new(OccupiedSlots::new(total_slots)),
            slot_available_notifier: Notify::new(),
        }
    }

    pub async fn acquire(&self) -> SlotsResult<Slot<Object>> {
        let mut was_waiting = false;
        loop {
            {
                let mut occupied_slots = self.occupied_slots.lock().expect(POISONED_MUTEX_ERROR);
                if let Some(slot) = occupied_slots.try_acquire()? {
                    if was_waiting && !occupied_slots.is_empty()? {
                        self.slot_available_notifier.notify_one();
                    }
                    return Ok(Slot::Occupied(slot));
                }
            }
            tokio::select! {
                _ = self.slot_available_notifier.notified() => {
                    was_waiting = true;
                    continue;
                }
                permit = self.total_slots_semaphore.clone().acquire_owned() => {
                    return Ok(Slot::Vacant(VacantSlot::new(permit?)));
                }
            }
        }
    }

    pub fn try_acquire(&self) -> SlotsResult<Option<Slot<Object>>> {
        if self.total_slots_semaphore.is_closed() {
            return Err(SlotsError::Closed);
        }
        {
            let mut occupied_slots = self.occupied_slots.lock().expect(POISONED_MUTEX_ERROR);
            if let Some(slot) = occupied_slots.try_acquire()? {
                return Ok(Some(Slot::Occupied(slot)));
            }
        }
        match self.total_slots_semaphore.clone().try_acquire_owned() {
            Ok(slot_permit) => Ok(Some(Slot::Vacant(VacantSlot::new(slot_permit)))),
            Err(TryAcquireError::NoPermits) => Ok(None),
            Err(TryAcquireError::Closed) => Err(SlotsError::Closed),
        }
    }

    pub fn try_acquire_vacant(&self) -> SlotsResult<Option<VacantSlot<Object>>> {
        match self.total_slots_semaphore.clone().try_acquire_owned() {
            Ok(slot_permit) => Ok(Some(VacantSlot::new(slot_permit))),
            Err(TryAcquireError::NoPermits) => Ok(None),
            Err(TryAcquireError::Closed) => Err(SlotsError::Closed),
        }
    }

    /// Release a slot back to the pool. If the pool is closed, the slot will be
    /// returned. The caller is responsible for destroying the object.
    #[must_use]
    pub fn try_release(&self, slot: OccupiedSlot<Object>) -> Option<OccupiedSlot<Object>> {
        let slot = self
            .occupied_slots
            .lock()
            .expect(POISONED_MUTEX_ERROR)
            .try_release(slot);
        if slot.is_none() {
            self.slot_available_notifier.notify_one();
        }
        slot
    }

    #[must_use]
    pub fn close(&self) -> Option<VecDeque<OccupiedSlot<Object>>> {
        self.total_slots_semaphore.close();
        self.occupied_slots
            .lock()
            .expect(POISONED_MUTEX_ERROR)
            .shutdown()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use googletest::prelude::*;

    use super::*;

    #[googletest::test]
    #[tokio::test]
    async fn single_vacant_slot_can_be_acquired_and_released() {
        let slots: Slots<usize> = Slots::new(1);

        let slot = slots.acquire().await;

        expect_that!(slot, ok(pat!(Slot::Vacant(anything()))));
        expect_that!(slots.try_acquire(), ok(none()));

        drop(slot);

        expect_that!(
            slots.try_acquire(),
            ok(some(pat!(Slot::Vacant(anything()))))
        );
    }

    #[googletest::test]
    #[tokio::test]
    async fn single_occupied_slot_can_be_released() {
        let slots: Slots<usize> = Slots::new(1);

        let slot = slots.acquire().await;
        let slot = match slot.unwrap() {
            Slot::Vacant(slot) => slot.occupy(42),
            Slot::Occupied(_slot) => panic!("Slot should be vacant as we haven't filled any yet"),
        };

        expect_that!(slots.try_acquire(), ok(none()));

        expect_that!(slots.try_release(slot), none());

        expect_that!(
            slots.try_acquire(),
            ok(some(pat!(Slot::Occupied(derefs_to(eq(&42))))))
        );
    }

    #[googletest::test]
    #[tokio::test]
    async fn single_slot_when_released_with_object_provides_it_to_the_next_aquire() {
        let slots: Slots<usize> = Slots::new(1);
        let slot = slots.acquire().await;
        let slot = match slot.unwrap() {
            Slot::Vacant(slot) => slot.occupy(42),
            Slot::Occupied(_slot) => panic!("Slot should be vacant as we haven't filled any yet"),
        };

        expect_that!(slots.try_release(slot), none());

        let slot = slots.acquire().await;

        expect_that!(slot, ok(pat!(Slot::Occupied(derefs_to(eq(&42))))));
    }

    #[googletest::test]
    #[tokio::test]
    async fn single_slot_when_slot_dropped_can_acquire_vacant_one() {
        let slots: Slots<usize> = Slots::new(1);
        let slot = slots.acquire().await;
        let slot = match slot.unwrap() {
            Slot::Vacant(slot) => slot.occupy(42),
            Slot::Occupied(_slot) => panic!("Slot should be vacant as we haven't filled any yet"),
        };
        expect_that!(slots.try_release(slot), none());
        let slot = slots.acquire().await;
        expect_that!(slot, ok(pat!(Slot::Occupied(derefs_to(eq(&42))))));
        drop(slot);

        let slot = slots.acquire().await;

        expect_that!(slot, ok(pat!(Slot::Vacant(anything()))));
    }

    #[googletest::test]
    #[tokio::test]
    async fn single_slot_notifies_when_occupied_slot_is_released() {
        let slots: Arc<Slots<usize>> = Arc::new(Slots::new(1));
        let slot = slots.acquire().await;
        let slot = match slot.unwrap() {
            Slot::Vacant(slot) => slot.occupy(42),
            Slot::Occupied(_slot) => panic!("Slot should be vacant as we haven't filled any yet"),
        };

        tokio::spawn({
            let slots = slots.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                expect_that!(slots.try_release(slot), none());
            }
        });

        let slot = tokio::time::timeout(Duration::from_millis(500), slots.acquire()).await;
        expect_that!(slot, ok(ok(pat!(Slot::Occupied(derefs_to(eq(&42)))))));
    }

    #[googletest::test]
    #[tokio::test]
    async fn single_slot_available_when_vacant_slot_is_released() {
        let slots: Arc<Slots<usize>> = Arc::new(Slots::new(1));
        let slot = slots.acquire().await.unwrap();
        expect_that!(slot, pat!(Slot::Vacant(anything())));

        tokio::spawn({
            async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                drop(slot);
            }
        });

        let slot = tokio::time::timeout(Duration::from_millis(500), slots.acquire()).await;
        expect_that!(slot, ok(ok(pat!(Slot::Vacant(anything())))));
    }

    #[googletest::test]
    #[tokio::test]
    async fn single_slot_becomes_available_when_future_is_cancelled() {
        let slots: Arc<Slots<usize>> = Arc::new(Slots::new(1));
        let slot = slots.acquire().await;
        let slot = match slot.unwrap() {
            Slot::Vacant(slot) => slot.occupy(42),
            Slot::Occupied(_slot) => panic!("Slot should be vacant as we haven't filled any yet"),
        };

        expect_that!(slots.try_release(slot), none());

        let future = tokio::spawn({
            let slots = slots.clone();
            async move {
                let slot = slots.acquire().await;
                expect_that!(slot, ok(pat!(Slot::Occupied(derefs_to(eq(&42))))));
                std::future::pending::<()>().await
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await; // Ensure it has acquired the slot
        future.abort();
        future.await.unwrap_err();
        expect_that!(
            slots.try_acquire(),
            ok(some(pat!(Slot::Vacant(anything()))))
        );
    }

    #[googletest::test]
    #[tokio::test]
    async fn multiple_slots_can_be_acquired_and_released() {
        let slots: Slots<usize> = Slots::new(2);
        let slot1 = slots.acquire().await;
        let slot2 = slots.acquire().await;

        expect_that!(slot1, ok(pat!(Slot::Vacant(anything()))));
        expect_that!(slot2, ok(pat!(Slot::Vacant(anything()))));
        expect_that!(slots.try_acquire(), ok(none()));

        drop(slot1);
        drop(slot2);

        expect_that!(
            slots.try_acquire(),
            ok(some(pat!(Slot::Vacant(anything()))))
        );
    }

    #[googletest::test]
    #[tokio::test]
    async fn multiple_slots_can_be_acquired_and_released_with_objects() {
        let slots: Slots<usize> = Slots::new(2);
        let slot1 = slots.acquire().await;
        let slot1 = match slot1.unwrap() {
            Slot::Vacant(slot) => slot.occupy(42),
            Slot::Occupied(_slot) => panic!("Slot1 should be vacant as we haven't filled any yet"),
        };
        let slot2 = slots.acquire().await;
        let slot2 = match slot2.unwrap() {
            Slot::Vacant(slot) => slot.occupy(43),
            Slot::Occupied(_slot) => panic!("Slot2 should be vacant as we haven't filled any yet"),
        };

        expect_that!(slots.try_release(slot1), none());
        expect_that!(slots.try_release(slot2), none());

        let slot = slots.try_acquire();
        expect_that!(slot, ok(some(pat!(Slot::Occupied(derefs_to(eq(&43)))))));
        let slot = slots.try_acquire();
        expect_that!(slot, ok(some(pat!(Slot::Occupied(derefs_to(eq(&42)))))));
    }

    #[googletest::test]
    #[tokio::test]
    async fn multiple_slots_when_waiting_ensure_notify_next_if_occupied_slots_are_not_empty() {
        let slots: Arc<Slots<&str>> = Arc::new(Slots::new(2));
        let slot1 = slots.acquire().await;
        let slot1 = match slot1.unwrap() {
            Slot::Vacant(slot) => slot.occupy("slot1"),
            Slot::Occupied(_slot) => panic!("Slot1 should be vacant as we haven't filled any yet"),
        };
        let slot2 = slots.acquire().await;
        let slot2 = match slot2.unwrap() {
            Slot::Vacant(slot) => slot.occupy("slot2"),
            Slot::Occupied(_slot) => panic!("Slot2 should be vacant as we haven't filled any yet"),
        };

        let waiter_1 = tokio::spawn({
            let slots = slots.clone();
            async move { slots.acquire().await }
        });
        let waiter_2 = tokio::spawn({
            let slots = slots.clone();
            async move { slots.acquire().await }
        });

        // Ensure that there are multiple occupied slots so the first waiter will notify the second.
        expect_that!(
            slots.occupied_slots.lock().unwrap().try_release(slot2),
            none()
        );
        expect_that!(slots.try_release(slot1), none());

        let waiter_1 = tokio::time::timeout(Duration::from_millis(500), waiter_1).await;
        let waiter_2 = tokio::time::timeout(Duration::from_millis(500), waiter_2).await;

        expect_that!(
            waiter_1,
            ok(ok(ok(pat!(Slot::Occupied(derefs_to(eq(&"slot1")))))))
        );
        expect_that!(
            waiter_2,
            ok(ok(ok(pat!(Slot::Occupied(derefs_to(eq(&"slot2")))))))
        );
    }

    #[googletest::test]
    #[tokio::test]
    async fn multiple_slots_close_returns_all_occupied_slots() {
        let slots: Slots<usize> = Slots::new(2);
        let slot1 = slots.try_acquire_vacant();
        let slot1 = slot1.unwrap().unwrap().occupy(42);
        expect_that!(slots.try_release(slot1), none());
        let slot2 = slots.try_acquire_vacant();
        let slot2 = slot2.unwrap().unwrap().occupy(43);
        expect_that!(slots.try_release(slot2), none());

        let occupied_slots = slots.close();

        expect_that!(
            occupied_slots,
            some(elements_are![derefs_to(eq(&42)), derefs_to(eq(&43))])
        );
    }

    #[googletest::test]
    #[tokio::test]
    async fn single_slot_close_triggers_error_if_waiting_for_slot() {
        let slots: Arc<Slots<usize>> = Arc::new(Slots::new(1));
        let _slot = slots.acquire().await;

        let waiter = tokio::spawn({
            let slots = slots.clone();
            async move { slots.acquire().await }
        });

        expect_that!(slots.close(), some(anything()));

        let waiter = tokio::time::timeout(Duration::from_millis(500), waiter).await;
        expect_that!(waiter, ok(ok(err(pat!(SlotsError::Closed)))));
    }

    #[googletest::test]
    #[tokio::test]
    async fn closed_slots_acquire_returns_error() {
        let slots: Slots<usize> = Slots::new(1);
        expect_that!(slots.close(), some(anything()));

        let slot = slots.acquire().await;
        expect_that!(slot, err(pat!(SlotsError::Closed)));
    }

    #[googletest::test]
    #[tokio::test]
    async fn closed_slots_try_acquire_returns_error() {
        let slots: Slots<usize> = Slots::new(1);
        expect_that!(slots.close(), some(anything()));

        let slot = slots.try_acquire();
        expect_that!(slot, err(pat!(SlotsError::Closed)));
    }
}
