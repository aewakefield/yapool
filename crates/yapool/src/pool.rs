mod config;
mod error;
mod object;

use std::{sync::Arc, time::Duration};

use config::ValidPoolConfig;
use error::PoolResult;
use tokio::task::JoinSet;

use crate::{
    Manager,
    slots::{OccupiedSlot, Slot, Slots},
};

pub use config::PoolConfig;
pub use error::PoolError;
pub use object::PoolObject;

/// Pool of objects managed by a [`Manager`].
///
/// # Examples
/// ```
/// use yapool::{Pool, PoolConfig, Manager};
/// use std::time::Duration;
///
/// struct MyObject;
///
/// #[derive(Debug)]
/// struct MyManager;
/// impl Manager for MyManager {
///     type Object = MyObject;
///     type Error = &'static str;
///     async fn create(&self) -> Result<Self::Object, Self::Error> {
///         Ok(MyObject)
///     }
/// }
///
/// #[tokio::main]
/// async fn main() {
///     let config = PoolConfig::default();
///     let manager = MyManager;
///     let pool = Pool::new(manager, config).await.unwrap();
///
///     // Get an object from the pool.
///     let object_1 = pool.acquire().await.unwrap();
///
///     // Get another object from the pool, don't wait if there's no object available.
///     let object_2 = pool.try_acquire().await.unwrap();
///
///     // Release the first object back to the pool.
///     drop(object_1);
///
///     // Close the pool when we're finished with it.
///     pool.close().await.unwrap();
///
///     // Objects can outlive the pool.
///     drop(pool);
///
///     // Drop the second object, it will be destroyed as the pool is closed.
///     drop(object_2);
/// }
/// ```
pub struct Pool<M: Manager> {
    inner: Arc<PoolInner<M>>,
}

pub struct PoolInner<M: Manager> {
    manager: M,
    slots: Slots<M::Object>,
    config: ValidPoolConfig,
}

impl<M: Manager> Pool<M> {
    /// Create a new pool with a [`Manager`] and a [`PoolConfig`].
    pub async fn new(manager: M, config: PoolConfig) -> PoolResult<Self, M> {
        let config = config.validate()?;

        let slots = Slots::new(config.max_objects);

        let pool = Self {
            inner: Arc::new(PoolInner {
                manager,
                slots,
                config,
            }),
        };

        for _ in 0..pool.inner.config.min_objects {
            let slot = pool.inner.slots.try_acquire_vacant()?;
            let slot = match slot {
                Some(slot) => slot,
                None => break,
            };
            let object = pool.inner.create_object().await?;
            let slot = slot.occupy(object);
            // This will only return the slot if the pool is closed. We are
            // creating it here so it cannot be closed. So it is safe to ignore
            // the result.
            let _ = pool.inner.slots.try_release(slot);
        }

        Ok(pool)
    }

    /// Acquire an object from the pool. Will first try to return an
    /// existing object if one is available, calling [`Manager::check_liveness`]
    /// first. If no object is available but there is space to create one it
    /// will create and return the new object. If there's no available object
    /// and no space to create one it will wait for an object to be released
    /// or for the pool to be closed.
    pub async fn acquire(&self) -> PoolResult<PoolObject<M>, M> {
        let slot = with_timeout(
            self.inner.config.acquire_timeout,
            self.inner.slots.acquire(),
        )
        .await??;
        self.inner.create_pool_object_from_slot(slot).await
    }

    /// Try to acquire an object from the pool. Will first try to return an
    /// existing object if one is available, calling [`Manager::check_liveness`]
    /// first. If no object is available but there is space to create one it
    /// will create and return the new object. If there's no available object
    /// and no space to create one it will return `None`.
    pub async fn try_acquire(&self) -> PoolResult<Option<PoolObject<M>>, M> {
        let slot = self.inner.slots.try_acquire()?;
        match slot {
            None => Ok(None),
            Some(slot) => Ok(Some(self.inner.create_pool_object_from_slot(slot).await?)),
        }
    }

    /// Close the pool. Will destroy all objects currently available in the
    /// pool, ones in use will be destroyed when released back to the pool.
    /// Calls currently waiting on [`Pool::acquire`] and future calls will
    /// receive a [`PoolError::Closed`] error.
    ///
    /// If close isn't called then the objects will not be destroyed.
    pub async fn close(&self) -> PoolResult<(), M> {
        with_timeout(
            self.inner.config.close_timeout,
            self.inner
                .slots
                .close()
                .into_iter()
                .flatten()
                .map(|slot| {
                    // Spawning a task to ensure the object is destroyed even
                    // if this future is cancelled.
                    tokio::spawn(self.inner.clone().destroy_object_in_slot(slot))
                })
                .collect::<JoinSet<_>>()
                .join_all(),
        )
        .await?;
        Ok(())
    }
}

impl<M: Manager> PoolInner<M> {
    fn release(self: Arc<Self>, mut slot: OccupiedSlot<M::Object>) {
        // Spawning a task rather than having an async function to avoid having
        // a future where it is cancelled and the object is just dropped.
        tokio::spawn(async move {
            match with_timeout(self.config.reset_timeout, self.manager.reset(&mut slot)).await {
                Ok(Ok(_)) => {
                    if let Some(slot) = self.slots.try_release(slot) {
                        let _ = self.destroy_object_in_slot(slot).await;
                    }
                }
                Ok(Err(_)) | Err(_) => {
                    let _ = self.destroy_object_in_slot(slot).await;
                }
            }
        });
    }

    async fn create_pool_object_from_slot(
        self: &Arc<Self>,
        slot: Slot<M::Object>,
    ) -> PoolResult<PoolObject<M>, M> {
        match slot {
            Slot::Vacant(slot) => {
                let object = self.create_object().await?;
                Ok(PoolObject::new(slot.occupy(object), self.clone()))
            }
            Slot::Occupied(slot) => Ok(PoolObject::new(slot, self.clone())),
        }
    }

    async fn create_object(&self) -> PoolResult<M::Object, M> {
        with_timeout(self.config.create_timeout, self.manager.create())
            .await?
            .map_err(PoolError::ManagerError)
    }

    async fn destroy_object_in_slot(
        self: Arc<Self>,
        slot: OccupiedSlot<M::Object>,
    ) -> PoolResult<(), M> {
        let (object, _slot) = slot.vacate();
        self.destroy_object(object).await
    }

    async fn destroy_object(&self, object: M::Object) -> PoolResult<(), M> {
        with_timeout(self.config.destroy_timeout, self.manager.destroy(object))
            .await?
            .map_err(PoolError::ManagerError)
    }
}

async fn with_timeout<Output>(
    timeout: Option<Duration>,
    f: impl Future<Output = Output>,
) -> Result<Output, tokio::time::error::Elapsed> {
    if let Some(timeout) = timeout {
        tokio::time::timeout(timeout, f).await
    } else {
        Ok(f.await)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use googletest::prelude::*;

    use super::*;

    #[derive(Debug)]
    struct TestManager {
        counter: AtomicUsize,
        check_is_destroyed: bool,
        create_delay: Duration,
        reset_delay: Duration,
        destroy_delay: Duration,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestObject {
        counter: usize,
        reset_counter: usize,
        check_is_destroyed: bool,
        is_destroyed: bool,
    }

    impl Drop for TestObject {
        fn drop(&mut self) {
            if self.check_is_destroyed {
                assert_that!(self.is_destroyed, eq(true));
            }
        }
    }

    impl TestManager {
        fn new() -> Self {
            Self {
                counter: AtomicUsize::new(0),
                create_delay: Duration::from_millis(0),
                reset_delay: Duration::from_millis(0),
                destroy_delay: Duration::from_millis(0),
                check_is_destroyed: true,
            }
        }
    }

    impl Manager for TestManager {
        type Object = TestObject;
        type Error = &'static str;

        async fn create(&self) -> std::result::Result<Self::Object, Self::Error> {
            let counter = self.counter.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(self.create_delay).await;
            Ok(TestObject {
                counter,
                reset_counter: 0,
                check_is_destroyed: self.check_is_destroyed,
                is_destroyed: false,
            })
        }

        async fn destroy(&self, mut object: Self::Object) -> std::result::Result<(), Self::Error> {
            tokio::time::sleep(self.destroy_delay).await;
            object.is_destroyed = true;
            Ok(())
        }

        async fn reset(&self, object: &mut Self::Object) -> std::result::Result<(), Self::Error> {
            tokio::time::sleep(self.reset_delay).await;
            object.reset_counter += 1;
            Ok(())
        }

        async fn check_liveness(
            &self,
            _object: &Self::Object,
        ) -> std::result::Result<bool, Self::Error> {
            Ok(true)
        }
    }

    #[googletest::test]
    #[tokio::test]
    async fn pool_with_min_objects_created() {
        let config = PoolConfig {
            max_objects: 10,
            min_objects: Some(5),
            ..Default::default()
        };
        let manager = TestManager::new();

        let pool = Pool::new(manager, config).await.unwrap();

        let object = pool.acquire().await;
        expect_that!(*object.unwrap(), pat!(&TestObject { counter: eq(4) }));
        expect_that!(pool.close().await, ok(()));
    }

    #[googletest::test]
    #[tokio::test]
    async fn pool_with_max_one_object_can_be_acquired_and_is_available_again_after_drop() {
        let config = PoolConfig {
            max_objects: 1,
            ..Default::default()
        };
        let manager = TestManager::new();
        let pool = Pool::new(manager, config).await.unwrap();

        let object = pool.acquire().await.unwrap();
        expect_that!(*object, pat!(&TestObject { counter: eq(0) }));
        expect_that!(pool.try_acquire().await, ok(none()));

        drop(object);

        expect_that!(
            *pool.acquire().await.unwrap(),
            pat!(&TestObject { counter: eq(0) })
        );
        expect_that!(pool.close().await, ok(()));
    }

    #[googletest::test]
    #[tokio::test]
    async fn pool_create_times_out_if_manager_is_slow() {
        let config = PoolConfig {
            max_objects: 1,
            min_objects: Some(0),
            create_timeout: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        let mut manager = TestManager::new();
        manager.create_delay = Duration::from_millis(200);
        let pool = Pool::new(manager, config).await.unwrap();

        let timeout_error = pool.acquire().await;

        expect_that!(timeout_error, err(pat!(PoolError::Timeout)));
        expect_that!(pool.close().await, ok(()));
    }

    #[googletest::test]
    #[tokio::test]
    async fn pool_acquire_times_out_if_no_object_is_available() {
        let config = PoolConfig {
            max_objects: 1,
            acquire_timeout: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        let manager = TestManager::new();
        let pool = Pool::new(manager, config).await.unwrap();
        let held_object = pool.acquire().await;
        expect_that!(&held_object, ok(anything()));

        let timeout_error = pool.acquire().await;

        expect_that!(timeout_error, err(pat!(PoolError::Timeout)));

        drop(held_object);
        expect_that!(pool.close().await, ok(()));
    }

    #[googletest::test]
    #[tokio::test]
    async fn pool_object_is_reset_when_dropped() {
        let config = PoolConfig {
            max_objects: 1,
            ..Default::default()
        };
        let manager = TestManager::new();
        let pool = Pool::new(manager, config).await.unwrap();

        let object = pool.acquire().await.unwrap();
        expect_that!(
            *object,
            pat!(&TestObject {
                reset_counter: eq(0)
            })
        );

        drop(object);

        let object = pool.acquire().await.unwrap();
        expect_that!(
            *object,
            pat!(&TestObject {
                reset_counter: eq(1)
            })
        );

        drop(object);
        expect_that!(pool.close().await, ok(()));
    }

    #[googletest::test]
    #[tokio::test]
    async fn pool_object_when_reset_is_slow_it_times_out_and_not_released_to_pool() {
        let config = PoolConfig {
            max_objects: 1,
            reset_timeout: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        let mut manager = TestManager::new();
        manager.reset_delay = Duration::from_millis(200);
        let pool = Pool::new(manager, config).await.unwrap();

        let object = pool.acquire().await.unwrap();
        expect_that!(
            *object,
            pat!(&TestObject {
                counter: eq(0),
                reset_counter: eq(0)
            })
        );

        drop(object);

        let object = pool.acquire().await.unwrap();
        expect_that!(
            *object,
            pat!(&TestObject {
                counter: eq(1),
                reset_counter: eq(0)
            })
        );

        drop(object);
        expect_that!(pool.close().await, ok(()));
    }

    #[googletest::test]
    #[tokio::test]
    async fn pool_close_when_destroy_is_slow_it_times_out() {
        let config = PoolConfig {
            max_objects: 1,
            min_objects: Some(1),
            destroy_timeout: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        let mut manager = TestManager::new();
        manager.destroy_delay = Duration::from_millis(500);
        manager.check_is_destroyed = false;
        let pool = Pool::new(manager, config).await.unwrap();

        expect_that!(
            tokio::time::timeout(Duration::from_millis(200), pool.close()).await,
            ok(ok(()))
        );
    }

    #[googletest::test]
    #[tokio::test]
    async fn pool_close_when_object_not_dropped_times_out() {
        let config = PoolConfig {
            max_objects: 1,
            min_objects: Some(1),
            close_timeout: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        let mut manager = TestManager::new();
        manager.destroy_delay = Duration::from_millis(500);
        manager.check_is_destroyed = false;
        let pool = Pool::new(manager, config).await.unwrap();

        expect_that!(
            tokio::time::timeout(Duration::from_millis(200), pool.close()).await,
            ok(err(pat!(PoolError::Timeout)))
        );
    }

    #[googletest::test]
    #[tokio::test]
    async fn pool_drops_when_all_objects_and_pool_are_dropped() {
        let config = PoolConfig {
            max_objects: 1,
            ..Default::default()
        };
        let manager = TestManager::new();
        let pool = Pool::new(manager, config).await.unwrap();
        let pool_inner = Arc::downgrade(&pool.inner);
        let object = pool.acquire().await.unwrap();

        drop(pool);
        expect_that!(pool_inner.strong_count(), eq(1));
        expect_that!(pool_inner.upgrade().is_some(), eq(true));

        drop(object);
        // Wait for the object to be destroyed
        tokio::time::sleep(Duration::from_millis(100)).await;
        expect_that!(pool_inner.strong_count(), eq(0));
        expect_that!(pool_inner.upgrade().is_none(), eq(true));
    }
}
