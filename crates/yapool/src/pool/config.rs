use std::time::Duration;

use crate::Manager;

use super::error::{PoolError, PoolResult};

/// Configuration for the pool.
///
/// ```
/// # use yapool::PoolConfig;
/// # use std::time::Duration;
/// let mut config = PoolConfig::default();
/// config.min_objects = Some(5);
/// ```
#[non_exhaustive]
pub struct PoolConfig {
    /// The maximum number of objects in the pool, must be greater than 0.
    pub max_objects: usize,
    /// The minimum number of objects in the pool, must be less than or equal
    /// to `max_objects`. The pool will try to keep at least this number of
    /// objects alive and will create them when creating the pool.
    pub min_objects: Option<usize>,
    /// The timeout for acquiring an object from the pool.
    pub acquire_timeout: Option<Duration>,
    /// The timeout for creating an object.
    pub create_timeout: Option<Duration>,
    /// The timeout for resetting an object.
    pub reset_timeout: Option<Duration>,
    /// The timeout for destroying an object.
    pub destroy_timeout: Option<Duration>,
    /// The timeout for closing the pool.
    pub close_timeout: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_objects: 10,
            min_objects: None,
            acquire_timeout: None,
            create_timeout: None,
            reset_timeout: None,
            destroy_timeout: None,
            close_timeout: None,
        }
    }
}

pub struct ValidPoolConfig {
    pub max_objects: usize,
    pub min_objects: usize,
    pub acquire_timeout: Option<Duration>,
    pub create_timeout: Option<Duration>,
    pub reset_timeout: Option<Duration>,
    pub destroy_timeout: Option<Duration>,
    pub close_timeout: Option<Duration>,
}

impl PoolConfig {
    pub(crate) fn validate<M: Manager>(self) -> PoolResult<ValidPoolConfig, M> {
        if self.max_objects == 0 {
            return Err(PoolError::InvalidConfig(
                "max_objects must be greater than 0",
            ));
        }

        if self
            .min_objects
            .map(|min| min > self.max_objects)
            .unwrap_or(false)
        {
            return Err(PoolError::InvalidConfig(
                "min_objects must be less than or equal to max_objects",
            ));
        }

        Ok(ValidPoolConfig {
            max_objects: self.max_objects,
            min_objects: self.min_objects.unwrap_or(self.max_objects / 2),
            acquire_timeout: self.acquire_timeout,
            create_timeout: self.create_timeout,
            reset_timeout: self.reset_timeout,
            destroy_timeout: self.destroy_timeout,
            close_timeout: self.close_timeout,
        })
    }
}
