use crate::{Manager, slots::SlotsError};

pub type PoolResult<T, M> = Result<T, PoolError<M>>;

/// Errors that can occur when using a [`crate::Pool`].
#[derive(Debug, thiserror::Error)]
pub enum PoolError<M: Manager> {
    /// Error from the [`Manager`].
    #[error("Manager error: {0}")]
    ManagerError(M::Error),
    /// Invalid config.
    #[error("Invalid config: {0}")]
    InvalidConfig(&'static str),
    /// The pool is closed and no new objects can be acquired.
    #[error("Pool is closed")]
    Closed,
    /// A timeout occured when waiting for an object or interracting with the
    /// manager.
    #[error("Timeout")]
    Timeout,
}

impl<M: Manager> From<SlotsError> for PoolError<M> {
    fn from(error: SlotsError) -> Self {
        match error {
            SlotsError::Closed => PoolError::Closed,
        }
    }
}

impl<M: Manager> From<tokio::time::error::Elapsed> for PoolError<M> {
    fn from(_error: tokio::time::error::Elapsed) -> Self {
        PoolError::Timeout
    }
}
