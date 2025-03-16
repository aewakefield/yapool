use tokio::sync::AcquireError;

pub type SlotsResult<T> = Result<T, SlotsError>;

#[derive(Debug, thiserror::Error)]
pub enum SlotsError {
    #[error("The slots are closed")]
    Closed,
}

impl From<AcquireError> for SlotsError {
    fn from(_error: AcquireError) -> Self {
        SlotsError::Closed
    }
}
