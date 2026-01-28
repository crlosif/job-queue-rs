use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("job not found")]
    NotFound,

    #[error("invalid job state")]
    InvalidState,

    #[error("database error: {0}")]
    Database(String),

    #[error("internal error: {0}")]
    Internal(String),
}
