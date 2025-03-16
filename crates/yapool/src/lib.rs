#![deny(missing_docs)]
#![doc = include_str!("../../../README.md")]

mod manager;
mod pool;
mod slots;

pub use manager::Manager;
pub use pool::{Pool, PoolConfig, PoolError, PoolObject};
