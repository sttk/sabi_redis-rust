// Copyright (C) 2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

//! This crate provides several `DataSrc` and `DataConn` derived classes to enable data access to
//! Redis within the Rust sabi framework.
//!
//! `RedisDataSrc` and `RedisDataConn` are designed for a standalone Redis server and provide
//! synchronous connections for processing Redis commands.
//!
//! Redis does not support transactions like relational databases (RDBs) and lacks the ability to
//! roll back updated data. Therefore, when used in conjunction with other databases, an
//! inconsistency can occur if an error happens mid-process: the RDB's updates might be rolled
//! back, but the Redis updates remain. To address this, this crate offers three features to give
//! developers an opportunity to revert updates: *"force back"*, *"pre-commit"* and *"post-commit"*.
//!
//! #### Force Back
//!
//! The `DataConn` derived class provided by this crate is equipped with the `add_force_back`
//! method. You can use this method to store functions in the `DataConn` that will be executed
//! during the rollback process within `sabi::txn!`.
//!
//! This is useful for things like deleting newly added data or reverting data that is unlikely to
//! have concurrent updates, such as session data. For data that might have concurrent updates,
//! it would likely require measures like using `WATCH`, `MULTI`, and `EXEC`.
//!
//! #### Pre-Commit
//!
//! The `DataConn` derived class provided by this crate is equipped with the `add_pre_commit`
//! method. You can use this method to store functions in the `DataConn` that will be executed
//! right before the commit process within `sabi::txn!`.
//!
//! By performing Redis updates after all other database updates, you can avoid the need for a
//! rollback if an error occurs with the other databases. This is a good option if you can meet
//! the condition that the updated data will not be re-fetched within the same transaction.
//!
//! #### Post-Commit
//!
//! The `DataConn` derived class provided by this crate is equipped with the `add_post_commit`
//! method. You can use this method to store functions in the `DataConn` that will be executed
//! after the commit process within `sabi::txn!`.
//!
//! Since it's executed after the commit of the updates to other databases is complete, there's no
//! need to consider rolling back Redis updates even if an error occurs with the other database
//! updates. However, you must be aware that if an error happens during the Redis update itself,
//! the partial updates to Redis cannot be undone, and the other database updates will already be
//! committed. It would be necessary to ensure that the impact on the system is not critical if
//! such a situation occurs, and to enable error detection so that manual recovery can be performed
//! later.

mod standalone;
pub use standalone::{RedisDataConn, RedisDataSrc, RedisDataSrcError};
