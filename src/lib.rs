// Copyright (C) 2025-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

//! # sabi-redis
//!
//! This crate provides [sabi](https://crates.io/crates/sabi) data sources for Redis.
//! It supports standalone, Sentinel, and Cluster configurations, in both synchronous and
//! asynchronous (Tokio) versions.
//!
//! ## Usage
//!
//! ### Standalone Configuration
//!
//! To use this crate, add it to your `Cargo.toml` with the desired features:
//!
//! ```toml
//! [dependencies]
//! sabi-redis = { version = "0.1", features = ["standalone", "standalone-async"] }
//! ```
//!
//! #### Synchronous
//!
//! ```rust
//! #[cfg(feature = "standalone")]
//! mod standalone {
//!     use override_macro::{overridable, override_with};
//!     use sabi::{setup, uses, DataHub, DataAcc};
//!     use sabi_redis::{RedisDataSrc, RedisDataConn};
//!     use redis::TypedCommands;
//!
//!     uses!("redis", RedisDataSrc::new("redis://127.0.0.1:6379/0"));
//!
//!     #[overridable]
//!     pub trait MyData {
//!         fn set_name(&mut self, name: &str) -> errs::Result<()>;
//!     }
//!
//!     fn my_logic(data: &mut impl MyData) -> errs::Result<()> {
//!         data.set_name("Tom")
//!     }
//!
//!     #[overridable]
//!     pub trait RedisDataAcc: DataAcc {
//!         fn set_name(&mut self, name: &str) -> errs::Result<()> {
//!             let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
//!             let redis_conn = data_conn.get_connection();
//!             redis_conn.set("name", name)
//!                 .map_err(|e| errs::Err::with_source("Redis SET command failed", e))?;
//!             data_conn.add_force_back(|redis_conn| {
//!                 redis_conn.del("name")
//!                     .map_err(|e| errs::Err::with_source("Redis DEL command failed", e))?;
//!                 Ok(())
//!             });
//!             Ok(())
//!         }
//!     }
//!
//!     impl RedisDataAcc for DataHub {}
//!
//!     #[override_with(RedisDataAcc)]
//!     impl MyData for DataHub {}
//!
//!     fn my_app() -> errs::Result<()> {
//!         let _auto_shutdown = setup()?;
//!
//!         let mut hub = DataHub::new();
//!         hub.txn(my_logic)
//!     }
//! }
//! ```
//!
//! #### Asynchronous (Tokio)
//!
//! ```rust
//! #[cfg(feature = "standalone-async")]
//! mod standalone_async {
//!     use override_macro::{overridable, override_with};
//!     use sabi::tokio::{setup_async, uses, logic, DataHub, DataAcc};
//!     use sabi_redis::{RedisDataSrcAsync, RedisDataConnAsync};
//!     use redis::AsyncTypedCommands;
//!
//!     uses!("redis", RedisDataSrcAsync::new("redis://127.0.0.1:6379/0"));
//!
//!     #[overridable]
//!     pub trait MyDataAsync {
//!         async fn set_name_async(&mut self, name: &str) -> errs::Result<()>;
//!     }
//!
//!     async fn my_logic_async(data: &mut impl MyDataAsync) -> errs::Result<()> {
//!         data.set_name_async("Tom").await
//!     }
//!
//!     #[overridable]
//!     pub trait RedisDataAccAsync: DataAcc {
//!         async fn set_name_async(&mut self, name: &str) -> errs::Result<()> {
//!             let data_conn = self.get_data_conn_async::<RedisDataConnAsync>("redis").await?;
//!             let redis_conn = data_conn.get_connection();
//!             redis_conn.set("name", name).await
//!                 .map_err(|e| errs::Err::with_source("Redis SET command failed", e))?;
//!             data_conn.add_force_back_async(async |mut redis_conn| {
//!                 redis_conn.del("name").await
//!                     .map_err(|e| errs::Err::with_source("Redis DEL command failed", e))?;
//!                 Ok(())
//!             }).await;
//!             Ok(())
//!         }
//!     }
//!
//!     impl RedisDataAccAsync for DataHub {}
//!
//!     #[override_with(RedisDataAccAsync)]
//!     impl MyDataAsync for DataHub {}
//!
//!     async fn my_app() -> errs::Result<()> {
//!         let _auto_shutdown = setup_async().await?;
//!
//!         let mut hub = DataHub::new();
//!         hub.txn_async(logic!(my_logic_async)).await
//!     }
//! }
//! ```
//!
//! ### Pub/Sub
//!
//! This crate also provides a way to receive Redis Pub/Sub messages and process them within a
//! `sabi` transaction.
//!
//! #### Synchronous
//!
//! ```rust
//! #[cfg(feature = "standalone")]
//! mod standalone {
//!     use override_macro::{overridable, override_with};
//!     use sabi::{DataHub, DataAcc};
//!     use sabi_redis::{RedisPubSubSubscriber, RedisPubSubMsgDataSrc, RedisPubSubMsgDataConn};
//!     use redis::ControlFlow;
//!
//!     #[overridable]
//!     pub trait MyData {
//!         fn receive_msg(&mut self) -> errs::Result<String>;
//!     }
//!
//!     fn my_logic(data: &mut impl MyData) -> errs::Result<()> {
//!         let s = data.receive_msg()?;
//!         // ...process a message string
//!         Ok(())
//!     }
//!
//!     #[overridable]
//!     pub trait RedisPubSubDataAcc: DataAcc {
//!         fn receive_msg(&mut self) -> errs::Result<String> {
//!             let data_conn = self.get_data_conn::<RedisPubSubMsgDataConn>("redis:pubsub")?;
//!             let msg = data_conn.get_message();
//!             msg.get_payload::<String>()
//!                 .map_err(|e| errs::Err::with_source("Fail to get a Redis PubSub message", e))
//!         }
//!     }
//!
//!     impl RedisPubSubDataAcc for DataHub {}
//!
//!     #[override_with(RedisPubSubDataAcc)]
//!     impl MyData for DataHub {}
//!
//!     fn subscribe() -> errs::Result<()> {
//!         let mut subscriber = RedisPubSubSubscriber::new("redis://127.0.0.1:6379/0");
//!         subscriber.subscribe("my-channel");
//!         subscriber.receive(|msg| {
//!             let mut hub = DataHub::new();
//!             hub.uses("redis:pubsub", RedisPubSubMsgDataSrc::new(msg));
//!             hub.txn(my_logic).unwrap();
//!             ControlFlow::Continue
//!         })
//!     }
//! }
//! ```
//!
//! #### Asynchronous (Tokio)
//!
//! ```rust
//! #[cfg(feature = "standalone-async")]
//! mod standalone_async {
//!     use override_macro::{overridable, override_with};
//!     use sabi::tokio::{logic, DataHub, DataAcc};
//!     use sabi_redis::{
//!         RedisPubSubSubscriberAsync, RedisPubSubMsgDataSrcAsync, RedisPubSubMsgDataConnAsync,
//!     };
//!     use redis::ControlFlow;
//!
//!     #[overridable]
//!     pub trait MyDataAsync {
//!         async fn receive_msg_async(&mut self) -> errs::Result<String>;
//!     }
//!
//!     async fn my_logic_async(data: &mut impl MyDataAsync) -> errs::Result<()> {
//!         let s = data.receive_msg_async().await?;
//!         // ...process a message string
//!         Ok(())
//!     }
//!
//!     #[overridable]
//!     pub trait RedisPubSubDataAccAsync: DataAcc {
//!         async fn receive_msg_async(&mut self) -> errs::Result<String> {
//!             let data_conn = self.
//!                 get_data_conn_async::<RedisPubSubMsgDataConnAsync>("redis:pubsub").await?;
//!             let msg = data_conn.get_message();
//!             msg.get_payload::<String>()
//!                 .map_err(|e| errs::Err::with_source("Fail to get a Redis PubSub message", e))
//!         }
//!     }
//!
//!     impl RedisPubSubDataAccAsync for DataHub {}
//!
//!     #[override_with(RedisPubSubDataAccAsync)]
//!     impl MyDataAsync for DataHub {}
//!
//!     async fn subscribe_async() -> errs::Result<()> {
//!         let mut subscriber = RedisPubSubSubscriberAsync::new("redis://127.0.0.1:6379/0");
//!         subscriber.subscribe("my-channel");
//!         subscriber.receive_async(async |msg| {
//!             let mut hub = DataHub::new();
//!             hub.uses("redis:pubsub", RedisPubSubMsgDataSrcAsync::new(msg));
//!             hub.txn_async(logic!(my_logic_async)).await.unwrap();
//!             ControlFlow::Continue
//!         }).await
//!     }
//! }
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "standalone")]
mod standalone_std;

#[cfg(feature = "standalone")]
#[cfg_attr(docsrs, doc(cfg(feature = "standalone")))]
pub use standalone_std::{
    RedisDataConn, RedisDataSrc, RedisError, RedisPubSubSubscriber, RedisPubSubSubscriberError,
};

#[cfg(feature = "standalone-async")]
mod standalone_tokio;

#[cfg(feature = "standalone-async")]
#[cfg_attr(docsrs, doc(cfg(feature = "standalone-async")))]
pub use standalone_tokio::{
    RedisDataConnAsync, RedisDataSrcAsync, RedisErrorAsync, RedisPubSubSubscriberAsync,
    RedisPubSubSubscriberErrorAsync,
};

#[cfg(feature = "sentinel")]
mod sentinel_std;

#[cfg(feature = "sentinel-async")]
mod sentinel_tokio;

/// A module for Redis Sentinel.
///
/// This module provides data sources for Redis Sentinel configurations.
///
/// ## Usage
///
/// ### Sentinel Configuration
///
/// To use this crate, add it to your `Cargo.toml` with the desired features:
///
/// ```toml
/// [dependencies]
/// sabi-redis = { version = "0.1", features = ["sentinel", "sentinel-async"] }
/// ```
///
/// #### Synchronous
///
/// ```rust
/// #[cfg(feature = "sentinel")]
/// mod sentinel {
///     use override_macro::{overridable, override_with};
///     use sabi::{setup, uses, DataHub, DataAcc};
///     use sabi_redis::sentinel::{RedisDataSrc, RedisDataConn};
///     use redis::sentinel::SentinelServerType;
///     use redis::TypedCommands;
///
///     uses!(
///         "redis",
///         RedisDataSrc::new(
///             vec![
///                 "redis://127.0.0.1:26379/",
///                 "redis://127.0.0.1:26380/",
///                 "redis://127.0.0.1:26381/",
///             ],
///             "mymaster",
///             SentinelServerType::Master,
///         )
///     );
///
///     #[overridable]
///     pub trait MyData {
///         fn set_name(&mut self, name: &str) -> errs::Result<()>;
///     }
///
///     fn my_logic(data: &mut impl MyData) -> errs::Result<()> {
///         data.set_name("Tom")
///     }
///
///     #[overridable]
///     pub trait RedisDataAcc: DataAcc {
///         fn set_name(&mut self, name: &str) -> errs::Result<()> {
///             let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
///             let redis_conn = data_conn.get_connection();
///             redis_conn.set("name", name)
///                 .map_err(|e| errs::Err::with_source("Redis SET command failed", e))?;
///             data_conn.add_force_back(|redis_conn| {
///                 redis_conn.del("name")
///                     .map_err(|e| errs::Err::with_source("Redis DEL command failed", e))?;
///                 Ok(())
///             });
///             Ok(())
///         }
///     }
///
///     impl RedisDataAcc for DataHub {}
///
///     #[override_with(RedisDataAcc)]
///     impl MyData for DataHub {}
///
///     fn my_app() -> errs::Result<()> {
///         let _auto_shutdown = setup()?;
///
///         let mut hub = DataHub::new();
///         hub.txn(my_logic)
///     }
/// }
/// ```
///
/// #### Asynchronous (Tokio)
///
/// ```rust
/// #[cfg(feature = "sentinel-async")]
/// mod sentinel_async {
///     use override_macro::{overridable, override_with};
///     use sabi::tokio::{logic, setup_async, uses, DataHub, DataAcc};
///     use sabi_redis::sentinel::{RedisDataSrcAsync, RedisDataConnAsync};
///     use redis::sentinel::SentinelServerType;
///     use redis::AsyncTypedCommands;
///
///     uses!(
///         "redis",
///         RedisDataSrcAsync::new(
///             vec![
///                "redis://127.0.0.1:26379/",
///                "redis://127.0.0.1:26380/",
///                "redis://127.0.0.1:26381/",
///             ],
///             "mymaster",
///             SentinelServerType::Master,
///         )
///     );
///
///     #[overridable]
///     pub trait MyDataAsync {
///         async fn set_name_async(&mut self, name: &str) -> errs::Result<()>;
///     }
///
///     async fn my_logic_async(data: &mut impl MyDataAsync) -> errs::Result<()> {
///         data.set_name_async("Tom").await
///     }
///
///     #[overridable]
///     pub trait RedisDataAccAsync: DataAcc {
///         async fn set_name_async(&mut self, name: &str) -> errs::Result<()> {
///             let data_conn = self.get_data_conn_async::<RedisDataConnAsync>("redis").await?;
///             let redis_conn = data_conn.get_connection();
///             redis_conn.set("name", name).await
///                 .map_err(|e| errs::Err::with_source("Redis SET command failed", e))?;
///             data_conn.add_force_back_async(async |mut redis_conn| {
///                 redis_conn.del("name").await
///                     .map_err(|e| errs::Err::with_source("Redis DEL command failed", e))?;
///                 Ok(())
///             }).await;
///             Ok(())
///         }
///     }
///
///     impl RedisDataAccAsync for DataHub {}
///
///     #[override_with(RedisDataAccAsync)]
///     impl MyDataAsync for DataHub {}
///
///     async fn my_app() -> errs::Result<()> {
///         let _auto_shutdown = setup_async().await?;
///
///         let mut hub = DataHub::new();
///         hub.txn_async(logic!(my_logic_async)).await
///     }
/// }
/// ```
///
/// ### Pub/Sub
///
/// This crate also provides a way to receive Redis Pub/Sub messages and process them within a
/// `sabi` transaction.
///
/// #### Synchronous
///
/// ```rust
/// #[cfg(feature = "sentinel")]
/// mod sentinel {
///     use override_macro::{overridable, override_with};
///     use sabi::{DataHub, DataAcc};
///     use sabi_redis::{RedisPubSubMsgDataSrc, RedisPubSubMsgDataConn};
///     use sabi_redis::sentinel::RedisPubSubSubscriber;
///     use redis::{ControlFlow, sentinel::SentinelServerType};
///
///     #[overridable]
///     pub trait MyData {
///         fn receive_msg(&mut self) -> errs::Result<String>;
///     }
///
///     fn my_logic(data: &mut impl MyData) -> errs::Result<()> {
///         let s = data.receive_msg()?;
///         // ...process a message string
///         Ok(())
///     }
///
///     #[overridable]
///     pub trait RedisPubSubDataAcc: DataAcc {
///         fn receive_msg(&mut self) -> errs::Result<String> {
///             let data_conn = self.get_data_conn::<RedisPubSubMsgDataConn>("redis:pubsub")?;
///             let msg = data_conn.get_message();
///             msg.get_payload::<String>()
///                 .map_err(|e| errs::Err::with_source("Fail to get a Redis PubSub message", e))
///         }
///     }
///
///     impl RedisPubSubDataAcc for DataHub {}
///
///     #[override_with(RedisPubSubDataAcc)]
///     impl MyData for DataHub {}
///
///     fn subscribe() -> errs::Result<()> {
///         let mut subscriber = RedisPubSubSubscriber::new(
///             vec![
///                "redis://127.0.0.1:26379/",
///                "redis://127.0.0.1:26380/",
///                "redis://127.0.0.1:26381/",
///             ],
///             "mymaster",
///             SentinelServerType::Master,
///         );
///         subscriber.subscribe("my-channel");
///         subscriber.receive(|msg| {
///             let mut hub = DataHub::new();
///             hub.uses("redis:pubsub", RedisPubSubMsgDataSrc::new(msg));
///             hub.txn(my_logic).unwrap();
///             ControlFlow::Continue
///         })
///     }
/// }
/// ```
///
/// #### Asynchronous (Tokio)
///
/// ```rust
/// #[cfg(feature = "sentinel-async")]
/// mod sentinel_async {
///     use override_macro::{overridable, override_with};
///     use sabi::tokio::{logic, DataHub, DataAcc};
///     use sabi_redis::{RedisPubSubMsgDataSrcAsync, RedisPubSubMsgDataConnAsync};
///     use sabi_redis::sentinel::RedisPubSubSubscriberAsync;
///     use redis::{ControlFlow, sentinel::SentinelServerType};
///
///     #[overridable]
///     pub trait MyDataAsync {
///         async fn receive_msg_async(&mut self) -> errs::Result<String>;
///     }
///
///     async fn my_logic_async(data: &mut impl MyDataAsync) -> errs::Result<()> {
///         let s = data.receive_msg_async().await?;
///         // ...process a message string
///         Ok(())
///     }
///
///     #[overridable]
///     pub trait RedisPubSubDataAccAsync: DataAcc {
///         async fn receive_msg_async(&mut self) -> errs::Result<String> {
///             let data_conn = self.
///                 get_data_conn_async::<RedisPubSubMsgDataConnAsync>("redis:pubsub").await?;
///             let msg = data_conn.get_message();
///             msg.get_payload::<String>()
///                 .map_err(|e| errs::Err::with_source("Fail to get a Redis PubSub message", e))
///         }
///     }
///
///     impl RedisPubSubDataAccAsync for DataHub {}
///
///     #[override_with(RedisPubSubDataAccAsync)]
///     impl MyDataAsync for DataHub {}
///
///     async fn subscribe_async() -> errs::Result<()> {
///         let mut subscriber = RedisPubSubSubscriberAsync::new(
///             vec![
///                "redis://127.0.0.1:26379/",
///                "redis://127.0.0.1:26380/",
///                "redis://127.0.0.1:26381/",
///             ],
///             "mymaster",
///             SentinelServerType::Master,
///         );
///         subscriber.subscribe("my-channel");
///         subscriber.receive_async(async |msg| {
///             let mut hub = DataHub::new();
///             hub.uses("redis:pubsub", RedisPubSubMsgDataSrcAsync::new(msg));
///             hub.txn_async(logic!(my_logic_async)).await.unwrap();
///             ControlFlow::Continue
///         }).await
///     }
/// }
/// ```
#[cfg(any(feature = "sentinel", feature = "sentinel-async"))]
pub mod sentinel {
    #[cfg(feature = "sentinel")]
    #[cfg_attr(docsrs, doc(cfg(feature = "sentinel")))]
    pub use crate::sentinel_std::{
        RedisDataConn, RedisDataSrc, RedisError, RedisPubSubSubscriber, RedisPubSubSubscriberError,
    };

    #[cfg(feature = "sentinel-async")]
    #[cfg_attr(docsrs, doc(cfg(feature = "sentinel-async")))]
    pub use crate::sentinel_tokio::{
        RedisDataConnAsync, RedisDataSrcAsync, RedisErrorAsync, RedisPubSubSubscriberAsync,
        RedisPubSubSubscriberErrorAsync,
    };
}

#[cfg(feature = "cluster")]
mod cluster_std;

#[cfg(feature = "cluster-async")]
mod cluster_tokio;

/// A module for Redis Cluster.
///
/// This module provides data sources for Redis Cluster configurations.
///
/// ## Usage
///
/// ## Cluster Configuration
///
/// To use this crate, add it to your `Cargo.toml` with the desired features:
///
/// ```toml
/// [dependencies]
/// sabi-redis = { version = "0.1", features = ["cluster", "cluster-async"] }
/// ```
///
/// #### Synchronous
///
/// ```rust
/// #[cfg(feature = "cluster")]
/// mod cluster {
///     use override_macro::{overridable, override_with};
///     use sabi::{setup, uses, DataHub, DataAcc};
///     use sabi_redis::cluster::{RedisDataSrc, RedisDataConn};
///     use redis::TypedCommands;
///
///     uses!("redis", RedisDataSrc::new(
///         vec![
///             "redis://127.0.0.1:7000/",
///             "redis://127.0.0.1:7001/",
///             "redis://127.0.0.1:7002/",
///         ],
///     ));
///
///     #[overridable]
///     pub trait MyData {
///         fn set_name(&mut self, name: &str) -> errs::Result<()>;
///     }
///
///     fn my_logic(data: &mut impl MyData) -> errs::Result<()> {
///         data.set_name("Tom")
///     }
///
///     #[overridable]
///     pub trait RedisDataAcc: DataAcc {
///         fn set_name(&mut self, name: &str) -> errs::Result<()> {
///             let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
///             let redis_conn = data_conn.get_connection();
///             redis_conn.set("name", name)
///                 .map_err(|e| errs::Err::with_source("Redis SET command failed", e))?;
///             data_conn.add_force_back(|redis_conn| {
///                 redis_conn.del("name")
///                     .map_err(|e| errs::Err::with_source("Redis DEL command failed", e))?;
///                 Ok(())
///             });
///             Ok(())
///         }
///     }
///
///     impl RedisDataAcc for DataHub {}
///
///     #[override_with(RedisDataAcc)]
///     impl MyData for DataHub {}
///
///     fn my_app() -> errs::Result<()> {
///         let _auto_shutdown = setup()?;
///
///         let mut hub = DataHub::new();
///         hub.txn(my_logic)
///     }
/// }
/// ```
///
/// #### Asynchronous (Tokio)
///
/// ```rust
/// #[cfg(feature = "cluster-async")]
/// mod cluster_async {
///     use override_macro::{overridable, override_with};
///     use sabi::tokio::{setup_async, logic, uses, DataHub, DataAcc};
///     use sabi_redis::cluster::{RedisDataSrcAsync, RedisDataConnAsync};
///     use redis::AsyncTypedCommands;
///
///     uses!("redis", RedisDataSrcAsync::new(
///         vec![
///             "redis://127.0.0.1:7000/",
///             "redis://127.0.0.1:7001/",
///             "redis://127.0.0.1:7002/",
///         ]
///     ));
///
///     #[overridable]
///     pub trait MyDataAsync {
///         async fn set_name_async(&mut self, name: &str) -> errs::Result<()>;
///     }
///
///     async fn my_logic_async(data: &mut impl MyDataAsync) -> errs::Result<()> {
///         data.set_name_async("Tom").await
///     }
///
///     #[overridable]
///     pub trait RedisDataAccAsync: DataAcc {
///         async fn set_name_async(&mut self, name: &str) -> errs::Result<()> {
///             let data_conn = self.get_data_conn_async::<RedisDataConnAsync>("redis").await?;
///             let redis_conn = data_conn.get_connection();
///             redis_conn.set("name", name).await
///                 .map_err(|e| errs::Err::with_source("Redis SET command failed", e))?;
///             data_conn.add_force_back_async(async |mut redis_conn| {
///                 redis_conn.del("name").await
///                     .map_err(|e| errs::Err::with_source("Redis DEL command failed", e))?;
///                 Ok(())
///             }).await;
///             Ok(())
///         }
///     }
///
///     impl RedisDataAccAsync for DataHub {}
///
///     #[override_with(RedisDataAccAsync)]
///     impl MyDataAsync for DataHub {}
///
///     async fn my_app() -> errs::Result<()> {
///         let _auto_shutdown = setup_async().await?;
///
///         let mut hub = DataHub::new();
///         hub.txn_async(logic!(my_logic_async)).await
///     }
/// }
/// ```
///
/// ### Pub/Sub
///
/// This crate also provides a way to receive Redis Pub/Sub messages and process them within a
/// `sabi` transaction.
///
/// #### Synchronous
///
/// ```rust
/// #[cfg(feature = "cluster")]
/// mod cluster {
///     use override_macro::{overridable, override_with};
///     use sabi::{DataHub, DataAcc};
///     use sabi_redis::{RedisPubSubMsgDataSrc, RedisPubSubMsgDataConn};
///     use sabi_redis::cluster::RedisPubSubSubscriber;
///     use redis::ControlFlow;
///
///     #[overridable]
///     pub trait MyData {
///         fn receive_msg(&mut self) -> errs::Result<String>;
///     }
///
///     fn my_logic(data: &mut impl MyData) -> errs::Result<()> {
///         let s = data.receive_msg()?;
///         // ...process a message string
///         Ok(())
///     }
///
///     #[overridable]
///     pub trait RedisPubSubDataAcc: DataAcc {
///         fn receive_msg(&mut self) -> errs::Result<String> {
///             let data_conn = self.get_data_conn::<RedisPubSubMsgDataConn>("redis:pubsub")?;
///             let msg = data_conn.get_message();
///             msg.get_payload::<String>()
///                 .map_err(|e| errs::Err::with_source("Fail to get a Redis PubSub message", e))
///         }
///     }
///
///     impl RedisPubSubDataAcc for DataHub {}
///
///     #[override_with(RedisPubSubDataAcc)]
///     impl MyData for DataHub {}
///
///     fn subscribe() -> errs::Result<()> {
///         let mut subscriber = RedisPubSubSubscriber::new(
///             vec![
///                "redis://127.0.0.1:7000/",
///                "redis://127.0.0.1:7001/",
///                "redis://127.0.0.1:7002/",
///             ],
///         );
///         subscriber.subscribe("my-channel");
///         subscriber.receive(|msg| {
///             let mut hub = DataHub::new();
///             hub.uses("redis:pubsub", RedisPubSubMsgDataSrc::new(msg));
///             hub.txn(my_logic).unwrap();
///             ControlFlow::Continue
///         })
///     }
/// }
/// ```
///
/// #### Asynchronous (Tokio)
///
/// ```rust
/// #[cfg(feature = "cluster-async")]
/// mod cluster_async {
///     use override_macro::{overridable, override_with};
///     use sabi::tokio::{logic, DataHub, DataAcc};
///     use sabi_redis::{RedisPubSubMsgDataSrcAsync, RedisPubSubMsgDataConnAsync};
///     use sabi_redis::cluster::RedisPubSubSubscriberAsync;
///     use redis::ControlFlow;
///
///     #[overridable]
///     pub trait MyDataAsync {
///         async fn receive_msg_async(&mut self) -> errs::Result<String>;
///     }
///
///     async fn my_logic_async(data: &mut impl MyDataAsync) -> errs::Result<()> {
///         let s = data.receive_msg_async().await?;
///         // ...process a message string
///         Ok(())
///     }
///
///     #[overridable]
///     pub trait RedisPubSubDataAccAsync: DataAcc {
///         async fn receive_msg_async(&mut self) -> errs::Result<String> {
///             let data_conn = self.
///                 get_data_conn_async::<RedisPubSubMsgDataConnAsync>("redis:pubsub").await?;
///             let msg = data_conn.get_message();
///             msg.get_payload::<String>()
///                 .map_err(|e| errs::Err::with_source("Fail to get a Redis PubSub message", e))
///         }
///     }
///
///     impl RedisPubSubDataAccAsync for DataHub {}
///
///     #[override_with(RedisPubSubDataAccAsync)]
///     impl MyDataAsync for DataHub {}
///
///     async fn subscribe_async() -> errs::Result<()> {
///         let mut subscriber = RedisPubSubSubscriberAsync::new(
///             vec![
///                "redis://127.0.0.1:7000/",
///                "redis://127.0.0.1:7001/",
///                "redis://127.0.0.1:7002/",
///             ],
///         );
///         subscriber.subscribe("my-channel");
///         subscriber.receive_async(async |msg| {
///             let mut hub = DataHub::new();
///             hub.uses("redis:pubsub", RedisPubSubMsgDataSrcAsync::new(msg));
///             hub.txn_async(logic!(my_logic_async)).await.unwrap();
///             ControlFlow::Continue
///         }).await
///     }
/// }
/// ```
#[cfg(any(feature = "cluster", feature = "cluster-async"))]
pub mod cluster {
    #[cfg(feature = "cluster")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cluster")))]
    pub use crate::cluster_std::{
        RedisDataConn, RedisDataSrc, RedisError, RedisPubSubSubscriber, RedisPubSubSubscriberError,
    };

    #[cfg(feature = "cluster-async")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cluster-async")))]
    pub use crate::cluster_tokio::{
        RedisDataConnAsync, RedisDataSrcAsync, RedisErrorAsync, RedisPubSubSubscriberAsync,
        RedisPubSubSubscriberErrorAsync,
    };
}

#[cfg(any(feature = "standalone", feature = "sentinel", feature = "cluster"))]
mod pubsub_msg_std;

#[cfg(any(feature = "standalone", feature = "sentinel", feature = "cluster"))]
pub use pubsub_msg_std::{RedisPubSubMsgDataConn, RedisPubSubMsgDataSrc};

#[cfg(any(
    feature = "standalone-async",
    feature = "sentinel-async",
    feature = "cluster-async"
))]
mod pubsub_msg_tokio;

#[cfg(any(
    feature = "standalone-async",
    feature = "sentinel-async",
    feature = "cluster-async"
))]
pub use pubsub_msg_tokio::{RedisPubSubMsgDataConnAsync, RedisPubSubMsgDataSrcAsync};

#[cfg(any(feature = "standalone", feature = "sentinel", feature = "cluster"))]
mod retry;

#[cfg(any(
    feature = "standalone-async",
    feature = "sentinel-async",
    feature = "cluster-async"
))]
mod retry_async;
