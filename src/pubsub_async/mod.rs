// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

#[cfg(any(
    feature = "standalone-async",
    feature = "sentinel-async",
    feature = "cluster-async"
))]
mod retry;

#[cfg(feature = "standalone-async")]
mod standalone;
#[cfg(feature = "standalone-async")]
pub use standalone::{RedisPubSubAsync, RedisPubSubAsyncError};

#[cfg(feature = "sentinel-async")]
mod sentinel;
#[cfg(feature = "sentinel-async")]
pub use sentinel::{RedisPubSubSentinelAsync, RedisPubSubSentinelAsyncError};

#[cfg(feature = "cluster-async")]
mod cluster;
#[cfg(feature = "cluster-async")]
pub use cluster::{RedisPubSubClusterAsync, RedisPubSubClusterAsyncError};

use redis::Msg;
use sabi::tokio::{AsyncGroup, DataConn, DataSrc};
use std::sync::Arc;

/// A data connection that carries a Redis Pub/Sub message for async operations.
///
/// This structure allows a received Pub/Sub message to be passed through the `sabi` data access
/// layer in an asynchronous context. It implements `DataConn`, enabling it to be retrieved by a
/// data access object.
pub struct RedisPubSubMsgAsyncDataConn {
    msg: Arc<Msg>,
}

impl RedisPubSubMsgAsyncDataConn {
    fn new(msg: Arc<Msg>) -> Self {
        Self { msg }
    }

    /// Returns a reference to the contained Redis Pub/Sub message.
    pub fn get_message(&self) -> &Msg {
        &self.msg
    }
}

impl DataConn for RedisPubSubMsgAsyncDataConn {
    async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }
    async fn rollback_async(&mut self, _ag: &mut AsyncGroup) {}
    fn close(&mut self) {}
}

/// A data source that wraps a Redis Pub/Sub message for async operations.
///
/// This structure is used to register a single received message into the `sabi` framework
/// so that it can be accessed via `RedisPubSubMsgAsyncDataConn` during an async data operation.
///
/// # Examples
///
/// ```rust,ignore
/// use sabi_redis::pubsub::{RedisPubSubMsgAsyncDataSrc, RedisPubSubMsgAsyncDataConn};
/// use sabi::tokio::DataHub;
/// use futures::StreamExt;
///
/// # async fn example() {
/// // Assume `msg` is a `redis::Msg` received from an async subscriber.
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let (mut sink, mut stream) = client.get_async_pubsub().await.unwrap().split();
/// # sink.subscribe("channel").await.unwrap();
/// # let msg = stream.next().await.unwrap();
///
/// let mut data = DataHub::new();
/// data.uses("redis/pubsub", RedisPubSubMsgAsyncDataSrc::new(msg));
///
/// data.run_async(|acc: &mut DataHub| async move {
///     let conn = acc.get_data_conn_async::<RedisPubSubMsgAsyncDataConn>("redis/pubsub").await?;
///     let message = conn.get_message();
///     // Process the message...
///     Ok(())
/// }).await.unwrap();
/// # }
/// ```
pub struct RedisPubSubMsgAsyncDataSrc {
    msg: Arc<Msg>,
}

impl RedisPubSubMsgAsyncDataSrc {
    /// Creates a new `RedisPubSubMsgAsyncDataSrc` with the given Redis message.
    pub fn new(msg: Msg) -> Self {
        Self { msg: Arc::new(msg) }
    }
}

impl DataSrc<RedisPubSubMsgAsyncDataConn> for RedisPubSubMsgAsyncDataSrc {
    async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    fn close(&mut self) {}

    async fn create_data_conn_async(&mut self) -> errs::Result<Box<RedisPubSubMsgAsyncDataConn>> {
        let msg = Arc::clone(&self.msg);
        Ok(Box::new(RedisPubSubMsgAsyncDataConn::new(msg)))
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use deadpool_redis::{Config, Runtime};
    use futures::StreamExt;
    use override_macro::{overridable, override_with};
    use redis::AsyncTypedCommands;
    use sabi::tokio::{logic, DataAcc, DataHub};
    use tokio::time;

    async fn sample_logic(data: &mut impl SampleAsyncData1) -> errs::Result<()> {
        match data.greet_async().await.unwrap().as_ref() {
            "Hello" => Ok(()),
            s => panic!("{}", s),
        }
    }

    #[overridable]
    trait SampleAsyncDataAcc1: DataAcc {
        async fn greet_async(&mut self) -> errs::Result<String> {
            let data_conn = self
                .get_data_conn_async::<RedisPubSubMsgAsyncDataConn>("redis/pubsub")
                .await?;
            let msg = data_conn.get_message();
            let payload: String = msg.get_payload().unwrap();
            Ok(payload)
        }
    }
    impl SampleAsyncDataAcc1 for DataHub {}

    #[overridable]
    trait SampleAsyncData1 {
        async fn greet_async(&mut self) -> errs::Result<String>;
    }
    #[override_with(SampleAsyncDataAcc1)]
    impl SampleAsyncData1 for DataHub {}

    #[tokio::test]
    async fn test() {
        // client/publish
        let handle = {
            let cfg = Config::from_url("redis://127.0.0.1/");
            let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

            let handle = tokio::spawn(async move {
                let mut conn = pool.get().await.unwrap();
                time::sleep(time::Duration::from_millis(100)).await;
                conn.publish("channel_1", "Hello".to_string())
                    .await
                    .unwrap();
            });

            handle
        };

        // server/subscribe
        {
            let client = redis::Client::open("redis://127.0.0.1/").unwrap();
            let (mut sink, mut stream) = client.get_async_pubsub().await.unwrap().split();
            sink.subscribe("channel_1").await.unwrap();

            loop {
                let msg = stream.next().await.unwrap();

                let mut data = DataHub::new();
                data.uses("redis/pubsub", RedisPubSubMsgAsyncDataSrc::new(msg));
                if data.run_async(logic!(sample_logic)).await.is_ok() {
                    break;
                }
            }
        }

        handle.await.unwrap();
    }
}
