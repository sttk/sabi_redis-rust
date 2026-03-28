// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use super::retry::Retry;
use futures::{stream::StreamExt, Future};
use redis::aio::PubSub;
use redis::{Client, ControlFlow, IntoConnectionInfo, Msg, ToRedisArgs};
use std::fmt::Debug;
use std::mem;

/// Errors that can occur during Redis Pub/Sub operations in async Cluster mode.
#[derive(Debug)]
pub enum RedisPubSubClusterAsyncError {
    /// Failed to open a Redis client with the provided address.
    FailToOpenClient,
    /// Failed to establish an async Pub/Sub connection to a Redis Cluster node.
    FailToGetAsyncPubSub,
    /// Failed to subscribe to the specified channels.
    FailToSubscribeToChannels,
    /// Failed to subscribe to the specified patterns.
    FailToSubscribeToChannelsWithPatterns,
    /// Failed to receive a message from the Redis Cluster.
    FailToGetMessage,
}

/// An async Redis Pub/Sub subscriber for Redis Cluster.
///
/// This structure provides a way to subscribe to channels and patterns on a Redis Cluster
/// and process received messages asynchronously. It attempts to connect to nodes in the
/// cluster and includes built-in retry logic.
pub struct RedisPubSubClusterAsync<I, A>
where
    I: IntoConnectionInfo + Sized + Debug + Clone,
    A: ToRedisArgs,
{
    addrs: Vec<I>,
    channels: Vec<A>,
    patterns: Vec<A>,
    retry: Retry,
}

impl<I, A> RedisPubSubClusterAsync<I, A>
where
    I: IntoConnectionInfo + Sized + Debug + Clone,
    A: ToRedisArgs,
{
    /// Creates a new `RedisPubSubClusterAsync` instance for the given cluster addresses.
    pub fn new(addrs: Vec<I>) -> Self {
        Self {
            addrs,
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: Retry::new(),
        }
    }

    /// Sets the retry parameters for connection and subscription failures.
    ///
    /// # Arguments
    ///
    /// * `max_count` - The maximum number of retry attempts.
    /// * `init_delay_ms` - The initial delay between retries in milliseconds.
    /// * `max_delay_ms` - The maximum delay between retries in milliseconds.
    pub fn set_retry(&mut self, max_count: u32, init_delay_ms: u64, max_delay_ms: u64) {
        self.retry = Retry::with_params(max_count, init_delay_ms, max_delay_ms);
    }

    /// Subscribes to a channel.
    pub fn subscribe(&mut self, channels: A) {
        self.channels.push(channels);
    }

    /// Subscribes to a pattern.
    pub fn psubscribe(&mut self, patterns: A) {
        self.patterns.push(patterns);
    }

    /// Starts receiving messages asynchronously and processes them with the provided closure.
    ///
    /// This method will continuously listen for messages. It will iterate
    /// through the provided cluster nodes to find an available one. If a connection
    /// or subscription error occurs, it will attempt to reconnect based on the
    /// retry configuration.
    ///
    /// The closure `f` is called for each received message. It should return
    /// a future that resolves to `ControlFlow::Continue` to keep listening or
    /// `ControlFlow::Break(value)` to stop and return the value.
    pub async fn receive_async<F, Fut, U>(mut self, mut f: F) -> errs::Result<U>
    where
        F: FnMut(Msg) -> Fut,
        Fut: Future<Output = ControlFlow<U>>,
    {
        let mut current_node_index = 0;
        let nodes = mem::take(&mut self.addrs);

        loop {
            let addr = &nodes[current_node_index];
            current_node_index = (current_node_index + 1) % nodes.len();

            let client = Client::open(addr.clone()).map_err(|e| {
                errs::Err::with_source(RedisPubSubClusterAsyncError::FailToOpenClient, e)
            })?;

            let pubsub: PubSub = match client.get_async_pubsub().await {
                Ok(pubsub) => pubsub,
                Err(e) => {
                    if self.retry.wait_with_backoff_async().await {
                        continue;
                    }
                    return Err(errs::Err::with_source(
                        RedisPubSubClusterAsyncError::FailToGetAsyncPubSub,
                        e,
                    ));
                }
            };
            let (mut sink, mut stream) = pubsub.split();

            for c in self.channels.iter() {
                sink.subscribe(c).await.map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubClusterAsyncError::FailToSubscribeToChannels,
                        e,
                    )
                })?;
            }

            for p in self.patterns.iter() {
                sink.psubscribe(p).await.map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubClusterAsyncError::FailToSubscribeToChannelsWithPatterns,
                        e,
                    )
                })?;
            }

            loop {
                match stream.next().await {
                    Some(msg) => {
                        self.retry.reset();
                        if let ControlFlow::Break(value) = f(msg).await {
                            return Ok(value);
                        }
                    }
                    None => {
                        if self.retry.wait_with_backoff_async().await {
                            continue;
                        }
                        return Err(errs::Err::new(
                            RedisPubSubClusterAsyncError::FailToGetMessage,
                        ));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::cluster_async::{RedisClusterAsyncDataConn, RedisClusterAsyncDataSrc};
    use crate::pubsub::{RedisPubSubMsgAsyncDataConn, RedisPubSubMsgAsyncDataSrc};
    use override_macro::{overridable, override_with};
    use redis::{AsyncTypedCommands, ControlFlow};
    use sabi::tokio::{logic, DataAcc, DataHub};
    use tokio::time;

    #[overridable]
    trait PublishData {
        async fn say_greet_async(&mut self, s: &str) -> errs::Result<()>;
    }

    async fn publish_logic_async(data: &mut impl PublishData) -> errs::Result<()> {
        data.say_greet_async("Hello").await?;
        Ok(())
    }

    #[overridable]
    trait SubscribeData {
        async fn receive_greet_async(&mut self) -> errs::Result<String>;
    }

    async fn subscribe_logic_async(data: &mut impl SubscribeData) -> errs::Result<()> {
        assert_eq!(data.receive_greet_async().await?, "Hello");
        Ok(())
    }

    #[overridable]
    trait RedisPubSubAsyncDataAcc: DataAcc {
        async fn say_greet_async(&mut self, s: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisClusterAsyncDataConn>("redis")
                .await?;
            let conn = data_conn.get_connection();
            time::sleep(time::Duration::from_millis(100)).await;
            conn.publish("channel-1", s).await.unwrap();
            Ok(())
        }

        async fn receive_greet_async(&mut self) -> errs::Result<String> {
            let data_conn = self
                .get_data_conn_async::<RedisPubSubMsgAsyncDataConn>("redis/pubsub")
                .await?;
            let msg = data_conn.get_message();
            let payload: String = msg.get_payload().unwrap();
            Ok(payload)
        }
    }
    impl RedisPubSubAsyncDataAcc for DataHub {}

    #[override_with(RedisPubSubAsyncDataAcc)]
    impl PublishData for DataHub {}

    #[override_with(RedisPubSubAsyncDataAcc)]
    impl SubscribeData for DataHub {}

    #[tokio::test]
    async fn test() {
        // publish
        {
            let _ = tokio::spawn(async {
                let mut data = DataHub::new();
                data.uses(
                    "redis",
                    RedisClusterAsyncDataSrc::new(vec![
                        "redis://127.0.0.1:7000/",
                        "redis://127.0.0.1:7001/",
                        "redis://127.0.0.1:7002/",
                    ]),
                );
                data.run_async(logic!(publish_logic_async)).await.unwrap();
            });
        }
        // subscribe
        {
            let mut pubsub = RedisPubSubClusterAsync::new(vec![
                "redis://127.0.0.1:7000/",
                "redis://127.0.0.1:7001/",
                "redis://127.0.0.1:7002/",
            ]);
            pubsub.subscribe("channel-1");
            let n = pubsub
                .receive_async(async |msg| {
                    let mut data = DataHub::new();
                    data.uses("redis/pubsub", RedisPubSubMsgAsyncDataSrc::new(msg));
                    data.run_async(logic!(subscribe_logic_async)).await.unwrap();
                    ControlFlow::Break(1)
                })
                .await
                .unwrap();
            assert_eq!(n, 1);
        }
    }
}
