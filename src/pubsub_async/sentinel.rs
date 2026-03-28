// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use super::retry::Retry;
use futures::{stream::StreamExt, Future};
use redis::sentinel::{
    SentinelClient, SentinelClientBuilder, SentinelNodeConnectionInfo, SentinelServerType,
};
use redis::{ControlFlow, IntoConnectionInfo, Msg, ToRedisArgs};
use std::fmt::Debug;

/// Errors that can occur during Redis Pub/Sub operations in async Sentinel mode.
#[derive(Debug)]
pub enum RedisPubSubSentinelAsyncError {
    /// The Sentinel configuration has already been consumed and is no longer available.
    SentinelConfigAlreadyConsumed,
    /// The Sentinel client builder has already been consumed and is no longer available.
    SentinelClientBuilderAlreadyConsumed,
    /// Failed to build a `SentinelClient` with the provided configuration.
    FailToBuildSentinelClient,
    /// Failed to establish an async connection to the Redis server via Sentinel.
    FailToGetClientOfServerType,
    /// Failed to establish an async Pub/Sub connection to the Redis server.
    FailToGetAsyncPubSub,
    /// Failed to subscribe to the specified channels.
    FailToSubscribeToChannels,
    /// Failed to subscribe to the specified patterns.
    FailToSubscribeToChannelsWithPatterns,
    /// Failed to receive a message from the Redis server.
    FailToGetMessage,
}

/// An async Redis Pub/Sub subscriber for Redis Sentinel.
///
/// This structure provides a way to subscribe to channels and patterns on a Redis setup
/// managed by Sentinel and process received messages asynchronously. It automatically
/// handles failovers and includes built-in retry logic.
pub struct RedisPubSubSentinelAsync<I, A>
where
    I: IntoConnectionInfo + Sized + Debug + Clone,
    A: ToRedisArgs,
{
    config: Config<I>,
    channels: Vec<A>,
    patterns: Vec<A>,
    retry: Retry,
}

enum Config<I>
where
    I: IntoConnectionInfo + Sized + Debug + Clone,
{
    Client(Option<Box<SentinelConfig<I>>>),
    Builder(Option<Box<SentinelClientBuilder>>),
}

struct SentinelConfig<I> {
    addrs: Vec<I>,
    service_name: String,
    node_conn_info: Option<SentinelNodeConnectionInfo>,
    server_type: SentinelServerType,
}

impl<I, A> RedisPubSubSentinelAsync<I, A>
where
    I: IntoConnectionInfo + Sized + Debug + Clone,
    A: ToRedisArgs,
{
    /// Creates a new `RedisPubSubSentinelAsync` instance.
    ///
    /// # Arguments
    ///
    /// * `addrs` - A list of Redis Sentinel addresses.
    /// * `service_name` - The name of the Redis service to monitor.
    pub fn new(addrs: Vec<I>, service_name: impl AsRef<str>) -> Self {
        Self {
            config: Config::Client(Some(Box::new(SentinelConfig {
                addrs,
                service_name: service_name.as_ref().to_string(),
                node_conn_info: None,
                server_type: SentinelServerType::Master,
            }))),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: Retry::new(),
        }
    }

    /// Creates a new `RedisPubSubSentinelAsync` with additional Sentinel client parameters.
    pub fn with_client_params(
        addrs: Vec<I>,
        service_name: impl AsRef<str>,
        node_conn_info: SentinelNodeConnectionInfo,
        server_type: SentinelServerType,
    ) -> Self {
        Self {
            config: Config::Client(Some(Box::new(SentinelConfig {
                addrs,
                service_name: service_name.as_ref().to_string(),
                node_conn_info: Some(node_conn_info),
                server_type,
            }))),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: Retry::new(),
        }
    }

    /// Creates a new `RedisPubSubSentinelAsync` using an existing `SentinelClientBuilder`.
    pub fn with_client_builder(client_builder: SentinelClientBuilder) -> Self {
        Self {
            config: Config::Builder(Some(Box::new(client_builder))),
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
    /// This method will continuously listen for messages. If a connection
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
        let mut sentinel_client = match &mut self.config {
            Config::Client(ref mut c) => {
                let cfg = c.take().ok_or_else(|| {
                    errs::Err::new(RedisPubSubSentinelAsyncError::SentinelConfigAlreadyConsumed)
                })?;
                SentinelClient::build(
                    cfg.addrs,
                    cfg.service_name,
                    cfg.node_conn_info,
                    cfg.server_type,
                )
                .map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubSentinelAsyncError::FailToBuildSentinelClient,
                        e,
                    )
                })
            }
            Config::Builder(ref mut b) => {
                let builder = b.take().ok_or_else(|| {
                    errs::Err::new(
                        RedisPubSubSentinelAsyncError::SentinelClientBuilderAlreadyConsumed,
                    )
                })?;
                builder.build().map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubSentinelAsyncError::FailToBuildSentinelClient,
                        e,
                    )
                })
            }
        }?;

        loop {
            let client = match sentinel_client.async_get_client().await {
                Ok(c) => c,
                Err(e) => {
                    if self.retry.wait_with_backoff_async().await {
                        continue;
                    }
                    return Err(errs::Err::with_source(
                        RedisPubSubSentinelAsyncError::FailToGetClientOfServerType,
                        e,
                    ));
                }
            };
            let pubsub = match client.get_async_pubsub().await {
                Ok(pubsub) => pubsub,
                Err(e) => {
                    if self.retry.wait_with_backoff_async().await {
                        continue;
                    }
                    return Err(errs::Err::with_source(
                        RedisPubSubSentinelAsyncError::FailToGetAsyncPubSub,
                        e,
                    ));
                }
            };
            let (mut sink, mut stream) = pubsub.split();

            for c in self.channels.iter() {
                sink.subscribe(c).await.map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubSentinelAsyncError::FailToSubscribeToChannels,
                        e,
                    )
                })?;
            }

            for p in self.patterns.iter() {
                sink.psubscribe(p).await.map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubSentinelAsyncError::FailToSubscribeToChannelsWithPatterns,
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
                            RedisPubSubSentinelAsyncError::FailToGetMessage,
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
    use crate::pubsub::{RedisPubSubMsgAsyncDataConn, RedisPubSubMsgAsyncDataSrc};
    use crate::sentinel_async::{RedisSentinelAsyncDataConn, RedisSentinelAsyncDataSrc};
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
                .get_data_conn_async::<RedisSentinelAsyncDataConn>("redis")
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
                    RedisSentinelAsyncDataSrc::new(
                        vec![
                            "redis://127.0.0.1:26479",
                            "redis://127.0.0.1:26480",
                            "redis://127.0.0.1:26481",
                        ],
                        "mymaster",
                    ),
                );
                data.run_async(logic!(publish_logic_async)).await.unwrap();
            });
        }

        // subscribe
        {
            let mut pubsub = RedisPubSubSentinelAsync::new(
                vec![
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
            );
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
