// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use redis::sentinel::{
    SentinelClient, SentinelClientBuilder, SentinelNodeConnectionInfo, SentinelServerType,
};
use redis::{Connection, ControlFlow, IntoConnectionInfo, Msg, ToRedisArgs};
use std::fmt::Debug;

use super::retry::Retry;

/// Errors that can occur during Redis Pub/Sub operations in Sentinel mode.
#[derive(Debug)]
pub enum RedisPubSubSentinelError {
    /// The Sentinel configuration has already been consumed and is no longer available.
    SentinelConfigAlreadyConsumed,
    /// The Sentinel client builder has already been consumed and is no longer available.
    SentinelClientBuilderAlreadyConsumed,
    /// Failed to build a `SentinelClient` with the provided configuration.
    FailToBuildSentinelClient,
    /// Failed to establish a connection to the Redis server via Sentinel.
    FailToGetConnection,
    /// Failed to subscribe to the specified channels.
    FailToSubscribeToChannels,
    /// Failed to subscribe to the specified patterns.
    FailToSubscribeToChannelsWithPatterns,
    /// Failed to receive a message from the Redis server.
    FailToGetMessage,
}

/// A Redis Pub/Sub subscriber for Redis Sentinel.
///
/// This structure provides a way to subscribe to channels and patterns on a Redis setup
/// managed by Sentinel and process received messages. It automatically handles
/// failovers and includes built-in retry logic.
pub struct RedisPubSubSentinel<I, A>
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

impl<I, A> RedisPubSubSentinel<I, A>
where
    I: IntoConnectionInfo + Sized + Debug + Clone,
    A: ToRedisArgs,
{
    /// Creates a new `RedisPubSubSentinel` instance.
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

    /// Creates a new `RedisPubSubSentinel` with additional Sentinel client parameters.
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

    /// Creates a new `RedisPubSubSentinel` using an existing `SentinelClientBuilder`.
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

    /// Starts receiving messages and processes them with the provided closure.
    ///
    /// This method will block and continuously listen for messages. If a connection
    /// or subscription error occurs, it will attempt to reconnect based on the
    /// retry configuration.
    ///
    /// The closure `f` is called for each received message. It should return
    /// `ControlFlow::Continue` to keep listening or `ControlFlow::Break(value)`
    /// to stop and return the value.
    pub fn receive<F, U>(mut self, mut f: F) -> errs::Result<U>
    where
        F: FnMut(Msg) -> ControlFlow<U>,
    {
        let mut client = match &mut self.config {
            Config::Client(ref mut c) => {
                let cfg = c.take().ok_or_else(|| {
                    errs::Err::new(RedisPubSubSentinelError::SentinelConfigAlreadyConsumed)
                })?;
                SentinelClient::build(
                    cfg.addrs,
                    cfg.service_name,
                    cfg.node_conn_info,
                    cfg.server_type,
                )
                .map_err(|e| {
                    errs::Err::with_source(RedisPubSubSentinelError::FailToBuildSentinelClient, e)
                })
            }
            Config::Builder(ref mut b) => {
                let builder = b.take().ok_or_else(|| {
                    errs::Err::new(RedisPubSubSentinelError::SentinelClientBuilderAlreadyConsumed)
                })?;
                builder.build().map_err(|e| {
                    errs::Err::with_source(RedisPubSubSentinelError::FailToBuildSentinelClient, e)
                })
            }
        }?;

        loop {
            let mut conn: Connection = match client.get_connection() {
                Ok(c) => c,
                Err(e) => {
                    if self.retry.wait_with_backoff() {
                        continue;
                    }
                    return Err(errs::Err::with_source(
                        RedisPubSubSentinelError::FailToGetConnection,
                        e,
                    ));
                }
            };
            let mut pubsub = conn.as_pubsub();

            for c in self.channels.iter() {
                pubsub.subscribe(c).map_err(|e| {
                    errs::Err::with_source(RedisPubSubSentinelError::FailToSubscribeToChannels, e)
                })?;
            }

            for p in self.patterns.iter() {
                pubsub.psubscribe(p).map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubSentinelError::FailToSubscribeToChannelsWithPatterns,
                        e,
                    )
                })?;
            }

            loop {
                match pubsub.get_message() {
                    Ok(msg) => {
                        self.retry.reset();
                        if let ControlFlow::Break(value) = f(msg) {
                            return Ok(value);
                        }
                    }
                    Err(e) => {
                        if self.retry.wait_with_backoff() {
                            continue;
                        }
                        return Err(errs::Err::with_source(
                            RedisPubSubSentinelError::FailToGetMessage,
                            e,
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
    use crate::pubsub::{RedisPubSubDataConn, RedisPubSubDataSrc};
    use crate::sentinel_sync::{RedisSentinelDataConn, RedisSentinelDataSrc};
    use override_macro::{overridable, override_with};
    use redis::{ControlFlow, TypedCommands};
    use sabi::{DataAcc, DataHub};

    #[overridable]
    trait PublishData {
        fn say_greet(&mut self, s: &str) -> errs::Result<()>;
    }

    fn publish_logic(data: &mut impl PublishData) -> errs::Result<()> {
        data.say_greet("Hello")?;
        Ok(())
    }

    #[overridable]
    trait SubscribeData {
        fn receive_greet(&mut self) -> errs::Result<String>;
    }

    fn subscribe_logic(data: &mut impl SubscribeData) -> errs::Result<()> {
        assert_eq!(data.receive_greet()?, "Hello");
        Ok(())
    }

    #[overridable]
    trait RedisPubSubDataAcc: DataAcc {
        fn say_greet(&mut self, s: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisSentinelDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;
            std::thread::sleep(std::time::Duration::from_millis(100));
            conn.publish("channel-2", s).unwrap();
            Ok(())
        }

        fn receive_greet(&mut self) -> errs::Result<String> {
            let data_conn = self.get_data_conn::<RedisPubSubDataConn>("redis/pubsub")?;
            let msg = data_conn.get_message();
            let payload: String = msg.get_payload().unwrap();
            Ok(payload)
        }
    }
    impl RedisPubSubDataAcc for DataHub {}

    #[override_with(RedisPubSubDataAcc)]
    impl PublishData for DataHub {}

    #[override_with(RedisPubSubDataAcc)]
    impl SubscribeData for DataHub {}

    #[test]
    fn test() -> errs::Result<()> {
        // publish
        {
            let _ = std::thread::spawn(move || {
                let mut data = DataHub::new();
                data.uses(
                    "redis",
                    RedisSentinelDataSrc::new(
                        vec![
                            "redis://127.0.0.1:26479",
                            "redis://127.0.0.1:26480",
                            "redis://127.0.0.1:26481",
                        ],
                        "mymaster",
                    ),
                );
                data.run(publish_logic).unwrap();
            });
        }

        // subscribe
        {
            let mut pubsub = RedisPubSubSentinel::new(
                vec![
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
            );
            pubsub.subscribe("channel-2");
            let n = pubsub.receive(|msg| {
                let mut data = DataHub::new();
                data.uses("redis/pubsub", RedisPubSubDataSrc::new(msg));
                data.run(subscribe_logic).unwrap();
                ControlFlow::Break(1)
            })?;
            assert_eq!(n, 1);
        }
        Ok(())
    }
}
