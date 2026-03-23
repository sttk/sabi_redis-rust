// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use redis::ControlFlow;
use redis::{Client, Connection, IntoConnectionInfo, Msg, ToRedisArgs};
use std::fmt::Debug;

use super::retry::Retry;

/// Errors that can occur during Redis Pub/Sub operations in standalone mode.
#[derive(Debug)]
pub enum RedisPubSubError {
    /// The Redis server address has already been consumed and is no longer available.
    AddressAlreadyConsumed,
    /// Failed to open a Redis client with the provided address.
    FailToOpenClient,
    /// Failed to establish a connection to the Redis server.
    FailToGetConnection,
    /// Failed to subscribe to the specified channels.
    FailToSubscribeToChannels,
    /// Failed to subscribe to the specified patterns.
    FailToSubscribeToChannelsWithPatterns,
    /// Failed to receive a message from the Redis server.
    FailToGetMessage,
}

/// A Redis Pub/Sub subscriber for standalone Redis instances.
///
/// This structure provides a way to subscribe to channels and patterns on a standalone
/// Redis server and process received messages. It includes built-in retry logic
/// for connection and subscription failures.
pub struct RedisPubSub<I, A>
where
    I: IntoConnectionInfo + Sized + Debug + Clone,
    A: ToRedisArgs,
{
    addr: Option<I>,
    channels: Vec<A>,
    patterns: Vec<A>,
    retry: Retry,
}

impl<I, A> RedisPubSub<I, A>
where
    I: IntoConnectionInfo + Sized + Debug + Clone,
    A: ToRedisArgs,
{
    /// Creates a new `RedisPubSub` instance for the given Redis address.
    pub fn new(addr: I) -> Self {
        Self {
            addr: Some(addr),
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
        let addr = self
            .addr
            .take()
            .ok_or_else(|| errs::Err::new(RedisPubSubError::AddressAlreadyConsumed))?;
        let client = Client::open(addr)
            .map_err(|e| errs::Err::with_source(RedisPubSubError::FailToOpenClient, e))?;

        loop {
            let mut conn: Connection = match client.get_connection() {
                Ok(c) => c,
                Err(e) => {
                    if self.retry.wait_with_backoff() {
                        continue;
                    }
                    return Err(errs::Err::with_source(
                        RedisPubSubError::FailToGetConnection,
                        e,
                    ));
                }
            };
            let mut pubsub = conn.as_pubsub();

            for c in self.channels.iter() {
                pubsub.subscribe(c).map_err(|e| {
                    errs::Err::with_source(RedisPubSubError::FailToSubscribeToChannels, e)
                })?;
            }

            for p in self.patterns.iter() {
                pubsub.psubscribe(p).map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubError::FailToSubscribeToChannelsWithPatterns,
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
                            RedisPubSubError::FailToGetMessage,
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
    use crate::standalone_sync::{RedisDataConn, RedisDataSrc};
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
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;
            std::thread::sleep(std::time::Duration::from_millis(100));
            conn.publish("channel-1", s).unwrap();
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
                data.uses("redis", RedisDataSrc::new("redis://127.0.0.1/"));
                data.run(publish_logic).unwrap();
            });
        }

        // subscribe
        {
            let mut pubsub = RedisPubSub::new("redis://127.0.0.1/");
            pubsub.subscribe("channel-1");
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
