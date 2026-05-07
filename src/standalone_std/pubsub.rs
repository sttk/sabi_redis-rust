// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use redis::{Client, Connection, ConnectionAddr, ConnectionInfo, ControlFlow, Msg, ToRedisArgs};
use std::fmt::Debug;

use crate::retry::Retry;

/// Errors related to Redis Pub/Sub subscriber for standalone environment.
#[derive(Debug)]
pub enum RedisPubSubSubscriberError {
    /// Indicates that the connection address has already been used and cannot be reused.
    AddressAlreadyConsumed,
    /// Failed to open a Redis client with the specified address string.
    FailToOpenClientOfAddr {
        /// The Redis connection address string.
        addr: String,
    },
    /// Failed to open a Redis client with the specified `ConnectionAddr`.
    FailToOpenClientOfConnAddr {
        /// The Redis `ConnectionAddr`.
        conn_addr: ConnectionAddr,
    },
    /// Failed to open a Redis client with the specified `ConnectionInfo`.
    FailToOpenClientOfConnInfo {
        /// The Redis `ConnectionInfo`.
        conn_info: ConnectionInfo,
    },
    /// Failed to get a connection from the client.
    FailToGetConnection {
        /// The Redis `ConnectionInfo`.
        conn_info: ConnectionInfo,
    },
    /// Failed to subscribe to the specified channels.
    FailToSubscribeToChannels {
        /// The Redis `ConnectionInfo`.
        conn_info: ConnectionInfo,
    },
    /// Failed to subscribe to the specified patterns.
    FailToSubscribeToChannelsWithPatterns {
        /// The Redis `ConnectionInfo`.
        conn_info: ConnectionInfo,
    },
    /// Failed to receive a message from the subscriber.
    FailToGetMessage {
        /// The Redis `ConnectionInfo`.
        conn_info: ConnectionInfo,
    },
}

/// A struct for subscribing to Redis channels and receiving messages for standalone environment.
pub struct RedisPubSubSubscriber<A>
where
    A: ToRedisArgs,
{
    addr: Option<RedisConfig>,
    channels: Vec<A>,
    patterns: Vec<A>,
    retry: Retry,
}

enum RedisConfig {
    String(String),
    ConnAddr(ConnectionAddr),
    ConnInfo(ConnectionInfo),
}

impl<A> RedisPubSubSubscriber<A>
where
    A: ToRedisArgs,
{
    /// Creates a new `RedisPubSubSubscriber` with a connection address string.
    ///
    /// # Arguments
    ///
    /// * `addr` - A string slice that holds the Redis connection address.
    ///
    /// # Returns
    ///
    /// A new instance of `RedisPubSubSubscriber`.
    pub fn new<S>(addr: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            addr: Some(RedisConfig::String(addr.as_ref().to_string())),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: Retry::new(),
        }
    }

    /// Creates a new `RedisPubSubSubscriber` with a `ConnectionAddr`.
    ///
    /// # Arguments
    ///
    /// * `conn_addr` - A Redis `ConnectionAddr`.
    ///
    /// # Returns
    ///
    /// A new instance of `RedisPubSubSubscriber`.
    pub fn with_conn_addr(conn_addr: ConnectionAddr) -> Self {
        Self {
            addr: Some(RedisConfig::ConnAddr(conn_addr)),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: Retry::new(),
        }
    }

    /// Creates a new `RedisPubSubSubscriber` with a `ConnectionInfo`.
    ///
    /// # Arguments
    ///
    /// * `conn_info` - A Redis `ConnectionInfo`.
    ///
    /// # Returns
    ///
    /// A new instance of `RedisPubSubSubscriber`.
    pub fn with_conn_info(conn_info: ConnectionInfo) -> Self {
        Self {
            addr: Some(RedisConfig::ConnInfo(conn_info)),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: Retry::new(),
        }
    }

    /// Sets the retry configuration for the subscriber.
    ///
    /// # Arguments
    ///
    /// * `max_count` - The maximum number of retry attempts.
    /// * `init_delay_ms` - The initial delay between retries in milliseconds.
    /// * `max_delay_ms` - The maximum delay between retries in milliseconds.
    pub fn set_retry(&mut self, max_count: u32, init_delay_ms: u64, max_delay_ms: u64) {
        self.retry = Retry::with_params(max_count, init_delay_ms, max_delay_ms);
    }

    /// Adds a channel to subscribe to.
    ///
    /// # Arguments
    ///
    /// * `channels` - The channel to subscribe to.
    pub fn subscribe(&mut self, channels: A) {
        self.channels.push(channels);
    }

    /// Adds a pattern to subscribe to.
    ///
    /// # Arguments
    ///
    /// * `patterns` - The pattern to subscribe to.
    pub fn psubscribe(&mut self, patterns: A) {
        self.patterns.push(patterns);
    }

    /// Starts receiving messages and calls the provided callback for each message.
    ///
    /// This method blocks until the callback returns `ControlFlow::Break`.
    ///
    /// # Arguments
    ///
    /// * `f` - A callback function that takes a `redis::Msg` and returns a `redis::ControlFlow`.
    ///
    /// # Returns
    ///
    /// A result containing the value returned by `ControlFlow::Break`, or an error.
    pub fn receive<F, U>(mut self, mut f: F) -> errs::Result<U>
    where
        F: FnMut(Msg) -> ControlFlow<U>,
    {
        let addr = self
            .addr
            .take()
            .ok_or_else(|| errs::Err::new(RedisPubSubSubscriberError::AddressAlreadyConsumed))?;
        let client = match addr {
            RedisConfig::String(addr) => Client::open(addr.as_str()).map_err(|e| {
                errs::Err::with_source(
                    RedisPubSubSubscriberError::FailToOpenClientOfAddr { addr },
                    e,
                )
            })?,
            RedisConfig::ConnAddr(conn_addr) => Client::open(conn_addr.clone()).map_err(|e| {
                errs::Err::with_source(
                    RedisPubSubSubscriberError::FailToOpenClientOfConnAddr { conn_addr },
                    e,
                )
            })?,
            RedisConfig::ConnInfo(conn_info) => Client::open(conn_info.clone()).map_err(|e| {
                errs::Err::with_source(
                    RedisPubSubSubscriberError::FailToOpenClientOfConnInfo { conn_info },
                    e,
                )
            })?,
        };

        loop {
            let mut conn: Connection = match client.get_connection() {
                Ok(c) => c,
                Err(e) => {
                    if self.retry.wait_with_backoff() {
                        continue;
                    }
                    return Err(errs::Err::with_source(
                        RedisPubSubSubscriberError::FailToGetConnection {
                            conn_info: client.get_connection_info().clone(),
                        },
                        e,
                    ));
                }
            };
            let mut pubsub = conn.as_pubsub();

            for c in self.channels.iter() {
                pubsub.subscribe(c).map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubSubscriberError::FailToSubscribeToChannels {
                            conn_info: client.get_connection_info().clone(),
                        },
                        e,
                    )
                })?;
            }

            for p in self.patterns.iter() {
                pubsub.psubscribe(p).map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubSubscriberError::FailToSubscribeToChannelsWithPatterns {
                            conn_info: client.get_connection_info().clone(),
                        },
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
                            RedisPubSubSubscriberError::FailToGetMessage {
                                conn_info: client.get_connection_info().clone(),
                            },
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
    use crate::RedisDataSrc;
    use redis::TypedCommands;
    use sabi::{AsyncGroup, DataSrc};
    use url::Url;

    fn publish(s: &str) {
        let s = s.to_string();
        let _ = std::thread::spawn(move || {
            let mut ds = RedisDataSrc::new("redis://127.0.0.1/0");
            let mut ag = AsyncGroup::new();
            ds.setup(&mut ag).unwrap();
            let mut dc = ds.create_data_conn().unwrap();
            let conn = dc.get_connection();
            std::thread::sleep(std::time::Duration::from_millis(100));
            conn.publish("channel-1", s).unwrap();
        });
    }

    mod test_new {
        use super::*;

        #[test]
        fn addr_is_str_and_ok() {
            publish("Hello");

            let mut subscriber = RedisPubSubSubscriber::new("redis://127.0.0.1/0");

            subscriber.subscribe("channel-1");
            subscriber
                .receive(|msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .unwrap();
        }

        #[test]
        fn addr_is_str_and_fail() {
            let mut subscriber = RedisPubSubSubscriber::new("xxxx");

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberError::FailToOpenClientOfAddr { addr }) =
                err.reason::<RedisPubSubSubscriberError>()
            else {
                panic!();
            };
            assert_eq!(addr, "xxxx");
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(
                format!("{src:?}"),
                "Redis URL did not parse - InvalidClientConfig"
            );
        }

        #[test]
        fn addr_is_string_and_ok() {
            publish("Hello");

            let mut subscriber = RedisPubSubSubscriber::new("redis://127.0.0.1/0".to_string());

            subscriber.subscribe("channel-1");
            subscriber
                .receive(|msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .unwrap();
        }

        #[test]
        fn addr_is_string_and_fail() {
            let mut subscriber = RedisPubSubSubscriber::new("xxxx".to_string());

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberError::FailToOpenClientOfAddr { addr }) =
                err.reason::<RedisPubSubSubscriberError>()
            else {
                panic!();
            };
            assert_eq!(addr, "xxxx");
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(
                format!("{src:?}"),
                "Redis URL did not parse - InvalidClientConfig"
            );
        }

        #[test]
        fn addr_is_url_and_ok() {
            publish("Hello");

            let Ok(url) = Url::parse("redis://127.0.0.1:6379/0") else {
                panic!("bad url");
            };
            let mut subscriber = RedisPubSubSubscriber::new(url);

            subscriber.subscribe("channel-1");
            subscriber
                .receive(|msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .unwrap();
        }

        #[test]
        fn addr_is_url_and_fail() {
            let Ok(url) = Url::parse("redis://xxxx") else {
                panic!("bad url");
            };
            let mut subscriber = RedisPubSubSubscriber::new(url);

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberError::FailToGetConnection { conn_info }) =
                err.reason::<RedisPubSubSubscriberError>()
            else {
                panic!();
            };
            #[cfg(target_os = "linux")]
            assert_eq!(format!("{conn_info:?}"), "ConnectionInfo { addr: Tcp(\"xxxx\", 6379), tcp_settings: TcpSettings { nodelay: false, keepalive: None, user_timeout: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(not(target_os = "linux"))]
            assert_eq!(format!("{conn_info:?}"), "ConnectionInfo { addr: Tcp(\"xxxx\", 6379), tcp_settings: TcpSettings { nodelay: false, keepalive: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            let Some(src) = err.source() else {
                panic!();
            };
            #[cfg(target_os = "linux")]
            assert_eq!(
                format!("{src:?}"),
                "failed to lookup address information: Temporary failure in name resolution"
            );
            #[cfg(not(target_os = "linux"))]
            assert_eq!(format!("{src:?}"), "failed to lookup address information: nodename nor servname provided, or not known");
        }
    }

    mod test_with_conn_addr {
        use super::*;

        #[test]
        fn ok() {
            publish("Hello");

            let conn_addr = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 6379);
            let mut subscriber = RedisPubSubSubscriber::with_conn_addr(conn_addr);

            subscriber.subscribe("channel-1");
            subscriber
                .receive(|msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .unwrap();
        }

        #[test]
        fn fail() {
            let conn_addr = redis::ConnectionAddr::Tcp("xxxx".to_string(), 6379);
            let mut subscriber = RedisPubSubSubscriber::with_conn_addr(conn_addr);

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberError::FailToGetConnection { conn_info }) =
                err.reason::<RedisPubSubSubscriberError>()
            else {
                panic!();
            };
            #[cfg(target_os = "linux")]
            assert_eq!(format!("{conn_info:?}"), "ConnectionInfo { addr: Tcp(\"xxxx\", 6379), tcp_settings: TcpSettings { nodelay: false, keepalive: None, user_timeout: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(not(target_os = "linux"))]
            assert_eq!(format!("{conn_info:?}"), "ConnectionInfo { addr: Tcp(\"xxxx\", 6379), tcp_settings: TcpSettings { nodelay: false, keepalive: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            let Some(src) = err.source() else {
                panic!();
            };
            #[cfg(target_os = "linux")]
            assert_eq!(
                format!("{src:?}"),
                "failed to lookup address information: Temporary failure in name resolution"
            );
            #[cfg(not(target_os = "linux"))]
            assert_eq!(format!("{src:?}"), "failed to lookup address information: nodename nor servname provided, or not known");
        }
    }

    mod test_with_conn_info {
        use super::*;
        use redis::IntoConnectionInfo;

        #[test]
        fn ok() {
            publish("Hello");

            let conn_info = "redis://127.0.0.1:6379/0".into_connection_info().unwrap();
            let mut subscriber = RedisPubSubSubscriber::with_conn_info(conn_info);

            subscriber.subscribe("channel-1");
            subscriber
                .receive(|msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .unwrap();
        }

        #[test]
        fn fail() {
            let conn_info = "redis://xxxx".into_connection_info().unwrap();
            let mut subscriber = RedisPubSubSubscriber::with_conn_info(conn_info);

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberError::FailToGetConnection { conn_info }) =
                err.reason::<RedisPubSubSubscriberError>()
            else {
                panic!();
            };
            #[cfg(target_os = "linux")]
            assert_eq!(format!("{conn_info:?}"), "ConnectionInfo { addr: Tcp(\"xxxx\", 6379), tcp_settings: TcpSettings { nodelay: false, keepalive: None, user_timeout: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(not(target_os = "linux"))]
            assert_eq!(format!("{conn_info:?}"), "ConnectionInfo { addr: Tcp(\"xxxx\", 6379), tcp_settings: TcpSettings { nodelay: false, keepalive: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            let Some(src) = err.source() else {
                panic!();
            };
            #[cfg(target_os = "linux")]
            assert_eq!(
                format!("{src:?}"),
                "failed to lookup address information: Temporary failure in name resolution"
            );
            #[cfg(not(target_os = "linux"))]
            assert_eq!(format!("{src:?}"), "failed to lookup address information: nodename nor servname provided, or not known");
        }
    }

    mod subscribe {
        use super::*;
        use redis::IntoConnectionInfo;

        #[test]
        fn ok() {
            publish("Hello");

            let conn_info = "redis://127.0.0.1:6379/0".into_connection_info().unwrap();
            let mut subscriber = RedisPubSubSubscriber::with_conn_info(conn_info);

            subscriber.subscribe("channel-1");
            subscriber
                .receive(|msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .unwrap();
        }
    }

    mod psubscribe {
        use super::*;
        use redis::IntoConnectionInfo;

        #[test]
        fn ok() {
            publish("Hello");

            let conn_info = "redis://127.0.0.1:6379/0".into_connection_info().unwrap();
            let mut subscriber = RedisPubSubSubscriber::with_conn_info(conn_info);

            subscriber.psubscribe("channel-*");
            subscriber
                .receive(|msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .unwrap();
        }
    }
}
