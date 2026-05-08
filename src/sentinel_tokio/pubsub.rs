// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use futures::{stream::StreamExt, Future};
use redis::sentinel::{
    SentinelClient, SentinelClientBuilder, SentinelNodeConnectionInfo, SentinelServerType,
};
use redis::{ConnectionAddr, ConnectionInfo, ControlFlow, Msg, ToRedisArgs};
use std::fmt::Debug;

use crate::retry_async::RetryAsync;

/// Errors related to asynchronous Redis Pub/Sub subscriber for Sentinel configuration.
#[derive(Debug)]
pub enum RedisPubSubSubscriberErrorAsync {
    /// Indicates that the Sentinel configuration has already been used and cannot be reused.
    SentinelConfigAlreadyConsumed,
    /// Failed to build a Sentinel client with the specified address strings.
    FailToBuildSentinelClientOfAddrs {
        /// The Sentinel connection address strings.
        addrs: Vec<String>,
        /// The service name of the Redis master.
        service_name: String,
        /// The type of Redis server (Master or Slave).
        server_type: SentinelServerType,
    },
    /// Failed to build a Sentinel client with the specified `ConnectionAddr`s.
    FailToBuildSentinelClientOfConnAddrs {
        /// The Sentinel `ConnectionAddr`s.
        conn_addrs: Vec<ConnectionAddr>,
        /// The service name of the Redis master.
        service_name: String,
        /// The type of Redis server (Master or Slave).
        server_type: SentinelServerType,
    },
    /// Failed to build a Sentinel client with the specified `ConnectionInfo`s.
    FailToBuildSentinelClientOfConnInfos {
        /// The Sentinel `ConnectionInfo`s.
        conn_infos: Vec<ConnectionInfo>,
        /// The service name of the Redis master.
        service_name: String,
        /// The type of Redis server (Master or Slave).
        server_type: SentinelServerType,
    },
    /// Failed to build a Sentinel client with the specified `SentinelClientBuilder`.
    FailToBuildSentinelClientWithClientBuilder,
    /// Failed to get a client of the specified server type.
    FailToGetClientOfServerType,
    /// Failed to get an asynchronous Pub/Sub connection.
    FailToGetAsyncPubSub,
    /// Failed to subscribe to the specified channels.
    FailToSubscribeToChannels,
    /// Failed to subscribe to the specified patterns.
    FailToSubscribeToChannelsWithPatterns,
    /// Failed to receive a message from the subscriber.
    FailToGetMessage,
}

/// A struct for subscribing to Redis channels and receiving messages asynchronously for Sentinel configuration.
pub struct RedisPubSubSubscriberAsync<A> {
    config: Option<SentinelConfig>,
    channels: Vec<A>,
    patterns: Vec<A>,
    retry: RetryAsync,
}

enum SentinelConfig {
    String(Box<ClientConfig<String>>),
    ConnAddr(Box<ClientConfig<ConnectionAddr>>),
    ConnInfo(Box<ClientConfig<ConnectionInfo>>),
    ClientBuilder(Box<SentinelClientBuilder>),
}

struct ClientConfig<T> {
    addrs: Vec<T>,
    service_name: String,
    node_conn_info: Option<SentinelNodeConnectionInfo>,
    server_type: SentinelServerType,
}

impl<A> RedisPubSubSubscriberAsync<A>
where
    A: ToRedisArgs,
{
    /// Creates a new `RedisPubSubSubscriberAsync` with Sentinel address strings.
    ///
    /// # Arguments
    ///
    /// * `addrs` - An iterator of string slices that hold the Sentinel connection addresses.
    /// * `service_name` - The service name of the Redis master.
    /// * `server_type` - The type of Redis server (Master or Slave).
    ///
    /// # Returns
    ///
    /// A new instance of `RedisPubSubSubscriberAsync`.
    pub fn new<I, S>(addrs: I, service_name: S, server_type: SentinelServerType) -> Self
    where
        I: IntoIterator<Item: AsRef<str>>,
        S: AsRef<str>,
    {
        Self {
            config: Some(SentinelConfig::String(Box::new(ClientConfig {
                addrs: addrs.into_iter().map(|s| s.as_ref().to_string()).collect(),
                service_name: service_name.as_ref().to_string(),
                node_conn_info: None,
                server_type,
            }))),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: RetryAsync::new(),
        }
    }

    /// Creates a new `RedisPubSubSubscriberAsync` with Sentinel address strings and node connection info.
    ///
    /// # Arguments
    ///
    /// * `addrs` - An iterator of string slices that hold the Sentinel connection addresses.
    /// * `service_name` - The service name of the Redis master.
    /// * `server_type` - The type of Redis server (Master or Slave).
    /// * `node_conn_info` - The connection info for the Redis node.
    ///
    /// # Returns
    ///
    /// A new instance of `RedisPubSubSubscriberAsync`.
    pub fn with_node_conn_info<I, S>(
        addrs: I,
        service_name: S,
        server_type: SentinelServerType,
        node_conn_info: SentinelNodeConnectionInfo,
    ) -> Self
    where
        I: IntoIterator<Item: AsRef<str>>,
        S: AsRef<str>,
    {
        Self {
            config: Some(SentinelConfig::String(Box::new(ClientConfig {
                addrs: addrs.into_iter().map(|s| s.as_ref().to_string()).collect(),
                service_name: service_name.as_ref().to_string(),
                node_conn_info: Some(node_conn_info),
                server_type,
            }))),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: RetryAsync::new(),
        }
    }

    /// Creates a new `RedisPubSubSubscriberAsync` with Sentinel `ConnectionAddr`s.
    ///
    /// # Arguments
    ///
    /// * `conn_addrs` - An iterator of `ConnectionAddr`s.
    /// * `service_name` - The service name of the Redis master.
    /// * `server_type` - The type of Redis server (Master or Slave).
    ///
    /// # Returns
    ///
    /// A new instance of `RedisPubSubSubscriberAsync`.
    pub fn with_conn_addrs<I, S>(
        conn_addrs: I,
        service_name: S,
        server_type: SentinelServerType,
    ) -> Self
    where
        I: IntoIterator<Item = ConnectionAddr>,
        S: AsRef<str>,
    {
        Self {
            config: Some(SentinelConfig::ConnAddr(Box::new(ClientConfig {
                addrs: conn_addrs.into_iter().collect(),
                service_name: service_name.as_ref().to_string(),
                node_conn_info: None,
                server_type,
            }))),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: RetryAsync::new(),
        }
    }

    /// Creates a new `RedisPubSubSubscriberAsync` with Sentinel `ConnectionAddr`s and node connection info.
    ///
    /// # Arguments
    ///
    /// * `conn_addrs` - An iterator of `ConnectionAddr`s.
    /// * `service_name` - The service name of the Redis master.
    /// * `server_type` - The type of Redis server (Master or Slave).
    /// * `node_conn_info` - The connection info for the Redis node.
    ///
    /// # Returns
    ///
    /// A new instance of `RedisPubSubSubscriberAsync`.
    pub fn with_conn_addrs_and_node_conn_info<I, S>(
        conn_addrs: I,
        service_name: S,
        server_type: SentinelServerType,
        node_conn_info: SentinelNodeConnectionInfo,
    ) -> Self
    where
        I: IntoIterator<Item = ConnectionAddr>,
        S: AsRef<str>,
    {
        Self {
            config: Some(SentinelConfig::ConnAddr(Box::new(ClientConfig {
                addrs: conn_addrs.into_iter().collect(),
                service_name: service_name.as_ref().to_string(),
                node_conn_info: Some(node_conn_info),
                server_type,
            }))),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: RetryAsync::new(),
        }
    }

    /// Creates a new `RedisPubSubSubscriberAsync` with Sentinel `ConnectionInfo`s.
    ///
    /// # Arguments
    ///
    /// * `conn_infos` - An iterator of `ConnectionInfo`s.
    /// * `service_name` - The service name of the Redis master.
    /// * `server_type` - The type of Redis server (Master or Slave).
    ///
    /// # Returns
    ///
    /// A new instance of `RedisPubSubSubscriberAsync`.
    pub fn with_conn_infos<I, S>(
        conn_infos: I,
        service_name: S,
        server_type: SentinelServerType,
    ) -> Self
    where
        I: IntoIterator<Item = ConnectionInfo>,
        S: AsRef<str>,
    {
        Self {
            config: Some(SentinelConfig::ConnInfo(Box::new(ClientConfig {
                addrs: conn_infos.into_iter().collect(),
                service_name: service_name.as_ref().to_string(),
                node_conn_info: None,
                server_type,
            }))),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: RetryAsync::new(),
        }
    }

    /// Creates a new `RedisPubSubSubscriberAsync` with Sentinel `ConnectionInfo`s and node connection info.
    ///
    /// # Arguments
    ///
    /// * `conn_infos` - An iterator of `ConnectionInfo`s.
    /// * `service_name` - The service name of the Redis master.
    /// * `server_type` - The type of Redis server (Master or Slave).
    /// * `node_conn_info` - The connection info for the Redis node.
    ///
    /// # Returns
    ///
    /// A new instance of `RedisPubSubSubscriberAsync`.
    pub fn with_conn_infos_and_node_conn_info<I, S>(
        conn_infos: I,
        service_name: S,
        server_type: SentinelServerType,
        node_conn_info: SentinelNodeConnectionInfo,
    ) -> Self
    where
        I: IntoIterator<Item = ConnectionInfo>,
        S: AsRef<str>,
    {
        Self {
            config: Some(SentinelConfig::ConnInfo(Box::new(ClientConfig {
                addrs: conn_infos.into_iter().collect(),
                service_name: service_name.as_ref().to_string(),
                node_conn_info: Some(node_conn_info),
                server_type,
            }))),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: RetryAsync::new(),
        }
    }

    /// Creates a new `RedisPubSubSubscriberAsync` with a `SentinelClientBuilder`.
    ///
    /// # Arguments
    ///
    /// * `client_builder` - A `redis::sentinel::SentinelClientBuilder`.
    ///
    /// # Returns
    ///
    /// A new instance of `RedisPubSubSubscriberAsync`.
    pub fn with_client_builder(client_builder: SentinelClientBuilder) -> Self {
        Self {
            config: Some(SentinelConfig::ClientBuilder(Box::new(client_builder))),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: RetryAsync::new(),
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
        self.retry = RetryAsync::with_params(max_count, init_delay_ms, max_delay_ms);
    }

    /// Adds a channel to subscribe to.
    ///
    /// # Arguments
    ///
    /// * `channel` - The channel to subscribe to.
    pub fn subscribe(&mut self, channel: A) {
        self.channels.push(channel);
    }

    /// Adds a pattern to subscribe to.
    ///
    /// # Arguments
    ///
    /// * `pattern` - The pattern to subscribe to.
    pub fn psubscribe(&mut self, pattern: A) {
        self.patterns.push(pattern);
    }

    /// Starts receiving messages asynchronously and calls the provided callback for each message.
    ///
    /// # Arguments
    ///
    /// * `f` - A callback function that takes a `redis::Msg` and returns a `Future` that resolves to a `redis::ControlFlow`.
    ///
    /// # Returns
    ///
    /// A result containing the value returned by `ControlFlow::Break`, or an error.
    pub async fn receive_async<F, Fut, U>(mut self, mut f: F) -> errs::Result<U>
    where
        F: FnMut(Msg) -> Fut,
        Fut: Future<Output = ControlFlow<U>>,
    {
        let cfg = self.config.take().ok_or_else(|| {
            errs::Err::new(RedisPubSubSubscriberErrorAsync::SentinelConfigAlreadyConsumed)
        })?;
        let mut client = match cfg {
            SentinelConfig::String(boxed_cfg) => {
                let cfg = *boxed_cfg;
                SentinelClient::build(
                    cfg.addrs.clone(),
                    cfg.service_name.clone(),
                    cfg.node_conn_info,
                    cfg.server_type.clone(),
                )
                .map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubSubscriberErrorAsync::FailToBuildSentinelClientOfAddrs {
                            addrs: cfg.addrs,
                            service_name: cfg.service_name,
                            server_type: cfg.server_type,
                        },
                        e,
                    )
                })?
            }
            SentinelConfig::ConnAddr(boxed_cfg) => {
                let cfg = *boxed_cfg;
                SentinelClient::build(
                    cfg.addrs.clone(),
                    cfg.service_name.clone(),
                    cfg.node_conn_info,
                    cfg.server_type.clone(),
                )
                .map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubSubscriberErrorAsync::FailToBuildSentinelClientOfConnAddrs {
                            conn_addrs: cfg.addrs,
                            service_name: cfg.service_name,
                            server_type: cfg.server_type,
                        },
                        e,
                    )
                })?
            }
            SentinelConfig::ConnInfo(boxed_cfg) => {
                let cfg = *boxed_cfg;
                SentinelClient::build(
                    cfg.addrs.clone(),
                    cfg.service_name.clone(),
                    cfg.node_conn_info,
                    cfg.server_type.clone(),
                )
                .map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubSubscriberErrorAsync::FailToBuildSentinelClientOfConnInfos {
                            conn_infos: cfg.addrs,
                            service_name: cfg.service_name,
                            server_type: cfg.server_type,
                        },
                        e,
                    )
                })?
            }
            SentinelConfig::ClientBuilder(client_builder) => {
                client_builder.build().map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubSubscriberErrorAsync::FailToBuildSentinelClientWithClientBuilder,
                        e,
                    )
                })?
            }
        };

        loop {
            let client_of_server_type = match client.async_get_client().await {
                Ok(c) => c,
                Err(e) => {
                    if self.retry.wait_with_backoff_async().await {
                        continue;
                    }
                    return Err(errs::Err::with_source(
                        RedisPubSubSubscriberErrorAsync::FailToGetClientOfServerType,
                        e,
                    ));
                }
            };
            let pubsub = match client_of_server_type.get_async_pubsub().await {
                Ok(pubsub) => pubsub,
                Err(e) => {
                    if self.retry.wait_with_backoff_async().await {
                        continue;
                    }
                    return Err(errs::Err::with_source(
                        RedisPubSubSubscriberErrorAsync::FailToGetAsyncPubSub,
                        e,
                    ));
                }
            };
            let (mut sink, mut stream) = pubsub.split();

            for c in self.channels.iter() {
                sink.subscribe(c).await.map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubSubscriberErrorAsync::FailToSubscribeToChannels,
                        e,
                    )
                })?;
            }

            for p in self.patterns.iter() {
                sink.psubscribe(p).await.map_err(|e| {
                    errs::Err::with_source(
                        RedisPubSubSubscriberErrorAsync::FailToSubscribeToChannelsWithPatterns,
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
                            RedisPubSubSubscriberErrorAsync::FailToGetMessage,
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
    use crate::sentinel::RedisDataSrcAsync;
    use redis::AsyncTypedCommands;
    use sabi::tokio::{AsyncGroup, DataSrc};
    use url::Url;

    async fn publish_async(s: &str) {
        let s = s.to_string();
        let _ = tokio::spawn(async {
            let mut ds = RedisDataSrcAsync::new(
                &[
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
                SentinelServerType::Master,
            );
            let mut ag = AsyncGroup::new();
            ds.setup_async(&mut ag).await.unwrap();
            let mut dc = ds.create_data_conn_async().await.unwrap();
            let conn = dc.get_connection();
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            conn.publish("channel-1", s).await.unwrap();
        });
    }

    mod test_new {
        use super::*;

        #[tokio::test]
        async fn addrs_are_strs_and_ok() {
            publish_async("Hello").await;

            let mut subscriber = RedisPubSubSubscriberAsync::new(
                &[
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
                SentinelServerType::Master,
            );

            subscriber.subscribe("channel-1");
            subscriber
                .receive_async(async |msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn addrs_are_strs_and_fail() {
            let mut subscriber = RedisPubSubSubscriberAsync::new(
                &["xxxx", "yyyy", "zzzz"],
                "mymaster",
                SentinelServerType::Master,
            );

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberErrorAsync::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisPubSubSubscriberErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["xxxx", "yyyy", "zzzz",]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(
                format!("{src:?}"),
                "Redis URL did not parse - InvalidClientConfig"
            );
        }

        #[tokio::test]
        async fn addrs_are_strings_and_ok() {
            publish_async("Hello").await;

            let mut subscriber = RedisPubSubSubscriberAsync::new(
                &[
                    "redis://127.0.0.1:26479".to_string(),
                    "redis://127.0.0.1:26480".to_string(),
                    "redis://127.0.0.1:26481".to_string(),
                ],
                "mymaster".to_string(),
                SentinelServerType::Master,
            );

            subscriber.subscribe("channel-1");
            subscriber
                .receive_async(async |msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn addrs_are_strings_and_fail() {
            let mut subscriber = RedisPubSubSubscriberAsync::new(
                &["xxxx".to_string(), "yyyy".to_string(), "zzzz".to_string()],
                "mymaster".to_string(),
                SentinelServerType::Master,
            );

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberErrorAsync::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisPubSubSubscriberErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["xxxx", "yyyy", "zzzz",]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(
                format!("{src:?}"),
                "Redis URL did not parse - InvalidClientConfig"
            );
        }

        #[tokio::test]
        async fn addrs_are_urls_and_ok() {
            publish_async("Hello").await;

            let Ok(url0) = Url::parse("redis://127.0.0.1:26479") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://127.0.0.1:26480") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://127.0.0.1:26481") else {
                panic!("bad url2");
            };
            let mut subscriber = RedisPubSubSubscriberAsync::new(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
            );

            subscriber.subscribe("channel-1");
            subscriber
                .receive_async(async |msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn addrs_are_urls_and_fail() {
            let Ok(url0) = Url::parse("redis://") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://") else {
                panic!("bad url2");
            };
            let mut subscriber = RedisPubSubSubscriberAsync::new(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
            );

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberErrorAsync::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisPubSubSubscriberErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["redis://", "redis://", "redis://",]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(format!("{src:?}"), "Missing hostname - InvalidClientConfig",);
        }
    }

    mod test_with_node_conn_info {
        use super::*;
        use url::Url;

        #[tokio::test]
        async fn addrs_are_strs_and_ok() {
            publish_async("Hello").await;

            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisPubSubSubscriberAsync::with_node_conn_info(
                &[
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );

            subscriber.subscribe("channel-1");
            subscriber
                .receive_async(async |msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn addrs_are_strs_and_fail() {
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisPubSubSubscriberAsync::with_node_conn_info(
                &["xxxx", "yyyy", "zzzz"],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberErrorAsync::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisPubSubSubscriberErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["xxxx", "yyyy", "zzzz",]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(
                format!("{src:?}"),
                "Redis URL did not parse - InvalidClientConfig"
            );
        }

        #[tokio::test]
        async fn addrs_are_strings_and_ok() {
            publish_async("Hello").await;

            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisPubSubSubscriberAsync::with_node_conn_info(
                &[
                    "redis://127.0.0.1:26479".to_string(),
                    "redis://127.0.0.1:26480".to_string(),
                    "redis://127.0.0.1:26481".to_string(),
                ],
                "mymaster".to_string(),
                SentinelServerType::Master,
                node_conn_info,
            );

            subscriber.subscribe("channel-1");
            subscriber
                .receive_async(async |msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn addrs_are_strings_and_fail() {
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisPubSubSubscriberAsync::with_node_conn_info(
                &["xxxx".to_string(), "yyyy".to_string(), "zzzz".to_string()],
                "mymaster".to_string(),
                SentinelServerType::Master,
                node_conn_info,
            );

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberErrorAsync::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisPubSubSubscriberErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["xxxx", "yyyy", "zzzz",]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(
                format!("{src:?}"),
                "Redis URL did not parse - InvalidClientConfig"
            );
        }

        #[tokio::test]
        async fn addrs_are_urls_and_ok() {
            publish_async("Hello").await;

            let Ok(url0) = Url::parse("redis://127.0.0.1:26479") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://127.0.0.1:26480") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://127.0.0.1:26481") else {
                panic!("bad url2");
            };
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisPubSubSubscriberAsync::with_node_conn_info(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );

            subscriber.subscribe("channel-1");
            subscriber
                .receive_async(async |msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn addrs_are_urls_and_fail() {
            let Ok(url0) = Url::parse("redis://") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://") else {
                panic!("bad url2");
            };
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisPubSubSubscriberAsync::with_node_conn_info(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );
            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberErrorAsync::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisPubSubSubscriberErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["redis://", "redis://", "redis://",]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(format!("{src:?}"), "Missing hostname - InvalidClientConfig",);
        }
    }

    mod test_with_conn_addrs {
        use super::*;

        #[tokio::test]
        async fn ok() {
            publish_async("Hello").await;

            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26479);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26480);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26481);

            let mut subscriber = RedisPubSubSubscriberAsync::with_conn_addrs(
                vec![conn_addr0, conn_addr1, conn_addr2],
                "mymaster",
                SentinelServerType::Master,
            );

            subscriber.subscribe("channel-1");
            subscriber
                .receive_async(async |msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn fail() {
            let mut subscriber = RedisPubSubSubscriberAsync::with_conn_addrs(
                vec![],
                "mymaster",
                SentinelServerType::Master,
            );

            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberErrorAsync::FailToBuildSentinelClientOfConnAddrs {
                conn_addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisPubSubSubscriberErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(conn_addrs, &[]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(
                format!("{src:?}"),
                "At least one sentinel is required - EmptySentinelList",
            );
        }
    }

    mod test_with_conn_addrs_and_node_conn_info {
        use super::*;

        #[tokio::test]
        async fn ok() {
            publish_async("Hello").await;

            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26479);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26480);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26481);

            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisPubSubSubscriberAsync::with_conn_addrs_and_node_conn_info(
                vec![conn_addr0, conn_addr1, conn_addr2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );

            subscriber.subscribe("channel-1");
            subscriber
                .receive_async(async |msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn fail() {
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisPubSubSubscriberAsync::with_conn_addrs_and_node_conn_info(
                vec![],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );

            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberErrorAsync::FailToBuildSentinelClientOfConnAddrs {
                conn_addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisPubSubSubscriberErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(conn_addrs, &[]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(
                format!("{src:?}"),
                "At least one sentinel is required - EmptySentinelList",
            );
        }
    }

    mod test_with_conn_infos {
        use super::*;
        use redis::IntoConnectionInfo;

        #[tokio::test]
        async fn ok() {
            publish_async("Hello").await;

            let conn_info0 = "redis://127.0.0.1:26479/0".into_connection_info().unwrap();
            let conn_info1 = "redis://127.0.0.1:26480/0".into_connection_info().unwrap();
            let conn_info2 = "redis://127.0.0.1:26481/0".into_connection_info().unwrap();

            let mut subscriber = RedisPubSubSubscriberAsync::with_conn_infos(
                vec![conn_info0, conn_info1, conn_info2],
                "mymaster",
                SentinelServerType::Master,
            );

            subscriber.subscribe("channel-1");
            subscriber
                .receive_async(async |msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn fail() {
            let mut subscriber = RedisPubSubSubscriberAsync::with_conn_infos(
                vec![],
                "mymaster",
                SentinelServerType::Master,
            );

            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberErrorAsync::FailToBuildSentinelClientOfConnInfos {
                conn_infos,
                service_name,
                server_type,
            }) = err.reason::<RedisPubSubSubscriberErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(conn_infos.len(), 0);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(
                format!("{src:?}"),
                "At least one sentinel is required - EmptySentinelList",
            );
        }
    }

    mod test_with_conn_infos_and_node_conn_info {
        use super::*;
        use redis::IntoConnectionInfo;

        #[tokio::test]
        async fn ok() {
            publish_async("Hello").await;

            let conn_info0 = "redis://127.0.0.1:26479/0".into_connection_info().unwrap();
            let conn_info1 = "redis://127.0.0.1:26480/0".into_connection_info().unwrap();
            let conn_info2 = "redis://127.0.0.1:26481/0".into_connection_info().unwrap();

            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisPubSubSubscriberAsync::with_conn_infos_and_node_conn_info(
                vec![conn_info0, conn_info1, conn_info2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );

            subscriber.subscribe("channel-1");
            subscriber
                .receive_async(async |msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn fail() {
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisPubSubSubscriberAsync::with_conn_infos_and_node_conn_info(
                vec![],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );

            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberErrorAsync::FailToBuildSentinelClientOfConnInfos {
                conn_infos,
                service_name,
                server_type,
            }) = err.reason::<RedisPubSubSubscriberErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(conn_infos.len(), 0);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(
                format!("{src:?}"),
                "At least one sentinel is required - EmptySentinelList",
            );
        }
    }

    mod test_with_client_builder {
        use super::*;

        #[tokio::test]
        async fn ok() {
            publish_async("Hello").await;

            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26479);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26480);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26481);
            let client_builder = SentinelClientBuilder::new(
                vec![conn_addr0, conn_addr1, conn_addr2],
                "mymaster",
                SentinelServerType::Master,
            )
            .unwrap();

            let mut subscriber = RedisPubSubSubscriberAsync::with_client_builder(client_builder);

            subscriber.subscribe("channel-1");
            subscriber
                .receive_async(async |msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn fail() {
            let client_builder =
                SentinelClientBuilder::new(vec![], "mymaster", SentinelServerType::Master).unwrap();

            let mut subscriber = RedisPubSubSubscriberAsync::with_client_builder(client_builder);

            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisPubSubSubscriberErrorAsync::FailToBuildSentinelClientWithClientBuilder) =
                err.reason::<RedisPubSubSubscriberErrorAsync>()
            else {
                panic!();
            };
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(
                format!("{src:?}"),
                "At least one sentinel is required - EmptySentinelList",
            );
        }
    }

    mod subscribe {
        use super::*;

        #[tokio::test]
        async fn ok() {
            publish_async("Hello").await;

            let mut subscriber = RedisPubSubSubscriberAsync::new(
                &[
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
                SentinelServerType::Master,
            );

            subscriber.subscribe("channel-1");
            subscriber
                .receive_async(async |msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .await
                .unwrap();
        }
    }

    mod psubscribe {
        use super::*;

        #[tokio::test]
        async fn ok() {
            publish_async("Hello").await;

            let mut subscriber = RedisPubSubSubscriberAsync::new(
                &[
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
                SentinelServerType::Master,
            );

            subscriber.psubscribe("channel-*");
            subscriber
                .receive_async(async |msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .await
                .unwrap();
        }
    }
}
