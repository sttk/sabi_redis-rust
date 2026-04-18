// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::retry_async::RetryAsync;
use futures::{stream::StreamExt, Future};
use redis::aio::PubSub;
use redis::{Client, ConnectionAddr, ConnectionInfo, ControlFlow, Msg, ToRedisArgs};
use std::fmt::Debug;

#[derive(Debug)]
pub enum RedisSubscriberErrorAsync {
    AddressAlreadyConsumed,
    FailToOpenClientOfAddr { addr: String },
    FailToOpenClientOfConnAddr { conn_addr: ConnectionAddr },
    FailToOpenClientOfConnInfo { conn_info: ConnectionInfo },
    FailToGetAsyncPubSub { conn_info: ConnectionInfo },
    FailToSubscribeToChannels { conn_info: ConnectionInfo },
    FailToSubscribeToChannelsWithPatterns { conn_info: ConnectionInfo },
    FailToGetMessage { conn_info: ConnectionInfo },
}

pub struct RedisSubscriberAsync<A>
where
    A: ToRedisArgs,
{
    addr: Option<RedisConfig>,
    channels: Vec<A>,
    patterns: Vec<A>,
    retry: RetryAsync,
}

enum RedisConfig {
    String(String),
    ConnAddr(ConnectionAddr),
    ConnInfo(ConnectionInfo),
}

impl<A> RedisSubscriberAsync<A>
where
    A: ToRedisArgs,
{
    pub fn new<S>(addr: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            addr: Some(RedisConfig::String(addr.as_ref().to_string())),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: RetryAsync::new(),
        }
    }

    pub fn with_conn_addr(conn_addr: ConnectionAddr) -> Self {
        Self {
            addr: Some(RedisConfig::ConnAddr(conn_addr)),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: RetryAsync::new(),
        }
    }

    pub fn with_conn_info(conn_info: ConnectionInfo) -> Self {
        Self {
            addr: Some(RedisConfig::ConnInfo(conn_info)),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: RetryAsync::new(),
        }
    }

    pub fn set_retry(&mut self, max_count: u32, init_delay_ms: u64, max_delay_ms: u64) {
        self.retry = RetryAsync::with_params(max_count, init_delay_ms, max_delay_ms);
    }

    pub fn subscribe(&mut self, channels: A) {
        self.channels.push(channels);
    }

    pub fn psubscribe(&mut self, patterns: A) {
        self.patterns.push(patterns);
    }

    pub async fn receive_async<F, Fut, U>(mut self, mut f: F) -> errs::Result<U>
    where
        F: FnMut(Msg) -> Fut,
        Fut: Future<Output = ControlFlow<U>>,
    {
        let addr = self
            .addr
            .take()
            .ok_or_else(|| errs::Err::new(RedisSubscriberErrorAsync::AddressAlreadyConsumed))?;
        let client = match addr {
            RedisConfig::String(addr) => Client::open(addr.as_str()).map_err(|e| {
                errs::Err::with_source(
                    RedisSubscriberErrorAsync::FailToOpenClientOfAddr { addr },
                    e,
                )
            })?,
            RedisConfig::ConnAddr(conn_addr) => Client::open(conn_addr.clone()).map_err(|e| {
                errs::Err::with_source(
                    RedisSubscriberErrorAsync::FailToOpenClientOfConnAddr { conn_addr },
                    e,
                )
            })?,
            RedisConfig::ConnInfo(conn_info) => Client::open(conn_info.clone()).map_err(|e| {
                errs::Err::with_source(
                    RedisSubscriberErrorAsync::FailToOpenClientOfConnInfo { conn_info },
                    e,
                )
            })?,
        };

        loop {
            let pubsub: PubSub = match client.get_async_pubsub().await {
                Ok(pubsub) => pubsub,
                Err(e) => {
                    if self.retry.wait_with_backoff_async().await {
                        continue;
                    }
                    return Err(errs::Err::with_source(
                        RedisSubscriberErrorAsync::FailToGetAsyncPubSub {
                            conn_info: client.get_connection_info().clone(),
                        },
                        e,
                    ));
                }
            };
            let (mut sink, mut stream) = pubsub.split();

            for c in self.channels.iter() {
                sink.subscribe(c).await.map_err(|e| {
                    errs::Err::with_source(
                        RedisSubscriberErrorAsync::FailToSubscribeToChannels {
                            conn_info: client.get_connection_info().clone(),
                        },
                        e,
                    )
                })?;
            }

            for p in self.patterns.iter() {
                sink.psubscribe(p).await.map_err(|e| {
                    errs::Err::with_source(
                        RedisSubscriberErrorAsync::FailToSubscribeToChannelsWithPatterns {
                            conn_info: client.get_connection_info().clone(),
                        },
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
                            RedisSubscriberErrorAsync::FailToGetMessage {
                                conn_info: client.get_connection_info().clone(),
                            },
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
    use crate::standalone_async::RedisDataSrcAsync;
    use redis::AsyncTypedCommands;
    use sabi::tokio::{AsyncGroup, DataSrc};
    use url::Url;

    async fn publish_async(s: &str) {
        let s = s.to_string();
        let _ = tokio::spawn(async move {
            let mut ds = RedisDataSrcAsync::new("redis://127.0.0.1:6379/8");
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
        async fn addr_is_str_and_ok() {
            publish_async("Hello").await;

            let mut subscriber = RedisSubscriberAsync::new("redis://127.0.0.1/0");

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
        async fn addr_is_str_and_fail() {
            let mut subscriber = RedisSubscriberAsync::new("xxxx");

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisSubscriberErrorAsync::FailToOpenClientOfAddr { addr }) =
                err.reason::<RedisSubscriberErrorAsync>()
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

        #[tokio::test]
        async fn addr_is_string_and_ok() {
            publish_async("Hello").await;

            let mut subscriber = RedisSubscriberAsync::new("redis://127.0.0.1/0".to_string());

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
        async fn addr_is_string_and_fail() {
            let mut subscriber = RedisSubscriberAsync::new("xxxx".to_string());

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisSubscriberErrorAsync::FailToOpenClientOfAddr { addr }) =
                err.reason::<RedisSubscriberErrorAsync>()
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

        #[tokio::test]
        async fn addr_is_url_and_ok() {
            publish_async("Hello").await;

            let Ok(url) = Url::parse("redis://127.0.0.1:6379/0") else {
                panic!("bad url");
            };
            let mut subscriber = RedisSubscriberAsync::new(url);

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
        async fn addr_is_url_and_fail() {
            let Ok(url) = Url::parse("redis://xxxx") else {
                panic!("bad url");
            };
            let mut subscriber = RedisSubscriberAsync::new(url);

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisSubscriberErrorAsync::FailToGetAsyncPubSub { conn_info }) =
                err.reason::<RedisSubscriberErrorAsync>()
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

        #[tokio::test]
        async fn ok() {
            publish_async("Hello").await;

            let conn_addr = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 6379);
            let mut subscriber = RedisSubscriberAsync::with_conn_addr(conn_addr);

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
            let conn_addr = redis::ConnectionAddr::Tcp("xxxx".to_string(), 6379);
            let mut subscriber = RedisSubscriberAsync::with_conn_addr(conn_addr);

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisSubscriberErrorAsync::FailToGetAsyncPubSub { conn_info }) =
                err.reason::<RedisSubscriberErrorAsync>()
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

        #[tokio::test]
        async fn ok() {
            publish_async("Hello").await;

            let conn_info = "redis://127.0.0.1:6379/0".into_connection_info().unwrap();
            let mut subscriber = RedisSubscriberAsync::with_conn_info(conn_info);

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
            let conn_info = "redis://xxxx".into_connection_info().unwrap();
            let mut subscriber = RedisSubscriberAsync::with_conn_info(conn_info);

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive_async(async |_msg| panic!()).await
            else {
                panic!();
            };
            let Ok(RedisSubscriberErrorAsync::FailToGetAsyncPubSub { conn_info }) =
                err.reason::<RedisSubscriberErrorAsync>()
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

        #[tokio::test]
        async fn ok() {
            publish_async("Hello").await;

            let conn_info = "redis://127.0.0.1:6379/0".into_connection_info().unwrap();
            let mut subscriber = RedisSubscriberAsync::with_conn_info(conn_info);

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
        use redis::IntoConnectionInfo;

        #[tokio::test]
        async fn ok() {
            publish_async("Hello").await;

            let conn_info = "redis://127.0.0.1:6379/0".into_connection_info().unwrap();
            let mut subscriber = RedisSubscriberAsync::with_conn_info(conn_info);

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
