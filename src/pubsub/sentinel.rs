// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use redis::sentinel::{
    SentinelClient, SentinelClientBuilder, SentinelNodeConnectionInfo, SentinelServerType,
};
use redis::{Connection, ConnectionAddr, ConnectionInfo, ControlFlow, Msg, ToRedisArgs};
use std::fmt::Debug;

use crate::retry::Retry;

#[derive(Debug)]
pub enum RedisSentinelSubscriberError {
    SentinelConfigAlreadyConsumed,
    FailToBuildSentinelClientOfAddrs {
        addrs: Vec<String>,
        service_name: String,
        server_type: SentinelServerType,
    },
    FailToBuildSentinelClientOfConnAddrs {
        conn_addrs: Vec<ConnectionAddr>,
        service_name: String,
        server_type: SentinelServerType,
    },
    FailToBuildSentinelClientOfConnInfos {
        conn_infos: Vec<ConnectionInfo>,
        service_name: String,
        server_type: SentinelServerType,
    },
    FailToBuildSentinelClientWithClientBuilder,
    FailToGetConnection,
    FailToSubscribeToChannels,
    FailToSubscribeToChannelsWithPatterns,
    FailToGetMessage,
}

pub struct RedisSentinelSubscriber<A>
where
    A: ToRedisArgs,
{
    config: Option<SentinelConfig>,
    channels: Vec<A>,
    patterns: Vec<A>,
    retry: Retry,
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

impl<A> RedisSentinelSubscriber<A>
where
    A: ToRedisArgs,
{
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
            retry: Retry::new(),
        }
    }

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
            retry: Retry::new(),
        }
    }

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
            retry: Retry::new(),
        }
    }

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
            retry: Retry::new(),
        }
    }

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
            retry: Retry::new(),
        }
    }

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
            retry: Retry::new(),
        }
    }

    pub fn with_client_builder(client_builder: SentinelClientBuilder) -> Self {
        Self {
            config: Some(SentinelConfig::ClientBuilder(Box::new(client_builder))),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: Retry::new(),
        }
    }

    pub fn set_retry(&mut self, max_count: u32, init_delay_ms: u64, max_delay_ms: u64) {
        self.retry = Retry::with_params(max_count, init_delay_ms, max_delay_ms);
    }

    pub fn subscribe(&mut self, channels: A) {
        self.channels.push(channels);
    }

    pub fn psubscribe(&mut self, patterns: A) {
        self.patterns.push(patterns);
    }

    pub fn receive<F, U>(mut self, mut f: F) -> errs::Result<U>
    where
        F: FnMut(Msg) -> ControlFlow<U>,
    {
        let cfg = self.config.take().ok_or_else(|| {
            errs::Err::new(RedisSentinelSubscriberError::SentinelConfigAlreadyConsumed)
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
                        RedisSentinelSubscriberError::FailToBuildSentinelClientOfAddrs {
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
                        RedisSentinelSubscriberError::FailToBuildSentinelClientOfConnAddrs {
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
                        RedisSentinelSubscriberError::FailToBuildSentinelClientOfConnInfos {
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
                        RedisSentinelSubscriberError::FailToBuildSentinelClientWithClientBuilder,
                        e,
                    )
                })?
            }
        };

        loop {
            let mut conn: Connection = match client.get_connection() {
                Ok(c) => c,
                Err(e) => {
                    if self.retry.wait_with_backoff() {
                        continue;
                    }
                    return Err(errs::Err::with_source(
                        RedisSentinelSubscriberError::FailToGetConnection,
                        e,
                    ));
                }
            };
            let mut pubsub = conn.as_pubsub();

            for c in self.channels.iter() {
                pubsub.subscribe(c).map_err(|e| {
                    errs::Err::with_source(
                        RedisSentinelSubscriberError::FailToSubscribeToChannels,
                        e,
                    )
                })?;
            }

            for p in self.patterns.iter() {
                pubsub.psubscribe(p).map_err(|e| {
                    errs::Err::with_source(
                        RedisSentinelSubscriberError::FailToSubscribeToChannelsWithPatterns,
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
                            RedisSentinelSubscriberError::FailToGetMessage,
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
    use crate::RedisSentinelDataSrc;
    use redis::TypedCommands;
    use sabi::{AsyncGroup, DataSrc};
    use url::Url;

    fn publish(s: &str) {
        let s = s.to_string();
        let _ = std::thread::spawn(move || {
            let mut ds = RedisSentinelDataSrc::new(
                &[
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
                SentinelServerType::Master,
            );
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
        fn addrs_are_strs_and_ok() {
            publish("Hello");

            let mut subscriber = RedisSentinelSubscriber::new(
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
                .receive(|msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .unwrap();
        }

        #[test]
        fn addrs_are_strs_and_fail() {
            let mut subscriber = RedisSentinelSubscriber::new(
                &["xxxx", "yyyy", "zzzz"],
                "mymaster",
                SentinelServerType::Master,
            );

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisSentinelSubscriberError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelSubscriberError>()
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

        #[test]
        fn addrs_are_strings_and_ok() {
            publish("Hello");

            let mut subscriber = RedisSentinelSubscriber::new(
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
                .receive(|msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .unwrap();
        }

        #[test]
        fn addrs_are_strings_and_fail() {
            let mut subscriber = RedisSentinelSubscriber::new(
                &["xxxx".to_string(), "yyyy".to_string(), "zzzz".to_string()],
                "mymaster".to_string(),
                SentinelServerType::Master,
            );

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisSentinelSubscriberError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelSubscriberError>()
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

        #[test]
        fn addrs_are_urls_and_ok() {
            publish("Hello");

            let Ok(url0) = Url::parse("redis://127.0.0.1:26479") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://127.0.0.1:26480") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://127.0.0.1:26481") else {
                panic!("bad url2");
            };
            let mut subscriber = RedisSentinelSubscriber::new(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
            );

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
        fn addrs_are_urls_and_fail() {
            let Ok(url0) = Url::parse("redis://") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://") else {
                panic!("bad url2");
            };
            let mut subscriber = RedisSentinelSubscriber::new(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
            );

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisSentinelSubscriberError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelSubscriberError>()
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

        #[test]
        fn addrs_are_strs_and_ok() {
            publish("Hello");

            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisSentinelSubscriber::with_node_conn_info(
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
                .receive(|msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .unwrap();
        }

        #[test]
        fn addrs_are_strs_and_fail() {
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisSentinelSubscriber::with_node_conn_info(
                &["xxxx", "yyyy", "zzzz"],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisSentinelSubscriberError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelSubscriberError>()
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

        #[test]
        fn addrs_are_strings_and_ok() {
            publish("Hello");

            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisSentinelSubscriber::with_node_conn_info(
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
                .receive(|msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .unwrap();
        }

        #[test]
        fn addrs_are_strings_and_fail() {
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisSentinelSubscriber::with_node_conn_info(
                &["xxxx".to_string(), "yyyy".to_string(), "zzzz".to_string()],
                "mymaster".to_string(),
                SentinelServerType::Master,
                node_conn_info,
            );

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisSentinelSubscriberError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelSubscriberError>()
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

        #[test]
        fn addrs_are_urls_and_ok() {
            publish("Hello");

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
            let mut subscriber = RedisSentinelSubscriber::with_node_conn_info(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );

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
        fn addrs_are_urls_and_fail() {
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
            let mut subscriber = RedisSentinelSubscriber::with_node_conn_info(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );
            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisSentinelSubscriberError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelSubscriberError>()
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

        #[test]
        fn ok() {
            publish("Hello");

            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26479);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26480);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26481);

            let mut subscriber = RedisSentinelSubscriber::with_conn_addrs(
                vec![conn_addr0, conn_addr1, conn_addr2],
                "mymaster",
                SentinelServerType::Master,
            );

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
            let mut subscriber = RedisSentinelSubscriber::with_conn_addrs(
                vec![],
                "mymaster",
                SentinelServerType::Master,
            );

            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisSentinelSubscriberError::FailToBuildSentinelClientOfConnAddrs {
                conn_addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelSubscriberError>()
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

        #[test]
        fn ok() {
            publish("Hello");

            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26479);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26480);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26481);

            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisSentinelSubscriber::with_conn_addrs_and_node_conn_info(
                vec![conn_addr0, conn_addr1, conn_addr2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );

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
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisSentinelSubscriber::with_conn_addrs_and_node_conn_info(
                vec![],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );

            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisSentinelSubscriberError::FailToBuildSentinelClientOfConnAddrs {
                conn_addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelSubscriberError>()
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

        #[test]
        fn ok() {
            publish("Hello");

            let conn_info0 = "redis://127.0.0.1:26479/0".into_connection_info().unwrap();
            let conn_info1 = "redis://127.0.0.1:26480/0".into_connection_info().unwrap();
            let conn_info2 = "redis://127.0.0.1:26481/0".into_connection_info().unwrap();

            let mut subscriber = RedisSentinelSubscriber::with_conn_infos(
                vec![conn_info0, conn_info1, conn_info2],
                "mymaster",
                SentinelServerType::Master,
            );

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
            let mut subscriber = RedisSentinelSubscriber::with_conn_infos(
                vec![],
                "mymaster",
                SentinelServerType::Master,
            );

            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisSentinelSubscriberError::FailToBuildSentinelClientOfConnInfos {
                conn_infos,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelSubscriberError>()
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

        #[test]
        fn ok() {
            publish("Hello");

            let conn_info0 = "redis://127.0.0.1:26479/0".into_connection_info().unwrap();
            let conn_info1 = "redis://127.0.0.1:26480/0".into_connection_info().unwrap();
            let conn_info2 = "redis://127.0.0.1:26481/0".into_connection_info().unwrap();

            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisSentinelSubscriber::with_conn_infos_and_node_conn_info(
                vec![conn_info0, conn_info1, conn_info2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );

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
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut subscriber = RedisSentinelSubscriber::with_conn_infos_and_node_conn_info(
                vec![],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );

            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisSentinelSubscriberError::FailToBuildSentinelClientOfConnInfos {
                conn_infos,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelSubscriberError>()
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

        #[test]
        fn ok() {
            publish("Hello");

            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26479);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26480);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26481);
            let client_builder = SentinelClientBuilder::new(
                vec![conn_addr0, conn_addr1, conn_addr2],
                "mymaster",
                SentinelServerType::Master,
            )
            .unwrap();

            let mut subscriber = RedisSentinelSubscriber::with_client_builder(client_builder);

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
            let client_builder =
                SentinelClientBuilder::new(vec![], "mymaster", SentinelServerType::Master).unwrap();

            let mut subscriber = RedisSentinelSubscriber::with_client_builder(client_builder);

            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisSentinelSubscriberError::FailToBuildSentinelClientWithClientBuilder) =
                err.reason::<RedisSentinelSubscriberError>()
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

        #[test]
        fn ok() {
            publish("Hello");

            let mut subscriber = RedisSentinelSubscriber::new(
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

        #[test]
        fn ok() {
            publish("Hello");

            let mut subscriber = RedisSentinelSubscriber::new(
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
                .receive(|msg| {
                    let payload: String = msg.get_payload().unwrap();
                    assert_eq!(payload, "Hello");
                    ControlFlow::Break(1)
                })
                .unwrap();
        }
    }
}
