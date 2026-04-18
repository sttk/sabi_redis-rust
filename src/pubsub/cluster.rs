// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use redis::{
    Client, Connection, ConnectionAddr, ConnectionInfo, ControlFlow, IntoConnectionInfo, Msg,
    ToRedisArgs,
};
use std::fmt::Debug;

use crate::retry::Retry;

#[derive(Debug)]
pub enum RedisClusterSubscriberError {
    ClusterConfigAlreadyConsumed,
    InvalidAddrs { addrs: Vec<String> },
    InvalidConnAddrs { conn_addrs: Vec<ConnectionAddr> },
    FailToOpenClient { conn_info: ConnectionInfo },
    FailToGetConnection { conn_info: ConnectionInfo },
    FailToSubscribeToChannels,
    FailToSubscribeToChannelsWithPatterns,
    FailToGetMessage,
}

pub struct RedisClusterSubscriber<A>
where
    A: ToRedisArgs,
{
    config: Option<RedisConfig>,
    channels: Vec<A>,
    patterns: Vec<A>,
    retry: Retry,
}

enum RedisConfig {
    String(Vec<String>),
    ConnAddr(Vec<ConnectionAddr>),
    ConnInfo(Vec<ConnectionInfo>),
}

impl<A> RedisClusterSubscriber<A>
where
    A: ToRedisArgs,
{
    pub fn new<I>(addrs: I) -> Self
    where
        I: IntoIterator<Item: AsRef<str>>,
    {
        Self {
            config: Some(RedisConfig::String(
                addrs.into_iter().map(|s| s.as_ref().to_string()).collect(),
            )),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: Retry::new(),
        }
    }

    pub fn with_conn_addrs<I>(conn_addrs: I) -> Self
    where
        I: IntoIterator<Item = ConnectionAddr>,
    {
        Self {
            config: Some(RedisConfig::ConnAddr(conn_addrs.into_iter().collect())),
            channels: Vec::new(),
            patterns: Vec::new(),
            retry: Retry::new(),
        }
    }

    pub fn with_conn_infos<I>(conn_infos: I) -> Self
    where
        I: IntoIterator<Item = ConnectionInfo>,
    {
        Self {
            config: Some(RedisConfig::ConnInfo(conn_infos.into_iter().collect())),
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
        let config = self.config.take();
        let conn_infos: Vec<ConnectionInfo> = match config {
            Some(RedisConfig::String(addrs)) => {
                let mut conn_infos = Vec::with_capacity(addrs.len());
                for addr in &addrs {
                    match addr.as_str().into_connection_info() {
                        Ok(info) => conn_infos.push(info),
                        Err(e) => {
                            return Err(errs::Err::with_source(
                                RedisClusterSubscriberError::InvalidAddrs { addrs },
                                e,
                            ))
                        }
                    }
                }
                conn_infos
            }
            Some(RedisConfig::ConnAddr(conn_addrs)) => {
                let mut conn_infos = Vec::with_capacity(conn_addrs.len());
                for conn_addr in &conn_addrs {
                    match conn_addr.clone().into_connection_info() {
                        Ok(info) => conn_infos.push(info),
                        Err(e) => {
                            return Err(errs::Err::with_source(
                                RedisClusterSubscriberError::InvalidConnAddrs { conn_addrs },
                                e,
                            ))
                        }
                    }
                }
                conn_infos
            }
            Some(RedisConfig::ConnInfo(conn_infos)) => conn_infos,
            None => {
                return Err(errs::Err::new(
                    RedisClusterSubscriberError::ClusterConfigAlreadyConsumed,
                ))
            }
        };

        let mut current_conn_info_index = 0;

        loop {
            let conn_info = conn_infos[current_conn_info_index].clone();
            current_conn_info_index = (current_conn_info_index + 1) % conn_infos.len();

            let client = Client::open(conn_info.clone()).map_err(|e| {
                errs::Err::with_source(
                    RedisClusterSubscriberError::FailToOpenClient {
                        conn_info: conn_info.clone(),
                    },
                    e,
                )
            })?;

            let mut conn: Connection = match client.get_connection() {
                Ok(c) => c,
                Err(e) => {
                    if self.retry.wait_with_backoff() {
                        continue;
                    }
                    return Err(errs::Err::with_source(
                        RedisClusterSubscriberError::FailToGetConnection { conn_info },
                        e,
                    ));
                }
            };
            let mut pubsub = conn.as_pubsub();

            for c in self.channels.iter() {
                pubsub.subscribe(c).map_err(|e| {
                    errs::Err::with_source(
                        RedisClusterSubscriberError::FailToSubscribeToChannels,
                        e,
                    )
                })?;
            }

            for p in self.patterns.iter() {
                pubsub.psubscribe(p).map_err(|e| {
                    errs::Err::with_source(
                        RedisClusterSubscriberError::FailToSubscribeToChannelsWithPatterns,
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
                            RedisClusterSubscriberError::FailToGetMessage,
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
    use crate::RedisClusterDataSrc;
    use redis::TypedCommands;
    use sabi::{AsyncGroup, DataSrc};
    use url::Url;

    fn publish(s: &str) {
        let s = s.to_string();
        let _ = std::thread::spawn(move || {
            let mut ds = RedisClusterDataSrc::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
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
        fn addrs_is_strs_and_ok() {
            publish("Hello");

            let mut subscriber = RedisClusterSubscriber::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);

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
        fn addrs_is_strs_and_fail() {
            let mut subscriber = RedisClusterSubscriber::new(&["xxxx", "yyyy", "zzzz"]);

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisClusterSubscriberError::InvalidAddrs { addrs }) =
                err.reason::<RedisClusterSubscriberError>()
            else {
                panic!();
            };
            assert_eq!(
                addrs,
                &["xxxx".to_string(), "yyyy".to_string(), "zzzz".to_string()]
            );
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(
                format!("{src:?}"),
                "Redis URL did not parse - InvalidClientConfig"
            );
        }

        #[test]
        fn addrs_is_strings_and_ok() {
            publish("Hello");

            let mut subscriber = RedisClusterSubscriber::new(vec![
                "redis://127.0.0.1:7000".to_string(),
                "redis://127.0.0.1:7001".to_string(),
                "redis://127.0.0.1:7002".to_string(),
            ]);

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
        fn addrs_is_strings_and_fail() {
            let mut subscriber = RedisClusterSubscriber::new(vec![
                "xxxx".to_string(),
                "yyyy".to_string(),
                "zzzz".to_string(),
            ]);

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisClusterSubscriberError::InvalidAddrs { addrs }) =
                err.reason::<RedisClusterSubscriberError>()
            else {
                panic!();
            };
            assert_eq!(
                addrs,
                &["xxxx".to_string(), "yyyy".to_string(), "zzzz".to_string()]
            );
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(
                format!("{src:?}"),
                "Redis URL did not parse - InvalidClientConfig"
            );
        }

        #[test]
        fn addrs_is_urls_and_ok() {
            publish("Hello");

            let Ok(url0) = Url::parse("redis://127.0.0.1:7000") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://127.0.0.1:7001") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://127.0.0.1:7002") else {
                panic!("bad url2");
            };
            let mut subscriber = RedisClusterSubscriber::new(&[url0, url1, url2]);

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
        fn addrs_is_urls_and_fail() {
            let Ok(url0) = Url::parse("redis://xxxx:7000") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://yyyy:7001") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://zzzz:7002") else {
                panic!("bad url2");
            };
            let mut subscriber = RedisClusterSubscriber::new(vec![url0, url1, url2]);

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisClusterSubscriberError::FailToGetConnection { conn_info }) =
                err.reason::<RedisClusterSubscriberError>()
            else {
                panic!();
            };
            #[cfg(target_os = "linux")]
            assert_eq!(format!("{conn_info:?}"), "ConnectionInfo { addr: Tcp(\"yyyy\", 7001), tcp_settings: TcpSettings { nodelay: false, keepalive: None, user_timeout: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(not(target_os = "linux"))]
            assert_eq!(format!("{conn_info:?}"), "ConnectionInfo { addr: Tcp(\"yyyy\", 7001), tcp_settings: TcpSettings { nodelay: false, keepalive: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            let Some(src) = err.source() else {
                panic!();
            };
            assert_eq!(
                format!("{src:?}"),
                "failed to lookup address information: nodename nor servname provided, or not known",
            );
        }
    }

    mod test_with_conn_addrs {
        use super::*;

        #[test]
        fn ok() {
            publish("Hello");

            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 7000);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 7001);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 7002);

            let mut subscriber =
                RedisClusterSubscriber::with_conn_addrs(vec![conn_addr0, conn_addr1, conn_addr2]);

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
            let conn_addr0 = redis::ConnectionAddr::Tcp("xxxx".to_string(), 7000);
            let conn_addr1 = redis::ConnectionAddr::Tcp("yyyy".to_string(), 7001);
            let conn_addr2 = redis::ConnectionAddr::Tcp("zzzz".to_string(), 7002);

            let mut subscriber =
                RedisClusterSubscriber::with_conn_addrs(vec![conn_addr0, conn_addr1, conn_addr2]);

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisClusterSubscriberError::FailToGetConnection { conn_info }) =
                err.reason::<RedisClusterSubscriberError>()
            else {
                panic!();
            };
            #[cfg(target_os = "linux")]
            assert_eq!(format!("{conn_info:?}"), "ConnectionInfo { addr: Tcp(\"yyyy\", 7001), tcp_settings: TcpSettings { nodelay: false, keepalive: None, user_timeout: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(not(target_os = "linux"))]
            assert_eq!(format!("{conn_info:?}"), "ConnectionInfo { addr: Tcp(\"yyyy\", 7001), tcp_settings: TcpSettings { nodelay: false, keepalive: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
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

    mod test_with_conn_infos {
        use super::*;

        #[test]
        fn ok() {
            publish("Hello");

            let conn_info0 = "redis://127.0.0.1:7000/0".into_connection_info().unwrap();
            let conn_info1 = "redis://127.0.0.1:7001/0".into_connection_info().unwrap();
            let conn_info2 = "redis://127.0.0.1:7002/0".into_connection_info().unwrap();

            let mut subscriber =
                RedisClusterSubscriber::with_conn_infos(vec![conn_info0, conn_info1, conn_info2]);

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
            let conn_info0 = "redis://xxxx:7000".into_connection_info().unwrap();
            let conn_info1 = "redis://yyyy:7001".into_connection_info().unwrap();
            let conn_info2 = "redis://zzzz:7002".into_connection_info().unwrap();

            let mut subscriber =
                RedisClusterSubscriber::with_conn_infos(vec![conn_info0, conn_info1, conn_info2]);

            subscriber.set_retry(1, 0, 0);
            subscriber.subscribe("channel-1");
            let Err(err): errs::Result<i32> = subscriber.receive(|_msg| panic!()) else {
                panic!();
            };
            let Ok(RedisClusterSubscriberError::FailToGetConnection { conn_info }) =
                err.reason::<RedisClusterSubscriberError>()
            else {
                panic!();
            };
            #[cfg(target_os = "linux")]
            assert_eq!(format!("{conn_info:?}"), "ConnectionInfo { addr: Tcp(\"yyyy\", 7001), tcp_settings: TcpSettings { nodelay: false, keepalive: None, user_timeout: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(not(target_os = "linux"))]
            assert_eq!(format!("{conn_info:?}"), "ConnectionInfo { addr: Tcp(\"yyyy\", 7001), tcp_settings: TcpSettings { nodelay: false, keepalive: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
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

        #[test]
        fn ok() {
            publish("Hello");

            let mut subscriber = RedisClusterSubscriber::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);

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

            let mut subscriber = RedisClusterSubscriber::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);

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
