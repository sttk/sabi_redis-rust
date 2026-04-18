// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use r2d2::{Builder, Pool, PooledConnection};
use redis::sentinel::{
    LockedSentinelClient, SentinelClient, SentinelClientBuilder, SentinelNodeConnectionInfo,
    SentinelServerType,
};
use redis::{Connection, ConnectionAddr, ConnectionInfo};
use sabi::{AsyncGroup, DataConn, DataSrc};

use std::fmt::Debug;
use std::mem;

#[derive(Debug)]
pub enum RedisSentinelError {
    NotSetupYet,
    AlreadySetup,
    FailToBuildSentinelClientOfAddrs {
        addrs: Vec<String>,
        service_name: String,
        server_type: SentinelServerType,
    },
    FailToBuildPoolOfAddrs {
        addrs: Vec<String>,
        service_name: String,
        server_type: SentinelServerType,
    },
    FailToBuildSentinelClientOfConnAddrs {
        conn_addrs: Vec<ConnectionAddr>,
        service_name: String,
        server_type: SentinelServerType,
    },
    FailToBuildSentinelClientWithClientBuilder,
    FailToBuildPoolOfConnAddrs {
        conn_addrs: Vec<ConnectionAddr>,
        service_name: String,
        server_type: SentinelServerType,
    },
    FailToBuildSentinelClientOfConnInfos {
        conn_infos: Vec<ConnectionInfo>,
        service_name: String,
        server_type: SentinelServerType,
    },
    FailToBuildPoolOfConnInfos {
        conn_infos: Vec<ConnectionInfo>,
        service_name: String,
        server_type: SentinelServerType,
    },
    FailToBuildPoolWithClientBuilder,
    FailToGetConnectionFromPool,
}

#[allow(clippy::type_complexity)]
pub struct RedisSentinelDataConn {
    conn: PooledConnection<LockedSentinelClient>,
    pre_commit_vec: Vec<Box<dyn FnMut(&mut Connection) -> errs::Result<()>>>,
    post_commit_vec: Vec<Box<dyn FnMut(&mut Connection) -> errs::Result<()>>>,
    force_back_vec: Vec<Box<dyn FnMut(&mut Connection) -> errs::Result<()>>>,
}

impl RedisSentinelDataConn {
    fn new(conn: PooledConnection<LockedSentinelClient>) -> Self {
        Self {
            conn,
            pre_commit_vec: Vec::new(),
            post_commit_vec: Vec::new(),
            force_back_vec: Vec::new(),
        }
    }

    pub fn get_connection(&mut self) -> &mut PooledConnection<LockedSentinelClient> {
        &mut self.conn
    }

    pub fn add_pre_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut Connection) -> errs::Result<()> + 'static,
    {
        self.pre_commit_vec.push(Box::new(f));
    }

    pub fn add_post_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut Connection) -> errs::Result<()> + 'static,
    {
        self.post_commit_vec.push(Box::new(f));
    }

    pub fn add_force_back<F>(&mut self, f: F)
    where
        F: FnMut(&mut Connection) -> errs::Result<()> + 'static,
    {
        self.force_back_vec.push(Box::new(f));
    }
}

impl DataConn for RedisSentinelDataConn {
    fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        for f in self.pre_commit_vec.iter_mut() {
            f(&mut self.conn)?;
        }
        Ok(())
    }

    fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    fn post_commit(&mut self, _ag: &mut AsyncGroup) {
        for f in self.post_commit_vec.iter_mut() {
            // The error are not exposed externally, but a notification is triggered when
            // errs::Err is created.
            let _ = f(&mut self.conn);
        }
    }

    fn rollback(&mut self, _ag: &mut AsyncGroup) {}

    fn should_force_back(&self) -> bool {
        true
    }

    fn force_back(&mut self, _ag: &mut AsyncGroup) {
        for f in self.force_back_vec.iter_mut().rev() {
            // The error are not exposed externally, but a notification is triggered when
            // errs::Err is created.
            let _ = f(&mut self.conn);
        }
    }

    fn close(&mut self) {}
}

pub struct RedisSentinelDataSrc {
    pool: Option<RedisPool>,
}

enum RedisPool {
    Object(Pool<LockedSentinelClient>),
    StringConfig(Box<(RedisConfig<String>, Builder<LockedSentinelClient>)>),
    ConnAddrConfig(Box<(RedisConfig<ConnectionAddr>, Builder<LockedSentinelClient>)>),
    ConnInfoConfig(Box<(RedisConfig<ConnectionInfo>, Builder<LockedSentinelClient>)>),
    ClientBuilderConfig(Box<(SentinelClientBuilder, Builder<LockedSentinelClient>)>),
}

struct RedisConfig<T> {
    addrs: Vec<T>,
    service_name: String,
    node_conn_info: Option<SentinelNodeConnectionInfo>,
    server_type: SentinelServerType,
}

impl RedisSentinelDataSrc {
    pub fn new<I, S>(addrs: I, service_name: S, server_type: SentinelServerType) -> Self
    where
        I: IntoIterator<Item: AsRef<str>>,
        S: AsRef<str>,
    {
        Self {
            pool: Some(RedisPool::StringConfig(Box::new((
                RedisConfig {
                    addrs: addrs.into_iter().map(|s| s.as_ref().to_string()).collect(),
                    service_name: service_name.as_ref().to_string(),
                    node_conn_info: None,
                    server_type,
                },
                Pool::builder(),
            )))),
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
            pool: Some(RedisPool::StringConfig(Box::new((
                RedisConfig {
                    addrs: addrs.into_iter().map(|s| s.as_ref().to_string()).collect(),
                    service_name: service_name.as_ref().to_string(),
                    node_conn_info: Some(node_conn_info),
                    server_type,
                },
                Pool::builder(),
            )))),
        }
    }

    pub fn with_pool_builder<I, S>(
        addrs: I,
        service_name: S,
        server_type: SentinelServerType,
        pool_builder: Builder<LockedSentinelClient>,
    ) -> Self
    where
        I: IntoIterator<Item: AsRef<str>>,
        S: AsRef<str>,
    {
        Self {
            pool: Some(RedisPool::StringConfig(Box::new((
                RedisConfig {
                    addrs: addrs.into_iter().map(|s| s.as_ref().to_string()).collect(),
                    service_name: service_name.as_ref().to_string(),
                    node_conn_info: None,
                    server_type,
                },
                pool_builder,
            )))),
        }
    }

    pub fn with_node_conn_info_and_pool_builder<I, S>(
        addrs: I,
        service_name: S,
        server_type: SentinelServerType,
        node_conn_info: SentinelNodeConnectionInfo,
        pool_builder: Builder<LockedSentinelClient>,
    ) -> Self
    where
        I: IntoIterator<Item: AsRef<str>>,
        S: AsRef<str>,
    {
        Self {
            pool: Some(RedisPool::StringConfig(Box::new((
                RedisConfig {
                    addrs: addrs.into_iter().map(|s| s.as_ref().to_string()).collect(),
                    service_name: service_name.as_ref().to_string(),
                    node_conn_info: Some(node_conn_info),
                    server_type,
                },
                pool_builder,
            )))),
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
            pool: Some(RedisPool::ConnAddrConfig(Box::new((
                RedisConfig {
                    addrs: conn_addrs.into_iter().collect(),
                    service_name: service_name.as_ref().to_string(),
                    node_conn_info: None,
                    server_type,
                },
                Pool::builder(),
            )))),
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
            pool: Some(RedisPool::ConnAddrConfig(Box::new((
                RedisConfig {
                    addrs: conn_addrs.into_iter().collect(),
                    service_name: service_name.as_ref().to_string(),
                    node_conn_info: Some(node_conn_info),
                    server_type,
                },
                Pool::builder(),
            )))),
        }
    }

    pub fn with_conn_addrs_and_pool_builder<I, S>(
        conn_addrs: I,
        service_name: S,
        server_type: SentinelServerType,
        pool_builder: Builder<LockedSentinelClient>,
    ) -> Self
    where
        I: IntoIterator<Item = ConnectionAddr>,
        S: AsRef<str>,
    {
        Self {
            pool: Some(RedisPool::ConnAddrConfig(Box::new((
                RedisConfig {
                    addrs: conn_addrs.into_iter().collect(),
                    service_name: service_name.as_ref().to_string(),
                    node_conn_info: None,
                    server_type,
                },
                pool_builder,
            )))),
        }
    }

    pub fn with_conn_addrs_and_node_conn_info_and_pool_builder<I, S>(
        conn_addrs: I,
        service_name: S,
        server_type: SentinelServerType,
        node_conn_info: SentinelNodeConnectionInfo,
        pool_builder: Builder<LockedSentinelClient>,
    ) -> Self
    where
        I: IntoIterator<Item = ConnectionAddr>,
        S: AsRef<str>,
    {
        Self {
            pool: Some(RedisPool::ConnAddrConfig(Box::new((
                RedisConfig {
                    addrs: conn_addrs.into_iter().collect(),
                    service_name: service_name.as_ref().to_string(),
                    node_conn_info: Some(node_conn_info),
                    server_type,
                },
                pool_builder,
            )))),
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
            pool: Some(RedisPool::ConnInfoConfig(Box::new((
                RedisConfig {
                    addrs: conn_infos.into_iter().collect(),
                    service_name: service_name.as_ref().to_string(),
                    node_conn_info: None,
                    server_type,
                },
                Pool::builder(),
            )))),
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
            pool: Some(RedisPool::ConnInfoConfig(Box::new((
                RedisConfig {
                    addrs: conn_infos.into_iter().collect(),
                    service_name: service_name.as_ref().to_string(),
                    node_conn_info: Some(node_conn_info),
                    server_type,
                },
                Pool::builder(),
            )))),
        }
    }

    pub fn with_conn_infos_and_pool_builder<I, S>(
        conn_infos: I,
        service_name: S,
        server_type: SentinelServerType,
        pool_builder: Builder<LockedSentinelClient>,
    ) -> Self
    where
        I: IntoIterator<Item = ConnectionInfo>,
        S: AsRef<str>,
    {
        Self {
            pool: Some(RedisPool::ConnInfoConfig(Box::new((
                RedisConfig {
                    addrs: conn_infos.into_iter().collect(),
                    service_name: service_name.as_ref().to_string(),
                    node_conn_info: None,
                    server_type,
                },
                pool_builder,
            )))),
        }
    }

    pub fn with_conn_infos_and_node_conn_info_and_pool_builder<I, S>(
        conn_infos: I,
        service_name: S,
        server_type: SentinelServerType,
        node_conn_info: SentinelNodeConnectionInfo,
        pool_builder: Builder<LockedSentinelClient>,
    ) -> Self
    where
        I: IntoIterator<Item = ConnectionInfo>,
        S: AsRef<str>,
    {
        Self {
            pool: Some(RedisPool::ConnInfoConfig(Box::new((
                RedisConfig {
                    addrs: conn_infos.into_iter().collect(),
                    service_name: service_name.as_ref().to_string(),
                    node_conn_info: Some(node_conn_info),
                    server_type,
                },
                pool_builder,
            )))),
        }
    }

    pub fn with_client_builder(client_builder: SentinelClientBuilder) -> Self {
        Self {
            pool: Some(RedisPool::ClientBuilderConfig(Box::new((
                client_builder,
                Pool::builder(),
            )))),
        }
    }

    pub fn with_client_builder_and_pool_builder(
        client_builder: SentinelClientBuilder,
        pool_builder: Builder<LockedSentinelClient>,
    ) -> Self {
        Self {
            pool: Some(RedisPool::ClientBuilderConfig(Box::new((
                client_builder,
                pool_builder,
            )))),
        }
    }
}

impl DataSrc<RedisSentinelDataConn> for RedisSentinelDataSrc {
    fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let pool_opt = mem::take(&mut self.pool);
        let pool_cfg = pool_opt.ok_or_else(|| errs::Err::new(RedisSentinelError::AlreadySetup))?;
        match pool_cfg {
            RedisPool::StringConfig(boxed_cfg) => {
                let (cfg, pool_builder) = *boxed_cfg;
                let addrs = cfg.addrs.iter().map(|s| s.as_str()).collect();
                let client = SentinelClient::build(
                    addrs,
                    cfg.service_name.clone(),
                    cfg.node_conn_info,
                    cfg.server_type.clone(),
                )
                .map_err(|e| {
                    errs::Err::with_source(
                        RedisSentinelError::FailToBuildSentinelClientOfAddrs {
                            addrs: cfg.addrs.clone(),
                            service_name: cfg.service_name.clone(),
                            server_type: cfg.server_type.clone(),
                        },
                        e,
                    )
                })?;
                let pool = pool_builder
                    .build(LockedSentinelClient::new(client))
                    .map_err(|e| {
                        errs::Err::with_source(
                            RedisSentinelError::FailToBuildPoolOfAddrs {
                                addrs: cfg.addrs,
                                service_name: cfg.service_name,
                                server_type: cfg.server_type,
                            },
                            e,
                        )
                    })?;
                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            RedisPool::ConnAddrConfig(boxed_cfg) => {
                let (cfg, pool_builder) = *boxed_cfg;
                let client = SentinelClient::build(
                    cfg.addrs.clone(),
                    cfg.service_name.clone(),
                    cfg.node_conn_info,
                    cfg.server_type.clone(),
                )
                .map_err(|e| {
                    errs::Err::with_source(
                        RedisSentinelError::FailToBuildSentinelClientOfConnAddrs {
                            conn_addrs: cfg.addrs.clone(),
                            service_name: cfg.service_name.clone(),
                            server_type: cfg.server_type.clone(),
                        },
                        e,
                    )
                })?;
                let pool = pool_builder
                    .build(LockedSentinelClient::new(client))
                    .map_err(|e| {
                        errs::Err::with_source(
                            RedisSentinelError::FailToBuildPoolOfConnAddrs {
                                conn_addrs: cfg.addrs,
                                service_name: cfg.service_name,
                                server_type: cfg.server_type,
                            },
                            e,
                        )
                    })?;
                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            RedisPool::ConnInfoConfig(boxed_cfg) => {
                let (cfg, pool_builder) = *boxed_cfg;
                let client = SentinelClient::build(
                    cfg.addrs.clone(),
                    cfg.service_name.clone(),
                    cfg.node_conn_info,
                    cfg.server_type.clone(),
                )
                .map_err(|e| {
                    errs::Err::with_source(
                        RedisSentinelError::FailToBuildSentinelClientOfConnInfos {
                            conn_infos: cfg.addrs.clone(),
                            service_name: cfg.service_name.clone(),
                            server_type: cfg.server_type.clone(),
                        },
                        e,
                    )
                })?;
                let pool = pool_builder
                    .build(LockedSentinelClient::new(client))
                    .map_err(|e| {
                        errs::Err::with_source(
                            RedisSentinelError::FailToBuildPoolOfConnInfos {
                                conn_infos: cfg.addrs,
                                service_name: cfg.service_name,
                                server_type: cfg.server_type,
                            },
                            e,
                        )
                    })?;
                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            RedisPool::ClientBuilderConfig(boxed_cfg) => {
                let (client_builder, pool_builder) = *boxed_cfg;
                let client = client_builder.build().map_err(|e| {
                    errs::Err::with_source(
                        RedisSentinelError::FailToBuildSentinelClientWithClientBuilder,
                        e,
                    )
                })?;
                let pool = pool_builder
                    .build(LockedSentinelClient::new(client))
                    .map_err(|e| {
                        errs::Err::with_source(
                            RedisSentinelError::FailToBuildPoolWithClientBuilder,
                            e,
                        )
                    })?;
                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            _ => Err(errs::Err::new(RedisSentinelError::AlreadySetup)),
        }
    }

    fn close(&mut self) {}

    fn create_data_conn(&mut self) -> errs::Result<Box<RedisSentinelDataConn>> {
        let pool = self
            .pool
            .as_mut()
            .ok_or_else(|| errs::Err::new(RedisSentinelError::NotSetupYet))?;
        match pool {
            RedisPool::Object(pool) => match pool.get() {
                Ok(conn) => Ok(Box::new(RedisSentinelDataConn::new(conn))),
                Err(e) => Err(errs::Err::with_source(
                    RedisSentinelError::FailToGetConnectionFromPool,
                    e,
                )),
            },
            _ => Err(errs::Err::new(RedisSentinelError::NotSetupYet)),
        }
    }
}

#[cfg(test)]
mod unit_tests_of_data_src {
    use super::*;

    mod test_new {
        use super::*;
        use url::Url;

        #[test]
        fn addrs_are_strs_and_ok() {
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
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_strs_and_fail() {
            let mut ds = RedisSentinelDataSrc::new(
                &["xxxx", "yyyy", "zzzz"],
                "mymaster",
                SentinelServerType::Master,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["xxxx", "yyyy", "zzzz",]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Redis URL did not parse - InvalidClientConfig",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_strings_and_ok() {
            let mut ds = RedisSentinelDataSrc::new(
                &[
                    "redis://127.0.0.1:26479".to_string(),
                    "redis://127.0.0.1:26480".to_string(),
                    "redis://127.0.0.1:26481".to_string(),
                ],
                "mymaster",
                SentinelServerType::Master,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_strings_and_fail() {
            let mut ds = RedisSentinelDataSrc::new(
                &["xxxx".to_string(), "yyyy".to_string(), "zzzz".to_string()],
                "mymaster",
                SentinelServerType::Master,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["xxxx", "yyyy", "zzzz",]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Redis URL did not parse - InvalidClientConfig",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_urls_and_ok() {
            let Ok(url0) = Url::parse("redis://127.0.0.1:26479") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://127.0.0.1:26480") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://127.0.0.1:26481") else {
                panic!("bad url2");
            };
            let mut ds = RedisSentinelDataSrc::new(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
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
            let mut ds = RedisSentinelDataSrc::new(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["redis://", "redis://", "redis://"]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Missing hostname - InvalidClientConfig",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_node_conn_info {
        use super::*;
        use url::Url;

        #[test]
        fn addrs_are_strs_and_ok() {
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_node_conn_info(
                &[
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_strs_and_fail() {
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_node_conn_info(
                &["xxxx", "yyyy", "zzzz"],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["xxxx", "yyyy", "zzzz",]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Redis URL did not parse - InvalidClientConfig",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_strings_and_ok() {
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_node_conn_info(
                &[
                    "redis://127.0.0.1:26479".to_string(),
                    "redis://127.0.0.1:26480".to_string(),
                    "redis://127.0.0.1:26481".to_string(),
                ],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_strings_and_fail() {
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_node_conn_info(
                &["xxxx".to_string(), "yyyy".to_string(), "zzzz".to_string()],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["xxxx", "yyyy", "zzzz",]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Redis URL did not parse - InvalidClientConfig",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_urls_and_ok() {
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
            let mut ds = RedisSentinelDataSrc::with_node_conn_info(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
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
            let mut ds = RedisSentinelDataSrc::with_node_conn_info(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["redis://", "redis://", "redis://"]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Missing hostname - InvalidClientConfig",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_pool_builder {
        use super::*;
        use r2d2::Pool;
        use url::Url;

        #[test]
        fn addrs_are_strs_and_ok() {
            let pb = Pool::builder().max_size(15);
            let mut ds = RedisSentinelDataSrc::with_pool_builder(
                &[
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
                SentinelServerType::Master,
                pb,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_strs_and_fail() {
            let pb = Pool::builder().max_size(15);
            let mut ds = RedisSentinelDataSrc::with_pool_builder(
                &["redis://111", "redis://111", "redis://111"],
                "mymaster",
                SentinelServerType::Master,
                pb,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildPoolOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["redis://111", "redis://111", "redis://111",]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"No route to host (os error 65)\"))",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_strings_and_ok() {
            let pb = Pool::builder().max_size(15);
            let mut ds = RedisSentinelDataSrc::with_pool_builder(
                &[
                    "redis://127.0.0.1:26479".to_string(),
                    "redis://127.0.0.1:26480".to_string(),
                    "redis://127.0.0.1:26481".to_string(),
                ],
                "mymaster",
                SentinelServerType::Master,
                pb,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_strings_and_fail() {
            let pb = Pool::builder().max_size(15);
            let mut ds = RedisSentinelDataSrc::with_pool_builder(
                &["xxxx".to_string(), "yyyy".to_string(), "zzzz".to_string()],
                "mymaster",
                SentinelServerType::Master,
                pb,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["xxxx", "yyyy", "zzzz",]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Redis URL did not parse - InvalidClientConfig",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_urls_and_ok() {
            let pb = Pool::builder().max_size(15);
            let Ok(url0) = Url::parse("redis://127.0.0.1:26479") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://127.0.0.1:26480") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://127.0.0.1:26481") else {
                panic!("bad url2");
            };
            let mut ds = RedisSentinelDataSrc::with_pool_builder(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
                pb,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_urls_and_fail() {
            let pb = Pool::builder().max_size(15);
            let Ok(url0) = Url::parse("redis://") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://") else {
                panic!("bad url2");
            };
            let mut ds = RedisSentinelDataSrc::with_pool_builder(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
                pb,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["redis://", "redis://", "redis://"]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Missing hostname - InvalidClientConfig",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_node_conn_info_and_pool_builder {
        use super::*;
        use url::Url;

        #[test]
        fn addrs_are_strs_and_ok() {
            let pb = Pool::builder().max_size(15);
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_node_conn_info_and_pool_builder(
                &[
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
                pb,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_strs_and_fail() {
            let pb = Pool::builder().max_size(15);
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_node_conn_info_and_pool_builder(
                &["xxxx", "yyyy", "zzzz"],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
                pb,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["xxxx", "yyyy", "zzzz",]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Redis URL did not parse - InvalidClientConfig",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_strings_and_ok() {
            let pb = Pool::builder().max_size(15);
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_node_conn_info_and_pool_builder(
                &[
                    "redis://127.0.0.1:26479".to_string(),
                    "redis://127.0.0.1:26480".to_string(),
                    "redis://127.0.0.1:26481".to_string(),
                ],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
                pb,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_strings_and_fail() {
            let pb = Pool::builder().max_size(15);
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_node_conn_info_and_pool_builder(
                &["xxxx".to_string(), "yyyy".to_string(), "zzzz".to_string()],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
                pb,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["xxxx", "yyyy", "zzzz",]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Redis URL did not parse - InvalidClientConfig",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_urls_and_ok() {
            let Ok(url0) = Url::parse("redis://127.0.0.1:26479") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://127.0.0.1:26480") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://127.0.0.1:26481") else {
                panic!("bad url2");
            };
            let pb = Pool::builder().max_size(15);
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_node_conn_info_and_pool_builder(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
                pb,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
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
            let pb = Pool::builder().max_size(15);
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_node_conn_info_and_pool_builder(
                &[url0, url1, url2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
                pb,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildSentinelClientOfAddrs {
                addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["redis://", "redis://", "redis://"]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Missing hostname - InvalidClientConfig",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_conn_addrs {
        use super::*;

        #[test]
        fn ok() {
            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26479);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26480);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26481);
            let mut ds = RedisSentinelDataSrc::with_conn_addrs(
                vec![conn_addr0, conn_addr1, conn_addr2],
                "mymaster",
                SentinelServerType::Master,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn fail() {
            let mut ds = RedisSentinelDataSrc::with_conn_addrs(
                vec![],
                "mymaster",
                SentinelServerType::Master,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildSentinelClientOfConnAddrs {
                conn_addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(conn_addrs, &[] as &[redis::ConnectionAddr]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "At least one sentinel is required - EmptySentinelList",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_conn_addrs_and_node_conn_info {
        use super::*;

        #[test]
        fn ok() {
            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26479);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26480);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26481);
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_conn_addrs_and_node_conn_info(
                vec![conn_addr0, conn_addr1, conn_addr2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn fail() {
            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1234);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1235);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1236);
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_conn_addrs_and_node_conn_info(
                vec![conn_addr0.clone(), conn_addr1.clone(), conn_addr2.clone()],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildPoolOfConnAddrs {
                conn_addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(conn_addrs, &[conn_addr0, conn_addr1, conn_addr2]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"Connection refused (os error 61)\"))",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_conn_addrs_and_pool_builder {
        use super::*;
        use r2d2::Pool;

        #[test]
        fn ok() {
            let pb = Pool::builder().max_size(15);
            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26479);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26480);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26481);
            let mut ds = RedisSentinelDataSrc::with_conn_addrs_and_pool_builder(
                vec![conn_addr0, conn_addr1, conn_addr2],
                "mymaster",
                SentinelServerType::Master,
                pb,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn fail() {
            let pb = Pool::builder().max_size(15);
            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1234);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1235);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1236);
            let mut ds = RedisSentinelDataSrc::with_conn_addrs_and_pool_builder(
                vec![conn_addr0.clone(), conn_addr1.clone(), conn_addr2.clone()],
                "mymaster",
                SentinelServerType::Master,
                pb,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildPoolOfConnAddrs {
                conn_addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(conn_addrs, &[conn_addr0, conn_addr1, conn_addr2]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"Connection refused (os error 61)\"))",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_conn_addrs_and_node_conn_info_and_pool_builder {
        use super::*;
        use r2d2::Pool;

        #[test]
        fn ok() {
            let pb = Pool::builder().max_size(15);
            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26479);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26480);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26481);
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_conn_addrs_and_node_conn_info_and_pool_builder(
                vec![conn_addr0, conn_addr1, conn_addr2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
                pb,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn fail() {
            let pb = Pool::builder().max_size(15);
            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1234);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1235);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1236);
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_conn_addrs_and_node_conn_info_and_pool_builder(
                vec![conn_addr0.clone(), conn_addr1.clone(), conn_addr2.clone()],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
                pb,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildPoolOfConnAddrs {
                conn_addrs,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(conn_addrs, &[conn_addr0, conn_addr1, conn_addr2]);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"Connection refused (os error 61)\"))",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_conn_infos {
        use super::*;
        use redis::IntoConnectionInfo;

        #[test]
        fn ok() {
            let conn_info0 = "redis://127.0.0.1:26479/0".into_connection_info().unwrap();
            let conn_info1 = "redis://127.0.0.1:26480/0".into_connection_info().unwrap();
            let conn_info2 = "redis://127.0.0.1:26481/0".into_connection_info().unwrap();
            let mut ds = RedisSentinelDataSrc::with_conn_infos(
                vec![conn_info0, conn_info1, conn_info2],
                "mymaster",
                SentinelServerType::Master,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn fail() {
            let mut ds = RedisSentinelDataSrc::with_conn_infos(
                vec![],
                "mymaster",
                SentinelServerType::Master,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildSentinelClientOfConnInfos {
                conn_infos,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(conn_infos.len(), 0);
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "At least one sentinel is required - EmptySentinelList",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_conn_infos_and_node_conn_info {
        use super::*;
        use redis::IntoConnectionInfo;

        #[test]
        fn ok() {
            let conn_info0 = "redis://127.0.0.1:26479/0".into_connection_info().unwrap();
            let conn_info1 = "redis://127.0.0.1:26480/0".into_connection_info().unwrap();
            let conn_info2 = "redis://127.0.0.1:26481/0".into_connection_info().unwrap();
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_conn_infos_and_node_conn_info(
                vec![conn_info0, conn_info1, conn_info2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn fail() {
            let conn_info0 = "redis://127.0.0.1:1234/0".into_connection_info().unwrap();
            let conn_info1 = "redis://127.0.0.1:1235/0".into_connection_info().unwrap();
            let conn_info2 = "redis://127.0.0.1:1236/0".into_connection_info().unwrap();
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_conn_infos_and_node_conn_info(
                vec![conn_info0.clone(), conn_info1.clone(), conn_info2.clone()],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildPoolOfConnInfos {
                conn_infos,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(conn_infos.len(), 3);
            assert_eq!(conn_infos[0].addr(), conn_info0.addr());
            assert_eq!(conn_infos[1].addr(), conn_info1.addr());
            assert_eq!(conn_infos[2].addr(), conn_info2.addr());
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"Connection refused (os error 61)\"))",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_conn_infos_and_pool_builder {
        use super::*;
        use r2d2::Pool;
        use redis::IntoConnectionInfo;

        #[test]
        fn ok() {
            let pb = Pool::builder().max_size(15);
            let conn_info0 = "redis://127.0.0.1:26479/0".into_connection_info().unwrap();
            let conn_info1 = "redis://127.0.0.1:26480/0".into_connection_info().unwrap();
            let conn_info2 = "redis://127.0.0.1:26481/0".into_connection_info().unwrap();
            let mut ds = RedisSentinelDataSrc::with_conn_infos_and_pool_builder(
                vec![conn_info0, conn_info1, conn_info2],
                "mymaster",
                SentinelServerType::Master,
                pb,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn fail() {
            let pb = Pool::builder().max_size(15);
            let conn_info0 = "redis://127.0.0.1:1234/0".into_connection_info().unwrap();
            let conn_info1 = "redis://127.0.0.1:1235/0".into_connection_info().unwrap();
            let conn_info2 = "redis://127.0.0.1:1236/0".into_connection_info().unwrap();
            let mut ds = RedisSentinelDataSrc::with_conn_infos_and_pool_builder(
                vec![conn_info0.clone(), conn_info1.clone(), conn_info2.clone()],
                "mymaster",
                SentinelServerType::Master,
                pb,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildPoolOfConnInfos {
                conn_infos,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(conn_infos.len(), 3);
            assert_eq!(conn_infos[0].addr(), conn_info0.addr());
            assert_eq!(conn_infos[1].addr(), conn_info1.addr());
            assert_eq!(conn_infos[2].addr(), conn_info2.addr());
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"Connection refused (os error 61)\"))",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_conn_infos_and_node_conn_info_and_pool_builder {
        use super::*;
        use r2d2::Pool;
        use redis::IntoConnectionInfo;

        #[test]
        fn ok() {
            let pb = Pool::builder().max_size(15);
            let conn_info0 = "redis://127.0.0.1:26479/0".into_connection_info().unwrap();
            let conn_info1 = "redis://127.0.0.1:26480/0".into_connection_info().unwrap();
            let conn_info2 = "redis://127.0.0.1:26481/0".into_connection_info().unwrap();
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_conn_infos_and_node_conn_info_and_pool_builder(
                vec![conn_info0, conn_info1, conn_info2],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
                pb,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn fail() {
            let pb = Pool::builder().max_size(15);
            let conn_info0 = "redis://127.0.0.1:1234/0".into_connection_info().unwrap();
            let conn_info1 = "redis://127.0.0.1:1235/0".into_connection_info().unwrap();
            let conn_info2 = "redis://127.0.0.1:1236/0".into_connection_info().unwrap();
            let node_conn_info = SentinelNodeConnectionInfo::default();
            let mut ds = RedisSentinelDataSrc::with_conn_infos_and_node_conn_info_and_pool_builder(
                vec![conn_info0.clone(), conn_info1.clone(), conn_info2.clone()],
                "mymaster",
                SentinelServerType::Master,
                node_conn_info,
                pb,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildPoolOfConnInfos {
                conn_infos,
                service_name,
                server_type,
            }) = err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(conn_infos.len(), 3);
            assert_eq!(conn_infos[0].addr(), conn_info0.addr());
            assert_eq!(conn_infos[1].addr(), conn_info1.addr());
            assert_eq!(conn_infos[2].addr(), conn_info2.addr());
            assert_eq!(service_name, "mymaster");
            assert_eq!(format!("{:?}", server_type), "Master");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"Connection refused (os error 61)\"))",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_client_builder {
        use super::*;

        #[test]
        fn ok() {
            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26479);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26480);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26481);
            let client_builder = SentinelClientBuilder::new(
                vec![conn_addr0, conn_addr1, conn_addr2],
                "mymaster",
                SentinelServerType::Master,
            )
            .unwrap();
            let mut ds = RedisSentinelDataSrc::with_client_builder(client_builder);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn fail() {
            let client_builder =
                SentinelClientBuilder::new(vec![], "mymaster", SentinelServerType::Master).unwrap();
            let mut ds = RedisSentinelDataSrc::with_client_builder(client_builder);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildSentinelClientWithClientBuilder) =
                err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "At least one sentinel is required - EmptySentinelList",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_client_builder_and_pool_builder {
        use super::*;

        #[test]
        fn ok() {
            let pb = Pool::builder().max_size(15);
            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26479);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26480);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26481);
            let client_builder = SentinelClientBuilder::new(
                vec![conn_addr0, conn_addr1, conn_addr2],
                "mymaster",
                SentinelServerType::Master,
            )
            .unwrap();
            let mut ds =
                RedisSentinelDataSrc::with_client_builder_and_pool_builder(client_builder, pb);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn fail() {
            let pb = Pool::builder().max_size(15);
            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1234);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1235);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1236);
            let client_builder = SentinelClientBuilder::new(
                vec![conn_addr0, conn_addr1, conn_addr2],
                "mymaster",
                SentinelServerType::Master,
            )
            .unwrap();
            let mut ds =
                RedisSentinelDataSrc::with_client_builder_and_pool_builder(client_builder, pb);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::FailToBuildPoolWithClientBuilder) =
                err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"Connection refused (os error 61)\"))",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_create_data_conn {
        use super::*;
        use redis::TypedCommands;

        #[test]
        fn ok() {
            const KEY: &str = "test_create_data_conn/sentinel";

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
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());

            let Ok(mut data_conn) = ds.create_data_conn() else {
                panic!("fail to create data_conn");
            };
            let redis_conn = data_conn.get_connection();

            redis_conn.set(KEY, "1").unwrap();
            let s = redis_conn.get(KEY).unwrap();
            redis_conn.del(KEY).unwrap();
            assert_eq!(s, Some("1".to_string()));

            ds.close();
        }

        #[test]
        fn fail() {
            let pb = Pool::builder()
                .max_size(1)
                .connection_timeout(std::time::Duration::from_secs(1));

            let mut ds = RedisSentinelDataSrc::with_pool_builder(
                &[
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
                SentinelServerType::Master,
                pb,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());

            let Ok(_data_conn) = ds.create_data_conn() else {
                panic!("fail to create data_conn");
            };
            let Err(err) = ds.create_data_conn() else {
                panic!("fail to create data_conn");
            };
            let Ok(RedisSentinelError::FailToGetConnectionFromPool) =
                err.reason::<RedisSentinelError>()
            else {
                panic!();
            };
            assert_eq!(format!("{:?}", err.source().unwrap()), "Error(None)",);

            ds.close();
        }

        #[test]
        fn not_setup_yet() {
            let mut ds = RedisSentinelDataSrc::new(
                &[
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
                SentinelServerType::Master,
            );
            let Err(err) = ds.create_data_conn() else {
                panic!("fail to create data_conn");
            };
            let Ok(RedisSentinelError::NotSetupYet) = err.reason::<RedisSentinelError>() else {
                panic!();
            };
        }
    }

    mod test_setup {
        use super::*;

        #[test]
        fn fail_due_to_setup_twice() {
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
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());

            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisSentinelError::AlreadySetup) = err.reason::<RedisSentinelError>() else {
                panic!("{err:?}");
            };
            let errors = ag.join();
            assert!(errors.is_empty());

            ds.close();
        }
    }
}

#[cfg(test)]
mod unit_tests_of_data_conn {
    use super::*;

    mod test_add_pre_commit {
        use super::*;
        use redis::TypedCommands;

        #[test]
        fn ok() {
            const KEY: &str = "test_add_pre_commit/sentinel";

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
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());

            let Ok(mut data_conn) = ds.create_data_conn() else {
                panic!("fail to create data_conn");
            };
            assert!(data_conn.should_force_back());

            data_conn.add_pre_commit(|redis_conn| {
                redis_conn.set(KEY, "1").unwrap();
                Ok(())
            });

            let mut ag = AsyncGroup::new();
            data_conn.pre_commit(&mut ag).unwrap();
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                redis_conn.del(KEY).unwrap();
                assert_eq!(s, Some("1".to_string()));
            }

            let mut ag = AsyncGroup::new();
            data_conn.commit(&mut ag).unwrap();
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.post_commit(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.rollback(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.force_back(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            data_conn.close();
            ds.close();
        }

        #[test]
        fn fail() {
            const KEY: &str = "test_add_pre_commit/sentinel/fail";

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
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());

            let Ok(mut data_conn) = ds.create_data_conn() else {
                panic!("fail to create data_conn");
            };
            assert!(data_conn.should_force_back());

            data_conn.add_pre_commit(|redis_conn| {
                redis_conn.set(KEY, "1").unwrap();
                Err(errs::Err::new("fail"))
            });

            let mut ag = AsyncGroup::new();
            let Err(err) = data_conn.pre_commit(&mut ag) else {
                panic!();
            };
            let s = err.reason::<&str>().unwrap();
            assert_eq!(*s, "fail");

            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                redis_conn.del(KEY).unwrap();
                assert_eq!(s, Some("1".to_string()));
            }

            let mut ag = AsyncGroup::new();
            data_conn.commit(&mut ag).unwrap();
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.post_commit(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.rollback(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.force_back(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            data_conn.close();
            ds.close();
        }
    }

    mod test_add_post_commit {
        use super::*;
        use redis::TypedCommands;

        #[test]
        fn ok() {
            const KEY: &str = "test_add_post_commit/sentinel";

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
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());

            let Ok(mut data_conn) = ds.create_data_conn() else {
                panic!("fail to create data_conn");
            };
            assert!(data_conn.should_force_back());

            data_conn.add_post_commit(|redis_conn| {
                redis_conn.set(KEY, "1").unwrap();
                Ok(())
            });

            let mut ag = AsyncGroup::new();
            data_conn.pre_commit(&mut ag).unwrap();
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.commit(&mut ag).unwrap();
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.post_commit(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                redis_conn.del(KEY).unwrap();
                assert_eq!(s, Some("1".to_string()));
            }

            let mut ag = AsyncGroup::new();
            data_conn.rollback(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.force_back(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            data_conn.close();
            ds.close();
        }

        #[test]
        fn fail() {
            const KEY: &str = "test_add_post_commit/sentinel/fail";

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
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());

            let Ok(mut data_conn) = ds.create_data_conn() else {
                panic!("fail to create data_conn");
            };
            assert!(data_conn.should_force_back());

            data_conn.add_post_commit(|redis_conn| {
                redis_conn.set(KEY, "1").unwrap();
                Err(errs::Err::new("fail"))
            });

            let mut ag = AsyncGroup::new();
            if let Err(err) = data_conn.pre_commit(&mut ag) {
                panic!("{:?}", err);
            };

            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.commit(&mut ag).unwrap();
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.post_commit(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                redis_conn.del(KEY).unwrap();
                assert_eq!(s, Some("1".to_string()));
            }

            let mut ag = AsyncGroup::new();
            data_conn.rollback(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.force_back(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            data_conn.close();
            ds.close();
        }
    }

    mod test_add_force_back {
        use super::*;
        use redis::TypedCommands;

        #[test]
        fn ok() {
            const KEY: &str = "test_add_force_back/sentinel";

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
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());

            let Ok(mut data_conn) = ds.create_data_conn() else {
                panic!("fail to create data_conn");
            };
            assert!(data_conn.should_force_back());

            data_conn.add_force_back(|redis_conn| {
                redis_conn.set(KEY, "1").unwrap();
                Ok(())
            });

            let mut ag = AsyncGroup::new();
            data_conn.pre_commit(&mut ag).unwrap();
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.commit(&mut ag).unwrap();
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.post_commit(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.rollback(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.force_back(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                redis_conn.del(KEY).unwrap();
                assert_eq!(s, Some("1".to_string()));
            }

            data_conn.close();
            ds.close();
        }

        #[test]
        fn fail() {
            const KEY: &str = "test_add_force_back/sentinel/fail";

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
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());

            let Ok(mut data_conn) = ds.create_data_conn() else {
                panic!("fail to create data_conn");
            };
            assert!(data_conn.should_force_back());

            data_conn.add_force_back(|redis_conn| {
                redis_conn.set(KEY, "1").unwrap();
                Err(errs::Err::new("fail"))
            });

            let mut ag = AsyncGroup::new();
            if let Err(err) = data_conn.pre_commit(&mut ag) {
                panic!("{:?}", err);
            };

            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.commit(&mut ag).unwrap();
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.post_commit(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.rollback(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.force_back(&mut ag);
            let errors = ag.join();
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).unwrap();
                redis_conn.del(KEY).unwrap();
                assert_eq!(s, Some("1".to_string()));
            }

            data_conn.close();
            ds.close();
        }
    }
}
