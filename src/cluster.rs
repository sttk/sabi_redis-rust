// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use sabi::{AsyncGroup, DataConn, DataSrc};

use r2d2::{Builder, Pool, PooledConnection};
use redis::cluster::{ClusterClient, ClusterClientBuilder, ClusterConnection};
use redis::{ConnectionAddr, ConnectionInfo};

use std::fmt::Debug;
use std::mem;

#[derive(Debug)]
pub enum RedisClusterError {
    NotSetupYet,
    AlreadySetup,
    FailToBuildClientOfAddrs { addrs: Vec<String> },
    FailToBuildPoolOfAddrs { addrs: Vec<String> },
    FailToBuildClientOfConnAddrs { conn_addrs: Vec<ConnectionAddr> },
    FailToBuildPoolOfConnAddrs { conn_addrs: Vec<ConnectionAddr> },
    FailToBuildClientOfConnInfos { conn_infos: Vec<ConnectionInfo> },
    FailToBuildPoolOfConnInfos { conn_infos: Vec<ConnectionInfo> },
    FailToBuildClientWithClientBuilder,
    FailToBuildPoolWithClientBuilder,
    FailToGetConnectionFromPool,
}

#[allow(clippy::type_complexity)]
pub struct RedisClusterDataConn {
    conn: PooledConnection<ClusterClient>,
    pre_commit_vec: Vec<Box<dyn FnMut(&mut ClusterConnection) -> errs::Result<()>>>,
    post_commit_vec: Vec<Box<dyn FnMut(&mut ClusterConnection) -> errs::Result<()>>>,
    force_back_vec: Vec<Box<dyn FnMut(&mut ClusterConnection) -> errs::Result<()>>>,
}

impl RedisClusterDataConn {
    fn new(conn: PooledConnection<ClusterClient>) -> Self {
        Self {
            conn,
            pre_commit_vec: Vec::new(),
            post_commit_vec: Vec::new(),
            force_back_vec: Vec::new(),
        }
    }

    pub fn get_connection(&mut self) -> &mut PooledConnection<ClusterClient> {
        &mut self.conn
    }

    pub fn add_pre_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut ClusterConnection) -> errs::Result<()> + 'static,
    {
        self.pre_commit_vec.push(Box::new(f));
    }

    pub fn add_post_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut ClusterConnection) -> errs::Result<()> + 'static,
    {
        self.post_commit_vec.push(Box::new(f));
    }

    pub fn add_force_back<F>(&mut self, f: F)
    where
        F: FnMut(&mut ClusterConnection) -> errs::Result<()> + 'static,
    {
        self.force_back_vec.push(Box::new(f));
    }
}

impl DataConn for RedisClusterDataConn {
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

pub struct RedisClusterDataSrc {
    pool: Option<RedisPool>,
}

enum RedisPool {
    Object(Pool<ClusterClient>),
    StringConfig(Box<(Vec<String>, Builder<ClusterClient>)>),
    ConnAddrConfig(Box<(Vec<ConnectionAddr>, Builder<ClusterClient>)>),
    ConnInfoConfig(Box<(Vec<ConnectionInfo>, Builder<ClusterClient>)>),
    ClientBuilderConfig(Box<(ClusterClientBuilder, Builder<ClusterClient>)>),
}

impl RedisClusterDataSrc {
    pub fn new<I>(addrs: I) -> Self
    where
        I: IntoIterator<Item: AsRef<str>>,
    {
        let addrs = addrs.into_iter().map(|s| s.as_ref().to_string()).collect();
        Self {
            pool: Some(RedisPool::StringConfig(Box::new((addrs, Pool::builder())))),
        }
    }

    pub fn with_pool_builder<I>(addrs: I, pool_builder: Builder<ClusterClient>) -> Self
    where
        I: IntoIterator<Item: AsRef<str>>,
    {
        let addrs = addrs.into_iter().map(|s| s.as_ref().to_string()).collect();
        Self {
            pool: Some(RedisPool::StringConfig(Box::new((addrs, pool_builder)))),
        }
    }

    pub fn with_conn_addrs<I>(conn_addrs: I) -> Self
    where
        I: IntoIterator<Item = ConnectionAddr>,
    {
        let conn_addrs = conn_addrs.into_iter().collect();
        Self {
            pool: Some(RedisPool::ConnAddrConfig(Box::new((
                conn_addrs,
                Pool::builder(),
            )))),
        }
    }

    pub fn with_conn_addrs_and_pool_builder<I>(
        conn_addrs: I,
        pool_builder: Builder<ClusterClient>,
    ) -> Self
    where
        I: IntoIterator<Item = ConnectionAddr>,
    {
        let conn_addrs = conn_addrs.into_iter().collect();
        Self {
            pool: Some(RedisPool::ConnAddrConfig(Box::new((
                conn_addrs,
                pool_builder,
            )))),
        }
    }

    pub fn with_conn_infos<I>(conn_infos: I) -> Self
    where
        I: IntoIterator<Item = ConnectionInfo>,
    {
        let conn_infos = conn_infos.into_iter().collect();
        Self {
            pool: Some(RedisPool::ConnInfoConfig(Box::new((
                conn_infos,
                Pool::builder(),
            )))),
        }
    }

    pub fn with_conn_infos_and_pool_builder<I>(
        conn_infos: I,
        pool_builder: Builder<ClusterClient>,
    ) -> Self
    where
        I: IntoIterator<Item = ConnectionInfo>,
    {
        let conn_infos = conn_infos.into_iter().collect();
        Self {
            pool: Some(RedisPool::ConnInfoConfig(Box::new((
                conn_infos,
                pool_builder,
            )))),
        }
    }

    pub fn with_client_builder(client_builder: ClusterClientBuilder) -> Self {
        Self {
            pool: Some(RedisPool::ClientBuilderConfig(Box::new((
                client_builder,
                Pool::builder(),
            )))),
        }
    }

    pub fn with_client_builder_and_pool_builder(
        client_builder: ClusterClientBuilder,
        pool_builder: Builder<ClusterClient>,
    ) -> Self {
        Self {
            pool: Some(RedisPool::ClientBuilderConfig(Box::new((
                client_builder,
                pool_builder,
            )))),
        }
    }
}

impl DataSrc<RedisClusterDataConn> for RedisClusterDataSrc {
    fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let pool_opt = mem::take(&mut self.pool);
        let pool_cfg = pool_opt.ok_or_else(|| errs::Err::new(RedisClusterError::AlreadySetup))?;
        match pool_cfg {
            RedisPool::StringConfig(boxed_cfg) => {
                let (addrs, pool_builder) = *boxed_cfg;
                let client = ClusterClient::new(addrs.clone()).map_err(|e| {
                    errs::Err::with_source(
                        RedisClusterError::FailToBuildClientOfAddrs {
                            addrs: addrs.clone(),
                        },
                        e,
                    )
                })?;
                let pool = pool_builder.build(client).map_err(|e| {
                    errs::Err::with_source(RedisClusterError::FailToBuildPoolOfAddrs { addrs }, e)
                })?;
                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            RedisPool::ConnAddrConfig(boxed_cfg) => {
                let (conn_addrs, pool_builder) = *boxed_cfg;
                let client = ClusterClient::new(conn_addrs.clone()).map_err(|e| {
                    errs::Err::with_source(
                        RedisClusterError::FailToBuildClientOfConnAddrs {
                            conn_addrs: conn_addrs.clone(),
                        },
                        e,
                    )
                })?;
                let pool = pool_builder.build(client).map_err(|e| {
                    errs::Err::with_source(
                        RedisClusterError::FailToBuildPoolOfConnAddrs { conn_addrs },
                        e,
                    )
                })?;
                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            RedisPool::ConnInfoConfig(boxed_cfg) => {
                let (conn_infos, pool_builder) = *boxed_cfg;
                let client = ClusterClient::new(conn_infos.clone()).map_err(|e| {
                    errs::Err::with_source(
                        RedisClusterError::FailToBuildClientOfConnInfos {
                            conn_infos: conn_infos.clone(),
                        },
                        e,
                    )
                })?;
                let pool = pool_builder.build(client).map_err(|e| {
                    errs::Err::with_source(
                        RedisClusterError::FailToBuildPoolOfConnInfos { conn_infos },
                        e,
                    )
                })?;
                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            RedisPool::ClientBuilderConfig(boxed_cfg) => {
                let (client_builder, pool_builder) = *boxed_cfg;
                let client = client_builder.build().map_err(|e| {
                    errs::Err::with_source(RedisClusterError::FailToBuildClientWithClientBuilder, e)
                })?;
                let pool = pool_builder.build(client).map_err(|e| {
                    errs::Err::with_source(RedisClusterError::FailToBuildPoolWithClientBuilder, e)
                })?;
                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            _ => Err(errs::Err::new(RedisClusterError::AlreadySetup)),
        }
    }

    fn close(&mut self) {}

    fn create_data_conn(&mut self) -> errs::Result<Box<RedisClusterDataConn>> {
        let pool = self
            .pool
            .as_mut()
            .ok_or_else(|| errs::Err::new(RedisClusterError::NotSetupYet))?;
        match pool {
            RedisPool::Object(pool) => match pool.get() {
                Ok(conn) => Ok(Box::new(RedisClusterDataConn::new(conn))),
                Err(e) => Err(errs::Err::with_source(
                    RedisClusterError::FailToGetConnectionFromPool,
                    e,
                )),
            },
            _ => Err(errs::Err::new(RedisClusterError::NotSetupYet)),
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
            let mut ds = RedisClusterDataSrc::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
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
            let mut ds = RedisClusterDataSrc::new(&["xxxx", "yyyy", "zzzz"]);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisClusterError::FailToBuildClientOfAddrs { addrs }) =
                err.reason::<RedisClusterError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["xxxx", "yyyy", "zzzz",]);
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
            let mut ds = RedisClusterDataSrc::new(&[
                "redis://127.0.0.1:7000".to_string(),
                "redis://127.0.0.1:7001".to_string(),
                "redis://127.0.0.1:7002".to_string(),
            ]);
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
            let mut ds =
                RedisClusterDataSrc::new(&["redis://xxxx", "redis://yyyy", "redis://zzzz"]);

            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisClusterError::FailToBuildPoolOfAddrs { addrs }) =
                err.reason::<RedisClusterError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["redis://xxxx", "redis://yyyy", "redis://zzzz",]);
            assert_eq!(format!("{:?}", err.source().unwrap()), "Error(Some(\"It failed to check startup nodes. - Io: Failed to connect to each cluster node (xxxx:6379: failed to lookup address information: nodename nor servname provided, or not known; yyyy:6379: failed to lookup address information: nodename nor servname provided, or not known; zzzz:6379: failed to lookup address information: nodename nor servname provided, or not known)\"))");
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addrs_are_urls_and_ok() {
            let Ok(url0) = Url::parse("redis://127.0.0.1:7000") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://127.0.0.1:7001") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://127.0.0.1:7002") else {
                panic!("bad url2");
            };
            let mut ds = RedisClusterDataSrc::new(&[url0, url1, url2]);
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
            let Ok(url0) = Url::parse("redis://xxxx") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://yyyy") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://zzzz") else {
                panic!("bad url2");
            };
            let mut ds = RedisClusterDataSrc::new(&[url0, url1, url2]);

            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisClusterError::FailToBuildPoolOfAddrs { addrs }) =
                err.reason::<RedisClusterError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["redis://xxxx", "redis://yyyy", "redis://zzzz",]);
            assert_eq!(format!("{:?}", err.source().unwrap()), "Error(Some(\"It failed to check startup nodes. - Io: Failed to connect to each cluster node (xxxx:6379: failed to lookup address information: nodename nor servname provided, or not known; yyyy:6379: failed to lookup address information: nodename nor servname provided, or not known; zzzz:6379: failed to lookup address information: nodename nor servname provided, or not known)\"))");
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_pool_builder {
        use super::*;
        use url::Url;

        #[test]
        fn addrs_are_strs_and_ok() {
            let pb = Pool::builder().max_size(15);
            let mut ds = RedisClusterDataSrc::with_pool_builder(
                &[
                    "redis://127.0.0.1:7000",
                    "redis://127.0.0.1:7001",
                    "redis://127.0.0.1:7002",
                ],
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
            let mut ds = RedisClusterDataSrc::with_pool_builder(&["xxxx", "yyyy", "zzzz"], pb);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisClusterError::FailToBuildClientOfAddrs { addrs }) =
                err.reason::<RedisClusterError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["xxxx", "yyyy", "zzzz",]);
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
            let mut ds = RedisClusterDataSrc::with_pool_builder(
                &[
                    "redis://127.0.0.1:7000".to_string(),
                    "redis://127.0.0.1:7001".to_string(),
                    "redis://127.0.0.1:7002".to_string(),
                ],
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
            let mut ds = RedisClusterDataSrc::with_pool_builder(
                &["xxxx".to_string(), "yyyy".to_string(), "zzzz".to_string()],
                pb,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisClusterError::FailToBuildClientOfAddrs { addrs }) =
                err.reason::<RedisClusterError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["xxxx", "yyyy", "zzzz",]);
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
            let Ok(url0) = Url::parse("redis://127.0.0.1:7000") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://127.0.0.1:7001") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://127.0.0.1:7002") else {
                panic!("bad url2");
            };
            let mut ds = RedisClusterDataSrc::with_pool_builder(&[url0, url1, url2], pb);
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
            let Ok(url0) = Url::parse("redis://xxxx") else {
                panic!("bad url0");
            };
            let Ok(url1) = Url::parse("redis://yyyy") else {
                panic!("bad url1");
            };
            let Ok(url2) = Url::parse("redis://zzzz") else {
                panic!("bad url2");
            };
            let mut ds = RedisClusterDataSrc::with_pool_builder(&[url0, url1, url2], pb);

            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisClusterError::FailToBuildPoolOfAddrs { addrs }) =
                err.reason::<RedisClusterError>()
            else {
                panic!();
            };
            assert_eq!(addrs, &["redis://xxxx", "redis://yyyy", "redis://zzzz",]);
            assert_eq!(format!("{:?}", err.source().unwrap()), "Error(Some(\"It failed to check startup nodes. - Io: Failed to connect to each cluster node (xxxx:6379: failed to lookup address information: nodename nor servname provided, or not known; yyyy:6379: failed to lookup address information: nodename nor servname provided, or not known; zzzz:6379: failed to lookup address information: nodename nor servname provided, or not known)\"))");
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_conn_addrs {
        use super::*;

        #[test]
        fn ok() {
            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 7000);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 7001);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 7002);
            let mut ds =
                RedisClusterDataSrc::with_conn_addrs(vec![conn_addr0, conn_addr1, conn_addr2]);
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
            let mut ds = RedisClusterDataSrc::with_conn_addrs(vec![]);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisClusterError::FailToBuildClientOfConnAddrs { conn_addrs }) =
                err.reason::<RedisClusterError>()
            else {
                panic!();
            };
            assert_eq!(conn_addrs, &[] as &[redis::ConnectionAddr]);
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Initial nodes can't be empty. - InvalidClientConfig",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_conn_addrs_and_pool_builder {
        use super::*;

        #[test]
        fn ok() {
            let pb = Pool::builder().max_size(15);
            let conn_addr0 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 7000);
            let conn_addr1 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 7001);
            let conn_addr2 = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 7002);
            let mut ds = RedisClusterDataSrc::with_conn_addrs_and_pool_builder(
                vec![conn_addr0, conn_addr1, conn_addr2],
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
            let mut ds = RedisClusterDataSrc::with_conn_addrs_and_pool_builder(
                vec![conn_addr0.clone(), conn_addr1.clone(), conn_addr2.clone()],
                pb,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisClusterError::FailToBuildPoolOfConnAddrs { conn_addrs }) =
                err.reason::<RedisClusterError>()
            else {
                panic!();
            };
            assert_eq!(conn_addrs, &[conn_addr0, conn_addr1, conn_addr2]);
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"It failed to check startup nodes. - Io: Failed to connect to each cluster node (127.0.0.1:1234: Connection refused (os error 61); 127.0.0.1:1235: Connection refused (os error 61); 127.0.0.1:1236: Connection refused (os error 61))\"))",
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
            let conn_info0 = "redis://127.0.0.1:7000/0".into_connection_info().unwrap();
            let conn_info1 = "redis://127.0.0.1:7001/0".into_connection_info().unwrap();
            let conn_info2 = "redis://127.0.0.1:7002/0".into_connection_info().unwrap();
            let mut ds =
                RedisClusterDataSrc::with_conn_infos(vec![conn_info0, conn_info1, conn_info2]);
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
            let mut ds = RedisClusterDataSrc::with_conn_infos(vec![]);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisClusterError::FailToBuildClientOfConnInfos { conn_infos }) =
                err.reason::<RedisClusterError>()
            else {
                panic!();
            };
            assert_eq!(conn_infos.len(), 0);
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Initial nodes can't be empty. - InvalidClientConfig"
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_conn_infos_and_pool_builder {
        use super::*;
        use redis::IntoConnectionInfo;

        #[test]
        fn ok() {
            let pb = Pool::builder().max_size(15);
            let conn_info0 = "redis://127.0.0.1:7000/0".into_connection_info().unwrap();
            let conn_info1 = "redis://127.0.0.1:7001/0".into_connection_info().unwrap();
            let conn_info2 = "redis://127.0.0.1:7002/0".into_connection_info().unwrap();
            let mut ds = RedisClusterDataSrc::with_conn_infos_and_pool_builder(
                vec![conn_info0, conn_info1, conn_info2],
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
            let mut ds = RedisClusterDataSrc::with_conn_infos_and_pool_builder(
                vec![conn_info0, conn_info1, conn_info2],
                pb,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisClusterError::FailToBuildPoolOfConnInfos { conn_infos }) =
                err.reason::<RedisClusterError>()
            else {
                panic!();
            };
            assert_eq!(conn_infos.len(), 3);
            assert_eq!(
                conn_infos[0].addr(),
                &redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1234)
            );
            assert_eq!(
                conn_infos[1].addr(),
                &redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1235)
            );
            assert_eq!(
                conn_infos[2].addr(),
                &redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1236)
            );
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"It failed to check startup nodes. - Io: Failed to connect to each cluster node (127.0.0.1:1234: Connection refused (os error 61); 127.0.0.1:1235: Connection refused (os error 61); 127.0.0.1:1236: Connection refused (os error 61))\"))",
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
            let client_builder = ClusterClientBuilder::new(vec![
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
            let mut ds = RedisClusterDataSrc::with_client_builder(client_builder);
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
            let client_builder = ClusterClientBuilder::new(vec![] as Vec<String>);
            let mut ds = RedisClusterDataSrc::with_client_builder(client_builder);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisClusterError::FailToBuildClientWithClientBuilder) =
                err.reason::<RedisClusterError>()
            else {
                panic!();
            };
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Initial nodes can't be empty. - InvalidClientConfig"
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
            let client_builder = ClusterClientBuilder::new(vec![
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
            let mut ds =
                RedisClusterDataSrc::with_client_builder_and_pool_builder(client_builder, pb);
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
            let client_builder = ClusterClientBuilder::new(vec![
                "redis://127.0.0.1:1234",
                "redis://127.0.0.1:1235",
                "redis://127.0.0.1:1236",
            ]);
            let mut ds =
                RedisClusterDataSrc::with_client_builder_and_pool_builder(client_builder, pb);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisClusterError::FailToBuildPoolWithClientBuilder) =
                err.reason::<RedisClusterError>()
            else {
                panic!();
            };
            assert_eq!(format!("{:?}", err.source().unwrap()), "Error(Some(\"It failed to check startup nodes. - Io: Failed to connect to each cluster node (127.0.0.1:1234: Connection refused (os error 61); 127.0.0.1:1235: Connection refused (os error 61); 127.0.0.1:1236: Connection refused (os error 61))\"))");
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

            let mut ds = RedisClusterDataSrc::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
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

            let mut ds = RedisClusterDataSrc::with_pool_builder(
                &[
                    "redis://127.0.0.1:7000",
                    "redis://127.0.0.1:7001",
                    "redis://127.0.0.1:7002",
                ],
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
            let Ok(RedisClusterError::FailToGetConnectionFromPool) =
                err.reason::<RedisClusterError>()
            else {
                panic!();
            };
            assert_eq!(format!("{:?}", err.source().unwrap()), "Error(None)",);

            ds.close();
        }

        #[test]
        fn not_setup_yet() {
            let mut ds = RedisClusterDataSrc::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
            let Err(err) = ds.create_data_conn() else {
                panic!("fail to create data_conn");
            };
            let Ok(RedisClusterError::NotSetupYet) = err.reason::<RedisClusterError>() else {
                panic!();
            };
        }
    }

    mod test_setup {
        use super::*;

        #[test]
        fn fail_due_to_setup_twice() {
            let mut ds = RedisClusterDataSrc::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);

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
            let Ok(RedisClusterError::AlreadySetup) = err.reason::<RedisClusterError>() else {
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

            let mut ds = RedisClusterDataSrc::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);

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

            let mut ds = RedisClusterDataSrc::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);

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

            let mut ds = RedisClusterDataSrc::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);

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

            let mut ds = RedisClusterDataSrc::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);

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

            let mut ds = RedisClusterDataSrc::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);

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

            let mut ds = RedisClusterDataSrc::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);

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
