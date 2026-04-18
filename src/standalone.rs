// Copyright (C) 2025-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use r2d2::{Builder, Pool, PooledConnection};
use redis::{Client, Connection, ConnectionAddr, ConnectionInfo};
use sabi::{AsyncGroup, DataConn, DataSrc};

use std::fmt::Debug;
use std::mem;

#[derive(Debug)]
pub enum RedisError {
    NotSetupYet,
    AlreadySetup,
    FailToOpenClientOfAddr { addr: String },
    FailToOpenClientOfConnAddr { conn_addr: ConnectionAddr },
    FailToOpenClientOfConnInfo { conn_info: ConnectionInfo },
    FailToBuildPool { conn_info: ConnectionInfo },
    FailToGetConnectionFromPool,
}

#[allow(clippy::type_complexity)]
pub struct RedisDataConn {
    conn: PooledConnection<Client>,
    pre_commit_vec: Vec<Box<dyn FnMut(&mut Connection) -> errs::Result<()>>>,
    post_commit_vec: Vec<Box<dyn FnMut(&mut Connection) -> errs::Result<()>>>,
    force_back_vec: Vec<Box<dyn FnMut(&mut Connection) -> errs::Result<()>>>,
}

impl RedisDataConn {
    fn new(conn: PooledConnection<Client>) -> Self {
        Self {
            conn,
            pre_commit_vec: Vec::new(),
            post_commit_vec: Vec::new(),
            force_back_vec: Vec::new(),
        }
    }

    pub fn get_connection(&mut self) -> &mut Connection {
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

impl DataConn for RedisDataConn {
    fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        for f in self.pre_commit_vec.iter_mut().rev() {
            f(&mut self.conn)?;
        }
        Ok(())
    }

    fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    fn post_commit(&mut self, _ag: &mut AsyncGroup) {
        for f in self.post_commit_vec.iter_mut().rev() {
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

pub struct RedisDataSrc {
    pool: Option<RedisPool>,
}

enum RedisPool {
    Object(Pool<Client>),
    StringConfig(String, Builder<Client>),
    ConnAddrConfig(ConnectionAddr, Builder<Client>),
    ConnInfoConfig(ConnectionInfo, Builder<Client>),
}

impl RedisDataSrc {
    pub fn new<S>(addr: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            pool: Some(RedisPool::StringConfig(
                addr.as_ref().to_string(),
                Pool::builder(),
            )),
        }
    }

    pub fn with_pool_builder<S>(addr: S, pool_builder: Builder<Client>) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            pool: Some(RedisPool::StringConfig(
                addr.as_ref().to_string(),
                pool_builder,
            )),
        }
    }

    pub fn with_conn_addr(conn_addr: ConnectionAddr) -> Self {
        Self {
            pool: Some(RedisPool::ConnAddrConfig(conn_addr, Pool::builder())),
        }
    }

    pub fn with_conn_addr_and_pool_builder(
        conn_addr: ConnectionAddr,
        pool_builder: Builder<Client>,
    ) -> Self {
        Self {
            pool: Some(RedisPool::ConnAddrConfig(conn_addr, pool_builder)),
        }
    }

    pub fn with_conn_info(conn_info: ConnectionInfo) -> Self {
        Self {
            pool: Some(RedisPool::ConnInfoConfig(conn_info, Pool::builder())),
        }
    }

    pub fn with_conn_info_and_pool_builder(
        conn_info: ConnectionInfo,
        pool_builder: Builder<Client>,
    ) -> Self {
        Self {
            pool: Some(RedisPool::ConnInfoConfig(conn_info, pool_builder)),
        }
    }
}

impl DataSrc<RedisDataConn> for RedisDataSrc {
    fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let pool_opt = mem::take(&mut self.pool);
        let pool = pool_opt.ok_or_else(|| errs::Err::new(RedisError::AlreadySetup))?;

        let (client, pool_builder) = match pool {
            RedisPool::StringConfig(addr, pool_builder) => {
                let client = Client::open(addr.as_str()).map_err(|e| {
                    errs::Err::with_source(RedisError::FailToOpenClientOfAddr { addr }, e)
                })?;
                (client, pool_builder)
            }
            RedisPool::ConnAddrConfig(conn_addr, pool_builder) => {
                let client = Client::open(conn_addr.clone()).map_err(|e| {
                    errs::Err::with_source(RedisError::FailToOpenClientOfConnAddr { conn_addr }, e)
                })?;
                (client, pool_builder)
            }
            RedisPool::ConnInfoConfig(conn_info, pool_builder) => {
                let client = Client::open(conn_info.clone()).map_err(|e| {
                    errs::Err::with_source(RedisError::FailToOpenClientOfConnInfo { conn_info }, e)
                })?;
                (client, pool_builder)
            }
            _ => return Err(errs::Err::new(RedisError::AlreadySetup)),
        };

        let conn_info = client.get_connection_info().clone();

        let pool = pool_builder
            .build(client)
            .map_err(|e| errs::Err::with_source(RedisError::FailToBuildPool { conn_info }, e))?;

        self.pool = Some(RedisPool::Object(pool));
        Ok(())
    }

    fn close(&mut self) {}

    fn create_data_conn(&mut self) -> errs::Result<Box<RedisDataConn>> {
        let pool = self
            .pool
            .as_mut()
            .ok_or_else(|| errs::Err::new(RedisError::NotSetupYet))?;

        match pool {
            RedisPool::Object(pool) => match pool.get() {
                Ok(conn) => Ok(Box::new(RedisDataConn::new(conn))),
                Err(e) => Err(errs::Err::with_source(
                    RedisError::FailToGetConnectionFromPool,
                    e,
                )),
            },
            _ => Err(errs::Err::new(RedisError::NotSetupYet)),
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
        fn addr_is_str_and_ok() {
            let mut ds = RedisDataSrc::new("redis://127.0.0.1:6379/0");
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addr_is_str_and_fail() {
            let mut ds = RedisDataSrc::new("xxxx");
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisError::FailToOpenClientOfAddr { addr }) = err.reason::<RedisError>() else {
                panic!();
            };
            assert_eq!(addr, "xxxx");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Redis URL did not parse - InvalidClientConfig"
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addr_is_string_and_ok() {
            let mut ds = RedisDataSrc::new("redis://127.0.0.1:6379/0".to_string());
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addr_is_string_and_fail() {
            let mut ds = RedisDataSrc::new("xxxx".to_string());
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisError::FailToOpenClientOfAddr { addr }) = err.reason::<RedisError>() else {
                panic!();
            };
            assert_eq!(addr, "xxxx");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Redis URL did not parse - InvalidClientConfig"
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addr_is_url_and_ok() {
            let Ok(url) = Url::parse("redis://127.0.0.1:6379/0") else {
                panic!("bad url");
            };
            let mut ds = RedisDataSrc::new(url);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addr_is_url_and_fail() {
            let Ok(url) = Url::parse("redis://") else {
                panic!("bad url");
            };
            let mut ds = RedisDataSrc::new(url);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisError::FailToOpenClientOfAddr { addr }) = err.reason::<RedisError>() else {
                panic!();
            };
            assert_eq!(addr, "redis://");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Missing hostname - InvalidClientConfig"
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
        fn addr_is_str_and_ok() {
            let pb = Pool::builder().max_size(15);
            let mut ds = RedisDataSrc::with_pool_builder("redis://127.0.0.1:6379/0", pb);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addr_is_str_and_fail() {
            let pb = Pool::builder().max_size(15);
            let mut ds = RedisDataSrc::with_pool_builder("xxxx", pb);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisError::FailToOpenClientOfAddr { addr }) = err.reason::<RedisError>() else {
                panic!();
            };
            assert_eq!(addr, "xxxx");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Redis URL did not parse - InvalidClientConfig"
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addr_is_string_and_ok() {
            let pb = Pool::builder().max_size(15);
            let mut ds =
                RedisDataSrc::with_pool_builder("redis://127.0.0.1:6379/0".to_string(), pb);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addr_is_string_and_fail() {
            let pb = Pool::builder().max_size(15);
            let mut ds = RedisDataSrc::with_pool_builder("xxxx".to_string(), pb);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisError::FailToOpenClientOfAddr { addr }) = err.reason::<RedisError>() else {
                panic!();
            };
            assert_eq!(addr, "xxxx");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Redis URL did not parse - InvalidClientConfig"
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addr_is_url_and_ok() {
            let pb = Pool::builder().max_size(15);
            let Ok(url) = Url::parse("redis://127.0.0.1:6379/0") else {
                panic!("bad url");
            };
            let mut ds = RedisDataSrc::with_pool_builder(url, pb);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }

        #[test]
        fn addr_is_url_and_fail() {
            let pb = Pool::builder().connection_timeout(std::time::Duration::from_secs(1));
            let Ok(url) = Url::parse("redis://111") else {
                panic!("bad url");
            };
            let mut ds = RedisDataSrc::with_pool_builder(url, pb);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisError::FailToBuildPool { conn_info }) = err.reason::<RedisError>() else {
                panic!();
            };
            #[cfg(target_os = "linux")]
            assert_eq!(format!("{:?}", conn_info), "ConnectionInfo { addr: Tcp(\"111\", 6379), tcp_settings: TcpSettings { nodelay: false, keepalive: None, user_timeout: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(not(target_os = "linux"))]
            assert_eq!(format!("{:?}", conn_info), "ConnectionInfo { addr: Tcp(\"111\", 6379), tcp_settings: TcpSettings { nodelay: false, keepalive: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(target_os = "linux")]
            assert_eq!(format!("{:?}", err.source().unwrap()), "Error(None)",);
            #[cfg(not(target_os = "linux"))]
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"No route to host (os error 65)\"))"
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_conn_addr {
        use super::*;

        #[test]
        fn ok() {
            let conn_addr = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 6379);
            let mut ds = RedisDataSrc::with_conn_addr(conn_addr);
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
            let conn_addr = redis::ConnectionAddr::Tcp("xxxx".to_string(), 1234);
            let mut ds = RedisDataSrc::with_conn_addr(conn_addr);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisError::FailToBuildPool { conn_info }) = err.reason::<RedisError>() else {
                panic!();
            };
            #[cfg(target_os = "linux")]
            assert_eq!(format!("{:?}", conn_info), "ConnectionInfo { addr: Tcp(\"xxxx\", 1234), tcp_settings: TcpSettings { nodelay: false, keepalive: None, user_timeout: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(not(target_os = "linux"))]
            assert_eq!(format!("{:?}", conn_info), "ConnectionInfo { addr: Tcp(\"xxxx\", 1234), tcp_settings: TcpSettings { nodelay: false, keepalive: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(target_os = "linux")]
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"failed to lookup address information: Temporary failure in name resolution\"))",
            );
            #[cfg(not(target_os = "linux"))]
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"failed to lookup address information: nodename nor servname provided, or not known\"))",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_conn_addr_and_pool_builder {
        use super::*;

        #[test]
        fn ok() {
            let pb = Pool::builder().connection_timeout(std::time::Duration::from_secs(1));
            let conn_addr = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 6379);
            let mut ds = RedisDataSrc::with_conn_addr_and_pool_builder(conn_addr, pb);
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
            let pb = Pool::builder().connection_timeout(std::time::Duration::from_secs(1));
            let conn_addr = redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 1234);
            let mut ds = RedisDataSrc::with_conn_addr_and_pool_builder(conn_addr, pb);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisError::FailToBuildPool { conn_info }) = err.reason::<RedisError>() else {
                panic!();
            };
            #[cfg(target_os = "linux")]
            assert_eq!(format!("{:?}", conn_info), "ConnectionInfo { addr: Tcp(\"127.0.0.1\", 1234), tcp_settings: TcpSettings { nodelay: false, keepalive: None, user_timeout: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(not(target_os = "linux"))]
            assert_eq!(format!("{:?}", conn_info), "ConnectionInfo { addr: Tcp(\"127.0.0.1\", 1234), tcp_settings: TcpSettings { nodelay: false, keepalive: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(target_os = "linux")]
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"Connection refused (os error 111)\"))",
            );
            #[cfg(not(target_os = "linux"))]
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"Connection refused (os error 61)\"))",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_conn_info {
        use super::*;
        use redis::IntoConnectionInfo;

        #[test]
        fn ok() {
            let conn_info = "redis://127.0.0.1:6379/0".into_connection_info().unwrap();
            let mut ds = RedisDataSrc::with_conn_info(conn_info);
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
            let conn_info = "redis://xxxx:6379/0".into_connection_info().unwrap();
            let mut ds = RedisDataSrc::with_conn_info(conn_info);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisError::FailToBuildPool { conn_info }) = err.reason::<RedisError>() else {
                panic!();
            };
            #[cfg(target_os = "linux")]
            assert_eq!(format!("{:?}", conn_info), "ConnectionInfo { addr: Tcp(\"xxxx\", 6379), tcp_settings: TcpSettings { nodelay: false, keepalive: None, user_timeout: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(not(target_os = "linux"))]
            assert_eq!(format!("{:?}", conn_info), "ConnectionInfo { addr: Tcp(\"xxxx\", 6379), tcp_settings: TcpSettings { nodelay: false, keepalive: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(target_os = "linux")]
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"failed to lookup address information: Temporary failure in name resolution\"))",
            );
            #[cfg(not(target_os = "linux"))]
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"failed to lookup address information: nodename nor servname provided, or not known\"))",
            );
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_conn_info_and_pool_builder {
        use super::*;
        use redis::IntoConnectionInfo;

        #[test]
        fn ok() {
            let pb = Pool::builder().connection_timeout(std::time::Duration::from_secs(1));
            let conn_info = "redis://127.0.0.1:6379/0".into_connection_info().unwrap();
            let mut ds = RedisDataSrc::with_conn_info_and_pool_builder(conn_info, pb);
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
            let pb = Pool::builder().connection_timeout(std::time::Duration::from_secs(1));
            let conn_info = "redis://127.0.0.1:9999/0".into_connection_info().unwrap();
            let mut ds = RedisDataSrc::with_conn_info_and_pool_builder(conn_info, pb);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup(&mut ag) else {
                panic!();
            };
            let Ok(RedisError::FailToBuildPool { conn_info }) = err.reason::<RedisError>() else {
                panic!();
            };
            #[cfg(target_os = "linux")]
            assert_eq!(format!("{:?}", conn_info), "ConnectionInfo { addr: Tcp(\"127.0.0.1\", 9999), tcp_settings: TcpSettings { nodelay: false, keepalive: None, user_timeout: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(not(target_os = "linux"))]
            assert_eq!(format!("{:?}", conn_info), "ConnectionInfo { addr: Tcp(\"127.0.0.1\", 9999), tcp_settings: TcpSettings { nodelay: false, keepalive: None }, redis: RedisConnectionInfo { db: 0, username: None, password: None, protocol: RESP2, skip_set_lib_name: false, lib_name: None, lib_ver: None } }");
            #[cfg(target_os = "linux")]
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Error(Some(\"Connection refused (os error 111)\"))",
            );
            #[cfg(not(target_os = "linux"))]
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
            let mut ds = RedisDataSrc::new("redis://127.0.0.1:6379/0");
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

            redis_conn.set("test_create_data_conn", "1").unwrap();
            let s = redis_conn.get("test_create_data_conn").unwrap();
            redis_conn.del("test_create_data_conn").unwrap();
            assert_eq!(s, Some("1".to_string()));

            ds.close();
        }

        #[test]
        fn fail() {
            let pb = Pool::builder()
                .max_size(1)
                .connection_timeout(std::time::Duration::from_secs(1));
            let mut ds = RedisDataSrc::with_pool_builder("redis://127.0.0.1:6379/0", pb);
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
            let Ok(RedisError::FailToGetConnectionFromPool) = err.reason::<RedisError>() else {
                panic!();
            };
            assert_eq!(format!("{:?}", err.source().unwrap()), "Error(None)",);

            ds.close();
        }

        #[test]
        fn not_setup_yet() {
            let mut ds = RedisDataSrc::new("redis://127.0.0.1:6379/0");
            let Err(err) = ds.create_data_conn() else {
                panic!("fail to create data_conn");
            };
            let Ok(RedisError::NotSetupYet) = err.reason::<RedisError>() else {
                panic!();
            };
            ds.close();
        }
    }

    mod test_setup {
        use super::*;

        #[test]
        fn fail_due_to_setup_twice() {
            let mut ds = RedisDataSrc::new("redis://127.0.0.1:6379/0");

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
            let Ok(RedisError::AlreadySetup) = err.reason::<RedisError>() else {
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
            const KEY: &str = "test_add_pre_commit";

            let mut ds = RedisDataSrc::new("redis://127.0.0.1:6379/0");
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
            const KEY: &str = "test_add_pre_commit/fail";

            let mut ds = RedisDataSrc::new("redis://127.0.0.1:6379/0");
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
            const KEY: &str = "test_add_post_commit";

            let mut ds = RedisDataSrc::new("redis://127.0.0.1:6379/0");
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
            const KEY: &str = "test_add_post_commit/fail";

            let mut ds = RedisDataSrc::new("redis://127.0.0.1:6379/0");
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
            const KEY: &str = "test_add_force_back";

            let mut ds = RedisDataSrc::new("redis://127.0.0.1:6379/0");
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
            const KEY: &str = "test_add_force_back/fail";

            let mut ds = RedisDataSrc::new("redis://127.0.0.1:6379/0");
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
