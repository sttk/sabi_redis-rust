// Copyright (C) 2025-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use r2d2::{Builder, Pool, PooledConnection};
use redis::{Client, Connection, IntoConnectionInfo};
use sabi::{AsyncGroup, DataConn, DataSrc};

use std::fmt::Debug;
use std::mem;

/// The error type for synchronous Redis operations.
#[derive(Debug)]
pub enum RedisSyncError {
    /// Indicates that the Redis data source has not been set up yet.
    NotSetupYet,
    /// Indicates that the Redis data source has already been set up.
    AlreadySetup,
    /// Indicates a failure to open a Redis client.
    FailToOpenClient,
    /// Indicates a failure to build a Redis connection pool.
    FailToBuildPool,
    /// Indicates a failure to get a connection from the pool.
    FailToGetConnectionFromPool,
}

#[allow(clippy::type_complexity)]
/// A data connection for a standalone Redis server, providing synchronous operations.
///
/// This structure holds a pooled connection and allows for adding hooks (pre-commit, post-commit,
/// and force-back) that are executed during the lifecycle of a data operation managed by `sabi`.
///
/// # Examples
/// ```
/// use sabi_redis::RedisDataConn;
/// use redis::Commands;
/// use sabi::DataAcc;
///
/// trait MyDataAcc: DataAcc {
///     fn set_value(&mut self, key: &str, val: &str) -> errs::Result<()> {
///         let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
///         let conn = data_conn.get_connection();
///         conn.set(key, val).map_err(|e| errs::Err::with_source("fail", e))
///     }
/// }
/// ```
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

    /// Gets a connection from the pool.
    ///
    /// # Returns
    /// Returns a mutable reference to a `Connection`.
    pub fn get_connection(&mut self) -> &mut Connection {
        &mut self.conn
    }

    /// Adds a function to be executed before a commit occurs in the `sabi` lifecycle.
    ///
    /// # Arguments
    /// * `f` - A closure or function that takes a mutable reference to a `Connection` and returns a `Result`.
    pub fn add_pre_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut Connection) -> errs::Result<()> + 'static,
    {
        self.pre_commit_vec.push(Box::new(f));
    }

    /// Adds a function to be executed after a successful commit in the `sabi` lifecycle.
    ///
    /// # Arguments
    /// * `f` - A closure or function that takes a mutable reference to a `Connection` and returns a `Result`.
    pub fn add_post_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut Connection) -> errs::Result<()> + 'static,
    {
        self.post_commit_vec.push(Box::new(f));
    }

    /// Adds a function to be executed when a rollback or forced recovery is triggered.
    ///
    /// # Arguments
    /// * `f` - A closure or function that takes a mutable reference to a `Connection` and returns a `Result`.
    pub fn add_force_back<F>(&mut self, f: F)
    where
        F: FnMut(&mut Connection) -> errs::Result<()> + 'static,
    {
        self.force_back_vec.push(Box::new(f));
    }
}

impl DataConn for RedisDataConn {
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
            // for error notification
            let _ = f(&mut self.conn);
        }
    }

    fn rollback(&mut self, _ag: &mut AsyncGroup) {}

    fn should_force_back(&self) -> bool {
        true
    }

    fn force_back(&mut self, _ag: &mut AsyncGroup) {
        for f in self.force_back_vec.iter_mut().rev() {
            // for error notification
            let _ = f(&mut self.conn);
        }
    }

    fn close(&mut self) {}
}

/// A data source for standalone Redis, used to initialize and provide `RedisDataConn` instances.
///
/// This struct implements the `DataSrc` trait from the `sabi` library.
///
/// # Examples
/// ```
/// use sabi_redis::RedisDataSrc;
/// use sabi::DataHub;
///
/// let mut data = DataHub::new();
/// data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/0"));
/// ```
///
/// # Type Parameters
/// * `T` - A type that can be converted into Redis connection info.
pub struct RedisDataSrc<T>
where
    T: IntoConnectionInfo + Sized + Debug,
{
    pool: Option<RedisPool<T>>,
}

enum RedisPool<T>
where
    T: IntoConnectionInfo + Sized + Debug,
{
    Object(Pool<Client>),
    Config(T, Builder<Client>),
}

impl<T> RedisDataSrc<T>
where
    T: IntoConnectionInfo + Sized + Debug,
{
    /// Creates a new `RedisDataSrc` with the given Redis address.
    ///
    /// # Arguments
    /// * `addr` - The Redis connection address (e.g., a URL string).
    ///
    /// # Returns
    /// Returns a new instance of `RedisDataSrc`.
    pub fn new(addr: T) -> Self {
        Self {
            pool: Some(RedisPool::Config(addr, Pool::builder())),
        }
    }

    /// Creates a new `RedisDataSrc` with the given Redis address and a custom pool builder.
    ///
    /// # Arguments
    /// * `addr` - The Redis connection address.
    /// * `pool_builder` - A pre-configured `r2d2::Builder`.
    ///
    /// # Returns
    /// Returns a new instance of `RedisDataSrc`.
    pub fn with_pool_builder(addr: T, pool_builder: Builder<Client>) -> Self {
        Self {
            pool: Some(RedisPool::Config(addr, pool_builder)),
        }
    }
}

impl<T> DataSrc<RedisDataConn> for RedisDataSrc<T>
where
    T: IntoConnectionInfo + Sized + Debug,
{
    fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let pool_opt = mem::take(&mut self.pool);
        let pool = pool_opt.ok_or_else(|| errs::Err::new(RedisSyncError::AlreadySetup))?;
        match pool {
            RedisPool::Config(addr, pool_config) => {
                let client = Client::open(addr)
                    .map_err(|e| errs::Err::with_source(RedisSyncError::FailToOpenClient, e))?;

                let pool = pool_config
                    .build(client)
                    .map_err(|e| errs::Err::with_source(RedisSyncError::FailToBuildPool, e))?;

                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            _ => Err(errs::Err::new(RedisSyncError::AlreadySetup)),
        }
    }

    fn close(&mut self) {}

    fn create_data_conn(&mut self) -> errs::Result<Box<RedisDataConn>> {
        let pool = self
            .pool
            .as_mut()
            .ok_or_else(|| errs::Err::new(RedisSyncError::NotSetupYet))?;
        match pool {
            RedisPool::Object(pool) => match pool.get() {
                Ok(conn) => Ok(Box::new(RedisDataConn::new(conn))),
                Err(e) => Err(errs::Err::with_source(
                    RedisSyncError::FailToGetConnectionFromPool,
                    e,
                )),
            },
            _ => Err(errs::Err::new(RedisSyncError::NotSetupYet)),
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use override_macro::{overridable, override_with};
    use redis::Commands;
    use sabi::{DataAcc, DataHub};
    use std::time;

    #[derive(Debug)]
    enum SampleError {
        FailToGetValue,
        FailToSetValue,
        FailToDelValue,
    }

    #[overridable]
    trait RedisSampleDataAcc: DataAcc {
        fn get_sample_key(&mut self) -> errs::Result<Option<String>> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
            let conn = data_conn.get_connection();
            conn.get("sample")
                .map_err(|e| errs::Err::with_source(SampleError::FailToGetValue, e))
        }
        fn set_sample_key(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
            let conn = data_conn.get_connection();
            conn.set("sample", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))
        }
        fn del_sample_key(&mut self) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
            let conn = data_conn.get_connection();
            conn.del("sample")
                .map_err(|e| errs::Err::with_source(SampleError::FailToDelValue, e))
        }

        fn set_sample_key_with_force_back(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;

            {
                let conn = data_conn.get_connection();
                conn.set::<&str, &str, ()>("sample_force_back", val)
                    .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;
            }

            data_conn.add_force_back(|conn| {
                conn.del("sample_force_back")
                    .map_err(|e| errs::Err::with_source("fail to force back", e))
            });

            {
                let conn = data_conn.get_connection();
                conn.set::<&str, &str, ()>("sample_force_back_2", val)
                    .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;
            }

            data_conn.add_force_back(|conn| {
                conn.del("sample_force_back_2")
                    .map_err(|e| errs::Err::with_source("fail to force back", e))
            });

            Ok(())
        }

        fn set_sample_key_with_pre_commit(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;

            let val_owned = val.to_string();

            data_conn.add_pre_commit(move |conn| {
                conn.set::<&str, &str, ()>("sample_pre_commit", &val_owned)
                    .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;
                Ok(())
            });

            Ok(())
        }

        fn set_sample_key_with_post_commit(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;

            let val_owned = val.to_string();

            data_conn.add_post_commit(move |conn| {
                conn.set::<&str, &str, ()>("sample_post_commit", &val_owned)
                    .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;
                Ok(())
            });

            Ok(())
        }
    }
    impl RedisSampleDataAcc for DataHub {}

    #[overridable]
    trait SampleData {
        fn get_sample_key(&mut self) -> errs::Result<Option<String>>;
        fn set_sample_key(&mut self, value: &str) -> errs::Result<()>;
        fn del_sample_key(&mut self) -> errs::Result<()>;
        fn set_sample_key_with_force_back(&mut self, val: &str) -> errs::Result<()>;
        fn set_sample_key_with_pre_commit(&mut self, val: &str) -> errs::Result<()>;
        fn set_sample_key_with_post_commit(&mut self, val: &str) -> errs::Result<()>;
    }
    #[override_with(RedisSampleDataAcc)]
    impl SampleData for DataHub {}

    fn sample_logic(data: &mut impl SampleData) -> errs::Result<()> {
        data.get_sample_key().expect("Data exists");
        data.set_sample_key("Hello")?;
        assert_eq!(data.get_sample_key().expect("No Data").unwrap(), "Hello");
        data.del_sample_key()?;
        Ok(())
    }

    #[test]
    fn test_new_by_str() -> Result<(), errs::Err> {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/0"));
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn test_new_by_conn_info() -> Result<(), errs::Err> {
        let ci: redis::ConnectionInfo = "redis://127.0.0.1:6379/1".parse().unwrap();

        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new(ci));
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn test_with_pool_builder() -> Result<(), errs::Err> {
        let builder = r2d2::Pool::<redis::Client>::builder()
            .max_size(100)
            .min_idle(Some(10))
            .max_lifetime(Some(time::Duration::from_secs(60 * 60)))
            .idle_timeout(Some(time::Duration::from_secs(5 * 60)))
            .connection_timeout(time::Duration::from_secs(30));

        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisDataSrc::with_pool_builder("redis://127.0.0.1:6379/2", builder),
        );
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn fail_due_to_invalid_addr() {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("xxxxx"));
        let err = data.run(sample_logic).unwrap_err();

        match err.reason::<sabi::DataHubError>().unwrap() {
            sabi::DataHubError::FailToSetupLocalDataSrcs { errors } => {
                assert_eq!(errors.len(), 1);
                assert_eq!(errors[0].0.as_ref(), "redis");
                if let Ok(r) = errors[0].1.reason::<RedisSyncError>() {
                    match r {
                        RedisSyncError::FailToOpenClient => {}
                        _ => panic!(),
                    }
                }
                let e = errors[0]
                    .1
                    .source()
                    .unwrap()
                    .downcast_ref::<redis::RedisError>()
                    .unwrap();
                assert_eq!(e.kind(), redis::ErrorKind::InvalidClientConfig);
                assert!(e.detail().is_none());
                assert!(e.code().is_none());
                assert_eq!(e.category(), "invalid client config");
            }
            _ => panic!(),
        }
    }

    fn sample_logic_with_force_back_ok(data: &mut impl SampleData) -> errs::Result<()> {
        data.set_sample_key_with_force_back("Good Morning")?;
        Ok(())
    }
    fn sample_logic_with_force_back_err(data: &mut impl SampleData) -> errs::Result<()> {
        data.set_sample_key_with_force_back("Good Afternoon")?;
        Err(errs::Err::new("XXX"))
    }
    fn sample_logic_with_pre_commit(data: &mut impl SampleData) -> errs::Result<()> {
        data.set_sample_key_with_pre_commit("Good Evening")?;
        Ok(())
    }
    fn sample_logic_with_post_commit(data: &mut impl SampleData) -> errs::Result<()> {
        data.set_sample_key_with_post_commit("Good Night")?;
        Ok(())
    }

    #[test]
    fn test_txn_and_force_back() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/3"));
        data.txn(sample_logic_with_force_back_ok)?;

        {
            let client = redis::Client::open("redis://127.0.0.1:6379/3").unwrap();
            let mut conn = client.get_connection().unwrap();

            let s: redis::RedisResult<Option<String>> = conn.get("sample_force_back");
            let _: redis::RedisResult<()> = conn.del("sample_force_back");
            assert_eq!(s.unwrap().unwrap(), "Good Morning");

            let s: redis::RedisResult<Option<String>> = conn.get("sample_force_back_2");
            let _: redis::RedisResult<()> = conn.del("sample_force_back_2");
            assert_eq!(s.unwrap().unwrap(), "Good Morning");
        }

        let err = data.txn(sample_logic_with_force_back_err).unwrap_err();
        assert_eq!(err.reason::<&str>().unwrap(), &"XXX");

        {
            let client = redis::Client::open("redis://127.0.0.1:6379/6").unwrap();
            let mut conn = client.get_connection().unwrap();

            let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back");
            let _: redis::RedisResult<()> = conn.del("sample_force_back");
            assert!(r.unwrap().is_none());

            let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back_2");
            let _: redis::RedisResult<()> = conn.del("sample_force_back_2");
            assert!(r.unwrap().is_none());
        }

        Ok(())
    }

    #[test]
    fn test_txn_and_pre_commit() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/4"));

        data.txn(sample_logic_with_pre_commit)?;

        {
            let client = redis::Client::open("redis://127.0.0.1:6379/4").unwrap();
            let mut conn = client.get_connection().unwrap();
            let s: redis::RedisResult<Option<String>> = conn.get("sample_pre_commit");
            let _: redis::RedisResult<()> = conn.del("sample_pre_commit");
            assert_eq!(s.unwrap().unwrap(), "Good Evening");
        }
        Ok(())
    }

    #[test]
    fn test_txn_and_post_commit() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/5"));

        data.txn(sample_logic_with_post_commit)?;

        {
            let client = redis::Client::open("redis://127.0.0.1:6379/5").unwrap();
            let mut conn = client.get_connection().unwrap();
            let s: redis::RedisResult<Option<String>> = conn.get("sample_post_commit");
            let _: redis::RedisResult<()> = conn.del("sample_post_commit");
            assert_eq!(s.unwrap().unwrap(), "Good Night");
        }
        Ok(())
    }
}
