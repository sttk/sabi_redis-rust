// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use r2d2::{Builder, Pool, PooledConnection};
use redis::sentinel::{
    LockedSentinelClient, SentinelClient, SentinelClientBuilder, SentinelNodeConnectionInfo,
    SentinelServerType,
};
use redis::{Connection, IntoConnectionInfo};
use sabi::{AsyncGroup, DataConn, DataSrc};

use std::fmt::Debug;
use std::{mem, time};

/// The error type for synchronous Redis Sentinel operations.
#[derive(Debug)]
pub enum RedisSentinelSyncError {
    /// Indicates that the Redis Sentinel data source has not been set up yet.
    NotSetupYet,
    /// Indicates that the Redis Sentinel data source has already been set up.
    AlreadySetup,
    /// Indicates a failure to parse connection addresses.
    FailToParseConnectionAddrs,
    /// Indicates a failure to create a Sentinel client builder.
    FailToCreateSentinelClientBuilder,
    /// Indicates a failure to build a Redis connection pool.
    FailToBuildPool,
    /// Indicates a failure to build a Redis Sentinel client.
    FailToBuildSentinelClient,
    /// Indicates a failure to get a connection from the pool.
    FailToGetConnectionFromPool,
}

#[allow(clippy::type_complexity)]
/// A data connection for Redis Sentinel, providing synchronous operations.
///
/// This structure holds a connection pool for a Redis Sentinel-managed setup 
/// and allows for adding hooks (pre-commit, post-commit, and force-back) 
/// that are executed during the lifecycle of a data operation managed by `sabi`.
///
/// # Examples
/// ```
/// use sabi_redis::RedisSentinelDataConn;
/// use redis::Commands;
/// use sabi::DataAcc;
///
/// trait MyDataAcc: DataAcc {
///     fn set_value(&mut self, key: &str, val: &str) -> errs::Result<()> {
///         let data_conn = self.get_data_conn::<RedisSentinelDataConn>("redis")?;
///         let mut conn = data_conn.get_connection()?;
///         conn.set(key, val).map_err(|e| errs::Err::with_source("fail", e))
///     }
/// }
/// ```
pub struct RedisSentinelDataConn {
    pool: Pool<LockedSentinelClient>,
    pre_commit_vec: Vec<Box<dyn FnMut(&mut Connection) -> errs::Result<()>>>,
    post_commit_vec: Vec<Box<dyn FnMut(&mut Connection) -> errs::Result<()>>>,
    force_back_vec: Vec<Box<dyn FnMut(&mut Connection) -> errs::Result<()>>>,
}

impl RedisSentinelDataConn {
    fn new(pool: Pool<LockedSentinelClient>) -> Self {
        Self {
            pool,
            pre_commit_vec: Vec::new(),
            post_commit_vec: Vec::new(),
            force_back_vec: Vec::new(),
        }
    }

    /// Gets a Sentinel-managed connection from the pool.
    ///
    /// # Returns
    /// Returns a `Result` containing a `PooledConnection<LockedSentinelClient>` on success, 
    /// or a `RedisSentinelSyncError::FailToGetConnectionFromPool` wrapped in `errs::Err` on failure.
    pub fn get_connection(&mut self) -> errs::Result<PooledConnection<LockedSentinelClient>> {
        self.pool.get().map_err(|e| {
            errs::Err::with_source(RedisSentinelSyncError::FailToGetConnectionFromPool, e)
        })
    }

    /// Gets a Sentinel-managed connection from the pool with a specific timeout.
    ///
    /// # Arguments
    /// * `timeout` - A `Duration` to wait for a connection before failing.
    ///
    /// # Returns
    /// Returns a `Result` containing a `PooledConnection<LockedSentinelClient>` on success, 
    /// or a `RedisSentinelSyncError::FailToGetConnectionFromPool` wrapped in `errs::Err` on failure.
    pub fn get_connection_with_timeout(
        &mut self,
        timeout: time::Duration,
    ) -> errs::Result<PooledConnection<LockedSentinelClient>> {
        self.pool.get_timeout(timeout).map_err(|e| {
            errs::Err::with_source(RedisSentinelSyncError::FailToGetConnectionFromPool, e)
        })
    }

    /// Tries to get a Sentinel-managed connection from the pool immediately without waiting.
    ///
    /// # Returns
    /// Returns `Some(PooledConnection<LockedSentinelClient>)` if a connection is available, otherwise `None`.
    pub fn try_get_connection(&self) -> Option<PooledConnection<LockedSentinelClient>> {
        self.pool.try_get()
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

impl DataConn for RedisSentinelDataConn {
    fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        match self.pool.get() {
            Ok(mut conn) => {
                for f in self.pre_commit_vec.iter_mut() {
                    f(&mut conn)?;
                }
                Ok(())
            }
            Err(e) => Err(errs::Err::with_source(
                RedisSentinelSyncError::FailToGetConnectionFromPool,
                e,
            )),
        }
    }

    fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    fn post_commit(&mut self, _ag: &mut AsyncGroup) {
        match self.pool.get() {
            Ok(mut conn) => {
                for f in self.post_commit_vec.iter_mut() {
                    // for error notification
                    let _ = f(&mut conn);
                }
            }
            Err(e) => {
                // for error notification
                let _ =
                    errs::Err::with_source(RedisSentinelSyncError::FailToGetConnectionFromPool, e);
            }
        };
    }

    fn rollback(&mut self, _ag: &mut AsyncGroup) {}

    fn should_force_back(&self) -> bool {
        true
    }

    fn force_back(&mut self, _ag: &mut AsyncGroup) {
        match self.pool.get() {
            Ok(mut conn) => {
                for f in self.force_back_vec.iter_mut().rev() {
                    // for error notification
                    let _ = f(&mut conn);
                }
            }
            Err(e) => {
                // for error notification
                let _ =
                    errs::Err::with_source(RedisSentinelSyncError::FailToGetConnectionFromPool, e);
            }
        };
    }

    fn close(&mut self) {}
}

/// A data source for Redis Sentinel, used to initialize and provide `RedisSentinelDataConn` instances.
///
/// This struct implements the `DataSrc` trait from the `sabi` library.
///
/// # Examples
/// ```
/// use sabi_redis::RedisSentinelDataSrc;
/// use sabi::DataHub;
///
/// let mut data = DataHub::new();
/// data.uses("redis", RedisSentinelDataSrc::new(
///     vec![
///         "redis://127.0.0.1:26479",
///         "redis://127.0.0.1:26480",
///         "redis://127.0.0.1:26481",
///     ],
///     "mymaster",
/// ));
/// ```
///
/// # Type Parameters
/// * `T` - A type that can be converted into Redis connection info.
pub struct RedisSentinelDataSrc<T>
where
    T: redis::IntoConnectionInfo,
{
    pool: Option<RedisPool<T>>,
}

enum RedisPool<T>
where
    T: IntoConnectionInfo,
{
    Object(Pool<LockedSentinelClient>),
    Client(
        #[allow(clippy::type_complexity)]
        Box<(
            Vec<T>,
            String,
            Option<SentinelNodeConnectionInfo>,
            SentinelServerType,
            Builder<LockedSentinelClient>,
        )>,
    ),
    Builder(Box<(SentinelClientBuilder, Builder<LockedSentinelClient>)>),
}

impl<T> RedisSentinelDataSrc<T>
where
    T: redis::IntoConnectionInfo,
{
    /// Creates a new `RedisSentinelDataSrc` with Sentinel addresses and a service name.
    ///
    /// # Arguments
    /// * `addrs` - A vector of items that can be converted into Redis connection info (Sentinel addresses).
    /// * `service_name` - The name of the Redis service to monitor.
    ///
    /// # Returns
    /// Returns a new instance of `RedisSentinelDataSrc`.
    pub fn new(addrs: Vec<T>, service_name: impl AsRef<str>) -> Self {
        Self {
            pool: Some(RedisPool::Client(Box::new((
                addrs,
                service_name.as_ref().to_string(),
                None,
                SentinelServerType::Master,
                Pool::builder(),
            )))),
        }
    }

    /// Creates a new `RedisSentinelDataSrc` with specific Sentinel client parameters.
    ///
    /// # Arguments
    /// * `addrs` - A vector of Sentinel addresses.
    /// * `service_name` - The name of the Redis service.
    /// * `node_conn_info` - Connection information for the Redis nodes.
    /// * `server_type` - The type of server to connect to (e.g., Master or Slave).
    ///
    /// # Returns
    /// Returns a new instance of `RedisSentinelDataSrc`.
    pub fn with_client_params(
        addrs: Vec<T>,
        service_name: impl AsRef<str>,
        node_conn_info: SentinelNodeConnectionInfo,
        server_type: SentinelServerType,
    ) -> Self {
        Self {
            pool: Some(RedisPool::Client(Box::new((
                addrs,
                service_name.as_ref().to_string(),
                Some(node_conn_info),
                server_type,
                Pool::builder(),
            )))),
        }
    }

    /// Creates a new `RedisSentinelDataSrc` with Sentinel client parameters and a custom pool builder.
    ///
    /// # Arguments
    /// * `addrs` - A vector of Sentinel addresses.
    /// * `service_name` - The name of the Redis service.
    /// * `node_conn_info` - Connection information for the Redis nodes.
    /// * `server_type` - The type of server to connect to.
    /// * `pool_builder` - A `r2d2::Builder` for Configuring the connection pool.
    ///
    /// # Returns
    /// Returns a new instance of `RedisSentinelDataSrc`.
    pub fn with_client_params_and_pool_builder(
        addrs: Vec<T>,
        service_name: impl AsRef<str>,
        node_conn_info: SentinelNodeConnectionInfo,
        server_type: SentinelServerType,
        pool_builder: Builder<LockedSentinelClient>,
    ) -> Self {
        Self {
            pool: Some(RedisPool::Client(Box::new((
                addrs,
                service_name.as_ref().to_string(),
                Some(node_conn_info),
                server_type,
                pool_builder,
            )))),
        }
    }
}

impl RedisSentinelDataSrc<&'static str> {
    /// Creates a new `RedisSentinelDataSrc` with a pre-configured `SentinelClientBuilder`.
    ///
    /// # Arguments
    /// * `client_builder` - A `SentinelClientBuilder` for the Sentinel setup.
    ///
    /// # Returns
    /// Returns a new instance of `RedisSentinelDataSrc`.
    pub fn with_client_builder(client_builder: SentinelClientBuilder) -> Self {
        Self {
            pool: Some(RedisPool::Builder(Box::new((
                client_builder,
                Pool::builder(),
            )))),
        }
    }

    /// Creates a new `RedisSentinelDataSrc` with a Sentinel client builder and a custom pool builder.
    ///
    /// # Arguments
    /// * `client_builder` - A `SentinelClientBuilder` for the Sentinel setup.
    /// * `pool_builder` - A `r2d2::Builder` for Configuring the connection pool.
    ///
    /// # Returns
    /// Returns a new instance of `RedisSentinelDataSrc`.
    pub fn with_client_builder_and_pool_builder(
        client_builder: SentinelClientBuilder,
        pool_builder: Builder<LockedSentinelClient>,
    ) -> Self {
        Self {
            pool: Some(RedisPool::Builder(Box::new((client_builder, pool_builder)))),
        }
    }
}

impl<T> DataSrc<RedisSentinelDataConn> for RedisSentinelDataSrc<T>
where
    T: redis::IntoConnectionInfo,
{
    fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let pool_opt = mem::take(&mut self.pool);
        let pool = pool_opt.ok_or_else(|| errs::Err::new(RedisSentinelSyncError::AlreadySetup))?;
        match pool {
            RedisPool::Client(info) => {
                let (addrs, service_name, node_conn_info, server_type, pool_builder) = *info;
                let client =
                    SentinelClient::build(addrs, service_name, node_conn_info, server_type)
                        .map_err(|e| {
                            errs::Err::with_source(
                                RedisSentinelSyncError::FailToBuildSentinelClient,
                                e,
                            )
                        })?;

                let pool = pool_builder
                    .build(LockedSentinelClient::new(client))
                    .map_err(|e| {
                        errs::Err::with_source(RedisSentinelSyncError::FailToBuildPool, e)
                    })?;

                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            RedisPool::Builder(builder) => {
                let (client_builder, pool_builder) = *builder;
                let client = client_builder.build().map_err(|e| {
                    errs::Err::with_source(RedisSentinelSyncError::FailToBuildSentinelClient, e)
                })?;

                let pool = pool_builder
                    .build(LockedSentinelClient::new(client))
                    .map_err(|e| {
                        errs::Err::with_source(RedisSentinelSyncError::FailToBuildPool, e)
                    })?;

                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }

            _ => Err(errs::Err::new(RedisSentinelSyncError::AlreadySetup)),
        }
    }

    fn close(&mut self) {}

    fn create_data_conn(&mut self) -> errs::Result<Box<RedisSentinelDataConn>> {
        let pool = self
            .pool
            .as_mut()
            .ok_or_else(|| errs::Err::new(RedisSentinelSyncError::NotSetupYet))?;
        match pool {
            RedisPool::Object(pool) => Ok(Box::new(RedisSentinelDataConn::new(pool.clone()))),
            _ => Err(errs::Err::new(RedisSentinelSyncError::NotSetupYet)),
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use override_macro::{overridable, override_with};
    use redis::sentinel::SentinelNodeConnectionInfo;
    use redis::{Commands, RedisConnectionInfo};
    use sabi::{DataAcc, DataHub};
    use std::time;

    #[derive(Debug)]
    enum SampleError {
        FailToGetValue,
        FailToSetValue,
        FailToDelValue,
    }

    #[overridable]
    trait RedisSentinelSampleDataAcc: DataAcc {
        fn get_sample_key(&mut self) -> errs::Result<Option<String>> {
            let data_conn = self.get_data_conn::<RedisSentinelDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;
            conn.get("sample_sentinel")
                .map_err(|e| errs::Err::with_source(SampleError::FailToGetValue, e))
        }
        fn set_sample_key(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisSentinelDataConn>("redis")?;
            let mut conn =
                data_conn.get_connection_with_timeout(time::Duration::from_millis(1000))?;
            conn.set("sample_sentinel", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToGetValue, e))
        }
        fn del_sample_key(&mut self) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisSentinelDataConn>("redis")?;
            let mut conn = data_conn.try_get_connection().unwrap();
            conn.del("sample_sentinel")
                .map_err(|e| errs::Err::with_source(SampleError::FailToDelValue, e))
        }

        fn set_sample_key_with_force_back(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisSentinelDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;

            conn.set::<&str, &str, ()>("sample_force_back_sentinel", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;

            data_conn.add_force_back(|conn| {
                conn.del("sample_force_back_sentinel")
                    .map_err(|e| errs::Err::with_source("fail to force back", e))
            });

            conn.set::<&str, &str, ()>("sample_force_back_sentinel_2", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;

            data_conn.add_force_back(|conn| {
                conn.del("sample_force_back_sentinel_2")
                    .map_err(|e| errs::Err::with_source("fail to force back", e))
            });

            Ok(())
        }

        fn set_sample_key_with_pre_commit(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisSentinelDataConn>("redis")?;

            let val_owned = val.to_string();

            data_conn.add_pre_commit(move |conn| {
                conn.set::<&str, &str, ()>("sample_pre_commit_sentinel", &val_owned)
                    .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;
                Ok(())
            });

            Ok(())
        }

        fn set_sample_key_with_post_commit(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisSentinelDataConn>("redis")?;

            let val_owned = val.to_string();

            data_conn.add_post_commit(move |conn| {
                conn.set::<&str, &str, ()>("sample_post_commit_sentinel", &val_owned)
                    .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;
                Ok(())
            });

            Ok(())
        }
    }
    impl RedisSentinelSampleDataAcc for DataHub {}

    #[overridable]
    trait SampleDataSentinel {
        fn get_sample_key(&mut self) -> errs::Result<Option<String>>;
        fn set_sample_key(&mut self, value: &str) -> errs::Result<()>;
        fn del_sample_key(&mut self) -> errs::Result<()>;
        fn set_sample_key_with_force_back(&mut self, val: &str) -> errs::Result<()>;
        fn set_sample_key_with_pre_commit(&mut self, val: &str) -> errs::Result<()>;
        fn set_sample_key_with_post_commit(&mut self, val: &str) -> errs::Result<()>;
    }
    #[override_with(RedisSentinelSampleDataAcc)]
    impl SampleDataSentinel for DataHub {}

    fn sample_logic(data: &mut impl SampleDataSentinel) -> errs::Result<()> {
        data.get_sample_key().expect("Data exists");
        data.set_sample_key("Hello")?;
        data.del_sample_key()?;
        Ok(())
    }

    #[test]
    fn test_new() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelDataSrc::new(
                vec![
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
            ),
        );
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn test_with_client_params() -> errs::Result<()> {
        let redis_connection_info = RedisConnectionInfo::default().set_db(1);
        let sentinel_node_connection_info =
            SentinelNodeConnectionInfo::default().set_redis_connection_info(redis_connection_info);

        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelDataSrc::with_client_params(
                vec![
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
                sentinel_node_connection_info,
                SentinelServerType::Master,
            ),
        );
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn test_with_client_params_and_pool_builder() -> errs::Result<()> {
        let redis_connection_info = RedisConnectionInfo::default().set_db(1);
        let sentinel_node_connection_info =
            SentinelNodeConnectionInfo::default().set_redis_connection_info(redis_connection_info);

        let pool_builder = Pool::<LockedSentinelClient>::builder()
            .max_size(100)
            .min_idle(Some(10))
            .max_lifetime(Some(time::Duration::from_secs(60 * 60)))
            .idle_timeout(Some(time::Duration::from_secs(5 * 60)))
            .connection_timeout(time::Duration::from_secs(30));

        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelDataSrc::with_client_params_and_pool_builder(
                vec![
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
                sentinel_node_connection_info,
                SentinelServerType::Master,
                pool_builder,
            ),
        );
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn test_with_client_builder() -> errs::Result<()> {
        let builder = SentinelClientBuilder::new(
            vec![
                redis::ConnectionAddr::Tcp(String::from("127.0.0.1"), 26479),
                redis::ConnectionAddr::Tcp(String::from("127.0.0.1"), 26480),
                redis::ConnectionAddr::Tcp(String::from("127.0.0.1"), 26481),
            ],
            "mymaster".to_string(),
            SentinelServerType::Master,
        )
        .unwrap();

        let mut data = DataHub::new();
        data.uses("redis", RedisSentinelDataSrc::with_client_builder(builder));
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn test_with_client_builder_and_pool_builder() -> errs::Result<()> {
        let builder = SentinelClientBuilder::new(
            vec![
                redis::ConnectionAddr::Tcp(String::from("127.0.0.1"), 26479),
                redis::ConnectionAddr::Tcp(String::from("127.0.0.1"), 26480),
                redis::ConnectionAddr::Tcp(String::from("127.0.0.1"), 26481),
            ],
            "mymaster".to_string(),
            SentinelServerType::Master,
        )
        .unwrap();

        let pool_builder = Pool::<LockedSentinelClient>::builder()
            .max_size(100)
            .min_idle(Some(10))
            .max_lifetime(Some(time::Duration::from_secs(60 * 60)))
            .idle_timeout(Some(time::Duration::from_secs(5 * 60)))
            .connection_timeout(time::Duration::from_secs(30));

        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelDataSrc::with_client_builder_and_pool_builder(builder, pool_builder),
        );
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn fail_to_setup() {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelDataSrc::new(vec!["xxxxxx"], "mymaster"),
        );

        if let Err(err) = data.run(sample_logic) {
            if let Ok(r) = err.reason::<sabi::DataHubError>() {
                match r {
                    sabi::DataHubError::FailToSetupLocalDataSrcs { errors } => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].0.as_ref(), "redis");
                        if let Ok(r) = errors[0].1.reason::<RedisSentinelSyncError>() {
                            match r {
                                RedisSentinelSyncError::FailToBuildSentinelClient => {}
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
                    _ => panic!("{:?}", err),
                }
            } else {
                panic!("{:?}", err)
            }
        } else {
            panic!();
        }
    }

    fn sample_logic_with_force_back_ok(data: &mut impl SampleDataSentinel) -> errs::Result<()> {
        data.set_sample_key_with_force_back("Good Afternoon")?;
        Ok(())
    }
    fn sample_logic_with_force_back_err(data: &mut impl SampleDataSentinel) -> errs::Result<()> {
        data.set_sample_key_with_force_back("Good Afternoon")?;
        Err(errs::Err::new("XXX"))
    }
    fn sample_logic_with_pre_commit(data: &mut impl SampleDataSentinel) -> errs::Result<()> {
        data.set_sample_key_with_pre_commit("Good Evening")?;
        Ok(())
    }
    fn sample_logic_with_post_commit(data: &mut impl SampleDataSentinel) -> errs::Result<()> {
        data.set_sample_key_with_post_commit("Good Night")?;
        Ok(())
    }

    #[test]
    fn test_with_force_back() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelDataSrc::new(
                vec![
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
            ),
        );

        let r = data.txn(sample_logic_with_force_back_ok);
        assert!(r.is_ok());

        {
            let mut sentinel = redis::sentinel::Sentinel::build(vec![
                "redis://127.0.0.1:26479",
                "redis://127.0.0.1:26480",
                "redis://127.0.0.1:26481",
            ])
            .unwrap();
            let client = sentinel.master_for("mymaster", None).unwrap();
            let mut conn = client.get_connection().unwrap();

            let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back_sentinel");
            let _: redis::RedisResult<()> = conn.del("sample_force_back_sentinel");
            assert_eq!(r.unwrap().unwrap(), "Good Afternoon");

            let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back_sentinel_2");
            let _: redis::RedisResult<()> = conn.del("sample_force_back_sentinel_2");
            assert_eq!(r.unwrap().unwrap(), "Good Afternoon");
        }

        if let Err(err) = data.txn(sample_logic_with_force_back_err) {
            assert_eq!(err.reason::<&str>().unwrap(), &"XXX");
        } else {
            panic!();
        }
        {
            let mut sentinel = redis::sentinel::Sentinel::build(vec![
                "redis://127.0.0.1:26479",
                "redis://127.0.0.1:26480",
                "redis://127.0.0.1:26481",
            ])
            .unwrap();
            let client = sentinel.master_for("mymaster", None).unwrap();
            let mut conn = client.get_connection().unwrap();

            let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back_sentinel");
            let _: redis::RedisResult<()> = conn.del("sample_force_back_sentinel");
            assert!(r.unwrap().is_none());

            let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back_sentinel_2");
            let _: redis::RedisResult<()> = conn.del("sample_force_back_sentinel_2");
            assert!(r.unwrap().is_none());
        }

        Ok(())
    }

    #[test]
    fn test_txn_and_pre_commit() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelDataSrc::new(
                vec![
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
            ),
        );
        data.txn(sample_logic_with_pre_commit)?;

        {
            let mut sentinel = redis::sentinel::Sentinel::build(vec![
                "redis://127.0.0.1:26479",
                "redis://127.0.0.1:26480",
                "redis://127.0.0.1:26481",
            ])
            .unwrap();
            let client = sentinel.master_for("mymaster", None).unwrap();
            let mut conn = client.get_connection().unwrap();

            let s: redis::RedisResult<Option<String>> = conn.get("sample_pre_commit_sentinel");
            let _: redis::RedisResult<()> = conn.del("sample_pre_commit_sentinel");
            assert_eq!(s.unwrap().unwrap(), "Good Evening");
        }

        Ok(())
    }

    #[test]
    fn test_txn_and_post_commit() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelDataSrc::new(
                vec![
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
            ),
        );
        data.txn(sample_logic_with_post_commit)?;

        {
            let mut sentinel = redis::sentinel::Sentinel::build(vec![
                "redis://127.0.0.1:26479",
                "redis://127.0.0.1:26480",
                "redis://127.0.0.1:26481",
            ])
            .unwrap();
            let client = sentinel.master_for("mymaster", None).unwrap();
            let mut conn = client.get_connection().unwrap();

            let s: redis::RedisResult<Option<String>> = conn.get("sample_post_commit_sentinel");
            let _: redis::RedisResult<()> = conn.del("sample_post_commit_sentinel");
            assert_eq!(s.unwrap().unwrap(), "Good Night");
        }

        Ok(())
    }
}
