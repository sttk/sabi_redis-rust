// Copyright (C) 2025-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use sabi::{AsyncGroup, DataConn, DataSrc};

use redis::sentinel::{
    LockedSentinelClient, SentinelClient, SentinelClientBuilder, SentinelNodeConnectionInfo,
    SentinelServerType,
};

use std::fmt::Debug;
use std::mem;

/// Represents a reason for errors that can occur during `RedisSentinelDataSrc` operations,
/// to be passed to `errs::Err`.
#[derive(Debug)]
pub enum RedisSentinelDataSrcError {
    /// Indicates that a connection was requested before `RedisSentinelDataSrc` was set up.
    NotSetupYet,
    /// Indicates that a setup operation was attempted on an already-configured `RedisSentinelDataSrc`.
    AlreadySetup,
    /// Indicates a failure to parse connection addresses.
    FailToParseConnectionAddrs,
    /// Indicates a failure to create `SentinelClientBuilder`.
    FailToCreateSentinelClientBuilder,
    /// Indicates a failure to build the Redis Sentinel connection pool.
    FailToBuildPool,
    /// Indicates a failure to build the `SentinelClient`.
    FailToBuildSentinelClient,
    /// Indicates a failure to get a Redis Sentinel connection from the pool.
    FailToGetConnectionFromPool,
    /// Generic failure reason, used when a more specific error is not available.
    FailTo,
}

/// A session-scoped connection to the Redis server managed by Redis Sentinel.
///
/// This struct holds a pooled `LockedSentinelClient` connection and provides a `get_connection` method
/// to access it. It also provides "pre-commit", "post-commit" and "force back" mechanisms for handling transaction failures.
/// Since Redis does not support rollbacks, changes are committed with each update operation.
/// The `add_force_back` method allows registering functions to manually revert changes
/// if an error occurs during a multi-step process or a transaction involving
/// other external data sources.
#[allow(clippy::type_complexity)]
pub struct RedisSentinelDataConn {
    pool: r2d2::Pool<LockedSentinelClient>,
    pre_commit_vec: Vec<Box<dyn FnMut(&mut redis::Connection) -> errs::Result<()>>>,
    post_commit_vec: Vec<Box<dyn FnMut(&mut redis::Connection) -> errs::Result<()>>>,
    force_back_vec: Vec<Box<dyn FnMut(&mut redis::Connection) -> errs::Result<()>>>,
}

impl RedisSentinelDataConn {
    /// Creates a new `RedisSentinelDataConn` instance.
    ///
    /// This is typically called internally by `RedisSentinelDataSrc`.
    fn new(pool: r2d2::Pool<LockedSentinelClient>) -> Self {
        Self {
            pool,
            pre_commit_vec: Vec::new(),
            post_commit_vec: Vec::new(),
            force_back_vec: Vec::new(),
        }
    }

    /// Retrieves a mutable reference to the underlying Redis connection from the pool.
    /// This method will wait for at most the configured connection timeout before returning an
    /// error.
    ///
    /// This reference allows direct access to Redis commands.
    pub fn get_connection(&mut self) -> errs::Result<r2d2::PooledConnection<LockedSentinelClient>> {
        self.pool.get().map_err(|e| {
            errs::Err::with_source(RedisSentinelDataSrcError::FailToGetConnectionFromPool, e)
        })
    }

    /// Adds a function to the list of "pre commit" operations.
    ///
    /// The provided function will be called after all other database update processes have
    /// finished, and right before their commit processes are made.
    pub fn add_pre_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut redis::Connection) -> errs::Result<()> + 'static,
    {
        self.pre_commit_vec.push(Box::new(f));
    }

    /// Adds a function to the list of "post commit" operations.
    ///
    /// The provided function will be called as post-transaction processes.
    pub fn add_post_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut redis::Connection) -> errs::Result<()> + 'static,
    {
        self.post_commit_vec.push(Box::new(f));
    }

    /// Adds a function to the list of "force back" operations.
    ///
    /// The provided function will be called if the transaction needs to be reverted
    /// due to a failure in a subsequent operation.
    pub fn add_force_back<F>(&mut self, f: F)
    where
        F: FnMut(&mut redis::Connection) -> errs::Result<()> + 'static,
    {
        self.force_back_vec.push(Box::new(f));
    }
}

impl DataConn for RedisSentinelDataConn {
    /// Executes all functions registered as "pre commit" processes.
    ///
    /// This method is called after all other database update processes have finished and right
    /// before their commit processes are made, Since Redis does not have a rollback feature,
    /// it is possible to maintain transactional consistency by performing updates at this timing,
    /// as long as the updated data are not searched for within the transaction.
    fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        match self.pool.get() {
            Ok(mut conn) => {
                for f in self.pre_commit_vec.iter_mut() {
                    f(&mut conn)?;
                }
                Ok(())
            }
            Err(e) => Err(errs::Err::with_source(
                RedisSentinelDataSrcError::FailToGetConnectionFromPool,
                e,
            )),
        }
    }

    /// Commits the transaction.
    ///
    /// Note that Redis does not have a native rollback mechanism. All changes are committed
    /// as they are executed. This method is provided to satisfy the `DataConn` trait but
    /// does not perform any action.
    fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    /// Executes all functions registered as "post commit" processes.
    ///
    /// This method is called as post-transaction processes. Since Redis does not have a rollback
    /// feature, it's acceptable to perform update processes at this point that can be manually
    /// recovered later, even if they fail.
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
                let _ = errs::Err::with_source(
                    RedisSentinelDataSrcError::FailToGetConnectionFromPool,
                    e,
                );
            }
        };
    }

    /// Rolls back the transaction.
    ///
    /// This method is provided to satisfy the `DataConn` trait but does not perform any action
    /// since Redis does not support native rollbacks. The `add_force_back` and `force_back`
    /// mechanism should be used instead.
    fn rollback(&mut self, _ag: &mut AsyncGroup) {}

    /// Checks if a "force back" operation is required.
    ///
    /// Always returns `true` because Redis operations are committed immediately,
    /// and `force_back` is the only way to undo a failure.
    fn should_force_back(&self) -> bool {
        true
    }

    /// Executes all registered "force back" functions.
    ///
    /// This method is intended to be called when a transaction fails to
    /// manually revert changes.
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
                let _ = errs::Err::with_source(
                    RedisSentinelDataSrcError::FailToGetConnectionFromPool,
                    e,
                );
            }
        };
    }

    /// Closes the connection.
    ///
    /// This method is provided to satisfy the `DataConn` trait but
    /// does not perform any action as pooled connections are managed by the `r2d2` pool.
    fn close(&mut self) {}
}

/// Manages a connection pool for a Redis Sentinel data source.
///
/// This struct is responsible for setting up the Redis Sentinel connection pool
/// using a `redis::sentinel::SentinelClient` and creating new `RedisSentinelDataConn` instances from the pool.
pub struct RedisSentinelDataSrc<T>
where
    T: redis::IntoConnectionInfo,
{
    pool: Option<RedisPool<T>>,
}

enum RedisPool<T>
where
    T: redis::IntoConnectionInfo,
{
    Object(r2d2::Pool<LockedSentinelClient>),
    Sentinel(
        Vec<T>,
        String,
        Option<SentinelNodeConnectionInfo>,
        r2d2::Builder<LockedSentinelClient>,
    ),
    SentinelClient(SentinelClientBuilder, r2d2::Builder<LockedSentinelClient>),
}

impl<T> RedisSentinelDataSrc<T>
where
    T: redis::IntoConnectionInfo,
{
    /// Creates a new `RedisSentinelDataSrc` instance.
    ///
    /// The connection information for Sentinel nodes and the Redis service name are stored,
    /// but the connection pool is not built until the `setup` method is called.
    ///
    /// - `sentinels`: A vector of connection information for the Sentinel nodes (e.g., "redis://127.0.0.1:26379").
    /// - `service_name`: The name of the Redis master service to connect to (as configured in Sentinel).
    pub fn new(sentinels: Vec<T>, service_name: &str) -> Self {
        Self {
            pool: Some(RedisPool::Sentinel(
                sentinels,
                service_name.to_string(),
                None,
                r2d2::Pool::builder(),
            )),
        }
    }

    /// Creates a new `RedisSentinelDataSrc` instance with specific node connection information.
    ///
    /// This allows configuring specific details for connecting to the Redis master,
    /// such as database selection or password.
    ///
    /// - `sentinels`: A vector of connection information for the Sentinel nodes.
    /// - `service_name`: The name of the Redis master service.
    /// - `info`: `SentinelNodeConnectionInfo` to customize the connection to the Redis master.
    pub fn with_node_connection_info(
        sentinels: Vec<T>,
        service_name: &str,
        info: SentinelNodeConnectionInfo,
    ) -> Self {
        Self {
            pool: Some(RedisPool::Sentinel(
                sentinels,
                service_name.to_string(),
                Some(info),
                r2d2::Pool::builder(),
            )),
        }
    }

    /// Creates a new `RedisSentinelDataSrc` instance with specific node connection information
    /// and a custom `r2d2::Pool::builder`.
    ///
    /// This provides fine-grained control over both the Redis master connection details
    /// and the connection pool's configuration.
    ///
    /// - `sentinels`: A vector of connection information for the Sentinel nodes.
    /// - `service_name`: The name of the Redis master service.
    /// - `info`: `SentinelNodeConnectionInfo` to customize the connection to the Redis master.
    /// - `builder`: A custom `r2d2::Pool::builder` to configure the connection pool.
    pub fn with_node_connection_info_and_pool_builder(
        sentinels: Vec<T>,
        service_name: &str,
        info: SentinelNodeConnectionInfo,
        builder: r2d2::Builder<LockedSentinelClient>,
    ) -> Self {
        Self {
            pool: Some(RedisPool::Sentinel(
                sentinels,
                service_name.to_string(),
                Some(info),
                builder,
            )),
        }
    }
}

impl RedisSentinelDataSrc<&'static str> {
    /// Creates a new `RedisSentinelDataSrc` instance using a pre-configured `SentinelClientBuilder`.
    ///
    /// This allows for advanced configuration of the `SentinelClient` before it is built.
    ///
    /// - `builder`: A `SentinelClientBuilder` instance.
    pub fn with_sentinel_client_builder(builder: SentinelClientBuilder) -> Self {
        Self {
            pool: Some(RedisPool::SentinelClient(builder, r2d2::Pool::builder())),
        }
    }

    /// Creates a new `RedisSentinelDataSrc` instance using a pre-configured `SentinelClientBuilder`
    /// and a custom `r2d2::Pool::builder`.
    ///
    /// This provides the most flexible way to configure the Redis Sentinel data source,
    /// allowing full control over both the `SentinelClient` and the connection pool.
    ///
    /// - `client_builder`: A `SentinelClientBuilder` instance.
    /// - `pool_builder`: A custom `r2d2::Pool::builder` to configure the connection pool.
    pub fn with_sentinel_client_and_pool_builder(
        client_builder: SentinelClientBuilder,
        pool_builder: r2d2::Builder<LockedSentinelClient>,
    ) -> Self {
        Self {
            pool: Some(RedisPool::SentinelClient(client_builder, pool_builder)),
        }
    }
}

impl<T> DataSrc<RedisSentinelDataConn> for RedisSentinelDataSrc<T>
where
    T: redis::IntoConnectionInfo,
{
    /// Sets up the Redis Sentinel connection pool.
    ///
    /// This method creates a `SentinelClient` and builds an `r2d2::Pool`.
    /// It must be called before `create_data_conn`. It will return an error if
    /// called more than once on the same instance.
    fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let pool_opt = mem::take(&mut self.pool);
        let pool =
            pool_opt.ok_or_else(|| errs::Err::new(RedisSentinelDataSrcError::AlreadySetup))?;
        match pool {
            RedisPool::Sentinel(sentinels, service_name, node_conn_info_opt, pool_builder) => {
                let client = SentinelClient::build(
                    sentinels,
                    service_name,
                    node_conn_info_opt,
                    SentinelServerType::Master,
                )
                .map_err(|e| {
                    errs::Err::with_source(RedisSentinelDataSrcError::FailToBuildSentinelClient, e)
                })?;

                let pool = pool_builder
                    .build(LockedSentinelClient::new(client))
                    .map_err(|e| {
                        errs::Err::with_source(RedisSentinelDataSrcError::FailToBuildPool, e)
                    })?;

                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            RedisPool::SentinelClient(client_builder, pool_builder) => {
                let client = client_builder.build().map_err(|e| {
                    errs::Err::with_source(RedisSentinelDataSrcError::FailToBuildSentinelClient, e)
                })?;

                let pool = pool_builder
                    .build(LockedSentinelClient::new(client))
                    .map_err(|e| {
                        errs::Err::with_source(RedisSentinelDataSrcError::FailToBuildPool, e)
                    })?;

                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            _ => Err(errs::Err::new(RedisSentinelDataSrcError::AlreadySetup)),
        }
    }

    /// Closes the data source.
    ///
    /// This method does not perform any action since the connection pool
    /// will be automatically dropped when the struct goes out of scope.
    fn close(&mut self) {}

    /// Creates a new `RedisSentinelDataConn` instance.
    ///
    /// This method retrieves a connection from the internal pool. It will return a
    /// `NotSetupYet` error if the data source has not been set up.
    fn create_data_conn(&mut self) -> errs::Result<Box<RedisSentinelDataConn>> {
        let pool = self
            .pool
            .as_mut()
            .ok_or_else(|| errs::Err::new(RedisSentinelDataSrcError::NotSetupYet))?;
        match pool {
            RedisPool::Object(pool) => Ok(Box::new(RedisSentinelDataConn::new(pool.clone()))),
            _ => Err(errs::Err::new(RedisSentinelDataSrcError::NotSetupYet)),
        }
    }
}

#[cfg(test)]
mod tests_of_sentinel_sync {
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
            let mut conn = data_conn.get_connection()?;
            conn.set("sample_sentinel", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToGetValue, e))
        }
        fn del_sample_key(&mut self) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisSentinelDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;
            conn.del("sample_sentinel")
                .map_err(|e| errs::Err::with_source(SampleError::FailToDelValue, e))
        }

        fn set_sample_key_with_force_back(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisSentinelDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;

            let log_opt: Option<String> = conn.get("log_sentinel").unwrap();
            let log = log_opt.unwrap_or("".to_string());
            let _: Option<()> = conn.set("log_sentinel", log + "LOG1.").unwrap();

            conn.set::<&str, &str, ()>("sample_force_back_sentinel", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;

            data_conn.add_force_back(|conn| {
                let log_opt: Option<String> = conn.get("log_sentinel").unwrap();
                let log = log_opt.unwrap_or("".to_string());
                let _: Option<()> = conn.set("log_sentinel", log + "LOG2.").unwrap();

                conn.del("sample_force_back_sentinel")
                    .map_err(|e| errs::Err::with_source("fail to force back", e))
            });

            let log_opt: Option<String> = conn.get("log_sentinel").unwrap();
            let log = log_opt.unwrap_or("".to_string());
            let _: Option<()> = conn.set("log_sentinel", log + "LOG3.").unwrap();

            conn.set::<&str, &str, ()>("sample_force_back_sentinel_2", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;

            data_conn.add_force_back(|conn| {
                let log_opt: Option<String> = conn.get("log_sentinel").unwrap();
                let log = log_opt.unwrap_or("".to_string());
                let _: Option<()> = conn.set("log_sentinel", log + "LOG4.").unwrap();

                conn.del("sample_force_back_sentinel_2")
                    .map_err(|e| errs::Err::with_source("fail to force back", e))
            });

            Ok(())
        }

        fn set_sample_key_in_pre_commit(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisSentinelDataConn>("redis")?;

            let val_owned = val.to_string();

            data_conn.add_pre_commit(move |conn| {
                conn.set::<&str, &str, ()>("sample_pre_commit_sentinel", &val_owned)
                    .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;
                Ok(())
            });

            Ok(())
        }

        fn set_sample_key_in_post_commit(&mut self, val: &str) -> errs::Result<()> {
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
        fn set_sample_key_in_pre_commit(&mut self, val: &str) -> errs::Result<()>;
        fn set_sample_key_in_post_commit(&mut self, val: &str) -> errs::Result<()>;
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
    fn ok_by_sentinel_uris() -> errs::Result<()> {
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
    fn ok_with_node_connection_info() -> errs::Result<()> {
        let redis_connection_info = RedisConnectionInfo::default().set_db(1);
        let sentinel_node_connection_info =
            SentinelNodeConnectionInfo::default().set_redis_connection_info(redis_connection_info);

        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelDataSrc::with_node_connection_info(
                vec![
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
                sentinel_node_connection_info,
            ),
        );
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn ok_with_node_connection_info_and_pool_builder() -> errs::Result<()> {
        let redis_connection_info = RedisConnectionInfo::default().set_db(1);
        let sentinel_node_connection_info =
            SentinelNodeConnectionInfo::default().set_redis_connection_info(redis_connection_info);

        let pool_builder = r2d2::Pool::<LockedSentinelClient>::builder()
            .max_size(100)
            .min_idle(Some(10))
            .max_lifetime(Some(time::Duration::from_secs(60 * 60)))
            .idle_timeout(Some(time::Duration::from_secs(5 * 60)))
            .connection_timeout(time::Duration::from_secs(30));

        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelDataSrc::with_node_connection_info_and_pool_builder(
                vec![
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
                sentinel_node_connection_info,
                pool_builder,
            ),
        );
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn ok_with_sentinel_client_builder() -> errs::Result<()> {
        let builder = SentinelClientBuilder::new(
            vec![
                redis::ConnectionAddr::Tcp(String::from("127.0.0.1"), 26479),
                redis::ConnectionAddr::Tcp(String::from("127.0.0.1"), 26480),
                redis::ConnectionAddr::Tcp(String::from("127.0.0.1"), 26481),
            ],
            "mymaster".to_string(),
            redis::sentinel::SentinelServerType::Master,
        )
        .unwrap();

        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelDataSrc::with_sentinel_client_builder(builder),
        );
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn with_sentinel_client_and_pool_builder() -> errs::Result<()> {
        let builder = SentinelClientBuilder::new(
            vec![
                redis::ConnectionAddr::Tcp(String::from("127.0.0.1"), 26479),
                redis::ConnectionAddr::Tcp(String::from("127.0.0.1"), 26480),
                redis::ConnectionAddr::Tcp(String::from("127.0.0.1"), 26481),
            ],
            "mymaster".to_string(),
            redis::sentinel::SentinelServerType::Master,
        )
        .unwrap();

        let pool_builder = r2d2::Pool::<LockedSentinelClient>::builder()
            .max_size(100)
            .min_idle(Some(10))
            .max_lifetime(Some(time::Duration::from_secs(60 * 60)))
            .idle_timeout(Some(time::Duration::from_secs(5 * 60)))
            .connection_timeout(time::Duration::from_secs(30));

        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelDataSrc::with_sentinel_client_and_pool_builder(builder, pool_builder),
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
                        if let Ok(r) = errors[0].1.reason::<RedisSentinelDataSrcError>() {
                            match r {
                                RedisSentinelDataSrcError::FailToBuildSentinelClient => {}
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

    fn sample_logic_in_txn_and_force_back(data: &mut impl SampleDataSentinel) -> errs::Result<()> {
        data.set_sample_key_with_force_back("Good Afternoon")?;
        Err(errs::Err::new("XXX"))
    }
    fn sample_logic_in_txn_and_pre_commit(data: &mut impl SampleDataSentinel) -> errs::Result<()> {
        data.set_sample_key_in_pre_commit("Good Evening")?;
        Ok(())
    }
    fn sample_logic_in_txn_and_post_commit(data: &mut impl SampleDataSentinel) -> errs::Result<()> {
        data.set_sample_key_in_post_commit("Good Night")?;
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
        data.txn(sample_logic_in_txn_and_pre_commit)?;

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
        data.txn(sample_logic_in_txn_and_post_commit)?;

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

    #[test]
    fn test_txn_and_force_back() -> errs::Result<()> {
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
        if let Err(err) = data.txn(sample_logic_in_txn_and_force_back) {
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

            let log: redis::RedisResult<Option<String>> = conn.get("log_sentinel");
            let _: redis::RedisResult<()> = conn.del("log_sentinel");
            assert_eq!(log.unwrap().unwrap(), "LOG1.LOG3.LOG4.LOG2.");
        }

        Ok(())
    }
}
