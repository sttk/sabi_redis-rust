// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use sabi::{AsyncGroup, DataConn, DataSrc};

use redis::cluster::{ClusterClient, ClusterClientBuilder, ClusterConnection};
use redis::IntoConnectionInfo;

use std::fmt::Debug;
use std::{mem, time};

/// Represents a reason for errors that can occur during `RedisClusterDataSrc` operations,
/// to be passed to `errs::Err`.
#[derive(Debug)]
pub enum RedisClusterDataSrcError {
    /// Indicates that a connection was requested before `RedisClusterDataSrc` was set up.
    NotSetupYet,

    /// Indicates that a setup operation was attempted on an already-configured `RedisClusterDataSrc`.
    AlreadySetup,

    /// Indicates a failure to get a Redis Cluster connection from the pool.
    FailToGetConnectionFromPool,

    /// Indicates a failure to build the Redis Cluster client.
    FailToBuildClient,

    /// Indicates a failure to build the Redis Cluster connection pool.
    FailToBuildPool,
}

/// A session-scoped connection to the Redis Cluster.
///
/// This struct holds a pooled Redis Cluster connection and provides a `get_connection` method
/// to access it. It also provides "pre-commit", "post-commit" and "force back" mechanisms for handling transaction failures.
/// Since Redis Cluster does not support rollbacks, changes are committed with each update operation.
/// The `add_force_back` method allows registering functions to manually revert changes
/// if an error occurs during a multi-step process or a transaction involving
/// other external data sources.
#[allow(clippy::type_complexity)]
pub struct RedisClusterDataConn {
    pool: r2d2::Pool<ClusterClient>,
    pre_commit_vec: Vec<Box<dyn FnMut(&mut ClusterConnection) -> errs::Result<()>>>,
    post_commit_vec: Vec<Box<dyn FnMut(&mut ClusterConnection) -> errs::Result<()>>>,
    force_back_vec: Vec<Box<dyn FnMut(&mut ClusterConnection) -> errs::Result<()>>>,
}

impl RedisClusterDataConn {
    /// Creates a new `RedisClusterDataConn` instance.
    ///
    /// This is typically called internally by `RedisClusterDataSrc`.
    fn new(pool: r2d2::Pool<ClusterClient>) -> Self {
        Self {
            pool,
            pre_commit_vec: Vec::new(),
            post_commit_vec: Vec::new(),
            force_back_vec: Vec::new(),
        }
    }

    /// Retrieves a mutable reference to the underlying Redis Cluster connection from the pool.
    /// This method will wait for at most the configured connection timeout before returning an
    /// error.
    ///
    /// This reference allows direct access to Redis Cluster commands.
    pub fn get_connection(&mut self) -> errs::Result<r2d2::PooledConnection<ClusterClient>> {
        self.pool.get().map_err(|e| {
            errs::Err::with_source(RedisClusterDataSrcError::FailToGetConnectionFromPool, e)
        })
    }

    /// Retrieves a mutable reference to the underlying Redis Cluster connection from the pool,
    /// waiting for at most `timeout`.
    ///
    /// This reference allows direct access to Redis Cluster commands.
    pub fn get_connection_with_timeout(
        &self,
        timeout: time::Duration,
    ) -> errs::Result<r2d2::PooledConnection<ClusterClient>> {
        self.pool.get_timeout(timeout).map_err(|e| {
            errs::Err::with_source(RedisClusterDataSrcError::FailToGetConnectionFromPool, e)
        })
    }

    /// Attempt to retrieves a mutable reference to the underlying Redis Cluster connection from the pool.
    /// This method will not block waiting to establish a new connection.
    ///
    /// This reference allows direct access to Redis Cluster commands.
    pub fn try_get_connection(&self) -> Option<r2d2::PooledConnection<ClusterClient>> {
        self.pool.try_get()
    }

    /// Adds a function to the list of "pre commit" operations.
    ///
    /// The provided function will be called after all other database update processes have
    /// finished, and right before their commit processes are made.
    pub fn add_pre_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut ClusterConnection) -> errs::Result<()> + 'static,
    {
        self.pre_commit_vec.push(Box::new(f));
    }

    /// Adds a function to the list of "post commit" operations.
    ///
    /// The provided function will be called as post-transaction processes.
    pub fn add_post_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut ClusterConnection) -> errs::Result<()> + 'static,
    {
        self.post_commit_vec.push(Box::new(f));
    }

    /// Adds a function to the list of "force back" operations.
    ///
    /// The provided function will be called if the transaction needs to be reverted
    /// due to a failure in a subsequent operation.
    pub fn add_force_back<F>(&mut self, f: F)
    where
        F: FnMut(&mut ClusterConnection) -> errs::Result<()> + 'static,
    {
        self.force_back_vec.push(Box::new(f));
    }
}

impl DataConn for RedisClusterDataConn {
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
                RedisClusterDataSrcError::FailToGetConnectionFromPool,
                e,
            )),
        }
    }

    /// Commits the transaction.
    ///
    /// Note that Redis Cluster does not have a native rollback mechanism. All changes are committed
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
                    RedisClusterDataSrcError::FailToGetConnectionFromPool,
                    e,
                );
            }
        };
    }

    /// Rolls back the transaction.
    ///
    /// This method is provided to satisfy the `DataConn` trait but does not perform any action
    /// since Redis Cluster does not support native rollbacks. The `add_force_back` and `force_back`
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
                    RedisClusterDataSrcError::FailToGetConnectionFromPool,
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

/// Manages a connection pool for a Redis Cluster data source.
///
/// This struct is responsible for setting up the Redis Cluster connection pool
/// using a `ClusterClient` and creating new `RedisClusterDataConn` instances from the pool.
///
/// `RedisClusterDataSrc` implements the `DataSrc` trait, allowing it to be used within a `DataHub`.
pub struct RedisClusterDataSrc {
    pool: Option<RedisPool>,
}

enum RedisPool {
    Object(r2d2::Pool<ClusterClient>),
    Builder(Box<ClusterClientBuilder>, r2d2::Builder<ClusterClient>),
}

impl RedisClusterDataSrc {
    /// Creates a new `RedisClusterDataSrc` instance with the specified Redis Cluster addresses.
    ///
    /// The `addrs` parameter can be an iterator of connection strings
    /// (e.g., `vec!["redis://127.0.0.1:7000/", "redis://127.0.0.1:7001/"]`).
    pub fn new<I, T>(addrs: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: IntoConnectionInfo,
    {
        let builder = ClusterClientBuilder::new(addrs);
        Self {
            pool: Some(RedisPool::Builder(Box::new(builder), r2d2::Pool::builder())),
        }
    }

    /// Creates a new `RedisClusterDataSrc` instance using an existing `ClusterClientBuilder`.
    pub fn with_client_builder(client_builder: ClusterClientBuilder) -> Self {
        Self {
            pool: Some(RedisPool::Builder(
                Box::new(client_builder),
                r2d2::Pool::builder(),
            )),
        }
    }

    /// Creates a new `RedisClusterDataSrc` instance using an existing `ClusterClientBuilder`
    /// and a custom pool builder.
    ///
    /// This allows for fine-tuning the connection pool settings.
    pub fn with_client_builder_and_pool_builder(
        client_builder: ClusterClientBuilder,
        pool_builder: r2d2::Builder<ClusterClient>,
    ) -> Self {
        Self {
            pool: Some(RedisPool::Builder(Box::new(client_builder), pool_builder)),
        }
    }
}

impl DataSrc<RedisClusterDataConn> for RedisClusterDataSrc {
    /// Sets up the Redis Cluster connection pool.
    ///
    /// This method creates a `ClusterClient` and builds an `r2d2::Pool`.
    /// It must be called before `create_data_conn`. It will return an error if
    /// called more than once on the same instance.
    fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let pool_opt = mem::take(&mut self.pool);
        let pool =
            pool_opt.ok_or_else(|| errs::Err::new(RedisClusterDataSrcError::AlreadySetup))?;
        match pool {
            RedisPool::Builder(conn_builder, pool_builder) => {
                let client = conn_builder.build().map_err(|e| {
                    errs::Err::with_source(RedisClusterDataSrcError::FailToBuildClient, e)
                })?;

                let pool = pool_builder.build(client).map_err(|e| {
                    errs::Err::with_source(RedisClusterDataSrcError::FailToBuildPool, e)
                })?;

                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            _ => Err(errs::Err::new(RedisClusterDataSrcError::AlreadySetup)),
        }
    }

    /// Closes the data source.
    ///
    /// This method does not perform any action since the connection pool
    /// will be automatically dropped when the struct goes out of scope.
    fn close(&mut self) {}

    /// Creates a new `RedisClusterDataConn` instance.
    ///
    /// This method retrieves a connection from the internal pool. It will return a
    /// `NotSetupYet` error if the data source has not been set up.
    fn create_data_conn(&mut self) -> errs::Result<Box<RedisClusterDataConn>> {
        let pool = self
            .pool
            .as_mut()
            .ok_or_else(|| errs::Err::new(RedisClusterDataSrcError::NotSetupYet))?;
        match pool {
            RedisPool::Object(pool) => Ok(Box::new(RedisClusterDataConn::new(pool.clone()))),
            _ => Err(errs::Err::new(RedisClusterDataSrcError::NotSetupYet)),
        }
    }
}

#[cfg(test)]
mod tests_of_cluster_sync {
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
    trait RedisClusterSampleDataAcc: DataAcc {
        fn get_sample_key(&mut self) -> errs::Result<Option<String>> {
            let data_conn = self.get_data_conn::<RedisClusterDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;
            conn.get("sample_cluster")
                .map_err(|e| errs::Err::with_source(SampleError::FailToGetValue, e))
        }
        fn set_sample_key(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisClusterDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;
            conn.set("sample_cluster", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))
        }
        fn del_sample_key(&mut self) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisClusterDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;
            conn.del("sample_cluster")
                .map_err(|e| errs::Err::with_source(SampleError::FailToDelValue, e))
        }

        fn set_sample_key_with_force_back(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisClusterDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;

            let log_opt: Option<String> = conn.get("log_cluster").unwrap();
            let log = log_opt.unwrap_or("".to_string());
            let _: Option<()> = conn.set("log_cluster", log + "LOG1.").unwrap();

            conn.set::<&str, &str, ()>("sample_force_back_cluster", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;

            data_conn.add_force_back(|conn| {
                let log_opt: Option<String> = conn.get("log_cluster").unwrap();
                let log = log_opt.unwrap_or("".to_string());
                let _: Option<()> = conn.set("log_cluster", log + "LOG2.").unwrap();

                conn.del("sample_force_back_cluster")
                    .map_err(|e| errs::Err::with_source("fail to force back", e))
            });

            let log_opt: Option<String> = conn.get("log_cluster").unwrap();
            let log = log_opt.unwrap_or("".to_string());
            let _: Option<()> = conn.set("log_cluster", log + "LOG3.").unwrap();

            conn.set::<&str, &str, ()>("sample_force_back_cluster_2", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;

            data_conn.add_force_back(|conn| {
                let log_opt: Option<String> = conn.get("log_cluster").unwrap();
                let log = log_opt.unwrap_or("".to_string());
                let _: Option<()> = conn.set("log_cluster", log + "LOG4.").unwrap();

                conn.del("sample_force_back_cluster_2")
                    .map_err(|e| errs::Err::with_source("fail to force back", e))
            });

            Ok(())
        }

        fn set_sample_key_in_pre_commit(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisClusterDataConn>("redis")?;

            let val_owned = val.to_string();

            data_conn.add_pre_commit(move |conn| {
                conn.set::<&str, &str, ()>("sample_pre_commit_cluster", &val_owned)
                    .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;
                Ok(())
            });

            Ok(())
        }

        fn set_sample_key_in_post_commit(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisClusterDataConn>("redis")?;

            let val_owned = val.to_string();

            data_conn.add_post_commit(move |conn| {
                conn.set::<&str, &str, ()>("sample_post_commit_cluster", &val_owned)
                    .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;
                Ok(())
            });

            Ok(())
        }
    }
    impl RedisClusterSampleDataAcc for DataHub {}

    #[overridable]
    trait SampleDataCluster {
        fn get_sample_key(&mut self) -> errs::Result<Option<String>>;
        fn set_sample_key(&mut self, value: &str) -> errs::Result<()>;
        fn del_sample_key(&mut self) -> errs::Result<()>;
        fn set_sample_key_with_force_back(&mut self, val: &str) -> errs::Result<()>;
        fn set_sample_key_in_pre_commit(&mut self, val: &str) -> errs::Result<()>;
        fn set_sample_key_in_post_commit(&mut self, val: &str) -> errs::Result<()>;
    }
    #[override_with(RedisClusterSampleDataAcc)]
    impl SampleDataCluster for DataHub {}

    fn sample_logic(data: &mut impl SampleDataCluster) -> errs::Result<()> {
        data.get_sample_key().expect("Data exists");

        data.set_sample_key("Hello")?;

        assert_eq!(data.get_sample_key().expect("No Data").unwrap(), "Hello");

        data.del_sample_key()?;
        Ok(())
    }

    #[test]
    fn ok_by_redis_uri() -> Result<(), Box<dyn std::error::Error>> {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisClusterDataSrc::new(vec![
                "redis://127.0.0.1:7000/",
                "redis://127.0.0.1:7001/",
                "redis://127.0.0.1:7002/",
            ]),
        );
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn ok_by_uri_and_pool_config() -> Result<(), Box<dyn std::error::Error>> {
        let client_builder = ClusterClientBuilder::new(vec![
            "redis://127.0.0.1:7000/",
            "redis://127.0.0.1:7001/",
            "redis://127.0.0.1:7002/",
        ]);
        let pool_builder = r2d2::Pool::<ClusterClient>::builder()
            .max_size(100)
            .min_idle(Some(10))
            .max_lifetime(Some(time::Duration::from_secs(60 * 60)))
            .idle_timeout(Some(time::Duration::from_secs(5 * 60)))
            .connection_timeout(time::Duration::from_secs(30));
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisClusterDataSrc::with_client_builder_and_pool_builder(client_builder, pool_builder),
        );
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn fail_to_setup() {
        let mut data = DataHub::new();
        data.uses("redis", RedisClusterDataSrc::new(vec!["xxxxx"]));
        let err = data.run(sample_logic).unwrap_err();

        match err.reason::<sabi::DataHubError>().unwrap() {
            sabi::DataHubError::FailToSetupLocalDataSrcs { errors } => {
                assert_eq!(errors.len(), 1);
                assert_eq!(errors[0].0.as_ref(), "redis");
                if let Ok(r) = errors[0].1.reason::<RedisClusterDataSrcError>() {
                    match r {
                        RedisClusterDataSrcError::FailToBuildClient => {}
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

    fn sample_logic_in_txn_and_force_back(data: &mut impl SampleDataCluster) -> errs::Result<()> {
        data.set_sample_key_with_force_back("Good Afternoon")?;
        Err(errs::Err::new("XXX"))
    }
    fn sample_logic_in_txn_and_pre_commit(data: &mut impl SampleDataCluster) -> errs::Result<()> {
        data.set_sample_key_in_pre_commit("Good Evening")?;
        Ok(())
    }
    fn sample_logic_in_txn_and_post_commit(data: &mut impl SampleDataCluster) -> errs::Result<()> {
        data.set_sample_key_in_post_commit("Good Night")?;
        Ok(())
    }

    #[test]
    fn test_txn_and_pre_commit() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisClusterDataSrc::new(vec![
                "redis://127.0.0.1:7000/",
                "redis://127.0.0.1:7001/",
                "redis://127.0.0.1:7002/",
            ]),
        );

        data.txn(sample_logic_in_txn_and_pre_commit)?;

        {
            let client = ClusterClient::new(vec![
                "redis://127.0.0.1:7000/",
                "redis://127.0.0.1:7001/",
                "redis://127.0.0.1:7002/",
            ])
            .unwrap();
            let mut conn = client.get_connection().unwrap();
            let s: redis::RedisResult<Option<String>> = conn.get("sample_pre_commit_cluster");
            let _: redis::RedisResult<()> = conn.del("sample_pre_commit_cluster");
            assert_eq!(s.unwrap().unwrap(), "Good Evening");
        }
        Ok(())
    }

    #[test]
    fn test_txn_and_post_commit() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisClusterDataSrc::new(vec![
                "redis://127.0.0.1:7000/",
                "redis://127.0.0.1:7001/",
                "redis://127.0.0.1:7002/",
            ]),
        );

        data.txn(sample_logic_in_txn_and_post_commit)?;

        {
            let client = ClusterClient::new(vec![
                "redis://127.0.0.1:7000/",
                "redis://127.0.0.1:7001/",
                "redis://127.0.0.1:7002/",
            ])
            .unwrap();
            let mut conn = client.get_connection().unwrap();
            let s: redis::RedisResult<Option<String>> = conn.get("sample_post_commit_cluster");
            let _: redis::RedisResult<()> = conn.del("sample_post_commit_cluster");
            assert_eq!(s.unwrap().unwrap(), "Good Night");
        }
        Ok(())
    }

    #[test]
    fn test_txn_and_force_back() {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisClusterDataSrc::new(vec![
                "redis://127.0.0.1:7000/",
                "redis://127.0.0.1:7001/",
                "redis://127.0.0.1:7002/",
            ]),
        );

        let err = data.txn(sample_logic_in_txn_and_force_back).unwrap_err();
        assert_eq!(err.reason::<&str>().unwrap(), &"XXX");

        {
            let client = ClusterClient::new(vec![
                "redis://127.0.0.1:7000/",
                "redis://127.0.0.1:7001/",
                "redis://127.0.0.1:7002/",
            ])
            .unwrap();
            let mut conn = client.get_connection().unwrap();

            let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back_cluster");
            let _: redis::RedisResult<()> = conn.del("sample_force_back_cluster");
            assert!(r.unwrap().is_none());

            let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back_cluster_2");
            let _: redis::RedisResult<()> = conn.del("sample_force_back_cluster_2");
            assert!(r.unwrap().is_none());

            let log: redis::RedisResult<Option<String>> = conn.get("log_cluster");
            let _: redis::RedisResult<()> = conn.del("log_cluster");
            assert_eq!(log.unwrap().unwrap(), "LOG1.LOG3.LOG4.LOG2.");
        }
    }
}
