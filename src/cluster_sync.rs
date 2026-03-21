// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use sabi::{AsyncGroup, DataConn, DataSrc};

use r2d2::{Builder, Pool, PooledConnection};
use redis::cluster::{ClusterClient, ClusterClientBuilder, ClusterConnection};
use redis::IntoConnectionInfo;

use std::fmt::Debug;
use std::{mem, time};

/// The error type for synchronous Redis Cluster operations.
#[derive(Debug)]
pub enum RedisClusterSyncError {
    /// Indicates that the Redis Cluster data source has not been set up yet.
    NotSetupYet,
    /// Indicates that the Redis Cluster data source has already been set up.
    AlreadySetup,
    /// Indicates a failure to get a connection from the pool.
    FailToGetConnectionFromPool,
    /// Indicates a failure to build a Redis Cluster client.
    FailToBuildClient,
    /// Indicates a failure to build a Redis connection pool.
    FailToBuildPool,
}

#[allow(clippy::type_complexity)]
/// A data connection for a Redis Cluster, providing synchronous operations.
///
/// This structure holds a connection pool for a Redis Cluster and allows for adding hooks
/// (pre-commit, post-commit, and force-back) that are executed during the lifecycle
/// of a data operation managed by `sabi`.
///
/// # Examples
/// ```
/// use sabi_redis::cluster::RedisClusterDataConn;
/// use redis::Commands;
/// use sabi::DataAcc;
///
/// trait MyDataAcc: DataAcc {
///     fn set_value(&mut self, key: &str, val: &str) -> errs::Result<()> {
///         let data_conn = self.get_data_conn::<RedisClusterDataConn>("redis")?;
///         let mut conn = data_conn.get_connection()?;
///         conn.set(key, val).map_err(|e| errs::Err::with_source("fail", e))
///     }
/// }
/// ```
pub struct RedisClusterDataConn {
    pool: Pool<ClusterClient>,
    pre_commit_vec: Vec<Box<dyn FnMut(&mut ClusterConnection) -> errs::Result<()>>>,
    post_commit_vec: Vec<Box<dyn FnMut(&mut ClusterConnection) -> errs::Result<()>>>,
    force_back_vec: Vec<Box<dyn FnMut(&mut ClusterConnection) -> errs::Result<()>>>,
}

impl RedisClusterDataConn {
    fn new(pool: Pool<ClusterClient>) -> Self {
        Self {
            pool,
            pre_commit_vec: Vec::new(),
            post_commit_vec: Vec::new(),
            force_back_vec: Vec::new(),
        }
    }

    /// Gets a cluster connection from the pool.
    ///
    /// # Returns
    /// Returns a `Result` containing a `PooledConnection<ClusterClient>` on success,
    /// or a `RedisClusterSyncError::FailToGetConnectionFromPool` wrapped in `errs::Err` on failure.
    pub fn get_connection(&mut self) -> errs::Result<PooledConnection<ClusterClient>> {
        self.pool.get().map_err(|e| {
            errs::Err::with_source(RedisClusterSyncError::FailToGetConnectionFromPool, e)
        })
    }

    /// Gets a cluster connection from the pool with a specific timeout.
    ///
    /// # Arguments
    /// * `timeout` - A `Duration` to wait for a connection before failing.
    ///
    /// # Returns
    /// Returns a `Result` containing a `PooledConnection<ClusterClient>` on success,
    /// or a `RedisClusterSyncError::FailToGetConnectionFromPool` wrapped in `errs::Err` on failure.
    pub fn get_connection_with_timeout(
        &self,
        timeout: time::Duration,
    ) -> errs::Result<PooledConnection<ClusterClient>> {
        self.pool.get_timeout(timeout).map_err(|e| {
            errs::Err::with_source(RedisClusterSyncError::FailToGetConnectionFromPool, e)
        })
    }

    /// Tries to get a cluster connection from the pool immediately without waiting.
    ///
    /// # Returns
    /// Returns `Some(PooledConnection<ClusterClient>)` if a connection is available, otherwise `None`.
    pub fn try_get_connection(&self) -> Option<PooledConnection<ClusterClient>> {
        self.pool.try_get()
    }

    /// Adds a function to be executed before a commit occurs in the `sabi` lifecycle.
    ///
    /// # Arguments
    /// * `f` - A closure or function that takes a mutable reference to a `ClusterConnection` and returns a `Result`.
    pub fn add_pre_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut ClusterConnection) -> errs::Result<()> + 'static,
    {
        self.pre_commit_vec.push(Box::new(f));
    }

    /// Adds a function to be executed after a successful commit in the `sabi` lifecycle.
    ///
    /// # Arguments
    /// * `f` - A closure or function that takes a mutable reference to a `ClusterConnection` and returns a `Result`.
    pub fn add_post_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut ClusterConnection) -> errs::Result<()> + 'static,
    {
        self.post_commit_vec.push(Box::new(f));
    }

    /// Adds a function to be executed when a rollback or forced recovery is triggered.
    ///
    /// # Arguments
    /// * `f` - A closure or function that takes a mutable reference to a `ClusterConnection` and returns a `Result`.
    pub fn add_force_back<F>(&mut self, f: F)
    where
        F: FnMut(&mut ClusterConnection) -> errs::Result<()> + 'static,
    {
        self.force_back_vec.push(Box::new(f));
    }
}

impl DataConn for RedisClusterDataConn {
    fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        match self.pool.get() {
            Ok(mut conn) => {
                for f in self.pre_commit_vec.iter_mut() {
                    f(&mut conn)?;
                }
                Ok(())
            }
            Err(e) => Err(errs::Err::with_source(
                RedisClusterSyncError::FailToGetConnectionFromPool,
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
                    errs::Err::with_source(RedisClusterSyncError::FailToGetConnectionFromPool, e);
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
                    errs::Err::with_source(RedisClusterSyncError::FailToGetConnectionFromPool, e);
            }
        };
    }

    fn close(&mut self) {}
}

/// A data source for Redis Cluster, used to initialize and provide `RedisClusterDataConn` instances.
///
/// This struct implements the `DataSrc` trait from the `sabi` library.
///
/// # Examples
/// ```
/// use sabi_redis::cluster::RedisClusterDataSrc;
/// use sabi::DataHub;
///
/// let mut data = DataHub::new();
/// data.uses("redis", RedisClusterDataSrc::new(vec![
///     "redis://127.0.0.1:7000/",
///     "redis://127.0.0.1:7001/",
///     "redis://127.0.0.1:7002/",
/// ]));
/// ```
pub struct RedisClusterDataSrc {
    pool: Option<RedisPool>,
}

enum RedisPool {
    Object(Pool<ClusterClient>),
    Builder(Box<(ClusterClientBuilder, Builder<ClusterClient>)>),
}

impl RedisClusterDataSrc {
    /// Creates a new `RedisClusterDataSrc` with the given cluster node addresses.
    ///
    /// # Arguments
    /// * `addrs` - An iterator of items that can be converted into Redis connection info.
    ///
    /// # Returns
    /// Returns a new instance of `RedisClusterDataSrc`.
    pub fn new<I, T>(addrs: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: IntoConnectionInfo,
    {
        let builder = ClusterClientBuilder::new(addrs);
        Self {
            pool: Some(RedisPool::Builder(Box::new((builder, Pool::builder())))),
        }
    }

    /// Creates a new `RedisClusterDataSrc` with a pre-configured `ClusterClientBuilder`.
    ///
    /// # Arguments
    /// * `client_builder` - A `ClusterClientBuilder` for the Redis Cluster.
    ///
    /// # Returns
    /// Returns a new instance of `RedisClusterDataSrc`.
    pub fn with_client_builder(client_builder: ClusterClientBuilder) -> Self {
        Self {
            pool: Some(RedisPool::Builder(Box::new((
                client_builder,
                Pool::builder(),
            )))),
        }
    }

    /// Creates a new `RedisClusterDataSrc` with a custom cluster client builder and pool builder.
    ///
    /// # Arguments
    /// * `client_builder` - A `ClusterClientBuilder` for the Redis Cluster.
    /// * `pool_builder` - A `r2d2::Builder` for configuring the connection pool.
    ///
    /// # Returns
    /// Returns a new instance of `RedisClusterDataSrc`.
    pub fn with_client_builder_and_pool_builder(
        client_builder: ClusterClientBuilder,
        pool_builder: Builder<ClusterClient>,
    ) -> Self {
        Self {
            pool: Some(RedisPool::Builder(Box::new((client_builder, pool_builder)))),
        }
    }
}

impl DataSrc<RedisClusterDataConn> for RedisClusterDataSrc {
    fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let pool_opt = mem::take(&mut self.pool);
        let pool = pool_opt.ok_or_else(|| errs::Err::new(RedisClusterSyncError::AlreadySetup))?;
        match pool {
            RedisPool::Builder(cfg) => {
                let (client_builder, pool_builder) = *cfg;
                let client = client_builder.build().map_err(|e| {
                    errs::Err::with_source(RedisClusterSyncError::FailToBuildClient, e)
                })?;

                let pool = pool_builder.build(client).map_err(|e| {
                    errs::Err::with_source(RedisClusterSyncError::FailToBuildPool, e)
                })?;

                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            _ => Err(errs::Err::new(RedisClusterSyncError::AlreadySetup)),
        }
    }

    fn close(&mut self) {}

    fn create_data_conn(&mut self) -> errs::Result<Box<RedisClusterDataConn>> {
        let pool = self
            .pool
            .as_mut()
            .ok_or_else(|| errs::Err::new(RedisClusterSyncError::NotSetupYet))?;
        match pool {
            RedisPool::Object(pool) => Ok(Box::new(RedisClusterDataConn::new(pool.clone()))),
            _ => Err(errs::Err::new(RedisClusterSyncError::NotSetupYet)),
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
    trait RedisClusterSampleDataAcc: DataAcc {
        fn get_sample_key(&mut self) -> errs::Result<Option<String>> {
            let data_conn = self.get_data_conn::<RedisClusterDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;
            conn.get("sample_cluster")
                .map_err(|e| errs::Err::with_source(SampleError::FailToGetValue, e))
        }
        fn set_sample_key(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisClusterDataConn>("redis")?;
            let mut conn =
                data_conn.get_connection_with_timeout(time::Duration::from_millis(1000))?;
            conn.set("sample_cluster", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))
        }
        fn del_sample_key(&mut self) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisClusterDataConn>("redis")?;
            let mut conn = data_conn.try_get_connection().unwrap();
            conn.del("sample_cluster")
                .map_err(|e| errs::Err::with_source(SampleError::FailToDelValue, e))
        }

        fn set_sample_key_with_force_back(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisClusterDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;

            conn.set::<&str, &str, ()>("sample_force_back_cluster", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;

            data_conn.add_force_back(|conn| {
                conn.del("sample_force_back_cluster")
                    .map_err(|e| errs::Err::with_source("fail to force back", e))
            });

            conn.set::<&str, &str, ()>("sample_force_back_cluster_2", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;

            data_conn.add_force_back(|conn| {
                conn.del("sample_force_back_cluster_2")
                    .map_err(|e| errs::Err::with_source("fail to force back", e))
            });

            Ok(())
        }

        fn set_sample_key_with_pre_commit(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisClusterDataConn>("redis")?;

            let val_owned = val.to_string();

            data_conn.add_pre_commit(move |conn| {
                conn.set::<&str, &str, ()>("sample_pre_commit_cluster", &val_owned)
                    .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;
                Ok(())
            });

            Ok(())
        }

        fn set_sample_key_with_post_commit(&mut self, val: &str) -> errs::Result<()> {
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
        fn set_sample_key_with_pre_commit(&mut self, val: &str) -> errs::Result<()>;
        fn set_sample_key_with_post_commit(&mut self, val: &str) -> errs::Result<()>;
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
    fn test_new() -> errs::Result<()> {
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
    fn test_with_pool_config() -> errs::Result<()> {
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
                if let Ok(r) = errors[0].1.reason::<RedisClusterSyncError>() {
                    match r {
                        RedisClusterSyncError::FailToBuildClient => {}
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

    fn sample_logic_with_force_back_ok(data: &mut impl SampleDataCluster) -> errs::Result<()> {
        data.set_sample_key_with_force_back("Good Afternoon")?;
        Ok(())
    }
    fn sample_logic_with_force_back_err(data: &mut impl SampleDataCluster) -> errs::Result<()> {
        data.set_sample_key_with_force_back("Good Afternoon")?;
        Err(errs::Err::new("XXX"))
    }
    fn sample_logic_with_pre_commit(data: &mut impl SampleDataCluster) -> errs::Result<()> {
        data.set_sample_key_with_pre_commit("Good Evening")?;
        Ok(())
    }
    fn sample_logic_with_post_commit(data: &mut impl SampleDataCluster) -> errs::Result<()> {
        data.set_sample_key_with_post_commit("Good Night")?;
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

        let r = data.txn(sample_logic_with_force_back_ok);
        assert!(r.is_ok());

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
            assert_eq!(r.unwrap().unwrap(), "Good Afternoon");

            let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back_cluster_2");
            let _: redis::RedisResult<()> = conn.del("sample_force_back_cluster_2");
            assert_eq!(r.unwrap().unwrap(), "Good Afternoon");
        }

        let err = data.txn(sample_logic_with_force_back_err).unwrap_err();
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
        }
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

        data.txn(sample_logic_with_pre_commit)?;

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

        data.txn(sample_logic_with_post_commit)?;

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
}
