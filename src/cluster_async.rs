// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use deadpool_redis::cluster::{Config, Connection, Pool, PoolConfig, Runtime};
use deadpool_redis::Timeouts;
use sabi::tokio::{AsyncGroup, DataConn, DataSrc};

use std::future::Future;
use std::{mem, pin};

/// Errors that can occur when using `RedisClusterAsyncDataSrc` or `RedisClusterAsyncDataConn`.
#[derive(Debug)]
pub enum RedisClusterAsyncDataSrcError {
    /// Indicates that the `RedisClusterAsyncDataSrc` has not been set up yet (i.e., `setup_async` was not called).
    NotSetupYet,

    /// Indicates that an attempt was made to set up `RedisClusterAsyncDataSrc` when it was already set up,
    /// or to set the internal pool when it was already set.
    AlreadySetup,

    /// Indicates a failure to build the Redis Cluster connection pool.
    FailToBuildPool,

    /// Indicates a failure to get a connection from the Redis Cluster pool.
    FailToGetConnectionFromPool,
}

type BoxedFuture = pin::Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'static>>;

/// `RedisClusterAsyncDataConn` is an asynchronous data connection for Redis Cluster,
/// implementing the `DataConn` trait from the `sabi::tokio` library. It manages Redis
/// connections from a pool and allows registration of asynchronous operations to be executed
/// at different transaction phases (pre-commit, post-commit, force-back).
pub struct RedisClusterAsyncDataConn {
    pool: Pool,
    pre_commit_vec: Vec<BoxedFuture>,
    post_commit_vec: Vec<BoxedFuture>,
    force_back_vec: Vec<BoxedFuture>,
}

impl RedisClusterAsyncDataConn {
    fn new(pool: Pool) -> Self {
        Self {
            pool,
            pre_commit_vec: Vec::new(),
            post_commit_vec: Vec::new(),
            force_back_vec: Vec::new(),
        }
    }

    /// Asynchronously retrieves a Redis connection from the internal connection pool.
    ///
    /// # Returns
    /// - `Ok(Connection)` if a connection is successfully retrieved.
    /// - `Err(errs::Err)` if there's a failure to get a connection from the pool.
    pub async fn get_connection_async(&mut self) -> errs::Result<Connection> {
        self.pool.get().await.map_err(|e| {
            errs::Err::with_source(
                RedisClusterAsyncDataSrcError::FailToGetConnectionFromPool,
                e,
            )
        })
    }

    /// Asynchronously retrieves a Redis connection from the internal connection pool, waiting
    /// for at most timeout.
    ///
    /// # Arguments
    /// - `timeouts`: Timeouts when getting a connection from a `Pool`.
    ///
    /// # Returns
    /// - `Ok(Connection)` if a connection is successfully retrieved.
    /// - `Err(errs::Err)` if there's a failure to get a connection from the pool.
    pub async fn get_connection_with_timeout_async(
        &mut self,
        timeouts: Timeouts,
    ) -> errs::Result<Connection> {
        self.pool.timeout_get(&timeouts).await.map_err(|e| {
            errs::Err::with_source(
                RedisClusterAsyncDataSrcError::FailToGetConnectionFromPool,
                e,
            )
        })
    }

    /// Adds an asynchronous function to be executed during the pre-commit phase.
    /// These functions are executed before the main transaction logic is considered "committed".
    ///
    /// If getting a connection from the pool fails when this method is called, an error future
    /// will be registered that will propagate the connection error during the pre-commit phase.
    ///
    /// # Arguments
    /// - `f`: An asynchronous closure that takes a `deadpool_redis::cluster::Connection` and
    ///   returns an `errs::Result<()>`.
    pub async fn add_pre_commit_async<F, Fut>(&mut self, mut f: F)
    where
        F: FnMut(Connection) -> Fut,
        Fut: Future<Output = errs::Result<()>> + Send + 'static,
    {
        match self.pool.get().await {
            Ok(pooled_conn) => {
                let fut = f(pooled_conn);
                self.pre_commit_vec.push(Box::pin(fut))
            }
            Err(e) => self.pre_commit_vec.push(Box::pin(async move {
                Err(errs::Err::with_source(
                    RedisClusterAsyncDataSrcError::FailToGetConnectionFromPool,
                    e,
                ))
            })),
        }
    }

    /// Adds an asynchronous function to be executed during the post-commit phase.
    /// These functions are executed after the main transaction logic has successfully completed.
    ///
    /// If getting a connection from the pool fails when this method is called, an error future
    /// will be registered that will propagate the connection error during the post-commit phase.
    ///
    /// # Arguments
    /// - `f`: An asynchronous closure that takes a `deadpool_redis::cluster::Connection` and
    ///   returns an `errs::Result<()>`.
    pub async fn add_post_commit_async<F, Fut>(&mut self, mut f: F)
    where
        F: FnMut(Connection) -> Fut,
        Fut: Future<Output = errs::Result<()>> + Send + 'static,
    {
        match self.pool.get().await {
            Ok(pooled_conn) => {
                let fut = f(pooled_conn);
                self.post_commit_vec.push(Box::pin(fut))
            }
            Err(e) => self.post_commit_vec.push(Box::pin(async move {
                Err(errs::Err::with_source(
                    RedisClusterAsyncDataSrcError::FailToGetConnectionFromPool,
                    e,
                ))
            })),
        }
    }

    /// Adds an asynchronous function to be executed during the force-back phase.
    /// These functions are executed if the main transaction logic fails and `should_force_back`
    /// returns `true`, allowing for cleanup or reversal of operations.
    ///
    /// If getting a connection from the pool fails when this method is called, an error future
    /// will be registered that will propagate the connection error during the force-back phase.
    ///
    /// # Arguments
    /// - `f`: An asynchronous closure that takes a `deadpool_redis::cluster::Connection` and
    ///   returns an `errs::Result<()>`.
    pub async fn add_force_back_async<F, Fut>(&mut self, mut f: F)
    where
        F: FnMut(Connection) -> Fut,
        Fut: Future<Output = errs::Result<()>> + Send + 'static,
    {
        match self.pool.get().await {
            Ok(pooled_conn) => {
                let fut = f(pooled_conn);
                self.force_back_vec.push(Box::pin(fut))
            }
            Err(e) => self.force_back_vec.push(Box::pin(async move {
                Err(errs::Err::with_source(
                    RedisClusterAsyncDataSrcError::FailToGetConnectionFromPool,
                    e,
                ))
            })),
        }
    }
}

impl DataConn for RedisClusterAsyncDataConn {
    /// Executes all registered pre-commit asynchronous operations.
    /// These operations are added to the provided `AsyncGroup` for concurrent execution.
    ///
    /// # Arguments
    /// - `ag`: An `AsyncGroup` to which the pre-commit futures will be added.
    ///
    /// # Returns
    /// - `Ok(())` if all futures are successfully added and scheduled.
    async fn pre_commit_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        let vec = mem::take(&mut self.pre_commit_vec);
        ag.add(async move {
            for fut in vec.into_iter() {
                fut.await?;
            }
            Ok(())
        });
        Ok(())
    }

    /// The commit phase for Redis Cluster is generally a no-op as commands are often executed immediately.
    /// This method always returns `Ok(())`.
    ///
    /// # Arguments
    /// - `_ag`: An `AsyncGroup` (unused in this implementation).
    ///
    /// # Returns
    /// - `Ok(())` always.
    async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    /// Executes all registered post-commit asynchronous operations.
    /// These operations are added to the provided `AsyncGroup` for concurrent execution.
    ///
    /// # Arguments
    /// - `ag`: An `AsyncGroup` to which the post-commit futures will be added.
    async fn post_commit_async(&mut self, ag: &mut AsyncGroup) {
        let vec = mem::take(&mut self.post_commit_vec);
        ag.add(async move {
            for fut in vec.into_iter() {
                fut.await?;
            }
            Ok(())
        });
    }

    /// Indicates whether force-back operations should be executed if a transaction fails.
    /// This implementation always returns `true`, meaning `force_back_async` will be called on error.
    ///
    /// # Returns
    /// - `true` always.
    fn should_force_back(&self) -> bool {
        true
    }

    /// The rollback phase for Redis Cluster is generally a no-op as commands are often executed immediately.
    /// Rollback logic is handled by `force_back_async`.
    ///
    /// # Arguments
    /// - `_ag`: An `AsyncGroup` (unused in this implementation).
    async fn rollback_async(&mut self, _ag: &mut AsyncGroup) {}

    /// Executes all registered force-back asynchronous operations.
    /// These operations are typically used to revert or clean up state if a transaction fails.
    /// They are added to the provided `AsyncGroup` for concurrent execution.
    ///
    /// # Arguments
    /// - `ag`: An `AsyncGroup` to which the force-back futures will be added.
    async fn force_back_async(&mut self, ag: &mut AsyncGroup) {
        let vec = mem::take(&mut self.force_back_vec);
        ag.add(async move {
            for fut in vec.into_iter() {
                fut.await?;
            }
            Ok(())
        });
    }

    /// Clears all registered pre-commit, post-commit, and force-back operations.
    /// This method is called to release resources and prepare the connection for reuse or disposal.
    fn close(&mut self) {
        self.pre_commit_vec.clear();
        self.post_commit_vec.clear();
        self.force_back_vec.clear();
    }
}

/// Manages an asynchronous connection pool for a Redis Cluster data source.
///
/// This struct is responsible for setting up the Redis Cluster connection pool
/// using `deadpool_redis` and creating new `RedisClusterAsyncDataConn` instances from the pool.
///
/// `RedisClusterAsyncDataSrc` implements the `DataSrc` trait from `sabi::tokio`, allowing it to be
/// used within an asynchronous `DataHub`.
pub struct RedisClusterAsyncDataSrc {
    pool: Option<RedisPool>,
}

enum RedisPool {
    Object(Pool),
    Config(Config),
}

impl RedisClusterAsyncDataSrc {
    /// Creates a new `RedisClusterAsyncDataSrc` instance with the specified Redis Cluster addresses.
    ///
    /// The `addrs` parameter can be an iterator of connection strings
    /// (e.g., `vec!["redis://127.0.0.1:7000/", "redis://127.0.0.1:7001/"]`).
    pub fn new<I, S>(addrs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let urls: Vec<String> = addrs.into_iter().map(|s| s.as_ref().to_string()).collect();
        Self {
            pool: Some(RedisPool::Config(Config {
                urls: Some(urls),
                connections: None,
                pool: None,
                read_from_replicas: false,
            })),
        }
    }

    /// Creates a new `RedisClusterAsyncDataSrc` instance with the specified Redis Cluster addresses
    /// and a custom pool configuration.
    ///
    /// This allows for fine-tuning the connection pool settings.
    pub fn with_addrs_and_pool_config<I, S>(addrs: I, pool_config: PoolConfig) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let urls = addrs.into_iter().map(|s| s.as_ref().to_string()).collect();
        Self {
            pool: Some(RedisPool::Config(Config {
                urls: Some(urls),
                connections: None,
                pool: Some(pool_config),
                read_from_replicas: false,
            })),
        }
    }

    /// Creates a new `RedisClusterAsyncDataSrc` instance using an existing `deadpool_redis::cluster::Config`.
    pub fn with_config(cfg: Config) -> Self {
        Self {
            pool: Some(RedisPool::Config(cfg)),
        }
    }
}

impl DataSrc<RedisClusterAsyncDataConn> for RedisClusterAsyncDataSrc {
    /// Asynchronously sets up the Redis Cluster connection pool.
    /// This method should be called once before attempting to create any
    /// `RedisClusterAsyncDataConn` instances.
    ///
    /// # Arguments
    /// - `_ag`: An `AsyncGroup` (unused in this implementation).
    ///
    /// # Returns
    /// - `Ok(())` if the pool is successfully built.
    /// - `Err(errs::Err)` if the data source is already set up, or if building the pool fails.
    async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let pool_opt = mem::take(&mut self.pool);
        let pool =
            pool_opt.ok_or_else(|| errs::Err::new(RedisClusterAsyncDataSrcError::AlreadySetup))?;
        match pool {
            RedisPool::Config(cfg) => {
                let pool = cfg.create_pool(Some(Runtime::Tokio1)).map_err(|e| {
                    errs::Err::with_source(RedisClusterAsyncDataSrcError::FailToBuildPool, e)
                })?;
                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            _ => Err(errs::Err::new(RedisClusterAsyncDataSrcError::AlreadySetup)),
        }
    }

    /// Closes the underlying Redis Cluster connection pool.
    fn close(&mut self) {
        if let Some(RedisPool::Object(pool)) = self.pool.as_mut() {
            pool.close()
        }
    }

    /// Asynchronously creates a new `RedisClusterAsyncDataConn` instance.
    ///
    /// # Returns
    /// - `Ok(Box<RedisClusterAsyncDataConn>)` containing a new data connection.
    /// - `Err(errs::Err)` if the data source has not been set up yet.
    async fn create_data_conn_async(&mut self) -> errs::Result<Box<RedisClusterAsyncDataConn>> {
        let pool = self
            .pool
            .as_mut()
            .ok_or_else(|| errs::Err::new(RedisClusterAsyncDataSrcError::NotSetupYet))?;
        match pool {
            RedisPool::Object(pool) => Ok(Box::new(RedisClusterAsyncDataConn::new(pool.clone()))),
            _ => Err(errs::Err::new(RedisClusterAsyncDataSrcError::NotSetupYet)),
        }
    }
}

#[cfg(test)]
mod tests_of_cluster_async {
    use super::*;
    use deadpool_redis::{ConnectionAddr, ConnectionInfo, Timeouts};
    use override_macro::{overridable, override_with};
    use redis::AsyncCommands;
    use sabi::tokio::{logic, DataAcc, DataHub};
    use std::time;

    #[derive(Debug)]
    enum SampleClusterAsyncError {
        FailToGetValue,
        FailToSetValue,
        FailToDelValue,
    }

    #[overridable]
    trait RedisClusterAsyncSampleDataAcc: DataAcc {
        async fn get_sample_key_async(&mut self) -> errs::Result<Option<String>> {
            let data_conn = self
                .get_data_conn_async::<RedisClusterAsyncDataConn>("redis")
                .await?;
            let mut conn = data_conn.get_connection_async().await?;
            conn.get("sample_cluster_async")
                .await
                .map_err(|e| errs::Err::with_source(SampleClusterAsyncError::FailToGetValue, e))
        }
        async fn set_sample_key_async(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisClusterAsyncDataConn>("redis")
                .await?;
            let mut conn = data_conn
                .get_connection_with_timeout_async(Timeouts::wait_millis(1000))
                .await?;
            conn.set("sample_cluster_async", val)
                .await
                .map_err(|e| errs::Err::with_source(SampleClusterAsyncError::FailToSetValue, e))
        }
        async fn del_sample_key_async(&mut self) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisClusterAsyncDataConn>("redis")
                .await?;
            let mut conn = data_conn.get_connection_async().await?;
            conn.del("sample_cluster_async")
                .await
                .map_err(|e| errs::Err::with_source(SampleClusterAsyncError::FailToDelValue, e))
        }

        async fn set_sample_key_with_force_back_async(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisClusterAsyncDataConn>("redis")
                .await?;
            let mut conn = data_conn
                .get_connection_with_timeout_async(Timeouts::wait_millis(1000))
                .await?;

            let log_opt: Option<String> = conn.get("log_cluster_async").await.unwrap();
            let log = log_opt.unwrap_or("".to_string());
            let _: Option<()> = conn.set("log_cluster_async", log + "LOG1.").await.unwrap();

            conn.set::<&str, &str, ()>("sample_force_back_cluster_async", val)
                .await
                .map_err(|e| errs::Err::with_source(SampleClusterAsyncError::FailToSetValue, e))?;

            data_conn
                .add_force_back_async(async |mut conn| {
                    let log_opt: Option<String> = conn.get("log_cluster_async").await.unwrap();
                    let log = log_opt.unwrap_or("".to_string());
                    let _: Option<()> = conn.set("log_cluster_async", log + "LOG2.").await.unwrap();

                    conn.del("sample_force_back_cluster_async")
                        .await
                        .map_err(|e| errs::Err::with_source("fail to force back", e))
                })
                .await;

            let log_opt: Option<String> = conn.get("log_cluster_async").await.unwrap();
            let log = log_opt.unwrap_or("".to_string());
            let _: Option<()> = conn.set("log_cluster_async", log + "LOG3.").await.unwrap();

            conn.set::<&str, &str, ()>("sample_force_back_cluster_async_2", val)
                .await
                .map_err(|e| errs::Err::with_source(SampleClusterAsyncError::FailToSetValue, e))?;

            data_conn
                .add_force_back_async(async |mut conn| {
                    let log_opt: Option<String> = conn.get("log_cluster_async").await.unwrap();
                    let log = log_opt.unwrap_or("".to_string());
                    let _: Option<()> = conn.set("log_cluster_async", log + "LOG4.").await.unwrap();

                    conn.del("sample_force_back_cluster_async_2")
                        .await
                        .map_err(|e| errs::Err::with_source("fail to force back", e))
                })
                .await;

            Ok(())
        }

        async fn set_sample_key_in_pre_commit_async(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisClusterAsyncDataConn>("redis")
                .await?;

            let val_owned = val.to_string();

            data_conn
                .add_pre_commit_async(move |mut conn| {
                    let value = val_owned.clone();
                    async move {
                        conn.set::<&str, &str, ()>("sample_pre_commit_cluster_async", &value)
                            .await
                            .map_err(|e| {
                                errs::Err::with_source(SampleClusterAsyncError::FailToSetValue, e)
                            })?;
                        Ok(())
                    }
                })
                .await;

            Ok(())
        }

        async fn set_sample_key_in_post_commit_async(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisClusterAsyncDataConn>("redis")
                .await?;

            let val_owned = val.to_string();

            data_conn
                .add_post_commit_async(move |mut conn| {
                    let value = val_owned.clone();
                    async move {
                        conn.set::<&str, &str, ()>("sample_post_commit_cluster_async", &value)
                            .await
                            .map_err(|e| {
                                errs::Err::with_source(SampleClusterAsyncError::FailToSetValue, e)
                            })?;
                        Ok(())
                    }
                })
                .await;

            Ok(())
        }
    }
    impl RedisClusterAsyncSampleDataAcc for DataHub {}

    #[overridable]
    trait SampleDataClusterAsync {
        async fn get_sample_key_async(&mut self) -> errs::Result<Option<String>>;
        async fn set_sample_key_async(&mut self, value: &str) -> errs::Result<()>;
        async fn del_sample_key_async(&mut self) -> errs::Result<()>;
        async fn set_sample_key_with_force_back_async(&mut self, val: &str) -> errs::Result<()>;
        async fn set_sample_key_in_pre_commit_async(&mut self, val: &str) -> errs::Result<()>;
        async fn set_sample_key_in_post_commit_async(&mut self, val: &str) -> errs::Result<()>;
    }
    #[override_with(RedisClusterAsyncSampleDataAcc)]
    impl SampleDataClusterAsync for DataHub {}

    async fn sample_logic_async(data: &mut impl SampleDataClusterAsync) -> errs::Result<()> {
        match data.get_sample_key_async().await? {
            Some(_) => panic!("Data exists"),
            None => {}
        }

        data.set_sample_key_async("Hello").await?;

        match data.get_sample_key_async().await? {
            Some(val) => assert_eq!(val, "Hello"),
            None => panic!("No data"),
        }

        data.del_sample_key_async().await?;
        Ok(())
    }

    #[tokio::test]
    async fn ok_by_cluster_urls() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisClusterAsyncDataSrc::new(vec![
                "redis://127.0.0.1:7000/",
                "redis://127.0.0.1:7001/",
                "redis://127.0.0.1:7002/",
            ]),
        );
        data.run_async(logic!(sample_logic_async)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn ok_by_cluster_urls_and_pool_config() -> errs::Result<()> {
        let pool_config = PoolConfig {
            max_size: 10,
            timeouts: Timeouts {
                wait: Some(time::Duration::from_secs(10)),
                create: Some(time::Duration::from_secs(11)),
                recycle: Some(time::Duration::from_secs(12)),
            },
            ..Default::default()
        };

        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisClusterAsyncDataSrc::with_addrs_and_pool_config(
                vec![
                    "redis://127.0.0.1:7000",
                    "redis://127.0.0.1:7001",
                    "redis://127.0.0.1:7002",
                ],
                pool_config,
            ),
        );
        data.run_async(logic!(sample_logic_async)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn ok_with_config() -> errs::Result<()> {
        let pool_config = PoolConfig {
            max_size: 10,
            timeouts: Timeouts {
                wait: Some(time::Duration::from_secs(10)),
                create: Some(time::Duration::from_secs(11)),
                recycle: Some(time::Duration::from_secs(12)),
            },
            ..Default::default()
        };

        let mut redis_connection_info_7000 = deadpool_redis::RedisConnectionInfo::default();
        redis_connection_info_7000.db = 1;
        let mut redis_connection_info_7001 = deadpool_redis::RedisConnectionInfo::default();
        redis_connection_info_7001.db = 1;
        let mut redis_connection_info_7002 = deadpool_redis::RedisConnectionInfo::default();
        redis_connection_info_7002.db = 1;

        let connections = vec![
            ConnectionInfo {
                addr: ConnectionAddr::Tcp("127.0.0.1".to_string(), 7000),
                redis: redis_connection_info_7000,
            },
            ConnectionInfo {
                addr: ConnectionAddr::Tcp("127.0.0.1".to_string(), 7001),
                redis: redis_connection_info_7001,
            },
            ConnectionInfo {
                addr: ConnectionAddr::Tcp("127.0.0.1".to_string(), 7002),
                redis: redis_connection_info_7002,
            },
        ];

        let cfg = Config {
            urls: None,
            connections: Some(connections),
            pool: Some(pool_config),
            read_from_replicas: true,
        };

        let mut data = DataHub::new();
        data.uses("redis", RedisClusterAsyncDataSrc::with_config(cfg));
        data.run_async(logic!(sample_logic_async)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn fail_to_setup() {
        let mut data = DataHub::new();
        data.uses("redis", RedisClusterAsyncDataSrc::new(vec!["xxxxxx"]));

        if let Err(err) = data.run_async(logic!(sample_logic_async)).await {
            if let Ok(r) = err.reason::<sabi::tokio::DataHubError>() {
                match r {
                    sabi::tokio::DataHubError::FailToSetupLocalDataSrcs { errors } => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].0.as_ref(), "redis");
                        if let Ok(r) = errors[0].1.reason::<RedisClusterAsyncDataSrcError>() {
                            match r {
                                RedisClusterAsyncDataSrcError::FailToBuildPool => {}
                                _ => panic!(),
                            }
                        }
                        let e = errors[0]
                            .1
                            .source()
                            .unwrap()
                            .downcast_ref::<deadpool_redis::CreatePoolError>()
                            .unwrap();
                        match e {
                            deadpool_redis::CreatePoolError::Config(ce) => match ce {
                                deadpool_redis::ConfigError::Redis(re) => {
                                    assert_eq!(re.kind(), redis::ErrorKind::InvalidClientConfig);
                                    assert_eq!(re.detail(), None);
                                    assert_eq!(re.code(), None);
                                    assert_eq!(re.category(), "invalid client config");
                                }
                                _ => panic!(),
                            },
                            _ => panic!("{e:?}"),
                        }
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!("{err:?}")
            }
        } else {
            panic!();
        }
    }

    async fn sample_logic_in_txn_and_force_back_async(
        data: &mut impl SampleDataClusterAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_with_force_back_async("Good Afternoon")
            .await?;
        Err(errs::Err::new("XXX"))
    }
    async fn sample_logic_in_txn_and_pre_commit_async(
        data: &mut impl SampleDataClusterAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_in_pre_commit_async("Good Evening")
            .await?;
        Ok(())
    }
    async fn sample_logic_in_txn_and_post_commit_async(
        data: &mut impl SampleDataClusterAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_in_post_commit_async("Good Night")
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_txn_and_pre_commit() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisClusterAsyncDataSrc::new(vec![
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]),
        );
        data.txn_async(logic!(sample_logic_in_txn_and_pre_commit_async))
            .await?;

        {
            let client = redis::cluster::ClusterClient::new(vec![
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ])
            .unwrap();
            let mut conn = client.get_async_connection().await.unwrap();

            let s: redis::RedisResult<Option<String>> =
                conn.get("sample_pre_commit_cluster_async").await;
            let _: redis::RedisResult<()> = conn.del("sample_pre_commit_cluster_async").await;
            assert_eq!(s.unwrap().unwrap(), "Good Evening");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_txn_and_post_commit() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisClusterAsyncDataSrc::new(vec![
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]),
        );
        data.txn_async(logic!(sample_logic_in_txn_and_post_commit_async))
            .await?;

        {
            let client = redis::cluster::ClusterClient::new(vec![
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ])
            .unwrap();

            let mut conn = client.get_async_connection().await.unwrap();

            let s: redis::RedisResult<Option<String>> =
                conn.get("sample_post_commit_cluster_async").await;
            let _: redis::RedisResult<()> = conn.del("sample_post_commit_cluster_async").await;
            assert_eq!(s.unwrap().unwrap(), "Good Night");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_txn_and_force_back() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisClusterAsyncDataSrc::new(vec![
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]),
        );
        if let Err(err) = data
            .txn_async(logic!(sample_logic_in_txn_and_force_back_async))
            .await
        {
            assert_eq!(err.reason::<&str>().unwrap(), &"XXX");
        } else {
            panic!();
        }

        {
            let client = redis::cluster::ClusterClient::new(vec![
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ])
            .unwrap();

            let mut conn = client.get_async_connection().await.unwrap();

            let r: redis::RedisResult<Option<String>> =
                conn.get("sample_force_back_cluster_async").await;
            let _: redis::RedisResult<()> = conn.del("sample_force_back_cluster_async").await;
            assert!(r.unwrap().is_none());

            let r: redis::RedisResult<Option<String>> =
                conn.get("sample_force_back_cluster_async_2").await;
            let _: redis::RedisResult<()> = conn.del("sample_force_back_cluster_async_2").await;
            assert!(r.unwrap().is_none());

            let log: redis::RedisResult<Option<String>> = conn.get("log_cluster_async").await;
            let _: redis::RedisResult<()> = conn.del("log_cluster_async").await;
            assert_eq!(log.unwrap().unwrap(), "LOG1.LOG3.LOG2.LOG4.");
        }
        Ok(())
    }
}
