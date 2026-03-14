// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use deadpool_redis::sentinel::{Config, Connection, Pool, PoolConfig, Runtime, SentinelServerType};
use sabi::tokio::{AsyncGroup, DataConn, DataSrc};

use std::future::Future;
use std::{mem, pin};

/// Errors that can occur when using `RedisSentinelAsyncDataSrc` or `RedisSentinelAsyncDataConn`.
#[derive(Debug)]
pub enum RedisSentinelAsyncDataSrcError {
    /// Indicates that the `RedisSentinelAsyncDataSrc` has not been set up yet (i.e., `setup_async` was not called).
    NotSetupYet,

    /// Indicates that an attempt was made to set up `RedisSentinelAsyncDataSrc` when it was already set up,
    /// or to set the internal pool when it was already set.
    AlreadySetup,

    /// Indicates a failure to build the Redis connection pool.
    FailToBuildPool,

    /// Indicates a failure to get a connection from the Redis pool.
    FailToGetConnectionFromPool,
}

type BoxedFuture = pin::Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'static>>;

/// `RedisSentinelAsyncDataConn` is an asynchronous data connection for Redis Sentinel,
/// implementing the `DataConn` trait from the `sabi::tokio` library. It manages Redis
/// connections from a pool and allows registration of asynchronous operations to be executed
/// at different transaction phases (pre-commit, post-commit, force-back).
pub struct RedisSentinelAsyncDataConn {
    pool: Pool,
    pre_commit_vec: Vec<BoxedFuture>,
    post_commit_vec: Vec<BoxedFuture>,
    force_back_vec: Vec<BoxedFuture>,
}

impl RedisSentinelAsyncDataConn {
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
                RedisSentinelAsyncDataSrcError::FailToGetConnectionFromPool,
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
    /// - `f`: An asynchronous closure that takes a `deadpool_redis::Connection` and returns
    ///   an `errs::Result<()>`.
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
                    RedisSentinelAsyncDataSrcError::FailToGetConnectionFromPool,
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
    /// - `f`: An asynchronous closure that takes a `deadpool_redis::Connection` and returns
    ///   an `errs::Result<()>`.
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
                    RedisSentinelAsyncDataSrcError::FailToGetConnectionFromPool,
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
    /// - `f`: An asynchronous closure that takes a `deadpool_redis::Connection` and returns
    ///   an `errs::Result<()>`.
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
                    RedisSentinelAsyncDataSrcError::FailToGetConnectionFromPool,
                    e,
                ))
            })),
        }
    }
}

impl DataConn for RedisSentinelAsyncDataConn {
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

    /// The commit phase for Redis is generally a no-op as commands are often executed immediately.
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

    /// The rollback phase for Redis is generally a no-op as commands are often executed immediately.
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

/// Manages an asynchronous connection pool for a Redis data source managed by Redis Sentinel.
///
/// This struct is responsible for setting up the Redis Sentinel connection pool
/// using `deadpool_redis` and creating new `RedisSentinelAsyncDataConn` instances from the pool.
///
/// `RedisSentinelAsyncDataSrc` implements the `DataSrc` trait from `sabi::tokio`, allowing it to be
/// used within an asynchronous `DataHub`.
pub struct RedisSentinelAsyncDataSrc {
    pool: Option<RedisPool>,
}

enum RedisPool {
    Object(Pool),
    Config(Box<Config>),
}

impl RedisSentinelAsyncDataSrc {
    /// Creates a new `RedisSentinelAsyncDataSrc` instance with the specified Sentinel addresses
    /// and master name.
    ///
    /// The `addrs` parameter is a list of Sentinel connection strings, and `master_name` is the
    /// name of the master group (e.g., "mymaster").
    pub fn new(addrs: Vec<impl AsRef<str>>, master_name: impl AsRef<str>) -> Self {
        let urls = addrs.into_iter().map(|s| s.as_ref().to_string()).collect();
        Self {
            pool: Some(RedisPool::Config(Box::new(Config {
                urls: Some(urls),
                server_type: SentinelServerType::Master,
                master_name: master_name.as_ref().to_string(),
                connections: None,
                node_connection_info: None,
                pool: None,
            }))),
        }
    }

    /// Creates a new `RedisSentinelAsyncDataSrc` instance with the specified Sentinel addresses,
    /// master name, and custom pool configuration.
    ///
    /// This allows for fine-tuning the connection pool settings.
    pub fn with_addrs_and_master_name_and_pool_config(
        addrs: Vec<impl AsRef<str>>,
        master_name: impl AsRef<str>,
        pool_config: PoolConfig,
    ) -> Self {
        let urls = addrs.into_iter().map(|s| s.as_ref().to_string()).collect();
        Self {
            pool: Some(RedisPool::Config(Box::new(Config {
                urls: Some(urls),
                server_type: SentinelServerType::Master,
                master_name: master_name.as_ref().to_string(),
                connections: None,
                node_connection_info: None,
                pool: Some(pool_config),
            }))),
        }
    }

    /// Creates a new `RedisSentinelAsyncDataSrc` instance using an existing
    /// `deadpool_redis::sentinel::Config`.
    pub fn with_config(cfg: Config) -> Self {
        Self {
            pool: Some(RedisPool::Config(Box::new(cfg))),
        }
    }
}

impl DataSrc<RedisSentinelAsyncDataConn> for RedisSentinelAsyncDataSrc {
    /// Asynchronously sets up the Redis Sentinel connection pool.
    /// This method should be called once before attempting to create any
    /// `RedisSentinelAsyncDataConn` instances.
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
            pool_opt.ok_or_else(|| errs::Err::new(RedisSentinelAsyncDataSrcError::AlreadySetup))?;
        match pool {
            RedisPool::Config(cfg) => {
                let pool = cfg.create_pool(Some(Runtime::Tokio1)).map_err(|e| {
                    errs::Err::with_source(RedisSentinelAsyncDataSrcError::FailToBuildPool, e)
                })?;
                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            _ => Err(errs::Err::new(RedisSentinelAsyncDataSrcError::AlreadySetup)),
        }
    }

    /// Closes the underlying Redis Sentinel connection pool.
    fn close(&mut self) {
        if let Some(RedisPool::Object(pool)) = self.pool.as_mut() {
            pool.close()
        }
    }

    /// Asynchronously creates a new `RedisSentinelAsyncDataConn` instance.
    ///
    /// # Returns
    /// - `Ok(Box<RedisSentinelAsyncDataConn>)` containing a new data connection.
    /// - `Err(errs::Err)` if the data source has not been set up yet.
    async fn create_data_conn_async(&mut self) -> errs::Result<Box<RedisSentinelAsyncDataConn>> {
        let pool = self
            .pool
            .as_mut()
            .ok_or_else(|| errs::Err::new(RedisSentinelAsyncDataSrcError::NotSetupYet))?;
        match pool {
            RedisPool::Object(pool) => Ok(Box::new(RedisSentinelAsyncDataConn::new(pool.clone()))),
            _ => Err(errs::Err::new(RedisSentinelAsyncDataSrcError::NotSetupYet)),
        }
    }
}

#[cfg(test)]
mod tests_of_sentinel_async {
    use super::*;
    use deadpool_redis::Timeouts;
    use override_macro::{overridable, override_with};
    use redis::AsyncCommands;
    use sabi::tokio::{logic, DataAcc, DataHub};
    use std::time;

    #[derive(Debug)]
    enum SampleSentinelAsyncError {
        FailToGetValue,
        FailToSetValue,
        FailToDelValue,
    }

    #[overridable]
    trait RedisSentinelAsyncSampleDataAcc: DataAcc {
        async fn get_sample_key_async(&mut self) -> errs::Result<Option<String>> {
            let data_conn = self
                .get_data_conn_async::<RedisSentinelAsyncDataConn>("redis")
                .await?;
            let mut conn = data_conn.get_connection_async().await?;
            conn.get("sample_sentinel_async")
                .await
                .map_err(|e| errs::Err::with_source(SampleSentinelAsyncError::FailToGetValue, e))
        }
        async fn set_sample_key_async(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisSentinelAsyncDataConn>("redis")
                .await?;
            let mut conn = data_conn.get_connection_async().await?;
            conn.set("sample_sentinel_async", val)
                .await
                .map_err(|e| errs::Err::with_source(SampleSentinelAsyncError::FailToSetValue, e))
        }
        async fn del_sample_key_async(&mut self) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisSentinelAsyncDataConn>("redis")
                .await?;
            let mut conn = data_conn.get_connection_async().await?;
            conn.del("sample_sentinel_async")
                .await
                .map_err(|e| errs::Err::with_source(SampleSentinelAsyncError::FailToDelValue, e))
        }

        async fn set_sample_key_with_force_back_async(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisSentinelAsyncDataConn>("redis")
                .await?;
            let mut conn = data_conn.get_connection_async().await?;

            let log_opt: Option<String> = conn.get("log_sentinel_async").await.unwrap();
            let log = log_opt.unwrap_or("".to_string());
            let _: Option<()> = conn.set("log_sentinel_async", log + "LOG1.").await.unwrap();

            conn.set::<&str, &str, ()>("sample_force_back_sentinel_async", val)
                .await
                .map_err(|e| errs::Err::with_source(SampleSentinelAsyncError::FailToSetValue, e))?;

            data_conn
                .add_force_back_async(async |mut conn| {
                    let log_opt: Option<String> = conn.get("log_sentinel_async").await.unwrap();
                    let log = log_opt.unwrap_or("".to_string());
                    let _: Option<()> =
                        conn.set("log_sentinel_async", log + "LOG2.").await.unwrap();

                    conn.del("sample_force_back_sentinel_async")
                        .await
                        .map_err(|e| errs::Err::with_source("fail to force back", e))
                })
                .await;

            let log_opt: Option<String> = conn.get("log_sentinel_async").await.unwrap();
            let log = log_opt.unwrap_or("".to_string());
            let _: Option<()> = conn.set("log_sentinel_async", log + "LOG3.").await.unwrap();

            conn.set::<&str, &str, ()>("sample_force_back_sentinel_async_2", val)
                .await
                .map_err(|e| errs::Err::with_source(SampleSentinelAsyncError::FailToSetValue, e))?;

            data_conn
                .add_force_back_async(async |mut conn| {
                    let log_opt: Option<String> = conn.get("log_sentinel_async").await.unwrap();
                    let log = log_opt.unwrap_or("".to_string());
                    let _: Option<()> =
                        conn.set("log_sentinel_async", log + "LOG4.").await.unwrap();

                    conn.del("sample_force_back_sentinel_async_2")
                        .await
                        .map_err(|e| errs::Err::with_source("fail to force back", e))
                })
                .await;

            Ok(())
        }

        async fn set_sample_key_in_pre_commit_async(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisSentinelAsyncDataConn>("redis")
                .await?;

            let val_owned = val.to_string();

            data_conn
                .add_pre_commit_async(move |mut conn| {
                    let value = val_owned.clone();
                    async move {
                        conn.set::<&str, &str, ()>("sample_pre_commit_sentinel_async", &value)
                            .await
                            .map_err(|e| {
                                errs::Err::with_source(SampleSentinelAsyncError::FailToSetValue, e)
                            })?;
                        Ok(())
                    }
                })
                .await;

            Ok(())
        }

        async fn set_sample_key_in_post_commit_async(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisSentinelAsyncDataConn>("redis")
                .await?;

            let val_owned = val.to_string();

            data_conn
                .add_post_commit_async(move |mut conn| {
                    let value = val_owned.clone();
                    async move {
                        conn.set::<&str, &str, ()>("sample_post_commit_sentinel_async", &value)
                            .await
                            .map_err(|e| {
                                errs::Err::with_source(SampleSentinelAsyncError::FailToSetValue, e)
                            })?;
                        Ok(())
                    }
                })
                .await;

            Ok(())
        }
    }
    impl RedisSentinelAsyncSampleDataAcc for DataHub {}

    #[overridable]
    trait SampleDataSentinelAsync {
        async fn get_sample_key_async(&mut self) -> errs::Result<Option<String>>;
        async fn set_sample_key_async(&mut self, value: &str) -> errs::Result<()>;
        async fn del_sample_key_async(&mut self) -> errs::Result<()>;
        async fn set_sample_key_with_force_back_async(&mut self, val: &str) -> errs::Result<()>;
        async fn set_sample_key_in_pre_commit_async(&mut self, val: &str) -> errs::Result<()>;
        async fn set_sample_key_in_post_commit_async(&mut self, val: &str) -> errs::Result<()>;
    }
    #[override_with(RedisSentinelAsyncSampleDataAcc)]
    impl SampleDataSentinelAsync for DataHub {}

    async fn sample_logic_async(data: &mut impl SampleDataSentinelAsync) -> errs::Result<()> {
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
    async fn ok_by_sentinel_urls() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelAsyncDataSrc::new(
                vec![
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
            ),
        );
        data.run_async(logic!(sample_logic_async)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn ok_by_sentinel_urls_and_pool_config() -> errs::Result<()> {
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
            RedisSentinelAsyncDataSrc::with_addrs_and_master_name_and_pool_config(
                vec![
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
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

        let mut redis_connection_info = deadpool_redis::RedisConnectionInfo::default();
        redis_connection_info.db = 1;

        let mut sentinel_node_connection_info =
            deadpool_redis::sentinel::SentinelNodeConnectionInfo::default();
        sentinel_node_connection_info.redis_connection_info = Some(redis_connection_info);

        let cfg = Config {
            urls: vec![
                "redis://127.0.0.1:26479".to_string(),
                "redis://127.0.0.1:26480".to_string(),
                "redis://127.0.0.1:26481".to_string(),
            ]
            .into(),
            server_type: SentinelServerType::Master,
            master_name: "mymaster".to_string(),
            connections: None,
            node_connection_info: Some(sentinel_node_connection_info),
            pool: Some(pool_config),
        };

        let mut data = DataHub::new();
        data.uses("redis", RedisSentinelAsyncDataSrc::with_config(cfg));
        data.run_async(logic!(sample_logic_async)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn fail_to_setup() {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelAsyncDataSrc::new(vec!["xxxxxx"], "mymaster"),
        );

        if let Err(err) = data.run_async(logic!(sample_logic_async)).await {
            if let Ok(r) = err.reason::<sabi::tokio::DataHubError>() {
                match r {
                    sabi::tokio::DataHubError::FailToSetupLocalDataSrcs { errors } => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].0.as_ref(), "redis");
                        if let Ok(r) = errors[0].1.reason::<RedisSentinelAsyncDataSrcError>() {
                            match r {
                                RedisSentinelAsyncDataSrcError::FailToBuildPool => {}
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
        data: &mut impl SampleDataSentinelAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_with_force_back_async("Good Afternoon")
            .await?;
        Err(errs::Err::new("XXX"))
    }
    async fn sample_logic_in_txn_and_pre_commit_async(
        data: &mut impl SampleDataSentinelAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_in_pre_commit_async("Good Evening")
            .await?;
        Ok(())
    }
    async fn sample_logic_in_txn_and_post_commit_async(
        data: &mut impl SampleDataSentinelAsync,
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
            RedisSentinelAsyncDataSrc::new(
                vec![
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
            ),
        );
        data.txn_async(logic!(sample_logic_in_txn_and_pre_commit_async))
            .await?;

        {
            let mut sentinel = redis::sentinel::Sentinel::build(vec![
                "redis://127.0.0.1:26479",
                "redis://127.0.0.1:26480",
                "redis://127.0.0.1:26481",
            ])
            .unwrap();

            let client = sentinel.async_master_for("mymaster", None).await.unwrap();
            let mut conn = client.get_multiplexed_async_connection().await.unwrap();

            let s: redis::RedisResult<Option<String>> =
                conn.get("sample_pre_commit_sentinel_async").await;
            let _: redis::RedisResult<()> = conn.del("sample_pre_commit_sentinel_async").await;
            assert_eq!(s.unwrap().unwrap(), "Good Evening");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_txn_and_post_commit() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelAsyncDataSrc::new(
                vec![
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
            ),
        );
        data.txn_async(logic!(sample_logic_in_txn_and_post_commit_async))
            .await?;

        {
            let mut sentinel = redis::sentinel::Sentinel::build(vec![
                "redis://127.0.0.1:26479",
                "redis://127.0.0.1:26480",
                "redis://127.0.0.1:26481",
            ])
            .unwrap();

            let client = sentinel.async_master_for("mymaster", None).await.unwrap();
            let mut conn = client.get_multiplexed_async_connection().await.unwrap();

            let s: redis::RedisResult<Option<String>> =
                conn.get("sample_post_commit_sentinel_async").await;
            let _: redis::RedisResult<()> = conn.del("sample_post_commit_sentinel_async").await;
            assert_eq!(s.unwrap().unwrap(), "Good Night");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_txn_and_force_back() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisSentinelAsyncDataSrc::new(
                vec![
                    "redis://127.0.0.1:26479",
                    "redis://127.0.0.1:26480",
                    "redis://127.0.0.1:26481",
                ],
                "mymaster",
            ),
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
            let mut sentinel = redis::sentinel::Sentinel::build(vec![
                "redis://127.0.0.1:26479",
                "redis://127.0.0.1:26480",
                "redis://127.0.0.1:26481",
            ])
            .unwrap();

            let client = sentinel.async_master_for("mymaster", None).await.unwrap();
            let mut conn = client.get_multiplexed_async_connection().await.unwrap();

            let r: redis::RedisResult<Option<String>> =
                conn.get("sample_force_back_sentinel_async").await;
            let _: redis::RedisResult<()> = conn.del("sample_force_back_sentinel_async").await;
            assert!(r.unwrap().is_none());

            let r: redis::RedisResult<Option<String>> =
                conn.get("sample_force_back_sentinel_async_2").await;
            let _: redis::RedisResult<()> = conn.del("sample_force_back_sentinel_async_2").await;
            assert!(r.unwrap().is_none());

            let log: redis::RedisResult<Option<String>> = conn.get("log_sentinel_async").await;
            let _: redis::RedisResult<()> = conn.del("log_sentinel_async").await;
            assert_eq!(log.unwrap().unwrap(), "LOG1.LOG3.LOG2.LOG4.");
        }
        Ok(())
    }
}
