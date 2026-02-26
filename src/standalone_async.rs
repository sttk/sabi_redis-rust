// Copyright (C) 2025-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use deadpool_redis::{Config, Connection, Pool, PoolConfig, Runtime};
use sabi::tokio::{AsyncGroup, DataConn, DataSrc};

use std::future::Future;
use std::{cell, fmt, mem, pin};

/// Errors that can occur when using `RedisAsyncDataSrc` or `RedisAsyncDataConn`.
#[derive(Debug)]
pub enum RedisAsyncDataSrcError {
    /// Indicates that the `RedisAsyncDataSrc` has not been set up yet (i.e., `setup_async` was not called).
    NotSetupYet,
    /// Indicates that an attempt was made to set up `RedisAsyncDataSrc` when it was already set up,
    /// or to set the internal pool when it was already set.
    AlreadySetup,
    /// Indicates a failure to build the Redis connection pool.
    FailToBuildPool {
        /// The connection information string used when building the pool.
        connection_info: String,
        /// The pool configuration string used when building the pool.
        pool_config: String,
    },
    /// Indicates a failure to get a connection from the Redis pool.
    FailToGetConnectionFromPool,
}

type BoxedFuture = pin::Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'static>>;

/// `RedisAsyncDataConn` is an asynchronous data connection for Redis, implementing the `DataConn` trait
/// from the `sabi::tokio` library. It manages Redis connections from a pool and allows
/// registration of asynchronous operations to be executed at different transaction phases
/// (pre-commit, post-commit, force-back).
pub struct RedisAsyncDataConn {
    pool: Pool,
    pre_commit_vec: Vec<BoxedFuture>,
    post_commit_vec: Vec<BoxedFuture>,
    force_back_vec: Vec<BoxedFuture>,
}

impl RedisAsyncDataConn {
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
        match self.pool.get().await {
            Ok(pooled_conn) => Ok(pooled_conn),
            Err(e) => Err(errs::Err::with_source(
                RedisAsyncDataSrcError::FailToGetConnectionFromPool,
                e,
            )),
        }
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
                    RedisAsyncDataSrcError::FailToGetConnectionFromPool,
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
                    RedisAsyncDataSrcError::FailToGetConnectionFromPool,
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
                    RedisAsyncDataSrcError::FailToGetConnectionFromPool,
                    e,
                ))
            })),
        }
    }
}

impl DataConn for RedisAsyncDataConn {
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

/// `RedisAsyncDataSrc` serves as an asynchronous data source for Redis, implementing the `DataSrc` trait
/// from the `sabi::tokio` library. It manages the creation and lifecycle of a `deadpool_redis` connection pool.
///
/// `T` is a type that can be converted into `redis::ConnectionInfo`.
pub struct RedisAsyncDataSrc<T>
where
    T: redis::IntoConnectionInfo + Sized + fmt::Debug,
{
    pool: cell::OnceCell<Pool>,
    conn_info: Option<T>,
    pool_config: Option<PoolConfig>,
}

impl<T> RedisAsyncDataSrc<T>
where
    T: redis::IntoConnectionInfo + Sized + fmt::Debug,
{
    /// Creates a new `RedisAsyncDataSrc` with the specified connection information and default
    /// pool configuration.
    ///
    /// The actual connection pool is built during the `setup_async` call.
    ///
    /// # Arguments
    /// - `conn_info`: Connection information for the Redis server (e.g., a URL string or `redis::ConnectionInfo`).
    ///
    /// # Returns
    /// A new `RedisAsyncDataSrc` instance.
    pub fn new(conn_info: T) -> Self {
        Self {
            pool: cell::OnceCell::new(),
            conn_info: Some(conn_info),
            pool_config: Some(PoolConfig::default()),
        }
    }

    /// Creates a new `RedisAsyncDataSrc` with the specified connection information and a custom
    /// pool configuration.
    ///
    /// The actual connection pool is built during the `setup_async` call.
    ///
    /// # Arguments
    /// - `conn_info`: Connection information for the Redis server (e.g., a URL string or `redis::ConnectionInfo`).
    /// - `pool_config`: Custom configuration for the `deadpool_redis` connection pool.
    ///
    /// # Returns
    /// A new `RedisAsyncDataSrc` instance.
    pub fn with_pool_config(conn_info: T, pool_config: PoolConfig) -> Self {
        Self {
            pool: cell::OnceCell::new(),
            conn_info: Some(conn_info),
            pool_config: Some(pool_config),
        }
    }
}

impl<T> DataSrc<RedisAsyncDataConn> for RedisAsyncDataSrc<T>
where
    T: redis::IntoConnectionInfo + Sized + Send + fmt::Debug,
{
    /// Asynchronously sets up the Redis connection pool.
    /// This method should be called once before attempting to create any `RedisAsyncDataConn` instances.
    ///
    /// # Arguments
    /// - `_ag`: An `AsyncGroup` (unused in this implementation, but required by the `DataSrc` trait).
    ///
    /// # Returns
    /// - `Ok(())` if the pool is successfully built and set.
    /// - `Err(errs::Err)` if the data source is already set up, or if there's a failure
    ///   to convert connection info or build the `deadpool_redis` pool.
    async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let conn_info_opt = mem::take(&mut self.conn_info);
        let pool_config_opt = mem::take(&mut self.pool_config);

        let (conn_info, pool_config) = conn_info_opt
            .zip(pool_config_opt)
            .ok_or_else(|| errs::Err::new(RedisAsyncDataSrcError::AlreadySetup))?;

        let conn_info_string = format!("{:?}", conn_info);

        let ci = conn_info.into_connection_info().map_err(|e| {
            errs::Err::with_source(
                RedisAsyncDataSrcError::FailToBuildPool {
                    connection_info: conn_info_string.clone(),
                    pool_config: format!("{:?}", pool_config),
                },
                e,
            )
        })?;

        let cfg = Config::from_connection_info(ci);
        let pool = cfg.create_pool(Some(Runtime::Tokio1)).map_err(|e| {
            errs::Err::with_source(
                RedisAsyncDataSrcError::FailToBuildPool {
                    connection_info: conn_info_string,
                    pool_config: format!("{:?}", pool_config),
                },
                e,
            )
        })?;

        if self.pool.set(pool).is_err() {
            Err(errs::Err::new(RedisAsyncDataSrcError::AlreadySetup))
        } else {
            Ok(())
        }
    }

    /// Closes the underlying Redis connection pool.
    /// This method is called to gracefully shut down the data source and release all connections.
    fn close(&mut self) {
        if let Some(pool) = self.pool.get() {
            pool.close();
        }
    }

    /// Asynchronously creates a new `RedisAsyncDataConn` instance.
    /// This method obtains a clone of the internal connection pool to create a new connection object.
    ///
    /// # Returns
    /// - `Ok(Box<RedisAsyncDataConn>)` containing a new data connection.
    /// - `Err(errs::Err)` if the data source has not been set up yet.
    async fn create_data_conn_async(&mut self) -> errs::Result<Box<RedisAsyncDataConn>> {
        if let Some(pool) = self.pool.get() {
            Ok(Box::new(RedisAsyncDataConn::new(pool.clone())))
        } else {
            Err(errs::Err::new(RedisAsyncDataSrcError::NotSetupYet))
        }
    }
}

#[cfg(test)]
mod test_async {
    use super::*;
    use deadpool_redis::Timeouts;
    use override_macro::{overridable, override_with};
    use redis::AsyncCommands;
    use sabi::tokio::{logic, DataAcc, DataHub};
    use std::time;

    #[derive(Debug)]
    enum SampleAsyncError {
        FailToGetValue,
        FailToSetValue,
        FailToDelValue,
    }

    #[overridable]
    trait RedisAsyncSampleDataAcc: DataAcc {
        async fn get_sample_key_async(&mut self) -> errs::Result<Option<String>> {
            let data_conn = self
                .get_data_conn_async::<RedisAsyncDataConn>("redis")
                .await?;
            let mut conn = data_conn.get_connection_async().await?;
            let result: redis::RedisResult<Option<String>> = conn.get("sample_async").await;
            match result {
                Ok(opt) => Ok(opt),
                Err(e) => Err(errs::Err::with_source(SampleAsyncError::FailToGetValue, e)),
            }
        }
        async fn set_sample_key_async(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisAsyncDataConn>("redis")
                .await?;
            let mut conn = data_conn.get_connection_async().await?;
            return match conn.set("sample_async", val).await {
                Ok(()) => Ok(()),
                Err(e) => Err(errs::Err::with_source(SampleAsyncError::FailToSetValue, e)),
            };
        }
        async fn del_sample_key_async(&mut self) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisAsyncDataConn>("redis")
                .await?;
            let mut conn = data_conn.get_connection_async().await?;
            return match conn.del("sample_async").await {
                Ok(()) => Ok(()),
                Err(e) => Err(errs::Err::with_source(SampleAsyncError::FailToDelValue, e)),
            };
        }

        async fn set_sample_key_with_force_back_async(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisAsyncDataConn>("redis")
                .await?;
            let mut conn = data_conn.get_connection_async().await?;

            let log_opt: Option<String> = conn.get("log_async").await.unwrap();
            let log = log_opt.unwrap_or("".to_string());
            let _: Option<()> = conn.set("log_async", log + "LOG1.").await.unwrap();

            if let Err(e) = conn
                .set::<&str, &str, ()>("sample_force_back_async", val)
                .await
            {
                return Err(errs::Err::with_source(SampleAsyncError::FailToSetValue, e));
            }

            data_conn
                .add_force_back_async(async |mut conn| {
                    let log_opt: Option<String> = conn.get("log_async").await.unwrap();
                    let log = log_opt.unwrap_or("".to_string());
                    let _: Option<()> = conn.set("log_async", log + "LOG2.").await.unwrap();

                    let r: redis::RedisResult<()> = conn.del("sample_force_back_async").await;
                    match r {
                        Ok(()) => Ok(()),
                        Err(e) => Err(errs::Err::with_source("fail to force back", e)),
                    }
                })
                .await;

            let log_opt: Option<String> = conn.get("log_async").await.unwrap();
            let log = log_opt.unwrap_or("".to_string());
            let _: Option<()> = conn.set("log_async", log + "LOG3.").await.unwrap();

            if let Err(e) = conn
                .set::<&str, &str, ()>("sample_force_back_async_2", val)
                .await
            {
                return Err(errs::Err::with_source(SampleAsyncError::FailToSetValue, e));
            }

            data_conn
                .add_force_back_async(async |mut conn| {
                    let log_opt: Option<String> = conn.get("log_async").await.unwrap();
                    let log = log_opt.unwrap_or("".to_string());
                    let _: Option<()> = conn.set("log_async", log + "LOG4.").await.unwrap();

                    let r: redis::RedisResult<()> = conn.del("sample_force_back_async_2").await;
                    match r {
                        Ok(()) => Ok(()),
                        Err(e) => Err(errs::Err::with_source("fail to force back", e)),
                    }
                })
                .await;

            Ok(())
        }

        async fn set_sample_key_in_pre_commit_async(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisAsyncDataConn>("redis")
                .await?;

            let val_owned = val.to_string();

            data_conn
                .add_pre_commit_async(move |mut conn| {
                    let value = val_owned.clone();
                    async move {
                        if let Err(e) = conn
                            .set::<&str, &str, ()>("sample_pre_commit_async", &value)
                            .await
                        {
                            return Err(errs::Err::with_source(
                                SampleAsyncError::FailToSetValue,
                                e,
                            ));
                        }
                        Ok(())
                    }
                })
                .await;

            Ok(())
        }

        async fn set_sample_key_in_post_commit_async(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisAsyncDataConn>("redis")
                .await?;

            let val_owned = val.to_string();

            data_conn
                .add_post_commit_async(move |mut conn| {
                    let value = val_owned.clone();
                    async move {
                        if let Err(e) = conn
                            .set::<&str, &str, ()>("sample_post_commit_async", &value)
                            .await
                        {
                            return Err(errs::Err::with_source(
                                SampleAsyncError::FailToSetValue,
                                e,
                            ));
                        }
                        Ok(())
                    }
                })
                .await;

            Ok(())
        }
    }
    impl RedisAsyncSampleDataAcc for DataHub {}

    #[overridable]
    trait SampleDataAsync {
        async fn get_sample_key_async(&mut self) -> errs::Result<Option<String>>;
        async fn set_sample_key_async(&mut self, value: &str) -> errs::Result<()>;
        async fn del_sample_key_async(&mut self) -> errs::Result<()>;
        async fn set_sample_key_with_force_back_async(&mut self, val: &str) -> errs::Result<()>;
        async fn set_sample_key_in_pre_commit_async(&mut self, val: &str) -> errs::Result<()>;
        async fn set_sample_key_in_post_commit_async(&mut self, val: &str) -> errs::Result<()>;
    }
    #[override_with(RedisAsyncSampleDataAcc)]
    impl SampleDataAsync for DataHub {}

    async fn sample_logic_async(data: &mut impl SampleDataAsync) -> errs::Result<()> {
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
    async fn ok_by_redis_uri() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses("redis", RedisAsyncDataSrc::new("redis://127.0.0.1:6379/0"));
        data.run_async(logic!(sample_logic_async)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn ok_by_connection_info() -> errs::Result<()> {
        let ci: redis::ConnectionInfo = "redis://127.0.0.1:6379/1".parse().unwrap();

        let mut data = DataHub::new();
        data.uses("redis", RedisAsyncDataSrc::new(ci));
        data.run_async(logic!(sample_logic_async)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn ok_by_uri_and_pool_config() -> errs::Result<()> {
        let ci: redis::ConnectionInfo = "redis://127.0.0.1:6379/1".parse().unwrap();
        let pc = PoolConfig {
            max_size: 10,
            timeouts: Timeouts {
                wait: Some(time::Duration::from_secs(10)),
                create: Some(time::Duration::from_secs(11)),
                recycle: Some(time::Duration::from_secs(12)),
            },
            ..Default::default()
        };

        let mut data = DataHub::new();
        data.uses("redis", RedisAsyncDataSrc::with_pool_config(ci, pc));
        data.run_async(logic!(sample_logic_async)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn fail_to_setup() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses("redis", RedisAsyncDataSrc::new("xxxxx"));

        if let Err(err) = data.run_async(logic!(sample_logic_async)).await {
            if let Ok(r) = err.reason::<sabi::tokio::DataHubError>() {
                match r {
                    sabi::tokio::DataHubError::FailToSetupLocalDataSrcs { errors } => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].0.as_ref(), "redis");
                        if let Ok(r) = errors[0].1.reason::<RedisAsyncDataSrcError>() {
                            match r {
                                RedisAsyncDataSrcError::FailToBuildPool {
                                    connection_info,
                                    pool_config,
                                } => {
                                    assert_eq!(connection_info, "\"xxxxx\"");
                                    assert_eq!(pool_config, "PoolConfig { max_size: 24, timeouts: Timeouts { wait: None, create: None, recycle: None }, queue_mode: Fifo }");
                                }
                                _ => panic!(),
                            }
                        } else {
                            panic!();
                        }
                    }
                    _ => panic!("{:?}", err),
                }
            } else {
                panic!();
            }
        } else {
            panic!();
        }
        Ok(())
    }

    async fn sample_logic_in_txn_and_commit_async(
        data: &mut impl SampleDataAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_with_force_back_async("Good Morning")
            .await?;
        Ok(())
    }
    async fn sample_logic_in_txn_and_force_back_async(
        data: &mut impl SampleDataAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_with_force_back_async("Good Afternoon")
            .await?;
        Err(errs::Err::new("XXX"))
    }
    async fn sample_logic_in_txn_and_pre_commit_async(
        data: &mut impl SampleDataAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_in_pre_commit_async("Good Evening")
            .await?;
        Ok(())
    }
    async fn sample_logic_in_txn_and_post_commit_async(
        data: &mut impl SampleDataAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_in_post_commit_async("Good Night")
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_txn_and_commit() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses("redis", RedisAsyncDataSrc::new("redis://127.0.0.1:6379/3"));
        data.txn_async(logic!(sample_logic_in_txn_and_commit_async))
            .await?;

        {
            let cfg = Config::from_url("redis://127.0.0.1:6379/3");
            let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
            let mut conn = pool.get().await.unwrap();

            let s: redis::RedisResult<Option<String>> = conn.get("sample_force_back_async").await;
            let _: redis::RedisResult<()> = conn.del("sample_force_back_async").await;
            assert_eq!(s.unwrap().unwrap(), "Good Morning");

            let s: redis::RedisResult<Option<String>> = conn.get("sample_force_back_async_2").await;
            let _: redis::RedisResult<()> = conn.del("sample_force_back_async_2").await;
            assert_eq!(s.unwrap().unwrap(), "Good Morning");

            let log: redis::RedisResult<Option<String>> = conn.get("log_async").await;
            let _: redis::RedisResult<()> = conn.del("log_async").await;
            assert_eq!(log.unwrap().unwrap(), "LOG1.LOG3.");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_txn_and_pre_commit() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses("redis", RedisAsyncDataSrc::new("redis://127.0.0.1:6379/4"));
        data.txn_async(logic!(sample_logic_in_txn_and_pre_commit_async))
            .await?;

        {
            let cfg = Config::from_url("redis://127.0.0.1:6379/4");
            let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
            let mut conn = pool.get().await.unwrap();

            let s: redis::RedisResult<Option<String>> = conn.get("sample_pre_commit_async").await;
            let _: redis::RedisResult<()> = conn.del("sample_pre_commit_async").await;
            assert_eq!(s.unwrap().unwrap(), "Good Evening");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_txn_and_post_commit() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses("redis", RedisAsyncDataSrc::new("redis://127.0.0.1:6379/5"));
        data.txn_async(logic!(sample_logic_in_txn_and_post_commit_async))
            .await?;

        {
            let cfg = Config::from_url("redis://127.0.0.1:6379/5");
            let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
            let mut conn = pool.get().await.unwrap();

            let s: redis::RedisResult<Option<String>> = conn.get("sample_post_commit_async").await;
            let _: redis::RedisResult<()> = conn.del("sample_post_commit_async").await;
            assert_eq!(s.unwrap().unwrap(), "Good Night");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_txn_and_force_back() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses("redis", RedisAsyncDataSrc::new("redis://127.0.0.1:6379/6"));
        if let Err(err) = data
            .txn_async(logic!(sample_logic_in_txn_and_force_back_async))
            .await
        {
            assert_eq!(err.reason::<&str>().unwrap(), &"XXX");
        } else {
            panic!();
        }

        {
            let cfg = Config::from_url("redis://127.0.0.1:6379/6");
            let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
            let mut conn = pool.get().await.unwrap();

            let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back_async").await;
            let _: redis::RedisResult<()> = conn.del("sample_force_back_async").await;
            assert!(r.unwrap().is_none());

            let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back_async_2").await;
            let _: redis::RedisResult<()> = conn.del("sample_force_back_async_2").await;
            assert!(r.unwrap().is_none());

            let log: redis::RedisResult<Option<String>> = conn.get("log_async").await;
            let _: redis::RedisResult<()> = conn.del("log_async").await;
            assert_eq!(log.unwrap().unwrap(), "LOG1.LOG3.LOG2.LOG4.");
        }
        Ok(())
    }
}
