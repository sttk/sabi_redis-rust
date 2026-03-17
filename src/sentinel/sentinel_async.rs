// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use deadpool_redis::sentinel::{Config, Connection, Pool, PoolConfig, Runtime, SentinelServerType};
use deadpool_redis::Timeouts;
use sabi::tokio::{AsyncGroup, DataConn, DataSrc};

use std::future::Future;
use std::{mem, pin};

/// The error type for asynchronous Redis Sentinel operations.
#[derive(Debug)]
pub enum RedisSentinelAsyncError {
    /// Indicates that the Redis Sentinel data source has not been set up yet.
    NotSetupYet,
    /// Indicates that the Redis Sentinel data source has already been set up.
    AlreadySetup,
    /// Indicates a failure to build a Redis connection pool.
    FailToBuildPool,
    /// Indicates a failure to get a connection from the pool.
    FailToGetConnectionFromPool,
}

type BoxedFuture = pin::Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'static>>;

/// A data connection for Redis Sentinel, providing asynchronous operations.
///
/// This structure holds an asynchronous connection pool for a Redis Sentinel-managed 
/// setup and allows for adding hooks (pre-commit, post-commit, and force-back) 
/// that are executed during the lifecycle of an asynchronous data operation managed by `sabi`.
///
/// # Examples
/// ```
/// use sabi_redis::RedisSentinelAsyncDataConn;
/// use redis::AsyncCommands;
/// use sabi::tokio::DataAcc;
///
/// trait MyDataAcc: DataAcc {
///     async fn set_value(&mut self, key: &str, val: &str) -> errs::Result<()> {
///         let data_conn = self.get_data_conn_async::<RedisSentinelAsyncDataConn>("redis").await?;
///         let mut conn = data_conn.get_connection_async().await?;
///         conn.set(key, val).await.map_err(|e| errs::Err::with_source("fail", e))
///     }
/// }
/// ```
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

    /// Gets an asynchronous Sentinel-managed connection from the pool.
    ///
    /// # Returns
    /// Returns a `Result` containing a `Connection` on success, 
    /// or a `RedisSentinelAsyncError::FailToGetConnectionFromPool` wrapped in `errs::Err` on failure.
    pub async fn get_connection_async(&mut self) -> errs::Result<Connection> {
        self.pool.get().await.map_err(|e| {
            errs::Err::with_source(RedisSentinelAsyncError::FailToGetConnectionFromPool, e)
        })
    }

    /// Gets an asynchronous Sentinel-managed connection from the pool with specific timeouts.
    ///
    /// # Arguments
    /// * `timeouts` - A `Timeouts` configuration for getting a connection.
    ///
    /// # Returns
    /// Returns a `Result` containing a `Connection` on success, 
    /// or a `RedisSentinelAsyncError::FailToGetConnectionFromPool` wrapped in `errs::Err` on failure.
    pub async fn get_connection_with_timeout_async(
        &mut self,
        timeouts: Timeouts,
    ) -> errs::Result<Connection> {
        self.pool.timeout_get(&timeouts).await.map_err(|e| {
            errs::Err::with_source(RedisSentinelAsyncError::FailToGetConnectionFromPool, e)
        })
    }

    /// Adds an asynchronous function to be executed before a commit occurs.
    ///
    /// # Arguments
    /// * `f` - An async closure or function that takes a `Connection` and returns a `Future`.
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
                    RedisSentinelAsyncError::FailToGetConnectionFromPool,
                    e,
                ))
            })),
        }
    }

    /// Adds an asynchronous function to be executed after a successful commit.
    ///
    /// # Arguments
    /// * `f` - An async closure or function that takes a `Connection` and returns a `Future`.
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
                    RedisSentinelAsyncError::FailToGetConnectionFromPool,
                    e,
                ))
            })),
        }
    }

    /// Adds an asynchronous function to be executed when a rollback occurs.
    ///
    /// # Arguments
    /// * `f` - An async closure or function that takes a `Connection` and returns a `Future`.
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
                    RedisSentinelAsyncError::FailToGetConnectionFromPool,
                    e,
                ))
            })),
        }
    }
}

impl DataConn for RedisSentinelAsyncDataConn {
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

    async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    async fn post_commit_async(&mut self, ag: &mut AsyncGroup) {
        let vec = mem::take(&mut self.post_commit_vec);
        ag.add(async move {
            for fut in vec.into_iter() {
                fut.await?;
            }
            Ok(())
        });
    }

    fn should_force_back(&self) -> bool {
        true
    }

    async fn rollback_async(&mut self, _ag: &mut AsyncGroup) {}

    async fn force_back_async(&mut self, ag: &mut AsyncGroup) {
        let vec = mem::take(&mut self.force_back_vec);
        ag.add(async move {
            for fut in vec.into_iter() {
                fut.await?;
            }
            Ok(())
        });
    }

    fn close(&mut self) {
        self.pre_commit_vec.clear();
        self.post_commit_vec.clear();
        self.force_back_vec.clear();
    }
}

/// A data source for Redis Sentinel, used to initialize and provide `RedisSentinelAsyncDataConn` instances.
///
/// This struct implements the `DataSrc` trait from the `sabi` library for asynchronous operations.
///
/// # Examples
/// ```
/// use sabi_redis::RedisSentinelAsyncDataSrc;
/// use sabi::tokio::DataHub;
///
/// let mut data = DataHub::new();
/// data.uses("redis", RedisSentinelAsyncDataSrc::new(
///     vec![
///         "redis://127.0.0.1:26479",
///         "redis://127.0.0.1:26480",
///         "redis://127.0.0.1:26481",
///     ],
///     "mymaster",
/// ));
/// ```
pub struct RedisSentinelAsyncDataSrc {
    pool: Option<RedisPool>,
}

enum RedisPool {
    Object(Pool),
    Config(Box<Config>),
}

impl RedisSentinelAsyncDataSrc {
    /// Creates a new `RedisSentinelAsyncDataSrc` with Sentinel addresses and a master name.
    ///
    /// # Arguments
    /// * `addrs` - An iterator of Sentinel addresses.
    /// * `master_name` - The name of the Redis master.
    ///
    /// # Returns
    /// Returns a new instance of `RedisSentinelAsyncDataSrc`.
    pub fn new<I, S>(addrs: I, master_name: S) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
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

    /// Creates a new `RedisSentinelAsyncDataSrc` with Sentinel addresses, a master name, and a custom pool configuration.
    ///
    /// # Arguments
    /// * `addrs` - An iterator of Sentinel addresses.
    /// * `master_name` - The name of the Redis master.
    /// * `pool_config` - A `PoolConfig` for the underlying connection pool.
    ///
    /// # Returns
    /// Returns a new instance of `RedisSentinelAsyncDataSrc`.
    pub fn with_pool_config<I, S>(addrs: I, master_name: S, pool_config: PoolConfig) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
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

    /// Creates a new `RedisSentinelAsyncDataSrc` with a complete `Config`.
    ///
    /// # Arguments
    /// * `cfg` - A `deadpool_redis::sentinel::Config` object.
    ///
    /// # Returns
    /// Returns a new instance of `RedisSentinelAsyncDataSrc`.
    pub fn with_config(cfg: Config) -> Self {
        Self {
            pool: Some(RedisPool::Config(Box::new(cfg))),
        }
    }
}

impl DataSrc<RedisSentinelAsyncDataConn> for RedisSentinelAsyncDataSrc {
    async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let pool_opt = mem::take(&mut self.pool);
        let pool = pool_opt.ok_or_else(|| errs::Err::new(RedisSentinelAsyncError::AlreadySetup))?;
        match pool {
            RedisPool::Config(cfg) => {
                let pool = cfg.create_pool(Some(Runtime::Tokio1)).map_err(|e| {
                    errs::Err::with_source(RedisSentinelAsyncError::FailToBuildPool, e)
                })?;
                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            _ => Err(errs::Err::new(RedisSentinelAsyncError::AlreadySetup)),
        }
    }

    fn close(&mut self) {
        if let Some(RedisPool::Object(pool)) = self.pool.as_mut() {
            pool.close()
        }
    }

    async fn create_data_conn_async(&mut self) -> errs::Result<Box<RedisSentinelAsyncDataConn>> {
        let pool = self
            .pool
            .as_mut()
            .ok_or_else(|| errs::Err::new(RedisSentinelAsyncError::NotSetupYet))?;
        match pool {
            RedisPool::Object(pool) => Ok(Box::new(RedisSentinelAsyncDataConn::new(pool.clone()))),
            _ => Err(errs::Err::new(RedisSentinelAsyncError::NotSetupYet)),
        }
    }
}

#[cfg(test)]
mod unit_tests {
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
            let mut conn = data_conn
                .get_connection_with_timeout_async(Timeouts::wait_millis(1000))
                .await?;
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
            let mut conn = data_conn
                .get_connection_with_timeout_async(Timeouts::wait_millis(1000))
                .await?;

            conn.set::<&str, &str, ()>("sample_force_back_sentinel_async", val)
                .await
                .map_err(|e| errs::Err::with_source(SampleSentinelAsyncError::FailToSetValue, e))?;

            data_conn
                .add_force_back_async(async |mut conn| {
                    conn.del("sample_force_back_sentinel_async")
                        .await
                        .map_err(|e| errs::Err::with_source("fail to force back", e))
                })
                .await;

            conn.set::<&str, &str, ()>("sample_force_back_sentinel_async_2", val)
                .await
                .map_err(|e| errs::Err::with_source(SampleSentinelAsyncError::FailToSetValue, e))?;

            data_conn
                .add_force_back_async(async |mut conn| {
                    conn.del("sample_force_back_sentinel_async_2")
                        .await
                        .map_err(|e| errs::Err::with_source("fail to force back", e))
                })
                .await;

            Ok(())
        }

        async fn set_sample_key_with_pre_commit_async(&mut self, val: &str) -> errs::Result<()> {
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

        async fn set_sample_key_with_post_commit_async(&mut self, val: &str) -> errs::Result<()> {
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
        async fn set_sample_key_with_pre_commit_async(&mut self, val: &str) -> errs::Result<()>;
        async fn set_sample_key_with_post_commit_async(&mut self, val: &str) -> errs::Result<()>;
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
    async fn test_new() -> errs::Result<()> {
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
    async fn test_with_pool_config() -> errs::Result<()> {
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
            RedisSentinelAsyncDataSrc::with_pool_config(
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
    async fn test_with_config() -> errs::Result<()> {
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
                        if let Ok(r) = errors[0].1.reason::<RedisSentinelAsyncError>() {
                            match r {
                                RedisSentinelAsyncError::FailToBuildPool => {}
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

    async fn sample_logic_with_force_back_async_ok(
        data: &mut impl SampleDataSentinelAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_with_force_back_async("Good Afternoon")
            .await?;
        Ok(())
    }
    async fn sample_logic_with_force_back_async_err(
        data: &mut impl SampleDataSentinelAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_with_force_back_async("Good Afternoon")
            .await?;
        Err(errs::Err::new("XXX"))
    }
    async fn sample_logic_with_pre_commit_async(
        data: &mut impl SampleDataSentinelAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_with_pre_commit_async("Good Evening")
            .await?;
        Ok(())
    }
    async fn sample_logic_with_post_commit_async(
        data: &mut impl SampleDataSentinelAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_with_post_commit_async("Good Night")
            .await?;
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

        let r = data
            .txn_async(logic!(sample_logic_with_force_back_async_ok))
            .await;
        assert!(r.is_ok());

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
            assert_eq!(r.unwrap().unwrap(), "Good Afternoon");

            let r: redis::RedisResult<Option<String>> =
                conn.get("sample_force_back_sentinel_async_2").await;
            let _: redis::RedisResult<()> = conn.del("sample_force_back_sentinel_async_2").await;
            assert_eq!(r.unwrap().unwrap(), "Good Afternoon");
        }

        if let Err(err) = data
            .txn_async(logic!(sample_logic_with_force_back_async_err))
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
        }

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
        data.txn_async(logic!(sample_logic_with_pre_commit_async))
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
        data.txn_async(logic!(sample_logic_with_post_commit_async))
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
}
