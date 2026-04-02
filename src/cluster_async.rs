// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use deadpool_redis::cluster::{ClusterConnection, Config, Connection, Pool, PoolConfig, Runtime};
use sabi::tokio::{AsyncGroup, DataConn, DataSrc};

use std::future::Future;
use std::{mem, pin};

/// The error type for asynchronous Redis Cluster operations.
#[derive(Debug)]
pub enum RedisClusterAsyncError {
    /// Indicates that the Redis Cluster data source has not been set up yet.
    NotSetupYet,
    /// Indicates that the Redis Cluster data source has already been set up.
    AlreadySetup,
    /// Indicates a failure to build a Redis connection pool.
    FailToBuildPool,
    /// Indicates a failure to get a connection from the pool.
    FailToGetConnectionFromPool,
}

type BoxedFuture = pin::Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'static>>;

/// A data connection for a Redis Cluster, providing asynchronous operations.
///
/// This structure holds an asynchronous connection pool for a Redis Cluster and allows
/// for adding hooks (pre-commit, post-commit, and force-back) that are executed during
/// the lifecycle of an asynchronous data operation managed by `sabi`.
///
/// # Examples
/// ```
/// use sabi_redis::cluster::RedisClusterAsyncDataConn;
/// use redis::AsyncCommands;
/// use sabi::tokio::DataAcc;
///
/// trait MyDataAcc: DataAcc {
///     async fn set_value(&mut self, key: &str, val: &str) -> errs::Result<()> {
///         let data_conn = self.get_data_conn_async::<RedisClusterAsyncDataConn>("redis").await?;
///         let mut conn = data_conn.get_connection();
///         conn.set(key, val).await.map_err(|e| errs::Err::with_source("fail", e))
///     }
/// }
/// ```
pub struct RedisClusterAsyncDataConn {
    conn: Connection,
    pre_commit_vec: Vec<BoxedFuture>,
    post_commit_vec: Vec<BoxedFuture>,
    force_back_vec: Vec<BoxedFuture>,
}

impl RedisClusterAsyncDataConn {
    fn new(conn: Connection) -> Self {
        Self {
            conn,
            pre_commit_vec: Vec::new(),
            post_commit_vec: Vec::new(),
            force_back_vec: Vec::new(),
        }
    }

    /// Gets an asynchronous cluster connection.
    ///
    /// # Returns
    /// Returns a mutable reference to a `ClusterConnection`.
    pub fn get_connection(&mut self) -> &mut ClusterConnection {
        &mut self.conn
    }

    /// Adds an asynchronous function to be executed before a commit occurs.
    ///
    /// # Arguments
    /// * `f` - An async closure or function that takes a `ClusterConnection` and returns a `Future`.
    pub async fn add_pre_commit_async<F, Fut>(&mut self, mut f: F)
    where
        F: FnMut(ClusterConnection) -> Fut,
        Fut: Future<Output = errs::Result<()>> + Send + 'static,
    {
        let fut = f(self.conn.clone());
        self.pre_commit_vec.push(Box::pin(fut))
    }

    /// Adds an asynchronous function to be executed after a successful commit.
    ///
    /// # Arguments
    /// * `f` - An async closure or function that takes a `ClusterConnection` and returns a `Future`.
    pub async fn add_post_commit_async<F, Fut>(&mut self, mut f: F)
    where
        F: FnMut(ClusterConnection) -> Fut,
        Fut: Future<Output = errs::Result<()>> + Send + 'static,
    {
        let fut = f(self.conn.clone());
        self.post_commit_vec.push(Box::pin(fut))
    }

    /// Adds an asynchronous function to be executed when a rollback occurs.
    ///
    /// # Arguments
    /// * `f` - An async closure or function that takes a `ClusterConnection` and returns a `Future`.
    pub async fn add_force_back_async<F, Fut>(&mut self, mut f: F)
    where
        F: FnMut(ClusterConnection) -> Fut,
        Fut: Future<Output = errs::Result<()>> + Send + 'static,
    {
        let fut = f(self.conn.clone());
        self.force_back_vec.push(Box::pin(fut))
    }
}

impl DataConn for RedisClusterAsyncDataConn {
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

/// A data source for Redis Cluster, used to initialize and provide `RedisClusterAsyncDataConn` instances.
///
/// This struct implements the `DataSrc` trait from the `sabi` library for asynchronous operations.
///
/// # Examples
/// ```
/// use sabi_redis::cluster::RedisClusterAsyncDataSrc;
/// use sabi::tokio::DataHub;
///
/// let mut data = DataHub::new();
/// data.uses("redis", RedisClusterAsyncDataSrc::new(vec![
///     "redis://127.0.0.1:7000/",
///     "redis://127.0.0.1:7001/",
///     "redis://127.0.0.1:7002/",
/// ]));
/// ```
pub struct RedisClusterAsyncDataSrc {
    pool: Option<RedisPool>,
}

enum RedisPool {
    Object(Pool),
    Config(Box<Config>),
}

impl RedisClusterAsyncDataSrc {
    /// Creates a new `RedisClusterAsyncDataSrc` with the given cluster node addresses.
    ///
    /// # Arguments
    /// * `addrs` - An iterator of items that can be converted into Redis connection info.
    ///
    /// # Returns
    /// Returns a new instance of `RedisClusterAsyncDataSrc`.
    pub fn new<I, S>(addrs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let urls: Vec<String> = addrs.into_iter().map(|s| s.as_ref().to_string()).collect();
        Self {
            pool: Some(RedisPool::Config(Box::new(Config {
                urls: Some(urls),
                connections: None,
                pool: None,
                read_from_replicas: false,
            }))),
        }
    }

    /// Creates a new `RedisClusterAsyncDataSrc` with cluster node addresses and a custom pool configuration.
    ///
    /// # Arguments
    /// * `addrs` - An iterator of cluster node addresses.
    /// * `pool_config` - A `PoolConfig` for the underlying connection pool.
    ///
    /// # Returns
    /// Returns a new instance of `RedisClusterAsyncDataSrc`.
    pub fn with_pool_config<I, S>(addrs: I, pool_config: PoolConfig) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let urls = addrs.into_iter().map(|s| s.as_ref().to_string()).collect();
        Self {
            pool: Some(RedisPool::Config(Box::new(Config {
                urls: Some(urls),
                connections: None,
                pool: Some(pool_config),
                read_from_replicas: false,
            }))),
        }
    }

    /// Creates a new `RedisClusterAsyncDataSrc` with a complete `Config`.
    ///
    /// # Arguments
    /// * `cfg` - A `deadpool_redis::cluster::Config` object.
    ///
    /// # Returns
    /// Returns a new instance of `RedisClusterAsyncDataSrc`.
    pub fn with_config(cfg: Config) -> Self {
        Self {
            pool: Some(RedisPool::Config(Box::new(cfg))),
        }
    }
}

impl DataSrc<RedisClusterAsyncDataConn> for RedisClusterAsyncDataSrc {
    async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let pool_opt = mem::take(&mut self.pool);
        let pool = pool_opt.ok_or_else(|| errs::Err::new(RedisClusterAsyncError::AlreadySetup))?;
        match pool {
            RedisPool::Config(cfg) => {
                let pool = cfg.create_pool(Some(Runtime::Tokio1)).map_err(|e| {
                    errs::Err::with_source(RedisClusterAsyncError::FailToBuildPool, e)
                })?;
                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            _ => Err(errs::Err::new(RedisClusterAsyncError::AlreadySetup)),
        }
    }

    fn close(&mut self) {
        if let Some(RedisPool::Object(pool)) = self.pool.as_mut() {
            pool.close()
        }
    }

    async fn create_data_conn_async(&mut self) -> errs::Result<Box<RedisClusterAsyncDataConn>> {
        let pool = self
            .pool
            .as_mut()
            .ok_or_else(|| errs::Err::new(RedisClusterAsyncError::NotSetupYet))?;
        match pool {
            RedisPool::Object(pool) => match pool.get().await {
                Ok(conn) => Ok(Box::new(RedisClusterAsyncDataConn::new(conn))),
                Err(e) => Err(errs::Err::with_source(
                    RedisClusterAsyncError::FailToGetConnectionFromPool,
                    e,
                )),
            },
            _ => Err(errs::Err::new(RedisClusterAsyncError::NotSetupYet)),
        }
    }
}

#[cfg(test)]
mod unit_tests {
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
            let conn = data_conn.get_connection();
            conn.get("sample_cluster_async")
                .await
                .map_err(|e| errs::Err::with_source(SampleClusterAsyncError::FailToGetValue, e))
        }
        async fn set_sample_key_async(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisClusterAsyncDataConn>("redis")
                .await?;
            let conn = data_conn.get_connection();
            conn.set("sample_cluster_async", val)
                .await
                .map_err(|e| errs::Err::with_source(SampleClusterAsyncError::FailToSetValue, e))
        }
        async fn del_sample_key_async(&mut self) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisClusterAsyncDataConn>("redis")
                .await?;
            let conn = data_conn.get_connection();
            conn.del("sample_cluster_async")
                .await
                .map_err(|e| errs::Err::with_source(SampleClusterAsyncError::FailToDelValue, e))
        }

        async fn set_sample_key_with_force_back_async(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisClusterAsyncDataConn>("redis")
                .await?;

            {
                let conn = data_conn.get_connection();
                conn.set::<&str, &str, ()>("sample_force_back_cluster_async", val)
                    .await
                    .map_err(|e| {
                        errs::Err::with_source(SampleClusterAsyncError::FailToSetValue, e)
                    })?;
            }

            data_conn
                .add_force_back_async(async |mut conn| {
                    conn.del("sample_force_back_cluster_async")
                        .await
                        .map_err(|e| errs::Err::with_source("fail to force back", e))
                })
                .await;

            {
                let conn = data_conn.get_connection();
                conn.set::<&str, &str, ()>("sample_force_back_cluster_async_2", val)
                    .await
                    .map_err(|e| {
                        errs::Err::with_source(SampleClusterAsyncError::FailToSetValue, e)
                    })?;
            }

            data_conn
                .add_force_back_async(async |mut conn| {
                    conn.del("sample_force_back_cluster_async_2")
                        .await
                        .map_err(|e| errs::Err::with_source("fail to force back", e))
                })
                .await;

            Ok(())
        }

        async fn set_sample_key_with_pre_commit_async(&mut self, val: &str) -> errs::Result<()> {
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

        async fn set_sample_key_with_post_commit_async(&mut self, val: &str) -> errs::Result<()> {
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
        async fn set_sample_key_with_pre_commit_async(&mut self, val: &str) -> errs::Result<()>;
        async fn set_sample_key_with_post_commit_async(&mut self, val: &str) -> errs::Result<()>;
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
    async fn test_new() -> errs::Result<()> {
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
    async fn test_with_ppol_config() -> errs::Result<()> {
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
            RedisClusterAsyncDataSrc::with_pool_config(
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
                        if let Ok(r) = errors[0].1.reason::<RedisClusterAsyncError>() {
                            match r {
                                RedisClusterAsyncError::FailToBuildPool => {}
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
        data: &mut impl SampleDataClusterAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_with_force_back_async("Good Afternoon")
            .await?;
        Ok(())
    }
    async fn sample_logic_with_force_back_async_err(
        data: &mut impl SampleDataClusterAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_with_force_back_async("Good Afternoon")
            .await?;
        Err(errs::Err::new("XXX"))
    }
    async fn sample_logic_with_pre_commit_async(
        data: &mut impl SampleDataClusterAsync,
    ) -> errs::Result<()> {
        data.set_sample_key_with_pre_commit_async("Good Evening")
            .await?;
        Ok(())
    }
    async fn sample_logic_with_post_commit_async(
        data: &mut impl SampleDataClusterAsync,
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
            RedisClusterAsyncDataSrc::new(vec![
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]),
        );

        let r = data
            .txn_async(logic!(sample_logic_with_force_back_async_ok))
            .await;
        assert!(r.is_ok());

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
            assert_eq!(r.unwrap().unwrap(), "Good Afternoon");

            let r: redis::RedisResult<Option<String>> =
                conn.get("sample_force_back_cluster_async_2").await;
            let _: redis::RedisResult<()> = conn.del("sample_force_back_cluster_async_2").await;
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
        }

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
        data.txn_async(logic!(sample_logic_with_pre_commit_async))
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
        data.txn_async(logic!(sample_logic_with_post_commit_async))
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
}
