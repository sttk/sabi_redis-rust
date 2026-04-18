// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use deadpool_redis::cluster::{ClusterConnection, Config, Connection, Pool, PoolConfig, Runtime};
use sabi::tokio::{AsyncGroup, DataConn, DataSrc};

use std::future::Future;
use std::{mem, pin};
use tokio::time;

#[derive(Debug)]
pub enum RedisClusterErrorAsync {
    NotSetupYet,
    AlreadySetup,
    FailToConnect { config: Config },
    FailToBuildPool { config: Config },
    FailToGetConnectionFromPool,
}

type BoxedFuture = pin::Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'static>>;

pub struct RedisClusterDataConnAsync {
    conn: Connection,
    pre_commit_vec: Vec<BoxedFuture>,
    post_commit_vec: Vec<BoxedFuture>,
    force_back_vec: Vec<BoxedFuture>,
}

impl RedisClusterDataConnAsync {
    fn new(conn: Connection) -> Self {
        Self {
            conn,
            pre_commit_vec: Vec::new(),
            post_commit_vec: Vec::new(),
            force_back_vec: Vec::new(),
        }
    }

    pub fn get_connection(&mut self) -> &mut ClusterConnection {
        &mut self.conn
    }

    pub async fn add_pre_commit_async<F, Fut>(&mut self, mut f: F)
    where
        F: FnMut(ClusterConnection) -> Fut,
        Fut: Future<Output = errs::Result<()>> + Send + 'static,
    {
        let fut = f(self.conn.clone());
        self.pre_commit_vec.push(Box::pin(fut))
    }

    pub async fn add_post_commit_async<F, Fut>(&mut self, mut f: F)
    where
        F: FnMut(ClusterConnection) -> Fut,
        Fut: Future<Output = errs::Result<()>> + Send + 'static,
    {
        let fut = f(self.conn.clone());
        self.post_commit_vec.push(Box::pin(fut))
    }

    pub async fn add_force_back_async<F, Fut>(&mut self, mut f: F)
    where
        F: FnMut(ClusterConnection) -> Fut,
        Fut: Future<Output = errs::Result<()>> + Send + 'static,
    {
        let fut = f(self.conn.clone());
        self.force_back_vec.push(Box::pin(fut))
    }
}

impl DataConn for RedisClusterDataConnAsync {
    async fn pre_commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let vec = mem::take(&mut self.pre_commit_vec);
        for fut in vec.into_iter() {
            fut.await?;
        }
        Ok(())
    }

    async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    async fn post_commit_async(&mut self, _ag: &mut AsyncGroup) {
        let vec = mem::take(&mut self.post_commit_vec);
        for fut in vec.into_iter() {
            // The error are not exposed externally, but a notification is triggered when
            // errs::Err is created.
            let _ = fut.await;
        }
    }

    fn should_force_back(&self) -> bool {
        true
    }

    async fn rollback_async(&mut self, _ag: &mut AsyncGroup) {}

    async fn force_back_async(&mut self, _ag: &mut AsyncGroup) {
        let vec = mem::take(&mut self.force_back_vec);
        for fut in vec.into_iter().rev() {
            // The error are not exposed externally, but a notification is triggered when
            // errs::Err is created.
            let _ = fut.await;
        }
    }

    fn close(&mut self) {
        self.pre_commit_vec.clear();
        self.post_commit_vec.clear();
        self.force_back_vec.clear();
    }
}

pub struct RedisClusterDataSrcAsync {
    pool: Option<RedisPool>,
}

enum RedisPool {
    Object(Pool),
    Config(Box<Config>),
}

impl RedisClusterDataSrcAsync {
    pub fn new<I>(addrs: I) -> Self
    where
        I: IntoIterator<Item: AsRef<str>>,
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

    pub fn with_pool_config<I>(addrs: I, pool_config: PoolConfig) -> Self
    where
        I: IntoIterator<Item: AsRef<str>>,
    {
        let urls: Vec<String> = addrs.into_iter().map(|s| s.as_ref().to_string()).collect();
        Self {
            pool: Some(RedisPool::Config(Box::new(Config {
                urls: Some(urls),
                connections: None,
                pool: Some(pool_config),
                read_from_replicas: false,
            }))),
        }
    }

    pub fn with_config(config: Config) -> Self {
        Self {
            pool: Some(RedisPool::Config(Box::new(config))),
        }
    }
}

impl DataSrc<RedisClusterDataConnAsync> for RedisClusterDataSrcAsync {
    async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let pool_opt = mem::take(&mut self.pool);
        let pool_cfg =
            pool_opt.ok_or_else(|| errs::Err::new(RedisClusterErrorAsync::AlreadySetup))?;
        match pool_cfg {
            RedisPool::Config(config) => {
                let pool = config.create_pool(Some(Runtime::Tokio1)).map_err(|e| {
                    errs::Err::with_source(
                        RedisClusterErrorAsync::FailToBuildPool {
                            config: *config.clone(),
                        },
                        e,
                    )
                })?;

                let mut timeouts = config.pool.map(|p| p.timeouts).unwrap_or_default();
                timeouts
                    .wait
                    .get_or_insert(time::Duration::from_millis(100));
                timeouts
                    .create
                    .get_or_insert(time::Duration::from_millis(100));

                pool.timeout_get(&timeouts).await.map_err(|e| {
                    errs::Err::with_source(
                        RedisClusterErrorAsync::FailToConnect { config: *config },
                        e,
                    )
                })?;
                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            _ => Err(errs::Err::new(RedisClusterErrorAsync::AlreadySetup)),
        }
    }

    fn close(&mut self) {
        if let Some(RedisPool::Object(pool)) = self.pool.as_mut() {
            pool.close();
        }
    }

    async fn create_data_conn_async(&mut self) -> errs::Result<Box<RedisClusterDataConnAsync>> {
        let pool = self
            .pool
            .as_mut()
            .ok_or_else(|| errs::Err::new(RedisClusterErrorAsync::NotSetupYet))?;
        match pool {
            RedisPool::Object(pool) => match pool.get().await {
                Ok(conn) => Ok(Box::new(RedisClusterDataConnAsync::new(conn))),
                Err(e) => Err(errs::Err::with_source(
                    RedisClusterErrorAsync::FailToGetConnectionFromPool,
                    e,
                )),
            },
            _ => Err(errs::Err::new(RedisClusterErrorAsync::NotSetupYet)),
        }
    }
}

#[cfg(test)]
mod unit_tests_of_data_src {
    use super::*;

    mod test_new {
        use super::*;
        use url::Url;

        #[tokio::test]
        async fn addrs_are_strs_and_ok() {
            let mut ds = RedisClusterDataSrcAsync::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());
            ds.close();
        }

        #[tokio::test]
        async fn addrs_are_strs_and_fail() {
            let mut ds = RedisClusterDataSrcAsync::new(&[
                "redis://xxxx:7000",
                "redis://xxxx:7001",
                "redis://xxxx:7002",
            ]);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup_async(&mut ag).await else {
                panic!();
            };
            let Ok(RedisClusterErrorAsync::FailToConnect { config }) =
                err.reason::<RedisClusterErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(format!("{:?}", config), "Config { urls: Some([\"redis://xxxx:7000\", \"redis://xxxx:7001\", \"redis://xxxx:7002\"]), connections: None, pool: None, read_from_replicas: false }");
            assert_eq!(format!("{:?}", err.source().unwrap()), "Backend(Failed to create initial connections - Io: failed to lookup address information: nodename nor servname provided, or not known)");
            let errors = ag.join_async().await;
            assert!(errors.is_empty());
            ds.close();
        }

        #[tokio::test]
        async fn addrs_are_strings_and_ok() {
            let mut ds = RedisClusterDataSrcAsync::new(&[
                "redis://127.0.0.1:7000".to_string(),
                "redis://127.0.0.1:7001".to_string(),
                "redis://127.0.0.1:7002".to_string(),
            ]);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());
            ds.close();
        }

        #[tokio::test]
        async fn addrs_are_strings_and_fail() {
            let mut ds = RedisClusterDataSrcAsync::new(&[
                "xxxx".to_string(),
                "yyyy".to_string(),
                "zzzz".to_string(),
            ]);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup_async(&mut ag).await else {
                panic!();
            };
            let Ok(RedisClusterErrorAsync::FailToBuildPool { config }) =
                err.reason::<RedisClusterErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(format!("{:?}", config), "Config { urls: Some([\"xxxx\", \"yyyy\", \"zzzz\"]), connections: None, pool: None, read_from_replicas: false }");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Config(Redis(Redis URL did not parse - InvalidClientConfig))"
            );
            let errors = ag.join_async().await;
            assert!(errors.is_empty());
            ds.close();
        }

        #[tokio::test]
        async fn addrs_are_urls_and_ok() {
            let Ok(url0) = Url::parse("redis://127.0.0.1:7000/0") else {
                panic!("bad url");
            };
            let Ok(url1) = Url::parse("redis://127.0.0.1:7001/0") else {
                panic!("bad url");
            };
            let Ok(url2) = Url::parse("redis://127.0.0.1:7002/0") else {
                panic!("bad url");
            };
            let mut ds = RedisClusterDataSrcAsync::new(&[url0, url1, url2]);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());
            ds.close();
        }

        #[tokio::test]
        async fn addrs_are_urls_and_fail() {
            let Ok(url0) = Url::parse("redis://") else {
                panic!("bad url");
            };
            let Ok(url1) = Url::parse("redis://") else {
                panic!("bad url");
            };
            let Ok(url2) = Url::parse("redis://") else {
                panic!("bad url");
            };
            let mut ds = RedisClusterDataSrcAsync::new(&[url0, url1, url2]);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup_async(&mut ag).await else {
                panic!();
            };
            let Ok(RedisClusterErrorAsync::FailToBuildPool { config }) =
                err.reason::<RedisClusterErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(format!("{:?}", config), "Config { urls: Some([\"redis://\", \"redis://\", \"redis://\"]), connections: None, pool: None, read_from_replicas: false }");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Config(Redis(Missing hostname - InvalidClientConfig))"
            );
            let errors = ag.join_async().await;
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_pool_config {
        use super::*;
        use deadpool_redis::Timeouts;
        use tokio::time;
        use url::Url;

        #[tokio::test]
        async fn addrs_are_strs_and_ok() {
            let pool_cfg = PoolConfig {
                max_size: 10,
                timeouts: Timeouts {
                    wait: Some(time::Duration::from_secs(10)),
                    create: Some(time::Duration::from_secs(11)),
                    recycle: Some(time::Duration::from_secs(12)),
                },
                ..Default::default()
            };
            let mut ds = RedisClusterDataSrcAsync::with_pool_config(
                &[
                    "redis://127.0.0.1:7000",
                    "redis://127.0.0.1:7001",
                    "redis://127.0.0.1:7002",
                ],
                pool_cfg,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());
            ds.close();
        }

        #[tokio::test]
        async fn addrs_are_strs_and_fail() {
            let pool_cfg = PoolConfig {
                max_size: 10,
                timeouts: Timeouts {
                    wait: Some(time::Duration::from_secs(10)),
                    create: Some(time::Duration::from_secs(11)),
                    recycle: Some(time::Duration::from_secs(12)),
                },
                ..Default::default()
            };
            let mut ds = RedisClusterDataSrcAsync::with_pool_config(
                &[
                    "redis://xxxx:7000",
                    "redis://xxxx:7001",
                    "redis://xxxx:7002",
                ],
                pool_cfg,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup_async(&mut ag).await else {
                panic!();
            };
            let Ok(RedisClusterErrorAsync::FailToConnect { config }) =
                err.reason::<RedisClusterErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(format!("{:?}", config), "Config { urls: Some([\"redis://xxxx:7000\", \"redis://xxxx:7001\", \"redis://xxxx:7002\"]), connections: None, pool: Some(PoolConfig { max_size: 10, timeouts: Timeouts { wait: Some(10s), create: Some(11s), recycle: Some(12s) }, queue_mode: Fifo }), read_from_replicas: false }");
            assert_eq!(format!("{:?}", err.source().unwrap()), "Backend(Failed to create initial connections - Io: failed to lookup address information: nodename nor servname provided, or not known)");
            let errors = ag.join_async().await;
            assert!(errors.is_empty());
            ds.close();
        }

        #[tokio::test]
        async fn addrs_are_strings_and_ok() {
            let pool_cfg = PoolConfig {
                max_size: 10,
                timeouts: Timeouts {
                    wait: Some(time::Duration::from_secs(10)),
                    create: Some(time::Duration::from_secs(11)),
                    recycle: Some(time::Duration::from_secs(12)),
                },
                ..Default::default()
            };
            let mut ds = RedisClusterDataSrcAsync::with_pool_config(
                &[
                    "redis://127.0.0.1:7000".to_string(),
                    "redis://127.0.0.1:7001".to_string(),
                    "redis://127.0.0.1:7002".to_string(),
                ],
                pool_cfg,
            );
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());
            ds.close();
        }

        #[tokio::test]
        async fn addrs_are_strings_and_fail() {
            let pool_cfg = PoolConfig {
                max_size: 10,
                timeouts: Timeouts {
                    wait: Some(time::Duration::from_secs(10)),
                    create: Some(time::Duration::from_secs(11)),
                    recycle: Some(time::Duration::from_secs(12)),
                },
                ..Default::default()
            };
            let mut ds = RedisClusterDataSrcAsync::with_pool_config(
                &["xxxx".to_string(), "yyyy".to_string(), "zzzz".to_string()],
                pool_cfg,
            );
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup_async(&mut ag).await else {
                panic!();
            };
            let Ok(RedisClusterErrorAsync::FailToBuildPool { config }) =
                err.reason::<RedisClusterErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(format!("{:?}", config), "Config { urls: Some([\"xxxx\", \"yyyy\", \"zzzz\"]), connections: None, pool: Some(PoolConfig { max_size: 10, timeouts: Timeouts { wait: Some(10s), create: Some(11s), recycle: Some(12s) }, queue_mode: Fifo }), read_from_replicas: false }");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Config(Redis(Redis URL did not parse - InvalidClientConfig))"
            );
            let errors = ag.join_async().await;
            assert!(errors.is_empty());
            ds.close();
        }

        #[tokio::test]
        async fn addrs_are_urls_and_ok() {
            let pool_cfg = PoolConfig {
                max_size: 10,
                timeouts: Timeouts {
                    wait: Some(time::Duration::from_secs(10)),
                    create: Some(time::Duration::from_secs(11)),
                    recycle: Some(time::Duration::from_secs(12)),
                },
                ..Default::default()
            };
            let Ok(url0) = Url::parse("redis://127.0.0.1:7000/0") else {
                panic!("bad url");
            };
            let Ok(url1) = Url::parse("redis://127.0.0.1:7001/0") else {
                panic!("bad url");
            };
            let Ok(url2) = Url::parse("redis://127.0.0.1:7002/0") else {
                panic!("bad url");
            };
            let mut ds = RedisClusterDataSrcAsync::with_pool_config(&[url0, url1, url2], pool_cfg);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());
            ds.close();
        }

        #[tokio::test]
        async fn addrs_are_urls_and_fail() {
            let pool_cfg = PoolConfig {
                max_size: 10,
                timeouts: Timeouts {
                    wait: Some(time::Duration::from_secs(10)),
                    create: Some(time::Duration::from_secs(11)),
                    recycle: Some(time::Duration::from_secs(12)),
                },
                ..Default::default()
            };
            let Ok(url0) = Url::parse("redis://") else {
                panic!("bad url");
            };
            let Ok(url1) = Url::parse("redis://") else {
                panic!("bad url");
            };
            let Ok(url2) = Url::parse("redis://") else {
                panic!("bad url");
            };
            let mut ds = RedisClusterDataSrcAsync::with_pool_config(&[url0, url1, url2], pool_cfg);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup_async(&mut ag).await else {
                panic!();
            };
            let Ok(RedisClusterErrorAsync::FailToBuildPool { config }) =
                err.reason::<RedisClusterErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(format!("{:?}", config), "Config { urls: Some([\"redis://\", \"redis://\", \"redis://\"]), connections: None, pool: Some(PoolConfig { max_size: 10, timeouts: Timeouts { wait: Some(10s), create: Some(11s), recycle: Some(12s) }, queue_mode: Fifo }), read_from_replicas: false }");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Config(Redis(Missing hostname - InvalidClientConfig))"
            );
            let errors = ag.join_async().await;
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_with_config {
        use super::*;
        use deadpool_redis::Timeouts;
        use tokio::time;

        #[tokio::test]
        async fn ok() {
            let pool_cfg = PoolConfig {
                max_size: 10,
                timeouts: Timeouts {
                    wait: Some(time::Duration::from_secs(10)),
                    create: Some(time::Duration::from_secs(11)),
                    recycle: Some(time::Duration::from_secs(12)),
                },
                ..Default::default()
            };
            let config = Config {
                urls: Some(vec![
                    "redis://127.0.0.1:7000".to_string(),
                    "redis://127.0.0.1:7001".to_string(),
                    "redis://127.0.0.1:7002".to_string(),
                ]),
                connections: None,
                pool: Some(pool_cfg),
                read_from_replicas: false,
            };
            let mut ds = RedisClusterDataSrcAsync::with_config(config);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());
            ds.close();
        }

        #[tokio::test]
        async fn fail() {
            let pool_cfg = PoolConfig {
                max_size: 10,
                timeouts: Timeouts {
                    wait: Some(time::Duration::from_secs(10)),
                    create: Some(time::Duration::from_secs(11)),
                    recycle: Some(time::Duration::from_secs(12)),
                },
                ..Default::default()
            };
            let config = Config {
                urls: Some(vec![
                    "xxxx".to_string(),
                    "xxxx".to_string(),
                    "xxxx".to_string(),
                ]),
                connections: None,
                pool: Some(pool_cfg),
                read_from_replicas: false,
            };
            let mut ds = RedisClusterDataSrcAsync::with_config(config);
            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup_async(&mut ag).await else {
                panic!();
            };
            let Ok(RedisClusterErrorAsync::FailToBuildPool { config }) =
                err.reason::<RedisClusterErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(format!("{:?}", config), "Config { urls: Some([\"xxxx\", \"xxxx\", \"xxxx\"]), connections: None, pool: Some(PoolConfig { max_size: 10, timeouts: Timeouts { wait: Some(10s), create: Some(11s), recycle: Some(12s) }, queue_mode: Fifo }), read_from_replicas: false }");
            assert_eq!(
                format!("{:?}", err.source().unwrap()),
                "Config(Redis(Redis URL did not parse - InvalidClientConfig))"
            );
            let errors = ag.join_async().await;
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_create_data_conn {
        use super::*;
        use redis::AsyncTypedCommands;

        #[tokio::test]
        async fn ok() {
            let mut ds = RedisClusterDataSrcAsync::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            let Ok(mut data_conn) = ds.create_data_conn_async().await else {
                panic!("fail to create data_conn");
            };
            let redis_conn = data_conn.get_connection();

            redis_conn.set("test_create_data_conn", "1").await.unwrap();
            let s = redis_conn.get("test_create_data_conn").await.unwrap();
            redis_conn.del("test_create_data_conn").await.unwrap();
            assert_eq!(s, Some("1".to_string()));

            ds.close();
        }

        #[tokio::test]
        async fn fail() {
            let mut pcfg = PoolConfig::default();
            pcfg.max_size = 1usize;
            pcfg.timeouts.create = Some(time::Duration::from_millis(100));
            pcfg.timeouts.wait = Some(time::Duration::from_millis(100));
            let mut ds = RedisClusterDataSrcAsync::with_pool_config(
                &[
                    "redis://127.0.0.1:7000",
                    "redis://127.0.0.1:7001",
                    "redis://127.0.0.1:7002",
                ],
                pcfg,
            );

            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            let Ok(_data_conn) = ds.create_data_conn_async().await else {
                panic!("fail to create data_conn");
            };
            let Err(err) = ds.create_data_conn_async().await else {
                panic!("fail to create data_conn");
            };
            let Ok(RedisClusterErrorAsync::FailToGetConnectionFromPool) =
                err.reason::<RedisClusterErrorAsync>()
            else {
                panic!();
            };
            assert_eq!(format!("{:?}", err.source().unwrap()), "Timeout(Wait)",);

            ds.close();
        }

        #[tokio::test]
        async fn not_setup_yet() {
            let mut ds = RedisClusterDataSrcAsync::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
            let Err(err) = ds.create_data_conn_async().await else {
                panic!("fail to create data_conn");
            };
            let Ok(RedisClusterErrorAsync::NotSetupYet) = err.reason::<RedisClusterErrorAsync>()
            else {
                panic!();
            };
        }
    }

    mod test_setup {
        use super::*;

        #[tokio::test]
        async fn fail_due_to_setup_twice() {
            let mut ds = RedisClusterDataSrcAsync::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            let mut ag = AsyncGroup::new();
            let Err(err) = ds.setup_async(&mut ag).await else {
                panic!();
            };
            let Ok(RedisClusterErrorAsync::AlreadySetup) = err.reason::<RedisClusterErrorAsync>()
            else {
                panic!("{err:?}");
            };
            let errors = ag.join_async().await;
            assert!(errors.is_empty());
        }
    }
}

#[cfg(test)]
mod unit_tests_of_data_conn {
    use super::*;

    mod test_add_pre_commit {
        use super::*;
        use redis::AsyncTypedCommands;

        #[tokio::test]
        async fn ok() {
            const KEY: &str = "test_add_pre_commit_async/cluster";

            let mut ds = RedisClusterDataSrcAsync::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            let Ok(mut data_conn) = ds.create_data_conn_async().await else {
                panic!("fail to create data_conn");
            };
            assert!(data_conn.should_force_back());

            data_conn
                .add_pre_commit_async(async |mut redis_conn| {
                    redis_conn.set(KEY, "1").await.unwrap();
                    Ok(())
                })
                .await;

            let mut ag = AsyncGroup::new();
            data_conn.pre_commit_async(&mut ag).await.unwrap();
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                redis_conn.del(KEY).await.unwrap();
                assert_eq!(s, Some("1".to_string()));
            }

            let mut ag = AsyncGroup::new();
            data_conn.commit_async(&mut ag).await.unwrap();
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.post_commit_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.rollback_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.force_back_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            data_conn.close();
            ds.close();
        }

        #[tokio::test]
        async fn fail() {
            const KEY: &str = "test_add_pre_commit_async/cluster/fail";

            let mut ds = RedisClusterDataSrcAsync::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            let Ok(mut data_conn) = ds.create_data_conn_async().await else {
                panic!("fail to create data_conn");
            };
            assert!(data_conn.should_force_back());

            data_conn
                .add_pre_commit_async(async |mut redis_conn| {
                    redis_conn.set(KEY, "1").await.unwrap();
                    Err(errs::Err::new("fail"))
                })
                .await;

            let mut ag = AsyncGroup::new();
            let Err(err) = data_conn.pre_commit_async(&mut ag).await else {
                panic!();
            };
            let s = err.reason::<&str>().unwrap();
            assert_eq!(*s, "fail");

            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                redis_conn.del(KEY).await.unwrap();
                assert_eq!(s, Some("1".to_string()));
            }

            let mut ag = AsyncGroup::new();
            data_conn.commit_async(&mut ag).await.unwrap();
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.post_commit_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.rollback_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.force_back_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            data_conn.close();
            ds.close();
        }
    }

    mod test_add_post_commit {
        use super::*;
        use redis::AsyncTypedCommands;

        #[tokio::test]
        async fn ok() {
            const KEY: &str = "test_add_post_commit_async/cluster";

            let mut ds = RedisClusterDataSrcAsync::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            let Ok(mut data_conn) = ds.create_data_conn_async().await else {
                panic!("fail to create data_conn");
            };
            assert!(data_conn.should_force_back());

            data_conn
                .add_post_commit_async(async |mut redis_conn| {
                    redis_conn.set(KEY, "1").await.unwrap();
                    Ok(())
                })
                .await;

            let mut ag = AsyncGroup::new();
            data_conn.pre_commit_async(&mut ag).await.unwrap();
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.commit_async(&mut ag).await.unwrap();
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.post_commit_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                redis_conn.del(KEY).await.unwrap();
                assert_eq!(s, Some("1".to_string()));
            }

            let mut ag = AsyncGroup::new();
            data_conn.rollback_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.force_back_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            data_conn.close();
            ds.close();
        }

        #[tokio::test]
        async fn fail() {
            const KEY: &str = "test_add_post_commit_async/cluster/fail";

            let mut ds = RedisClusterDataSrcAsync::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            let Ok(mut data_conn) = ds.create_data_conn_async().await else {
                panic!("fail to create data_conn");
            };
            assert!(data_conn.should_force_back());

            data_conn
                .add_post_commit_async(async |mut redis_conn| {
                    redis_conn.set(KEY, "1").await.unwrap();
                    Err(errs::Err::new("fail"))
                })
                .await;

            let mut ag = AsyncGroup::new();
            if let Err(err) = data_conn.pre_commit_async(&mut ag).await {
                panic!("{err:?}");
            };
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.commit_async(&mut ag).await.unwrap();
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.post_commit_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                redis_conn.del(KEY).await.unwrap();
                assert_eq!(s, Some("1".to_string()));
            }

            let mut ag = AsyncGroup::new();
            data_conn.rollback_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.force_back_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            data_conn.close();
            ds.close();
        }
    }

    mod test_add_force_back {
        use super::*;
        use redis::AsyncTypedCommands;

        #[tokio::test]
        async fn ok() {
            const KEY: &str = "test_add_force_back_async/cluster";

            let mut ds = RedisClusterDataSrcAsync::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            let Ok(mut data_conn) = ds.create_data_conn_async().await else {
                panic!("fail to create data_conn");
            };
            assert!(data_conn.should_force_back());

            data_conn
                .add_force_back_async(async |mut redis_conn| {
                    redis_conn.set(KEY, "1").await.unwrap();
                    Ok(())
                })
                .await;

            let mut ag = AsyncGroup::new();
            data_conn.pre_commit_async(&mut ag).await.unwrap();
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.commit_async(&mut ag).await.unwrap();
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.post_commit_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.rollback_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.force_back_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                redis_conn.del(KEY).await.unwrap();
                assert_eq!(s, Some("1".to_string()));
            }

            data_conn.close();
            ds.close();
        }

        #[tokio::test]
        async fn fail() {
            const KEY: &str = "test_add_force_back_async/cluster/fail";

            let mut ds = RedisClusterDataSrcAsync::new(&[
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ]);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            let Ok(mut data_conn) = ds.create_data_conn_async().await else {
                panic!("fail to create data_conn");
            };
            assert!(data_conn.should_force_back());

            data_conn
                .add_force_back_async(async |mut redis_conn| {
                    redis_conn.set(KEY, "1").await.unwrap();
                    Err(errs::Err::new("fail"))
                })
                .await;

            let mut ag = AsyncGroup::new();
            if let Err(err) = data_conn.pre_commit_async(&mut ag).await {
                panic!("{:?}", err);
            };

            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.commit_async(&mut ag).await.unwrap();
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.post_commit_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.rollback_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                assert_eq!(s, None);
            }

            let mut ag = AsyncGroup::new();
            data_conn.force_back_async(&mut ag).await;
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            {
                let redis_conn = data_conn.get_connection();
                let s = redis_conn.get(KEY).await.unwrap();
                redis_conn.del(KEY).await.unwrap();
                assert_eq!(s, Some("1".to_string()));
            }

            data_conn.close();
            ds.close();
        }
    }
}
