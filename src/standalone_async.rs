// Copyright (C) 2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use deadpool::managed;
use errs::Err;
use redis;
use sabi::{AsyncGroup, DataConn, DataSrc};
use std::future::Future;
use std::{cell, fmt, mem, pin, sync, sync::atomic, time};

#[derive(Debug)]
pub enum RedisAsyncDataSrcError {
    NotSetupYet,
    AlreadySetup,
    FailToCreateConnectionManager {
        connection_info: String,
    },
    FailToBuildPool {
        connection_info: String,
        pool_config: String,
    },
    FailToGetConnectionFromPool,
}

type BoxedFuture = pin::Pin<Box<dyn Future<Output = Result<(), Err>> + Send>>;

pub struct RedisAsyncDataConn {
    pool: managed::Pool<RedisConnectionManager>,

    pre_commit_vec: Vec<BoxedFuture>,
    post_commit_vec: Vec<BoxedFuture>,
    force_back_vec: Vec<BoxedFuture>,
}

impl RedisAsyncDataConn {
    fn new(pool: managed::Pool<RedisConnectionManager>) -> Self {
        Self {
            pool,
            pre_commit_vec: Vec::new(),
            post_commit_vec: Vec::new(),
            force_back_vec: Vec::new(),
        }
    }

    pub async fn get_connection_async(&mut self) -> Result<redis::aio::MultiplexedConnection, Err> {
        match self.pool.get().await {
            Ok(pooled_conn /* Object<RedisConnectionManager> */) => {
                let redis_conn: &redis::aio::MultiplexedConnection = &sync::Arc::new(pooled_conn);
                Ok(redis_conn.clone())
            }
            Err(e) => Err(Err::with_source(
                RedisAsyncDataSrcError::FailToGetConnectionFromPool,
                e,
            )),
        }
    }

    pub async fn add_pre_commit<F>(&mut self, mut f: F)
    where
        F: FnMut(&redis::aio::MultiplexedConnection) -> BoxedFuture,
    {
        let pool_fut = self.pool.clone();
        match pool_fut.get().await {
            Ok(pooled_conn) => {
                let redis_conn: &redis::aio::MultiplexedConnection = &pooled_conn;
                self.pre_commit_vec.push(Box::pin(f(redis_conn)));
            }
            Err(e) => self.pre_commit_vec.push(Box::pin(async move {
                Err(Err::with_source(
                    RedisAsyncDataSrcError::FailToGetConnectionFromPool,
                    e,
                ))
            })),
        }
    }

    pub async fn add_post_commit<F>(&mut self, mut f: F)
    where
        F: FnMut(&redis::aio::MultiplexedConnection) -> BoxedFuture,
    {
        let pool_fut = self.pool.clone();
        match pool_fut.get().await {
            Ok(pooled_conn) => {
                let redis_conn: &redis::aio::MultiplexedConnection = &pooled_conn;
                self.post_commit_vec.push(Box::pin(f(redis_conn)));
            }
            Err(e) => self.post_commit_vec.push(Box::pin(async move {
                Err(Err::with_source(
                    RedisAsyncDataSrcError::FailToGetConnectionFromPool,
                    e,
                ))
            })),
        }
    }

    pub async fn add_force_back<F>(&mut self, mut f: F)
    where
        F: FnMut(&redis::aio::MultiplexedConnection) -> BoxedFuture,
    {
        let pool_fut = self.pool.clone();
        match pool_fut.get().await {
            Ok(pooled_conn) => {
                let redis_conn: &redis::aio::MultiplexedConnection = &pooled_conn;
                self.force_back_vec.push(Box::pin(f(redis_conn)));
            }
            Err(e) => self.force_back_vec.push(Box::pin(async move {
                Err(Err::with_source(
                    RedisAsyncDataSrcError::FailToGetConnectionFromPool,
                    e,
                ))
            })),
        }
    }
}

impl DataConn for RedisAsyncDataConn {
    fn pre_commit(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> {
        let mut vec = Vec::<BoxedFuture>::new();
        mem::swap(&mut vec, &mut self.pre_commit_vec);

        ag.add(async move {
            for fut in vec.into_iter() {
                if let Err(err) = fut.await {
                    return Err(err);
                }
            }
            Ok(())
        });
        Ok(())
    }

    fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
        Ok(())
    }

    fn post_commit(&mut self, ag: &mut AsyncGroup) {
        let mut vec = Vec::<BoxedFuture>::new();
        mem::swap(&mut vec, &mut self.post_commit_vec);

        ag.add(async move {
            for fut in vec.into_iter() {
                if let Err(err) = fut.await {
                    return Err(err);
                }
            }
            Ok(())
        });
    }

    fn rollback(&mut self, _ag: &mut AsyncGroup) {}

    fn should_force_back(&self) -> bool {
        true
    }

    fn force_back(&mut self, ag: &mut AsyncGroup) {
        let mut vec = Vec::<BoxedFuture>::new();
        mem::swap(&mut vec, &mut self.force_back_vec);

        ag.add(async move {
            for fut in vec.into_iter().rev() {
                if let Err(err) = fut.await {
                    return Err(err);
                }
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

pub struct RedisAsyncDataSrc<T>
where
    T: redis::IntoConnectionInfo + Sized + fmt::Debug,
{
    pool: cell::OnceCell<managed::Pool<RedisConnectionManager>>,
    conn_info: Option<T>,
    pool_config: Option<managed::PoolConfig>,
}

impl<T> RedisAsyncDataSrc<T>
where
    T: redis::IntoConnectionInfo + Sized + fmt::Debug,
{
    pub fn new(conn_info: T) -> Self {
        Self {
            pool: cell::OnceCell::new(),
            conn_info: Some(conn_info),
            pool_config: Some(managed::PoolConfig {
                max_size: 10,
                // Although deadpool's default value is None (unlimited), it should be configured
                // with a specific value to avoid the risk of deadlocks.
                timeouts: managed::Timeouts {
                    wait: Some(time::Duration::from_secs(1)),
                    create: Some(time::Duration::from_secs(3)),
                    recycle: Some(time::Duration::from_secs(500)),
                },
                ..Default::default()
            }),
        }
    }

    pub fn with_pool_config(conn_info: T, pool_config: managed::PoolConfig) -> Self {
        Self {
            pool: cell::OnceCell::new(),
            conn_info: Some(conn_info),
            pool_config: Some(pool_config),
        }
    }
}

impl<T> DataSrc<RedisAsyncDataConn> for RedisAsyncDataSrc<T>
where
    T: redis::IntoConnectionInfo + Sized + fmt::Debug,
{
    fn setup(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
        let mut conn_info_opt = None;
        mem::swap(&mut conn_info_opt, &mut self.conn_info);

        let mut pool_cfg_opt = None;
        mem::swap(&mut pool_cfg_opt, &mut self.pool_config);

        if let (Some(conn_info), Some(pool_cfg)) = (conn_info_opt, pool_cfg_opt) {
            let conn_info_string = format!("{:?}", conn_info);

            match RedisConnectionManager::new(conn_info) {
                Ok(manager) => {
                    let pool_builder = managed::Pool::builder(manager).config(pool_cfg);
                    match pool_builder.build() {
                        Ok(pool) => {
                            if let Err(_pool) = self.pool.set(pool) {
                                Err(Err::new(RedisAsyncDataSrcError::AlreadySetup))
                            } else {
                                Ok(())
                            }
                        }
                        Err(e_p) => Err(Err::with_source(
                            RedisAsyncDataSrcError::FailToBuildPool {
                                connection_info: conn_info_string,
                                pool_config: format!("{:?}", pool_cfg),
                            },
                            e_p,
                        )),
                    }
                }
                Err(e_m) => Err(Err::with_source(
                    RedisAsyncDataSrcError::FailToCreateConnectionManager {
                        connection_info: conn_info_string,
                    },
                    e_m,
                )),
            }
        } else {
            Err(Err::new(RedisAsyncDataSrcError::AlreadySetup))
        }
    }

    fn close(&mut self) {}

    fn create_data_conn(&mut self) -> Result<Box<RedisAsyncDataConn>, Err> {
        if let Some(pool) = self.pool.get() {
            return Ok(Box::new(RedisAsyncDataConn::new(pool.clone())));
        } else {
            return Err(Err::new(RedisAsyncDataSrcError::NotSetupYet));
        }
    }
}

#[derive(Debug)]
struct RedisConnectionManager {
    client: redis::Client,
    ping_number: atomic::AtomicUsize,
}

impl RedisConnectionManager {
    pub fn new<T: redis::IntoConnectionInfo>(conn_info: T) -> redis::RedisResult<Self> {
        Ok(Self {
            client: redis::Client::open(conn_info)?,
            ping_number: atomic::AtomicUsize::new(0),
        })
    }
}

impl managed::Manager for RedisConnectionManager {
    type Type = redis::aio::MultiplexedConnection;
    type Error = redis::RedisError;

    async fn create(&self) -> Result<redis::aio::MultiplexedConnection, redis::RedisError> {
        self.client.get_multiplexed_async_connection().await
    }

    async fn recycle(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        _: &managed::Metrics,
    ) -> managed::RecycleResult<redis::RedisError> {
        let ping_number = self
            .ping_number
            .fetch_add(1, atomic::Ordering::Relaxed)
            .to_string();

        // Using pipeline to avoid roundtrip for UNWATCH
        let (n,) = redis::Pipeline::with_capacity(2)
            .cmd("UNWATCH")
            .ignore()
            .cmd("PING")
            .arg(&ping_number)
            .query_async::<(String,)>(conn)
            .await?;

        if n == ping_number {
            Ok(())
        } else {
            Err(managed::RecycleError::message("Invalid PING response"))
        }
    }
}
