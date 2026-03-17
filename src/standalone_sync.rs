// Copyright (C) 2025-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use r2d2::{Builder, Pool, PooledConnection};
use redis::{Client, Connection, IntoConnectionInfo};
use sabi::{AsyncGroup, DataConn, DataSrc};

use std::fmt::Debug;
use std::{mem, time};

#[derive(Debug)]
pub enum RedisSyncError {
    NotSetupYet,
    AlreadySetup,
    FailToOpenClient,
    FailToBuildPool,
    FailToGetConnectionFromPool,
}

#[allow(clippy::type_complexity)]
pub struct RedisDataConn {
    pool: Pool<Client>,
    pre_commit_vec: Vec<Box<dyn FnMut(&mut Connection) -> errs::Result<()>>>,
    post_commit_vec: Vec<Box<dyn FnMut(&mut Connection) -> errs::Result<()>>>,
    force_back_vec: Vec<Box<dyn FnMut(&mut Connection) -> errs::Result<()>>>,
}

impl RedisDataConn {
    fn new(pool: Pool<Client>) -> Self {
        Self {
            pool,
            pre_commit_vec: Vec::new(),
            post_commit_vec: Vec::new(),
            force_back_vec: Vec::new(),
        }
    }

    pub fn get_connection(&mut self) -> errs::Result<PooledConnection<Client>> {
        self.pool
            .get()
            .map_err(|e| errs::Err::with_source(RedisSyncError::FailToGetConnectionFromPool, e))
    }

    pub fn get_connection_with_timeout(
        &self,
        timeout: time::Duration,
    ) -> errs::Result<PooledConnection<Client>> {
        self.pool
            .get_timeout(timeout)
            .map_err(|e| errs::Err::with_source(RedisSyncError::FailToGetConnectionFromPool, e))
    }

    pub fn try_get_connection(&self) -> Option<PooledConnection<Client>> {
        self.pool.try_get()
    }

    pub fn add_pre_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut Connection) -> errs::Result<()> + 'static,
    {
        self.pre_commit_vec.push(Box::new(f));
    }

    pub fn add_post_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut Connection) -> errs::Result<()> + 'static,
    {
        self.post_commit_vec.push(Box::new(f));
    }

    pub fn add_force_back<F>(&mut self, f: F)
    where
        F: FnMut(&mut Connection) -> errs::Result<()> + 'static,
    {
        self.force_back_vec.push(Box::new(f));
    }
}

impl DataConn for RedisDataConn {
    fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        match self.pool.get() {
            Ok(mut conn) => {
                for f in self.pre_commit_vec.iter_mut() {
                    f(&mut conn)?;
                }
                Ok(())
            }
            Err(e) => Err(errs::Err::with_source(
                RedisSyncError::FailToGetConnectionFromPool,
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
                let _ = errs::Err::with_source(RedisSyncError::FailToGetConnectionFromPool, e);
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
                let _ = errs::Err::with_source(RedisSyncError::FailToGetConnectionFromPool, e);
            }
        };
    }

    fn close(&mut self) {}
}

pub struct RedisDataSrc<T>
where
    T: IntoConnectionInfo + Sized + Debug,
{
    pool: Option<RedisPool<T>>,
}

enum RedisPool<T>
where
    T: IntoConnectionInfo + Sized + Debug,
{
    Object(Pool<Client>),
    Config(Box<(T, Builder<Client>)>),
}

impl<T> RedisDataSrc<T>
where
    T: IntoConnectionInfo + Sized + Debug,
{
    pub fn new(addr: T) -> Self {
        Self {
            pool: Some(RedisPool::Config(Box::new((addr, Pool::builder())))),
        }
    }

    pub fn with_pool_builder(addr: T, pool_builder: Builder<Client>) -> Self {
        Self {
            pool: Some(RedisPool::Config(Box::new((addr, pool_builder)))),
        }
    }
}

impl<T> DataSrc<RedisDataConn> for RedisDataSrc<T>
where
    T: IntoConnectionInfo + Sized + Debug,
{
    fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let pool_opt = mem::take(&mut self.pool);
        let pool = pool_opt.ok_or_else(|| errs::Err::new(RedisSyncError::AlreadySetup))?;
        match pool {
            RedisPool::Config(cfg) => {
                let (addr, pool_config) = *cfg;

                let client = Client::open(addr)
                    .map_err(|e| errs::Err::with_source(RedisSyncError::FailToOpenClient, e))?;

                let pool = pool_config
                    .build(client)
                    .map_err(|e| errs::Err::with_source(RedisSyncError::FailToBuildPool, e))?;

                self.pool = Some(RedisPool::Object(pool));
                Ok(())
            }
            _ => Err(errs::Err::new(RedisSyncError::AlreadySetup)),
        }
    }

    fn close(&mut self) {}

    fn create_data_conn(&mut self) -> errs::Result<Box<RedisDataConn>> {
        let pool = self
            .pool
            .as_mut()
            .ok_or_else(|| errs::Err::new(RedisSyncError::NotSetupYet))?;
        match pool {
            RedisPool::Object(pool) => Ok(Box::new(RedisDataConn::new(pool.clone()))),
            _ => Err(errs::Err::new(RedisSyncError::NotSetupYet)),
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
    trait RedisSampleDataAcc: DataAcc {
        fn get_sample_key(&mut self) -> errs::Result<Option<String>> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;
            conn.get("sample")
                .map_err(|e| errs::Err::with_source(SampleError::FailToGetValue, e))
        }
        fn set_sample_key(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
            let mut conn =
                data_conn.get_connection_with_timeout(time::Duration::from_millis(1000))?;
            conn.set("sample", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))
        }
        fn del_sample_key(&mut self) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
            let mut conn = data_conn.try_get_connection().unwrap();
            conn.del("sample")
                .map_err(|e| errs::Err::with_source(SampleError::FailToDelValue, e))
        }

        fn set_sample_key_with_force_back(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;

            conn.set::<&str, &str, ()>("sample_force_back", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;

            data_conn.add_force_back(|conn| {
                conn.del("sample_force_back")
                    .map_err(|e| errs::Err::with_source("fail to force back", e))
            });

            conn.set::<&str, &str, ()>("sample_force_back_2", val)
                .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;

            data_conn.add_force_back(|conn| {
                conn.del("sample_force_back_2")
                    .map_err(|e| errs::Err::with_source("fail to force back", e))
            });

            Ok(())
        }

        fn set_sample_key_with_pre_commit(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;

            let val_owned = val.to_string();

            data_conn.add_pre_commit(move |conn| {
                conn.set::<&str, &str, ()>("sample_pre_commit", &val_owned)
                    .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;
                Ok(())
            });

            Ok(())
        }

        fn set_sample_key_with_post_commit(&mut self, val: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;

            let val_owned = val.to_string();

            data_conn.add_post_commit(move |conn| {
                conn.set::<&str, &str, ()>("sample_post_commit", &val_owned)
                    .map_err(|e| errs::Err::with_source(SampleError::FailToSetValue, e))?;
                Ok(())
            });

            Ok(())
        }
    }
    impl RedisSampleDataAcc for DataHub {}

    #[overridable]
    trait SampleData {
        fn get_sample_key(&mut self) -> errs::Result<Option<String>>;
        fn set_sample_key(&mut self, value: &str) -> errs::Result<()>;
        fn del_sample_key(&mut self) -> errs::Result<()>;
        fn set_sample_key_with_force_back(&mut self, val: &str) -> errs::Result<()>;
        fn set_sample_key_with_pre_commit(&mut self, val: &str) -> errs::Result<()>;
        fn set_sample_key_with_post_commit(&mut self, val: &str) -> errs::Result<()>;
    }
    #[override_with(RedisSampleDataAcc)]
    impl SampleData for DataHub {}

    fn sample_logic(data: &mut impl SampleData) -> errs::Result<()> {
        data.get_sample_key().expect("Data exists");
        data.set_sample_key("Hello")?;
        assert_eq!(data.get_sample_key().expect("No Data").unwrap(), "Hello");
        data.del_sample_key()?;
        Ok(())
    }

    #[test]
    fn test_new_by_str() -> Result<(), errs::Err> {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/0"));
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn test_new_by_conn_info() -> Result<(), errs::Err> {
        let ci: redis::ConnectionInfo = "redis://127.0.0.1:6379/1".parse().unwrap();

        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new(ci));
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn test_with_pool_builder() -> Result<(), errs::Err> {
        let builder = r2d2::Pool::<redis::Client>::builder()
            .max_size(100)
            .min_idle(Some(10))
            .max_lifetime(Some(time::Duration::from_secs(60 * 60)))
            .idle_timeout(Some(time::Duration::from_secs(5 * 60)))
            .connection_timeout(time::Duration::from_secs(30));

        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisDataSrc::with_pool_builder("redis://127.0.0.1:6379/2", builder),
        );
        data.run(sample_logic)?;
        Ok(())
    }

    #[test]
    fn fail_due_to_invalid_addr() {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("xxxxx"));
        let err = data.run(sample_logic).unwrap_err();

        match err.reason::<sabi::DataHubError>().unwrap() {
            sabi::DataHubError::FailToSetupLocalDataSrcs { errors } => {
                assert_eq!(errors.len(), 1);
                assert_eq!(errors[0].0.as_ref(), "redis");
                if let Ok(r) = errors[0].1.reason::<RedisSyncError>() {
                    match r {
                        RedisSyncError::FailToOpenClient => {}
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

    fn sample_logic_with_force_back_ok(data: &mut impl SampleData) -> errs::Result<()> {
        data.set_sample_key_with_force_back("Good Morning")?;
        Ok(())
    }
    fn sample_logic_with_force_back_err(data: &mut impl SampleData) -> errs::Result<()> {
        data.set_sample_key_with_force_back("Good Afternoon")?;
        Err(errs::Err::new("XXX"))
    }
    fn sample_logic_with_pre_commit(data: &mut impl SampleData) -> errs::Result<()> {
        data.set_sample_key_with_pre_commit("Good Evening")?;
        Ok(())
    }
    fn sample_logic_with_post_commit(data: &mut impl SampleData) -> errs::Result<()> {
        data.set_sample_key_with_post_commit("Good Night")?;
        Ok(())
    }

    #[test]
    fn test_txn_and_force_back() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/3"));
        data.txn(sample_logic_with_force_back_ok)?;

        {
            let client = redis::Client::open("redis://127.0.0.1:6379/3").unwrap();
            let mut conn = client.get_connection().unwrap();

            let s: redis::RedisResult<Option<String>> = conn.get("sample_force_back");
            let _: redis::RedisResult<()> = conn.del("sample_force_back");
            assert_eq!(s.unwrap().unwrap(), "Good Morning");

            let s: redis::RedisResult<Option<String>> = conn.get("sample_force_back_2");
            let _: redis::RedisResult<()> = conn.del("sample_force_back_2");
            assert_eq!(s.unwrap().unwrap(), "Good Morning");
        }

        let err = data.txn(sample_logic_with_force_back_err).unwrap_err();
        assert_eq!(err.reason::<&str>().unwrap(), &"XXX");

        {
            let client = redis::Client::open("redis://127.0.0.1:6379/6").unwrap();
            let mut conn = client.get_connection().unwrap();

            let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back");
            let _: redis::RedisResult<()> = conn.del("sample_force_back");
            assert!(r.unwrap().is_none());

            let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back_2");
            let _: redis::RedisResult<()> = conn.del("sample_force_back_2");
            assert!(r.unwrap().is_none());
        }

        Ok(())
    }

    #[test]
    fn test_txn_and_pre_commit() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/4"));

        data.txn(sample_logic_with_pre_commit)?;

        {
            let client = redis::Client::open("redis://127.0.0.1:6379/4").unwrap();
            let mut conn = client.get_connection().unwrap();
            let s: redis::RedisResult<Option<String>> = conn.get("sample_pre_commit");
            let _: redis::RedisResult<()> = conn.del("sample_pre_commit");
            assert_eq!(s.unwrap().unwrap(), "Good Evening");
        }
        Ok(())
    }

    #[test]
    fn test_txn_and_post_commit() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/5"));

        data.txn(sample_logic_with_post_commit)?;

        {
            let client = redis::Client::open("redis://127.0.0.1:6379/5").unwrap();
            let mut conn = client.get_connection().unwrap();
            let s: redis::RedisResult<Option<String>> = conn.get("sample_post_commit");
            let _: redis::RedisResult<()> = conn.del("sample_post_commit");
            assert_eq!(s.unwrap().unwrap(), "Good Night");
        }
        Ok(())
    }
}
