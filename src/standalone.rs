// Copyright (C) 2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use errs::Err;
use r2d2;
use redis;
use sabi::{AsyncGroup, DataConn, DataSrc};

use std::cell;
use std::fmt::Debug;
use std::mem;

#[derive(Debug)]
pub enum RedisDataSrcError {
    NotSetupYet,
    AlreadySetup,
    FailToOpenClient { connection_info: String },
    FailToBuildPool,
    FailToGetConnectionFromPool,
}

pub struct RedisDataConn {
    conn: r2d2::PooledConnection<redis::Client>,
}

impl RedisDataConn {
    pub fn get_connection(&mut self) -> &mut redis::Connection {
        &mut self.conn
    }
}

impl DataConn for RedisDataConn {
    fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
        Ok(())
    }
    fn rollback(&mut self, _ag: &mut AsyncGroup) {}
    fn should_force_back(&self) -> bool {
        true
    }
    fn force_back(&mut self, _ag: &mut AsyncGroup) {}
    fn close(&mut self) {}
}

pub struct RedisDataSrc<T>
where
    T: redis::IntoConnectionInfo + Sized + Debug,
{
    pool: cell::OnceCell<r2d2::Pool<redis::Client>>,
    conn_info: Option<T>,
}

impl<T> RedisDataSrc<T>
where
    T: redis::IntoConnectionInfo + Sized + Debug,
{
    pub fn new(conn_info: T) -> Self {
        Self {
            pool: cell::OnceCell::new(),
            conn_info: Some(conn_info),
        }
    }
}

impl<T> DataSrc<RedisDataConn> for RedisDataSrc<T>
where
    T: redis::IntoConnectionInfo + Sized + Debug,
{
    fn setup(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
        let mut conn_info_opt = None;
        mem::swap(&mut conn_info_opt, &mut self.conn_info);

        if let Some(conn_info) = conn_info_opt {
            let conn_info_string = format!("{:?}", conn_info);
            match redis::Client::open(conn_info) {
                Ok(client) => match r2d2::Pool::builder().build(client) {
                    Ok(pool) => {
                        if let Err(_) = self.pool.set(pool) {
                            return Err(Err::new(RedisDataSrcError::AlreadySetup));
                        } else {
                            return Ok(());
                        }
                    }
                    Err(e_p) => Err(Err::with_source(RedisDataSrcError::FailToBuildPool, e_p)),
                },
                Err(e_c) => Err(Err::with_source(
                    RedisDataSrcError::FailToOpenClient {
                        connection_info: conn_info_string,
                    },
                    e_c,
                )),
            }
        } else {
            return Err(Err::new(RedisDataSrcError::AlreadySetup));
        }
    }

    fn close(&mut self) {}

    fn create_data_conn(&mut self) -> Result<Box<RedisDataConn>, Err> {
        if let Some(pool) = self.pool.get() {
            return match pool.get() {
                Ok(conn) => Ok(Box::new(RedisDataConn { conn })),
                Err(e) => Err(Err::with_source(
                    RedisDataSrcError::FailToGetConnectionFromPool,
                    e,
                )),
            };
        } else {
            return Err(Err::new(RedisDataSrcError::NotSetupYet));
        }
    }
}

#[cfg(test)]
mod test_redis {
    use super::*;
    use override_macro::{overridable, override_with};
    use redis::Commands;
    use sabi::{DataAcc, DataHub};

    #[derive(Debug)]
    enum SampleError {
        FailToGetValue,
        FailToSetValue,
        FailToDelValue,
    }

    #[overridable]
    trait RedisSampleDataAcc: DataAcc {
        fn get_sample_key(&mut self) -> Result<Option<String>, Err> {
            let redis_dc = self.get_data_conn::<RedisDataConn>("redis")?;
            let conn: &mut redis::Connection = redis_dc.get_connection();
            let rslt: redis::RedisResult<Option<String>> = conn.get("sample");
            return match rslt {
                Ok(opt) => Ok(opt),
                Err(e) => Err(Err::with_source(SampleError::FailToGetValue, e)),
            };
        }
        fn set_sample_key(&mut self, val: &str) -> Result<(), Err> {
            let redis_dc = self.get_data_conn::<RedisDataConn>("redis")?;
            let conn: &mut redis::Connection = redis_dc.get_connection();
            return match conn.set("sample", val) {
                Ok(()) => Ok(()),
                Err(e) => Err(Err::with_source(SampleError::FailToSetValue, e)),
            };
        }
        fn del_sample_key(&mut self) -> Result<(), Err> {
            let redis_dc = self.get_data_conn::<RedisDataConn>("redis")?;
            let conn: &mut redis::Connection = redis_dc.get_connection();
            return match conn.del("sample") {
                Ok(()) => Ok(()),
                Err(e) => Err(Err::with_source(SampleError::FailToDelValue, e)),
            };
        }
    }
    impl RedisSampleDataAcc for DataHub {}

    #[overridable]
    trait SampleData {
        fn get_sample_key(&mut self) -> Result<Option<String>, Err>;
        fn set_sample_key(&mut self, value: &str) -> Result<(), Err>;
        fn del_sample_key(&mut self) -> Result<(), Err>;
    }
    #[override_with(RedisSampleDataAcc)]
    impl SampleData for DataHub {}

    fn sample_logic(data: &mut impl SampleData) -> Result<(), Err> {
        match data.get_sample_key()? {
            Some(_) => panic!("Data exists"),
            None => {}
        }

        data.set_sample_key("Hello")?;

        match data.get_sample_key()? {
            Some(val) => assert_eq!(val, "Hello"),
            None => panic!("No data"),
        }

        data.del_sample_key()?;
        Ok(())
    }

    #[test]
    fn ok() {
        let mut hub = DataHub::new();
        hub.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379"));
        if let Err(err) = hub.run(sample_logic) {
            panic!("{:?}", err);
        }
    }

    #[test]
    fn fail() {
        let mut hub = DataHub::new();
        hub.uses("redis", RedisDataSrc::new("xxxxx"));
        if let Err(err) = hub.run(sample_logic) {
            if let Ok(r) = err.reason::<sabi::DataHubError>() {
                match r {
                    sabi::DataHubError::FailToSetupLocalDataSrcs { errors } => {
                        assert_eq!(errors.len(), 1);
                        let err_redis = errors.get("redis").unwrap();
                        match err_redis.reason::<RedisDataSrcError>().unwrap() {
                            RedisDataSrcError::FailToOpenClient { connection_info } => {
                                assert_eq!(connection_info, "\"xxxxx\"");
                            }
                            _ => panic!(),
                        }
                    }
                    _ => panic!("{:?}", err),
                }
            } else {
                panic!("{:?}", err);
            }
        } else {
            panic!();
        }
    }
}
