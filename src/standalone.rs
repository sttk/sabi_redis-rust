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

/// Represents a reason for errors that can occur during `RedisDataSrc` operations,
/// to be passed to `errs::Err`.
#[derive(Debug)]
pub enum RedisDataSrcError {
    /// Indicates that a connection was requested before `RedisDataSrc` was set up.
    NotSetupYet,

    /// Indicates that a setup operation was attempted on an already-configured `RedisDataSrc`.
    AlreadySetup,

    /// Indicates a failure to open a connection from the `redis::Client` to the Redis server.
    /// Contains the connection information string that caused the failure.
    FailToOpenClient {
        /// The connection information string that caused the failure.
        connection_info: String,
    },

    /// Indicates a failure to build the Redis connection pool.
    FailToBuildPool,

    /// Indicates a failure to get a Redis connection from the pool.
    FailToGetConnectionFromPool,
}

/// A session-scoped connection to the Redis server.
///
/// This struct holds a pooled Redis connection and provides a `get_connection` method
/// to access it. It also provides a "force back" mechanism for handling transaction failures.
/// Since Redis does not support rollbacks, changes are committed with each update operation.
/// The `add_force_back` method allows registering functions to manually revert changes
/// if an error occurs during a multi-step process or a transaction involving
/// other external data sources.
pub struct RedisDataConn {
    conn: r2d2::PooledConnection<redis::Client>,
    force_back_vec: Vec<fn(&mut redis::Connection) -> Result<(), Err>>,
}

impl RedisDataConn {
    /// Returns a mutable reference to the underlying Redis connection.
    ///
    /// This reference allows direct access to Redis commands.
    pub fn get_connection(&mut self) -> &mut redis::Connection {
        &mut self.conn
    }

    /// Adds a function to the list of "force back" operations.
    ///
    /// The provided function will be called if the transaction needs to be reverted
    /// due to a failure in a subsequent operation.
    pub fn add_force_back(&mut self, f: fn(&mut redis::Connection) -> Result<(), Err>) {
        self.force_back_vec.push(f);
    }
}

impl DataConn for RedisDataConn {
    /// Commits the transaction.
    ///
    /// Note that Redis does not have a native rollback mechanism. All changes are committed
    /// as they are executed. This method is provided to satisfy the `DataConn` trait but
    /// does not perform any action.
    fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
        Ok(())
    }

    /// Rolls back the transaction.
    ///
    /// This method is provided to satisfy the `DataConn` trait but does not perform any action
    /// since Redis does not support native rollbacks. The `force_back` mechanism should
    /// be used instead.
    fn rollback(&mut self, _ag: &mut AsyncGroup) {}

    /// Checks if a "force back" operation is required.
    ///
    /// Always returns `true` because Redis operations are committed immediately,
    /// and `force_back` is the only way to undo a failure.
    fn should_force_back(&self) -> bool {
        true
    }

    /// Executes all registered "force back" functions.
    ///
    /// This method is intended to be called when a transaction fails to
    /// manually revert changes.
    fn force_back(&mut self, _ag: &mut AsyncGroup) {
        for f in self.force_back_vec.iter() {
            let _ = f(&mut self.conn);
        }
    }

    /// Closes the connection.
    ///
    /// This method is provided to satisfy the `DataConn` trait but
    /// does not perform any action as pooled connections are managed by the `r2d2` pool.
    fn close(&mut self) {}
}

/// Manages a connection pool for a Redis data source.
///
/// This struct is responsible for setting up the Redis connection pool
/// using a `redis::Client` and creating new `RedisDataConn` instances from the pool.
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
    /// Creates a new `RedisDataSrc` instance.
    ///
    /// The connection information is stored, but the connection pool is not built
    /// until the `setup` method is called.
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
    /// Sets up the Redis connection pool.
    ///
    /// This method creates a `redis::Client` and builds an `r2d2::Pool`.
    /// It must be called before `create_data_conn`. It will return an error if
    /// called more than once on the same instance.
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

    /// Closes the data source.
    ///
    /// This method does not perform any action since the connection pool
    /// will be automatically dropped when the struct goes out of scope.
    fn close(&mut self) {}

    /// Creates a new `RedisDataConn` instance.
    ///
    /// This method retrieves a connection from the internal pool. It will return a
    /// `NotSetupYet` error if the data source has not been set up.
    fn create_data_conn(&mut self) -> Result<Box<RedisDataConn>, Err> {
        if let Some(pool) = self.pool.get() {
            return match pool.get() {
                Ok(conn) => Ok(Box::new(RedisDataConn {
                    conn,
                    force_back_vec: Vec::new(),
                })),
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

        fn set_sample_key_with_force_back(&mut self, val: &str) -> Result<(), Err> {
            let redis_dc = self.get_data_conn::<RedisDataConn>("redis")?;
            let conn: &mut redis::Connection = redis_dc.get_connection();

            let result = conn.set("sample_force_back", val);

            redis_dc.add_force_back(|conn| {
                let r: redis::RedisResult<()> = conn.del("sample_force_back");
                match r {
                    Ok(()) => Ok(()),
                    Err(e) => Err(Err::with_source("fail to force back", e)),
                }
            });

            return match result {
                Ok(()) => Ok(()),
                Err(e) => Err(Err::with_source(SampleError::FailToSetValue, e)),
            };
        }
    }
    impl RedisSampleDataAcc for DataHub {}

    #[overridable]
    trait SampleData {
        fn get_sample_key(&mut self) -> Result<Option<String>, Err>;
        fn set_sample_key(&mut self, value: &str) -> Result<(), Err>;
        fn del_sample_key(&mut self) -> Result<(), Err>;
        fn set_sample_key_with_force_back(&mut self, val: &str) -> Result<(), Err>;
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
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379"));
        if let Err(err) = sabi::run!(sample_logic, data) {
            panic!("{:?}", err);
        }
    }

    #[test]
    fn fail_to_setup() {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("xxxxx"));
        if let Err(err) = sabi::run!(sample_logic, data) {
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

    fn sample_logic_with_force_back_and_commit(data: &mut impl SampleData) -> Result<(), Err> {
        data.set_sample_key_with_force_back("Good Morning")?;
        Ok(())
    }
    fn sample_logic_with_force_back_and_force_back(data: &mut impl SampleData) -> Result<(), Err> {
        data.set_sample_key_with_force_back("Good Afternoon")?;
        Err(Err::new("XXX"))
    }

    fn txn_and_commit() {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379"));

        if let Err(err) = sabi::txn!(sample_logic_with_force_back_and_commit, data) {
            panic!("{:?}", err);
        }

        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let mut conn = client.get_connection().unwrap();
        let s: redis::RedisResult<Option<String>> = conn.get("sample_force_back");
        assert_eq!(s.unwrap().unwrap(), "Good Morning");

        let _: redis::RedisResult<()> = conn.del("sample_force_back");
    }

    fn txn_and_force_back() {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379"));

        if let Err(err) = sabi::txn!(sample_logic_with_force_back_and_force_back, data) {
            assert_eq!(err.reason::<&str>().unwrap(), &"XXX");
        } else {
            panic!();
        }

        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let mut conn = client.get_connection().unwrap();
        let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back");
        assert!(r.unwrap().is_none());
    }

    #[test]
    fn force_back() {
        txn_and_commit();
        txn_and_force_back();
    }
}
