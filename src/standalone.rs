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
///
/// ## Example
///
/// ```rust
/// use errs;
/// use override_macro::{overridable, override_with};
/// use redis::TypedCommands;
/// use sabi;
/// use sabi_redis::{RedisDataSrc, RedisDataConn};
///
/// fn main() -> Result<(), errs::Err> {
///     // Register a `RedisDataSrc` instance to connect to a Redis server with the key "redis".
///     sabi::uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/0"));
///
///     // In this setup process, the registered `RedisDataSrc` instance cnnects to a Redis server.
///     let _auto_shutdown = sabi::setup()?;
///
///     my_app()
/// }
///
/// fn my_app() -> Result<(), errs::Err> {
///     let mut data = sabi::DataHub::new();
///     sabi::txn!(my_logic, data)
/// }
///
/// fn my_logic(data: &mut impl MyData) -> Result<(), errs::Err> {
///     let greeting = data.get_greeting()?;
///     data.say_greeting(&greeting)
/// }
///
/// #[overridable]
/// trait MyData {
///     fn get_greeting(&mut self) -> Result<String, errs::Err>;
///     fn say_greeting(&mut self, greeting: &str) -> Result<(), errs::Err>;
/// }
///
/// #[overridable]
/// trait GettingDataAcc: sabi::DataAcc {
///     fn get_greeting(&mut self) -> Result<String, errs::Err> {
///         Ok("Hello!".to_string())
///     }
/// }
///
/// #[overridable]
/// trait RedisSayingDataAcc: sabi::DataAcc {
///     fn say_greeting(&mut self, greeting: &str) -> Result<(), errs::Err> {
///         // Retrieve a `RedisDataConn` instance by the key "redis".
///         let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
///
///         // Get a Redis connection to execute Redis synchronous commands.
///         let mut redis_conn = data_conn.get_connection()?;
///
///         if let Err(e) = redis_conn.set("greeting", greeting) {
///             return Err(errs::Err::with_source("fail to set gretting", e));
///         }
///
///         // Register a force back process to revert updates to Redis when an error occurs.
///         data_conn.add_force_back(|redis_conn| {
///             let result = redis_conn.del("greeting");
///             if let Err(e) = result {
///                 return Err(errs::Err::with_source("fail to force back", e));
///             }
///             Ok(())
///         });
///
///         Ok(())
///     }
/// }
///
/// impl GettingDataAcc for sabi::DataHub {}
/// impl RedisSayingDataAcc for sabi::DataHub {}
///
/// #[override_with(GettingDataAcc, RedisSayingDataAcc)]
/// impl MyData for sabi::DataHub {}
/// ```
pub struct RedisDataConn {
    pool: r2d2::Pool<redis::Client>,
    pre_commit_vec: Vec<Box<dyn FnMut(&mut redis::Connection) -> Result<(), Err>>>,
    post_commit_vec: Vec<Box<dyn FnMut(&mut redis::Connection) -> Result<(), Err>>>,
    force_back_vec: Vec<Box<dyn FnMut(&mut redis::Connection) -> Result<(), Err>>>,
}

impl RedisDataConn {
    fn new(pool: r2d2::Pool<redis::Client>) -> Self {
        Self {
            pool,
            pre_commit_vec: Vec::new(),
            post_commit_vec: Vec::new(),
            force_back_vec: Vec::new(),
        }
    }

    /// Returns a mutable reference to the underlying Redis connection.
    ///
    /// This reference allows direct access to Redis commands.
    pub fn get_connection(&mut self) -> Result<r2d2::PooledConnection<redis::Client>, Err> {
        self.pool
            .get()
            .map_err(|e| Err::with_source(RedisDataSrcError::FailToGetConnectionFromPool, e))
    }

    /// Adds a function to the list of "pre commit" operations.
    ///
    /// The provided function will be called after all other database update processes have
    /// finished, and right before their commit processes are made,
    pub fn add_pre_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut redis::Connection) -> Result<(), Err> + 'static,
    {
        self.pre_commit_vec.push(Box::new(f));
    }

    /// Adds a function to the list of "post commit" operations.
    ///
    /// The provided function will be called as post-transaction processes.
    pub fn add_post_commit<F>(&mut self, f: F)
    where
        F: FnMut(&mut redis::Connection) -> Result<(), Err> + 'static,
    {
        self.post_commit_vec.push(Box::new(f));
    }

    /// Adds a function to the list of "force back" operations.
    ///
    /// The provided function will be called if the transaction needs to be reverted
    /// due to a failure in a subsequent operation.
    pub fn add_force_back<F>(&mut self, f: F)
    where
        F: FnMut(&mut redis::Connection) -> Result<(), Err> + 'static,
    {
        self.force_back_vec.push(Box::new(f));
    }
}

impl DataConn for RedisDataConn {
    /// Executes all functions registered as "pre commit" processes.
    ///
    /// This method is called after all other database update processes have finished and right
    /// before their commit processes are made, Since Redis does not have a rollback feature,
    /// it is possible to maintain transactional consistency by performing updates at this timing,
    /// as long as the updated data are not searched for within the transaction.
    fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
        match self.pool.get() {
            Ok(mut conn) => {
                for f in self.pre_commit_vec.iter_mut() {
                    f(&mut conn)?;
                }
                Ok(())
            }
            Err(e) => Err(Err::with_source(
                RedisDataSrcError::FailToGetConnectionFromPool,
                e,
            )),
        }
    }

    /// Commits the transaction.
    ///
    /// Note that Redis does not have a native rollback mechanism. All changes are committed
    /// as they are executed. This method is provided to satisfy the `DataConn` trait but
    /// does not perform any action.
    fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
        Ok(())
    }

    /// Executes all functions registered as "post commit" processes.
    ///
    /// This method is called as post-transaction processes. Since Redis does not have a rollback
    /// feature, it's acceptable to perform update processes at this point that can be manually
    /// recovered later, even if they fail.
    fn post_commit(&mut self, _ag: &mut AsyncGroup) {
        match self.pool.get() {
            Ok(mut conn) => {
                for f in self.post_commit_vec.iter_mut() {
                    let _ = f(&mut conn);
                }
            }
            Err(e) => {
                // for error notification
                let _ = Err::with_source(RedisDataSrcError::FailToGetConnectionFromPool, e);
            }
        };
    }

    /// Rolls back the transaction.
    ///
    /// This method is provided to satisfy the `DataConn` trait but does not perform any action
    /// since Redis does not support native rollbacks. The `add_force_back` and `force_back`
    /// mechanism should be used instead.
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
        match self.pool.get() {
            Ok(mut conn) => {
                for f in self.force_back_vec.iter_mut() {
                    let _ = f(&mut conn);
                }
            }
            Err(e) => {
                // for error notification
                let _ = Err::with_source(RedisDataSrcError::FailToGetConnectionFromPool, e);
            }
        };
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
            return Ok(Box::new(RedisDataConn::new(pool.clone())));
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
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;
            let rslt: redis::RedisResult<Option<String>> = conn.get("sample");
            return match rslt {
                Ok(opt) => Ok(opt),
                Err(e) => Err(Err::with_source(SampleError::FailToGetValue, e)),
            };
        }
        fn set_sample_key(&mut self, val: &str) -> Result<(), Err> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;
            return match conn.set("sample", val) {
                Ok(()) => Ok(()),
                Err(e) => Err(Err::with_source(SampleError::FailToSetValue, e)),
            };
        }
        fn del_sample_key(&mut self) -> Result<(), Err> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;
            return match conn.del("sample") {
                Ok(()) => Ok(()),
                Err(e) => Err(Err::with_source(SampleError::FailToDelValue, e)),
            };
        }

        fn set_sample_key_with_force_back(&mut self, val: &str) -> Result<(), Err> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
            let mut conn = data_conn.get_connection()?;

            if let Err(e) = conn.set::<&str, &str, ()>("sample_force_back", val) {
                return Err(Err::with_source(SampleError::FailToSetValue, e));
            }

            data_conn.add_force_back(|conn| {
                let r: redis::RedisResult<()> = conn.del("sample_force_back");
                match r {
                    Ok(()) => Ok(()),
                    Err(e) => Err(Err::with_source("fail to force back", e)),
                }
            });

            if let Err(e) = conn.set::<&str, &str, ()>("sample_force_back_2", val) {
                return Err(Err::with_source(SampleError::FailToSetValue, e));
            }

            data_conn.add_force_back(|conn| {
                let r: redis::RedisResult<()> = conn.del("sample_force_back_2");
                match r {
                    Ok(()) => Ok(()),
                    Err(e) => Err(Err::with_source("fail to force back", e)),
                }
            });

            Ok(())
        }

        fn set_sample_key_in_pre_commit(&mut self, val: &str) -> Result<(), Err> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;

            let val_owned = val.to_string();

            data_conn.add_pre_commit(move |conn| {
                if let Err(e) = conn.set::<&str, &str, ()>("sample_pre_commit", &val_owned) {
                    return Err(Err::with_source(SampleError::FailToSetValue, e));
                }
                Ok(())
            });

            Ok(())
        }

        fn set_sample_key_in_post_commit(&mut self, val: &str) -> Result<(), Err> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;

            let val_owned = val.to_string();

            data_conn.add_post_commit(move |conn| {
                if let Err(e) = conn.set::<&str, &str, ()>("sample_post_commit", &val_owned) {
                    return Err(Err::with_source(SampleError::FailToSetValue, e));
                }
                Ok(())
            });

            Ok(())
        }
    }
    impl RedisSampleDataAcc for DataHub {}

    #[overridable]
    trait SampleData {
        fn get_sample_key(&mut self) -> Result<Option<String>, Err>;
        fn set_sample_key(&mut self, value: &str) -> Result<(), Err>;
        fn del_sample_key(&mut self) -> Result<(), Err>;
        fn set_sample_key_with_force_back(&mut self, val: &str) -> Result<(), Err>;
        fn set_sample_key_in_pre_commit(&mut self, val: &str) -> Result<(), Err>;
        fn set_sample_key_in_post_commit(&mut self, val: &str) -> Result<(), Err>;
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
    fn ok_by_redis_uri() {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/0"));
        if let Err(err) = sabi::run!(sample_logic, data) {
            panic!("{:?}", err);
        }
    }

    #[test]
    fn ok_by_connection_info() {
        let ci = redis::ConnectionInfo {
            addr: redis::ConnectionAddr::Tcp(String::from("127.0.0.1"), 6379),
            redis: redis::RedisConnectionInfo {
                db: 1,
                username: None,
                password: None,
                protocol: redis::ProtocolVersion::RESP3,
            },
        };
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new(ci));
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

    fn sample_logic_in_txn_and_commit(data: &mut impl SampleData) -> Result<(), Err> {
        data.set_sample_key_with_force_back("Good Morning")?;
        Ok(())
    }
    fn sample_logic_in_txn_and_force_back(data: &mut impl SampleData) -> Result<(), Err> {
        data.set_sample_key_with_force_back("Good Afternoon")?;
        Err(Err::new("XXX"))
    }
    fn sample_logic_in_txn_and_pre_commit(data: &mut impl SampleData) -> Result<(), Err> {
        data.set_sample_key_in_pre_commit("Good Evening")?;
        Ok(())
    }
    fn sample_logic_in_txn_and_post_commit(data: &mut impl SampleData) -> Result<(), Err> {
        data.set_sample_key_in_post_commit("Good Night")?;
        Ok(())
    }

    #[test]
    fn test_txn_and_commit() {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/2"));
        if let Err(err) = sabi::txn!(sample_logic_in_txn_and_commit, data) {
            panic!("{:?}", err);
        }

        let client = redis::Client::open("redis://127.0.0.1:6379/2").unwrap();
        let mut conn = client.get_connection().unwrap();
        let s: redis::RedisResult<Option<String>> = conn.get("sample_force_back");
        assert_eq!(s.unwrap().unwrap(), "Good Morning");
        let s: redis::RedisResult<Option<String>> = conn.get("sample_force_back_2");
        assert_eq!(s.unwrap().unwrap(), "Good Morning");

        let _: redis::RedisResult<()> = conn.del("sample_force_back");
    }

    #[test]
    fn tst_txn_and_pre_commit() {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/4"));

        if let Err(err) = sabi::txn!(sample_logic_in_txn_and_pre_commit, data) {
            panic!("{:?}", err);
        }

        let client = redis::Client::open("redis://127.0.0.1:6379/4").unwrap();
        let mut conn = client.get_connection().unwrap();
        let s: redis::RedisResult<Option<String>> = conn.get("sample_pre_commit");
        assert_eq!(s.unwrap().unwrap(), "Good Evening");
    }

    #[test]
    fn tst_txn_and_post_commit() {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/5"));

        if let Err(err) = sabi::txn!(sample_logic_in_txn_and_post_commit, data) {
            panic!("{:?}", err);
        }

        let client = redis::Client::open("redis://127.0.0.1:6379/5").unwrap();
        let mut conn = client.get_connection().unwrap();
        let s: redis::RedisResult<Option<String>> = conn.get("sample_post_commit");
        assert_eq!(s.unwrap().unwrap(), "Good Night");
    }

    #[test]
    fn test_txn_and_force_back() {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/3"));

        if let Err(err) = sabi::txn!(sample_logic_in_txn_and_force_back, data) {
            assert_eq!(err.reason::<&str>().unwrap(), &"XXX");
        } else {
            panic!();
        }

        let client = redis::Client::open("redis://127.0.0.1:6379/3").unwrap();
        let mut conn = client.get_connection().unwrap();
        let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back");
        assert!(r.unwrap().is_none());
        let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back_2");
        assert!(r.unwrap().is_none());
    }
}
