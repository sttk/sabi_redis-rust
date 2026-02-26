// Copyright (C) 2025-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use errs::Err;
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
    /// Contains the connection information string and connection pooling configuration string
    /// that caused the failure.
    FailToOpenClient {
        /// The connection information string.
        connection_info: String,

        /// The connection pooling configuration string.
        pool_config: String,
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
/// fn main() -> errs::Result<()> {
///     // Register a `RedisDataSrc` instance to connect to a Redis server with the key "redis".
///     sabi::uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/0"));
///
///     // In this setup process, the registered `RedisDataSrc` instance connects to a Redis server.
///     let _auto_shutdown = sabi::setup()?;
///
///     my_app()
/// }
///
/// fn my_app() -> errs::Result<()> {
///     let mut data = sabi::DataHub::new();
///     data.txn(my_logic)
/// }
///
/// fn my_logic(data: &mut impl MyData) -> errs::Result<()> {
///     let greeting = data.get_greeting()?;
///     data.say_greeting(&greeting)
/// }
///
/// #[overridable]
/// trait MyData {
///     fn get_greeting(&mut self) -> errs::Result<String>;
///     fn say_greeting(&mut self, greeting: &str) -> errs::Result<()>;
/// }
///
/// #[overridable]
/// trait GettingDataAcc: sabi::DataAcc {
///     fn get_greeting(&mut self) -> errs::Result<String> {
///         Ok("Hello!".to_string())
///     }
/// }
///
/// #[overridable]
/// trait RedisSayingDataAcc: sabi::DataAcc {
///     fn say_greeting(&mut self, greeting: &str) -> errs::Result<()> {
///         // Retrieve a `RedisDataConn` instance by the key "redis".
///         let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
///
///         // Get a Redis connection to execute Redis synchronous commands.
///         let mut redis_conn = data_conn.get_connection()?;
///
///         redis_conn.set("greeting", greeting)
///             .map_err(|e| errs::Err::with_source("fail to set greeting", e))?;
///
///         // Register a force back process to revert updates to Redis when an error occurs.
///         data_conn.add_force_back(|redis_conn| {
///             redis_conn.del("greeting")
///                 .map_err(|e| errs::Err::with_source("fail to force back", e))?;
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
#[allow(clippy::type_complexity)]
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
                for f in self.force_back_vec.iter_mut().rev() {
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
///     // In this setup process, the registered `RedisDataSrc` instance connects to a Redis server.
///     let _auto_shutdown = sabi::setup()?;
///
///     my_app()
/// }
///
/// fn my_app() -> Result<(), errs::Err> {
///     let mut data = sabi::DataHub::new();
///     data.txn(my_logic)
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
///         redis_conn.set("greeting", greeting)
///             .map_err(|e| errs::Err::with_source("fail to set greeting", e))?;
///
///         // Register a force back process to revert updates to Redis when an error occurs.
///         data_conn.add_force_back(|redis_conn| {
///             redis_conn.del("greeting")
///                 .map_err(|e| errs::Err::with_source("fail to force back", e))?;
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
pub struct RedisDataSrc<T>
where
    T: redis::IntoConnectionInfo + Sized + Debug,
{
    pool: cell::OnceCell<r2d2::Pool<redis::Client>>,
    conn_info: Option<T>,
    pool_builder: Option<r2d2::Builder<redis::Client>>,
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
            pool_builder: Some(r2d2::Pool::builder()),
        }
    }

    /// Creates a new `RedisDataSrc` instance with connection pooling configurations.
    ///
    /// The connection information is stored, but the connection pool is not built
    /// until the `setup` method is called.
    pub fn with_pool_config(conn_info: T, pool_config: r2d2::Builder<redis::Client>) -> Self {
        Self {
            pool: cell::OnceCell::new(),
            conn_info: Some(conn_info),
            pool_builder: Some(pool_config),
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

        let mut pool_builder_opt = None;
        mem::swap(&mut pool_builder_opt, &mut self.pool_builder);

        if let (Some(conn_info), Some(pool_builder)) = (conn_info_opt, pool_builder_opt) {
            let conn_info_string = format!("{:?}", conn_info);

            match redis::Client::open(conn_info) {
                Ok(client) => match pool_builder.build(client) {
                    Ok(pool) => {
                        if self.pool.set(pool).is_err() {
                            Err(Err::new(RedisDataSrcError::AlreadySetup))
                        } else {
                            Ok(())
                        }
                    }
                    Err(e) => Err(Err::with_source(RedisDataSrcError::FailToBuildPool, e)),
                },
                Err(e) => Err(Err::with_source(
                    RedisDataSrcError::FailToOpenClient {
                        connection_info: conn_info_string,
                        pool_config: format!("{:?}", pool_builder),
                    },
                    e,
                )),
            }
        } else {
            Err(Err::new(RedisDataSrcError::AlreadySetup))
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
            Ok(Box::new(RedisDataConn::new(pool.clone())))
        } else {
            Err(Err::new(RedisDataSrcError::NotSetupYet))
        }
    }
}

#[cfg(test)]
mod test_sync {
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

            let log_opt: Option<String> = conn.get("log").unwrap();
            let log = log_opt.unwrap_or("".to_string());
            let _: Option<()> = conn.set("log", log + "LOG1.").unwrap();

            if let Err(e) = conn.set::<&str, &str, ()>("sample_force_back", val) {
                return Err(Err::with_source(SampleError::FailToSetValue, e));
            }

            data_conn.add_force_back(|conn| {
                let log_opt: Option<String> = conn.get("log").unwrap();
                let log = log_opt.unwrap_or("".to_string());
                let _: Option<()> = conn.set("log", log + "LOG2.").unwrap();

                let r: redis::RedisResult<()> = conn.del("sample_force_back");
                match r {
                    Ok(()) => Ok(()),
                    Err(e) => Err(Err::with_source("fail to force back", e)),
                }
            });

            let log_opt: Option<String> = conn.get("log").unwrap();
            let log = log_opt.unwrap_or("".to_string());
            let _: Option<()> = conn.set("log", log + "LOG3.").unwrap();

            if let Err(e) = conn.set::<&str, &str, ()>("sample_force_back_2", val) {
                return Err(Err::with_source(SampleError::FailToSetValue, e));
            }

            data_conn.add_force_back(|conn| {
                let log_opt: Option<String> = conn.get("log").unwrap();
                let log = log_opt.unwrap_or("".to_string());
                let _: Option<()> = conn.set("log", log + "LOG4.").unwrap();

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
        if let Err(err) = data.run(sample_logic) {
            panic!("{:?}", err);
        }
    }

    #[test]
    fn ok_by_connection_info() {
        let ci: redis::ConnectionInfo = "redis://127.0.0.1:6379/1".parse().unwrap();

        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new(ci));
        if let Err(err) = data.run(sample_logic) {
            panic!("{:?}", err);
        }
    }

    #[test]
    fn ok_by_uri_and_pool_config() {
        let builder = r2d2::Pool::<redis::Client>::builder()
            .max_size(100)
            .min_idle(Some(10))
            .max_lifetime(Some(time::Duration::from_secs(60 * 60)))
            .idle_timeout(Some(time::Duration::from_secs(5 * 60)))
            .connection_timeout(time::Duration::from_secs(30));
        let mut data = DataHub::new();
        data.uses(
            "redis",
            RedisDataSrc::with_pool_config("redis://127.0.0.1:6379/2", builder),
        );
        if let Err(err) = data.run(sample_logic) {
            panic!("{:?}", err);
        }
    }

    #[test]
    fn fail_to_setup() {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("xxxxx"));
        if let Err(err) = data.run(sample_logic) {
            if let Ok(r) = err.reason::<sabi::DataHubError>() {
                match r {
                    sabi::DataHubError::FailToSetupLocalDataSrcs { errors } => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].0.as_ref(), "redis");
                        if let Ok(r) = errors[0].1.reason::<RedisDataSrcError>() {
                            match r {
                                RedisDataSrcError::FailToOpenClient {
                                    connection_info,
                                    pool_config,
                                } => {
                                    assert_eq!(connection_info, "\"xxxxx\"");
                                    assert_eq!(pool_config, "Builder { max_size: 10, min_idle: None, test_on_check_out: true, max_lifetime: Some(1800s), idle_timeout: Some(600s), connection_timeout: 30s, error_handler: LoggingErrorHandler, event_handler: NopEventHandler, connection_customizer: NopConnectionCustomizer }");
                                }
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
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/3"));
        if let Err(err) = data.txn(sample_logic_in_txn_and_commit) {
            panic!("{:?}", err);
        }

        {
            let client = redis::Client::open("redis://127.0.0.1:6379/3").unwrap();
            let mut conn = client.get_connection().unwrap();

            let s: redis::RedisResult<Option<String>> = conn.get("sample_force_back");
            let _: redis::RedisResult<()> = conn.del("sample_force_back");
            assert_eq!(s.unwrap().unwrap(), "Good Morning");

            let s: redis::RedisResult<Option<String>> = conn.get("sample_force_back_2");
            let _: redis::RedisResult<()> = conn.del("sample_force_back_2");
            assert_eq!(s.unwrap().unwrap(), "Good Morning");

            let log: redis::RedisResult<Option<String>> = conn.get("log");
            let _: redis::RedisResult<()> = conn.del("log");
            assert_eq!(log.unwrap().unwrap(), "LOG1.LOG3.");
        }
    }

    #[test]
    fn test_txn_and_pre_commit() {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/4"));

        if let Err(err) = data.txn(sample_logic_in_txn_and_pre_commit) {
            panic!("{:?}", err);
        }

        {
            let client = redis::Client::open("redis://127.0.0.1:6379/4").unwrap();
            let mut conn = client.get_connection().unwrap();
            let s: redis::RedisResult<Option<String>> = conn.get("sample_pre_commit");
            let _: redis::RedisResult<()> = conn.del("sample_pre_commit");
            assert_eq!(s.unwrap().unwrap(), "Good Evening");
        }
    }

    #[test]
    fn test_txn_and_post_commit() {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/5"));

        if let Err(err) = data.txn(sample_logic_in_txn_and_post_commit) {
            panic!("{:?}", err);
        }

        {
            let client = redis::Client::open("redis://127.0.0.1:6379/5").unwrap();
            let mut conn = client.get_connection().unwrap();
            let s: redis::RedisResult<Option<String>> = conn.get("sample_post_commit");
            let _: redis::RedisResult<()> = conn.del("sample_post_commit");
            assert_eq!(s.unwrap().unwrap(), "Good Night");
        }
    }

    #[test]
    fn test_txn_and_force_back() {
        let mut data = DataHub::new();
        data.uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/6"));

        if let Err(err) = data.txn(sample_logic_in_txn_and_force_back) {
            assert_eq!(err.reason::<&str>().unwrap(), &"XXX");
        } else {
            panic!();
        }

        {
            let client = redis::Client::open("redis://127.0.0.1:6379/6").unwrap();
            let mut conn = client.get_connection().unwrap();

            let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back");
            let _: redis::RedisResult<()> = conn.del("sample_force_back");
            assert!(r.unwrap().is_none());

            let r: redis::RedisResult<Option<String>> = conn.get("sample_force_back_2");
            let _: redis::RedisResult<()> = conn.del("sample_force_back_2");
            assert!(r.unwrap().is_none());

            let log: redis::RedisResult<Option<String>> = conn.get("log");
            let _: redis::RedisResult<()> = conn.del("log");
            assert_eq!(log.unwrap().unwrap(), "LOG1.LOG3.LOG4.LOG2.");
        }
    }
}
