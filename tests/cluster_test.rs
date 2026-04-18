#[cfg(feature = "cluster")]
#[cfg(test)]
mod integration_tests {
    use logic_part::*;

    use redis::{cluster::ClusterClient, TypedCommands};
    use sabi::{setup, uses, DataHub};
    use sabi_redis::RedisClusterDataSrc;

    uses!(
        "redis",
        RedisClusterDataSrc::new(&[
            "redis://127.0.0.1:7000",
            "redis://127.0.0.1:7001",
            "redis://127.0.0.1:7002",
        ])
    );

    mod logic_part {
        use override_macro::overridable;

        #[overridable]
        pub(crate) trait MyData {
            fn get_greeting(&mut self) -> errs::Result<String>;
            fn say_greeting(&mut self, greeting: &str) -> errs::Result<()>;
        }

        pub(crate) fn my_logic_ok(data: &mut impl MyData) -> errs::Result<()> {
            let greeting = data.get_greeting()?;
            data.say_greeting(&greeting)
        }

        pub(crate) fn my_logic_fail(data: &mut impl MyData) -> errs::Result<()> {
            let greeting = data.get_greeting()?;
            data.say_greeting(&greeting)?;
            Err(errs::Err::new("fail"))
        }
    }

    mod data_acc_part {
        use override_macro::overridable;

        use redis::TypedCommands;
        use sabi::DataAcc;
        use sabi_redis::RedisClusterDataConn;

        #[overridable]
        pub(crate) trait GettingDataAcc: DataAcc {
            fn get_greeting(&mut self) -> errs::Result<String> {
                Ok("Hello!".to_string())
            }
        }

        #[overridable]
        pub(crate) trait RedisSayingDataAcc: DataAcc {
            fn say_greeting(&mut self, greeting: &str) -> errs::Result<()> {
                let data_conn = self.get_data_conn::<RedisClusterDataConn>("redis")?;
                let redis_conn = data_conn.get_connection();

                redis_conn
                    .set("greeting", greeting)
                    .map_err(|e| errs::Err::with_source("fail to set greeting", e))?;

                data_conn.add_force_back(|redis_conn| {
                    redis_conn
                        .del("greeting")
                        .map_err(|e| errs::Err::with_source("fail to force back", e))?;
                    Ok(())
                });

                Ok(())
            }
        }
    }

    mod data_hub_part {
        use override_macro::override_with;
        use sabi::DataHub;

        use super::data_acc_part::*;
        use super::logic_part::*;

        impl GettingDataAcc for DataHub {}
        impl RedisSayingDataAcc for DataHub {}

        // This declaration needs to follow any required traits that carry the #[overridable]
        // attribute.
        #[override_with(GettingDataAcc, RedisSayingDataAcc)]
        impl MyData for DataHub {}
    }

    fn my_app_ok() -> errs::Result<()> {
        let mut hub = DataHub::new();
        hub.txn(my_logic_ok)
    }

    fn my_app_fail() -> errs::Result<()> {
        let mut hub = DataHub::new();
        hub.txn(my_logic_fail)
    }

    #[test]
    fn test() -> errs::Result<()> {
        let _auto_shutdown = setup()?;

        // ok
        {
            my_app_ok()?;

            let client = ClusterClient::new(vec![
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ])
            .unwrap();
            let mut conn = client.get_connection().unwrap();
            let s: redis::RedisResult<Option<String>> = conn.get("greeting");
            let _: redis::RedisResult<usize> = conn.del("greeting");
            assert_eq!(s.unwrap().unwrap(), "Hello!");
        }
        // fail
        {
            let Err(err) = my_app_fail() else {
                panic!();
            };
            assert_eq!(err.reason::<&str>().unwrap(), &"fail");

            let client = ClusterClient::new(vec![
                "redis://127.0.0.1:7000",
                "redis://127.0.0.1:7001",
                "redis://127.0.0.1:7002",
            ])
            .unwrap();
            let mut conn = client.get_connection().unwrap();
            let s: redis::RedisResult<Option<String>> = conn.get("greeting");
            let _: redis::RedisResult<usize> = conn.del("greeting");
            assert!(s.unwrap().is_none());
        }
        Ok(())
    }
}
