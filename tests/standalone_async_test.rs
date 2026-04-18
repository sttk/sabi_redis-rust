#[cfg(feature = "standalone-async")]
#[cfg(test)]
mod integration_tests {
    use logic_part::*;

    use redis::AsyncTypedCommands;
    use sabi::tokio::{logic, setup_async, uses, DataHub};
    use sabi_redis::RedisDataSrcAsync;

    uses!("redis", RedisDataSrcAsync::new("redis://127.0.0.1:6379/11"));

    mod data_acc_part {
        use override_macro::overridable;
        use redis::AsyncTypedCommands;
        use sabi::tokio::{DataAcc, DataHub};
        use sabi_redis::RedisDataConnAsync;

        #[overridable]
        pub(crate) trait GettingDataAccAsync: DataAcc {
            async fn get_greeting_async(&mut self) -> errs::Result<String> {
                Ok("Hello!".to_string())
            }
        }
        impl GettingDataAccAsync for DataHub {}

        #[overridable]
        pub(crate) trait RedisSayingDataAccAsync: DataAcc {
            async fn say_greeting_async(&mut self, greeting: &str) -> errs::Result<()> {
                let data_conn = self
                    .get_data_conn_async::<RedisDataConnAsync>("redis")
                    .await?;
                let redis_conn = data_conn.get_connection();

                redis_conn
                    .set("greeting", greeting)
                    .await
                    .map_err(|e| errs::Err::with_source("fail to set greeting", e))?;

                data_conn
                    .add_force_back_async(async |mut redis_conn| {
                        redis_conn
                            .del("greeting")
                            .await
                            .map_err(|e| errs::Err::with_source("fail to force back", e))?;
                        Ok(())
                    })
                    .await;

                Ok(())
            }
        }
        impl RedisSayingDataAccAsync for DataHub {}
    }

    mod logic_part {
        use super::data_acc_part::*;
        use override_macro::{overridable, override_with};
        use sabi::tokio::DataHub;

        #[overridable]
        pub(crate) trait MyDataAsync {
            async fn get_greeting_async(&mut self) -> errs::Result<String>;
            async fn say_greeting_async(&mut self, greeting: &str) -> errs::Result<()>;
        }

        // This declaration needs to follow any required traits that carry the #[overridable]
        // attribute.
        #[override_with(GettingDataAccAsync, RedisSayingDataAccAsync)]
        impl MyDataAsync for DataHub {}

        pub(crate) async fn my_logic_ok_async(data: &mut impl MyDataAsync) -> errs::Result<()> {
            let greeting = data.get_greeting_async().await?;
            data.say_greeting_async(&greeting).await
        }

        pub(crate) async fn my_logic_fail_async(data: &mut impl MyDataAsync) -> errs::Result<()> {
            let greeting = data.get_greeting_async().await?;
            data.say_greeting_async(&greeting).await?;
            Err(errs::Err::new("fail"))
        }
    }

    async fn my_app_ok_async() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.txn_async(logic!(my_logic_ok_async)).await
    }

    async fn my_app_fail_async() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.txn_async(logic!(my_logic_fail_async)).await
    }

    #[tokio::test]
    async fn test() -> errs::Result<()> {
        let _auto_shutdown = setup_async().await?;

        // ok
        {
            my_app_ok_async().await?;

            let client = redis::Client::open("redis://127.0.0.1:6379/11").unwrap();
            let mut conn = client.get_multiplexed_async_connection().await.unwrap();
            let s: redis::RedisResult<Option<String>> = conn.get("greeting").await;
            let _: redis::RedisResult<usize> = conn.del("greeting").await;
            assert_eq!(s.unwrap().unwrap(), "Hello!");
        }
        // fail
        {
            let Err(err) = my_app_fail_async().await else {
                panic!();
            };
            assert_eq!(err.reason::<&str>().unwrap(), &"fail");

            let client = redis::Client::open("redis://127.0.0.1:6379/11").unwrap();
            let mut conn = client.get_multiplexed_async_connection().await.unwrap();
            let s: redis::RedisResult<Option<String>> = conn.get("greeting").await;
            let _: redis::RedisResult<usize> = conn.del("greeting").await;
            assert!(s.unwrap().is_none());
        }
        Ok(())
    }
}
