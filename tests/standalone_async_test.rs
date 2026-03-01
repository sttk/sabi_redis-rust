#[cfg(feature = "standalone-async")]
#[cfg(test)]
mod integration_tests_of_standalone_async {
    use override_macro::{overridable, override_with};
    use redis::AsyncCommands;
    use sabi::tokio::{logic, setup_async, uses, DataAcc, DataHub};
    use sabi_redis::{RedisAsyncDataConn, RedisAsyncDataSrc};

    #[overridable]
    trait MyDataAsync {
        async fn get_greeting_async(&mut self) -> errs::Result<String>;
        async fn say_greeting_async(&mut self, greeting: &str) -> errs::Result<()>;
    }

    #[overridable]
    trait GettingAsyncDataAcc: DataAcc {
        async fn get_greeting_async(&mut self) -> errs::Result<String> {
            Ok("Hello".to_string())
        }
    }

    #[overridable]
    trait RedisAsyncSayingDataAcc: DataAcc {
        async fn say_greeting_async(&mut self, greeting: &str) -> errs::Result<()> {
            let data_conn = self
                .get_data_conn_async::<RedisAsyncDataConn>("redis")
                .await?;
            let mut redis_conn = data_conn.get_connection_async().await?;

            redis_conn
                .set::<_, _, ()>("greeting", greeting)
                .await
                .map_err(|e| errs::Err::with_source("fail to set greeting", e))?;

            data_conn
                .add_force_back_async(async |mut redis_conn| {
                    redis_conn
                        .del::<_, ()>("greeting")
                        .await
                        .map_err(|e| errs::Err::with_source("fail to force back", e))?;
                    Ok(())
                })
                .await;

            Ok(())
        }
    }

    impl GettingAsyncDataAcc for DataHub {}
    impl RedisAsyncSayingDataAcc for DataHub {}

    #[override_with(GettingAsyncDataAcc, RedisAsyncSayingDataAcc)]
    impl MyDataAsync for DataHub {}

    async fn my_logic_async(data: &mut impl MyDataAsync) -> errs::Result<()> {
        let greeting = data.get_greeting_async().await?;
        data.say_greeting_async(&greeting).await
    }

    async fn my_app_async() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.txn_async(logic!(my_logic_async)).await
    }

    #[tokio::test]
    async fn test() -> errs::Result<()> {
        uses("redis", RedisAsyncDataSrc::new("redis://127.0.0.1:6379/11"))?;
        let _auto_shutdown = setup_async().await?;
        my_app_async().await
    }
}
