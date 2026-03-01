#[cfg(feature = "standalone-sync")]
#[cfg(test)]
mod integration_tests_of_standalone_sync {
    use override_macro::{overridable, override_with};
    use redis::TypedCommands;
    use sabi::{uses, DataAcc, DataHub};
    use sabi_redis::{RedisDataConn, RedisDataSrc};

    #[overridable]
    trait MyData {
        fn get_greeting(&mut self) -> errs::Result<String>;
        fn say_greeting(&mut self, greeting: &str) -> errs::Result<()>;
    }

    #[overridable]
    trait GettingDataAcc: DataAcc {
        fn get_greeting(&mut self) -> errs::Result<String> {
            Ok("Hello!".to_string())
        }
    }

    #[overridable]
    trait RedisSayingDataAcc: DataAcc {
        fn say_greeting(&mut self, greeting: &str) -> errs::Result<()> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
            let mut redis_conn = data_conn.get_connection()?;

            if let Err(e) = redis_conn.set("greeting", greeting) {
                return Err(errs::Err::with_source("fail to set greeting", e));
            }

            data_conn.add_force_back(|redis_conn| {
                if let Err(e) = redis_conn.del("greeting") {
                    return Err(errs::Err::with_source("fail to force back", e));
                }
                Ok(())
            });

            Ok(())
        }
    }

    impl GettingDataAcc for DataHub {}
    impl RedisSayingDataAcc for DataHub {}

    #[override_with(GettingDataAcc, RedisSayingDataAcc)]
    impl MyData for DataHub {}

    fn my_logic(data: &mut impl MyData) -> errs::Result<()> {
        let greeting = data.get_greeting()?;
        data.say_greeting(&greeting)
    }

    fn my_app() -> errs::Result<()> {
        let mut data = DataHub::new();
        data.txn(my_logic)
    }

    #[test]
    fn test() -> errs::Result<()> {
        uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/10"))?;

        let _auto_shutdown = sabi::setup()?;

        my_app()
    }
}
