#[cfg(test)]
mod integration_tests_of_sabi_redis {
    use errs;
    use override_macro::{overridable, override_with};
    use redis::TypedCommands;
    use sabi;
    use sabi_redis::{RedisDataConn, RedisDataSrc};

    #[test]
    fn test() -> Result<(), errs::Err> {
        sabi::uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/10"));

        let _auto_shutdown = sabi::setup()?;

        my_app()
    }

    fn my_app() -> Result<(), errs::Err> {
        let mut data = sabi::DataHub::new();
        sabi::txn!(my_logic, data)
    }

    fn my_logic(data: &mut impl MyData) -> Result<(), errs::Err> {
        let greeting = data.get_greeting()?;
        data.say_greeting(&greeting)
    }

    #[overridable]
    trait MyData {
        fn get_greeting(&mut self) -> Result<String, errs::Err>;
        fn say_greeting(&mut self, greeting: &str) -> Result<(), errs::Err>;
    }

    #[overridable]
    trait GettingDataAcc: sabi::DataAcc {
        fn get_greeting(&mut self) -> Result<String, errs::Err> {
            Ok("Hello!".to_string())
        }
    }

    #[overridable]
    trait RedisSayingDataAcc: sabi::DataAcc {
        fn say_greeting(&mut self, greeting: &str) -> Result<(), errs::Err> {
            let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
            let mut redis_conn = data_conn.get_connection()?;

            if let Err(e) = redis_conn.set("greeting", greeting) {
                return Err(errs::Err::with_source("fail to set greeting", e));
            }

            data_conn.add_force_back(|redis_conn| {
                let result = redis_conn.del("greeting");
                if let Err(e) = result {
                    return Err(errs::Err::with_source("fail to force back", e));
                }
                Ok(())
            });

            Ok(())
        }
    }

    impl GettingDataAcc for sabi::DataHub {}
    impl RedisSayingDataAcc for sabi::DataHub {}

    #[override_with(GettingDataAcc, RedisSayingDataAcc)]
    impl MyData for sabi::DataHub {}
}
