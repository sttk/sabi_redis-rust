#[cfg(feature = "standalone-async")]
#[cfg(test)]
mod integration_tests {

    mod logic_part {
        use override_macro::overridable;

        #[overridable]
        pub(crate) trait SendLogicData {
            async fn send_greeting_message_async(&mut self, s: &str) -> errs::Result<()>;
        }

        pub(crate) async fn send_logic_async(data: &mut impl SendLogicData) -> errs::Result<()> {
            data.send_greeting_message_async("Good morning!!").await?;
            Ok(())
        }

        #[overridable]
        pub(crate) trait ReceiveLogicData {
            async fn receive_message_async(&mut self) -> errs::Result<String>;
        }

        pub(crate) async fn receive_logic_async(
            data: &mut impl ReceiveLogicData,
        ) -> errs::Result<()> {
            let s = data.receive_message_async().await?;
            assert_eq!(s, "Good morning!!");
            Ok(())
        }
    }

    mod data_acc_part {
        use override_macro::overridable;
        use redis::AsyncTypedCommands;
        use sabi::tokio::DataAcc;
        use sabi_redis::{RedisDataConnAsync, RedisPubSubMsgDataConnAsync};

        #[derive(Debug)]
        pub(crate) enum PubSubError {
            FailToPublishMessage,
            FailToGetMessage,
        }

        #[overridable]
        pub(crate) trait RedisDataAcc: DataAcc {
            async fn send_greeting_message_async(&mut self, s: &str) -> errs::Result<()> {
                let data_conn = self
                    .get_data_conn_async::<RedisDataConnAsync>("redis")
                    .await?;
                let redis_conn = data_conn.get_connection();
                redis_conn
                    .publish("channel-1", s)
                    .await
                    .map_err(|e| errs::Err::with_source(PubSubError::FailToPublishMessage, e))?;
                Ok(())
            }
        }

        #[overridable]
        pub(crate) trait RedisPubSubDataAcc: DataAcc {
            async fn receive_message_async(&mut self) -> errs::Result<String> {
                let data_conn = self
                    .get_data_conn_async::<RedisPubSubMsgDataConnAsync>("redis/pubsub")
                    .await?;
                let msg = data_conn.get_message();
                msg.get_payload::<String>()
                    .map_err(|e| errs::Err::with_source(PubSubError::FailToGetMessage, e))
            }
        }
    }

    mod data_hub_part {
        use super::data_acc_part::*;
        use super::logic_part::*;

        use override_macro::override_with;
        use sabi::tokio::DataHub;

        impl RedisDataAcc for DataHub {}
        impl RedisPubSubDataAcc for DataHub {}

        #[override_with(RedisDataAcc)]
        impl SendLogicData for DataHub {}

        #[override_with(RedisPubSubDataAcc)]
        impl ReceiveLogicData for DataHub {}
    }

    mod controller_part {
        use super::logic_part::*;

        use redis::ControlFlow;
        use sabi::tokio::{logic, DataHub};
        use sabi_redis::{
            RedisDataSrcAsync, RedisPubSubMsgDataSrcAsync, RedisPubSubSubscriberAsync,
        };

        #[tokio::test]
        async fn ok() {
            // publish
            {
                let _ = tokio::spawn(async {
                    let mut hub = DataHub::new();
                    hub.uses("redis", RedisDataSrcAsync::new("redis://127.0.0.1/0"));
                    hub.txn_async(logic!(send_logic_async)).await.unwrap();
                });
            }

            let mut subscriber = RedisPubSubSubscriberAsync::new("redis://127.0.0.1/0");
            subscriber.subscribe("channel-1");
            subscriber
                .receive_async(async |msg| {
                    let mut hub = DataHub::new();
                    hub.uses("redis/pubsub", RedisPubSubMsgDataSrcAsync::new(msg));
                    if let Err(err) = hub.txn_async(logic!(receive_logic_async)).await {
                        panic!("{err:?}");
                    }
                    ControlFlow::Break(1)
                })
                .await
                .unwrap();
        }
    }
}
