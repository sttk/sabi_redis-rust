#[cfg(feature = "standalone")]
#[cfg(test)]
mod integration_tests {

    mod logic_part {
        use override_macro::overridable;

        #[overridable]
        pub(crate) trait SendLogicData {
            fn send_greeting_message(&mut self, s: &str) -> errs::Result<()>;
        }

        pub(crate) fn send_logic(data: &mut impl SendLogicData) -> errs::Result<()> {
            data.send_greeting_message("Good morning!!")?;
            Ok(())
        }

        #[overridable]
        pub(crate) trait ReceiveLogicData {
            fn receive_message(&mut self) -> errs::Result<String>;
        }

        pub(crate) fn receive_logic(data: &mut impl ReceiveLogicData) -> errs::Result<()> {
            let s = data.receive_message()?;
            assert_eq!(s, "Good morning!!");
            Ok(())
        }
    }

    mod data_acc_part {
        use override_macro::overridable;
        use redis::{Msg, TypedCommands};
        use sabi::DataAcc;
        use sabi_redis::{pubsub, RedisDataConn};

        #[derive(Debug)]
        pub(crate) enum PubSubError {
            FailToPublishMessage,
            FailToGetMessage,
        }

        #[overridable]
        pub(crate) trait RedisDataAcc: DataAcc {
            fn send_greeting_message(&mut self, s: &str) -> errs::Result<()> {
                let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
                let redis_conn = data_conn.get_connection();
                redis_conn.publish("channel-1", s).map_err(|e| {
                    errs::Err::with_source(
                        PubSubError::FailToPublishMessage,
                        e,
                    )
                })?;
                Ok(())
            }
        }

        #[overridable]
        pub(crate) trait RedisPubSubDataAcc: DataAcc {
            fn receive_message(&mut self) -> errs::Result<String> {
                let data_conn = self.get_data_conn::<pubsub::RedisMsgDataConn>("redis/pubsub")?;
                let msg = data_conn.get_message();
                msg.get_payload::<String>().map_err(|e| {
                    errs::Err::with_source(PubSubError::FailToGetMessage, e)
                })
            }
        }
    }

    mod data_hub_part {
        use super::data_acc_part::*;
        use super::logic_part::*;

        use override_macro::override_with;
        use sabi::DataHub;

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
        use sabi::DataHub;
        use sabi_redis::pubsub::{RedisMsgDataSrc, RedisSubscriber};
        use sabi_redis::RedisDataSrc;

        #[test]
        fn ok() {
            let _ = std::thread::spawn(move || {
                let mut hub = DataHub::new();
                hub.uses("redis", RedisDataSrc::new("redis://127.0.0.1/0"));
                hub.txn(send_logic)
            });

            let mut subscriber = RedisSubscriber::new("redis://127.0.0.1/0");
            subscriber.subscribe("channel-1");
            subscriber
                .receive(|msg| {
                    let mut hub = DataHub::new();
                    hub.uses("redis/pubsub", RedisMsgDataSrc::new(msg));
                    if let Err(err) = hub.txn(receive_logic) {
                        panic!("{err:?}");
                    }
                    ControlFlow::Break(1)
                })
                .unwrap();
        }
    }
}
