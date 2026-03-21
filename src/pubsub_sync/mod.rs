// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use redis::Msg;
use sabi::{AsyncGroup, DataConn, DataSrc};
use std::sync::Arc;

/// A data connection that carries a Redis Pub/Sub message.
///
/// This structure allows a received Pub/Sub message to be passed through the `sabi` data access
/// layer. It implements `DataConn`, enabling it to be retrieved by a data access object.
pub struct RedisPubSubDataConn {
    msg: Arc<Msg>,
}

impl RedisPubSubDataConn {
    fn new(msg: Arc<Msg>) -> Self {
        Self { msg }
    }

    /// Returns a reference to the contained Redis Pub/Sub message.
    pub fn get_message(&self) -> &Msg {
        &self.msg
    }
}

impl DataConn for RedisPubSubDataConn {
    fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }
    fn rollback(&mut self, _ag: &mut AsyncGroup) {}
    fn close(&mut self) {}
}

/// A data source that wraps a Redis Pub/Sub message.
///
/// This structure is used to register a single received message into the `sabi` framework
/// so that it can be accessed via `RedisPubSubDataConn` during a data operation.
///
/// # Examples
///
/// ```rust,ignore
/// use sabi_redis::pubsub::{RedisPubSubDataSrc, RedisPubSubDataConn};
/// use sabi::DataHub;
///
/// // Assume `msg` is a `redis::Msg` received from a subscriber.
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let mut con = client.get_connection().unwrap();
/// # let mut pubsub = con.as_pubsub();
/// # pubsub.subscribe(&["channel"]).unwrap();
/// # let msg = pubsub.get_message().unwrap();
///
/// let mut data = DataHub::new();
/// data.uses("redis/pubsub", RedisPubSubDataSrc::new(msg));
///
/// data.run(|acc: &mut DataHub| {
///     let conn = acc.get_data_conn::<RedisPubSubDataConn>("redis/pubsub")?;
///     let message = conn.get_message();
///     // Process the message...
///     Ok(())
/// }).unwrap();
/// ```
pub struct RedisPubSubDataSrc {
    msg: Arc<Msg>,
}

impl RedisPubSubDataSrc {
    /// Creates a new `RedisPubSubDataSrc` with the given Redis message.
    pub fn new(msg: Msg) -> Self {
        Self { msg: Arc::new(msg) }
    }
}

impl DataSrc<RedisPubSubDataConn> for RedisPubSubDataSrc {
    fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    fn close(&mut self) {}

    fn create_data_conn(&mut self) -> errs::Result<Box<RedisPubSubDataConn>> {
        let msg = Arc::clone(&self.msg);
        Ok(Box::new(RedisPubSubDataConn::new(msg)))
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use override_macro::{overridable, override_with};
    use r2d2::Pool;
    use redis::TypedCommands;
    use std::{thread, time};

    fn sample_logic(data: &mut impl SampleData1) -> errs::Result<()> {
        match data.greet()?.as_ref() {
            "Hello" => Ok(()),
            s => panic!("{}", s),
        }
    }

    #[overridable]
    trait SampleDataAcc1: sabi::DataAcc {
        fn greet(&mut self) -> errs::Result<String> {
            let data_conn = self.get_data_conn::<RedisPubSubDataConn>("redis/pubsub")?;
            let msg = data_conn.get_message();
            let payload: String = msg.get_payload().unwrap();
            Ok(payload)
        }
    }
    impl SampleDataAcc1 for sabi::DataHub {}

    #[overridable]
    trait SampleData1 {
        fn greet(&mut self) -> errs::Result<String>;
    }
    #[override_with(SampleDataAcc1)]
    impl SampleData1 for sabi::DataHub {}

    #[test]
    fn test() {
        // client/publish
        let handle = {
            let client = redis::Client::open("redis://127.0.0.1/").unwrap();
            let pool = Pool::builder().build(client).unwrap();

            let handle = thread::spawn(move || {
                let mut conn = pool.get().unwrap();
                thread::sleep(time::Duration::from_millis(100));
                conn.publish("channel_1", "Hello".to_string()).unwrap();
            });

            handle
        };

        // server/subscribe
        {
            let client = redis::Client::open("redis://127.0.0.1/").unwrap();
            let mut con = client.get_connection().unwrap();
            let mut pubsub = con.as_pubsub();
            pubsub.subscribe(&["channel_1", "channel_2"]).unwrap();

            loop {
                let msg = pubsub.get_message().unwrap();

                let mut data = sabi::DataHub::new();
                data.uses("redis/pubsub", RedisPubSubDataSrc::new(msg));
                if data.run(sample_logic).is_ok() {
                    break;
                }
            }
        }

        handle.join().unwrap();
    }
}
