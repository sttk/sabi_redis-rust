// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use redis::Msg;
use sabi::{AsyncGroup, DataConn, DataSrc};
use std::sync::Arc;

/// A struct that holds a Redis Pub/Sub message as a data connection.
pub struct RedisPubSubMsgDataConn {
    msg: Arc<Msg>,
}

impl RedisPubSubMsgDataConn {
    fn new(msg: Arc<Msg>) -> Self {
        Self { msg }
    }

    /// Returns a reference to the underlying Redis Pub/Sub message.
    ///
    /// # Returns
    ///
    /// A reference to the `redis::Msg`.
    pub fn get_message(&self) -> &Msg {
        &self.msg
    }
}

impl DataConn for RedisPubSubMsgDataConn {
    fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }
    fn rollback(&mut self, _ag: &mut AsyncGroup) {}
    fn close(&mut self) {}

    fn should_force_back(&self) -> bool {
        true
    }
}

/// A struct that holds a Redis Pub/Sub message as a data source.
pub struct RedisPubSubMsgDataSrc {
    msg: Arc<Msg>,
}

impl RedisPubSubMsgDataSrc {
    /// Creates a new `RedisPubSubMsgDataSrc` with a Redis Pub/Sub message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A `redis::Msg` received from a Pub/Sub subscriber.
    ///
    /// # Returns
    ///
    /// A new instance of `RedisPubSubMsgDataSrc`.
    pub fn new(msg: Msg) -> Self {
        Self { msg: Arc::new(msg) }
    }
}

impl DataSrc<RedisPubSubMsgDataConn> for RedisPubSubMsgDataSrc {
    fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    fn close(&mut self) {}

    fn create_data_conn(&mut self) -> errs::Result<Box<RedisPubSubMsgDataConn>> {
        let msg = Arc::clone(&self.msg);
        Ok(Box::new(RedisPubSubMsgDataConn::new(msg)))
    }
}

#[cfg(test)]
mod unit_tests_of_data_src {
    use super::*;
    use redis::{PushInfo, PushKind, Value};

    mod test_new {
        use super::*;

        #[test]
        fn ok() {
            let pi = PushInfo {
                kind: PushKind::Message,
                data: vec![Value::Int(123i64), Value::SimpleString("hello".to_string())],
            };
            let msg = Msg::from_push_info(pi).unwrap();
            let mut ds = RedisPubSubMsgDataSrc::new(msg);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());
            ds.close();
        }
    }

    mod test_create_data_conn {
        use super::*;

        #[test]
        fn ok() {
            let pi = PushInfo {
                kind: PushKind::Message,
                data: vec![Value::Int(123i64), Value::SimpleString("hello".to_string())],
            };
            let msg = Msg::from_push_info(pi).unwrap();
            let mut ds = RedisPubSubMsgDataSrc::new(msg);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup(&mut ag) {
                panic!("{err:?}");
            }
            let errors = ag.join();
            assert!(errors.is_empty());

            let Ok(data_conn) = ds.create_data_conn() else {
                panic!("fail to create data_conn");
            };
            let msg = data_conn.get_message();
            assert_eq!(msg.get_channel::<i64>().unwrap(), 123i64);
            assert_eq!(msg.get_payload::<String>().unwrap(), "hello");

            ds.close();
        }
    }
}

#[cfg(test)]
mod unit_tests_of_data_conn {
    use super::*;
    use redis::{PushInfo, PushKind, Value};

    #[test]
    fn test() {
        let pi = PushInfo {
            kind: PushKind::Message,
            data: vec![Value::Int(123i64), Value::SimpleString("hello".to_string())],
        };
        let msg = Msg::from_push_info(pi).unwrap();
        let mut ds = RedisPubSubMsgDataSrc::new(msg);
        let mut ag = AsyncGroup::new();
        if let Err(err) = ds.setup(&mut ag) {
            panic!("{err:?}");
        }
        let errors = ag.join();
        assert!(errors.is_empty());

        let Ok(mut data_conn) = ds.create_data_conn() else {
            panic!("fail to create data_conn");
        };
        let msg = data_conn.get_message();
        assert_eq!(msg.get_channel::<i64>().unwrap(), 123i64);
        assert_eq!(msg.get_payload::<String>().unwrap(), "hello");

        let mut ag = AsyncGroup::new();
        data_conn.pre_commit(&mut ag).unwrap();

        let mut ag = AsyncGroup::new();
        data_conn.commit(&mut ag).unwrap();

        let mut ag = AsyncGroup::new();
        data_conn.post_commit(&mut ag);

        let mut ag = AsyncGroup::new();
        data_conn.rollback(&mut ag);

        let mut ag = AsyncGroup::new();
        data_conn.force_back(&mut ag);

        assert!(data_conn.should_force_back());

        ds.close();
    }
}
