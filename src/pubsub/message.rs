// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use redis::Msg;
use sabi::{AsyncGroup, DataConn, DataSrc};
use std::sync::Arc;

pub struct RedisMsgDataConn {
    msg: Arc<Msg>,
}

impl RedisMsgDataConn {
    fn new(msg: Arc<Msg>) -> Self {
        Self { msg }
    }

    pub fn get_message(&self) -> &Msg {
        &self.msg
    }
}

impl DataConn for RedisMsgDataConn {
    fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }
    fn rollback(&mut self, _ag: &mut AsyncGroup) {}
    fn close(&mut self) {}

    fn should_force_back(&self) -> bool {
        true
    }
}

pub struct RedisMsgDataSrc {
    msg: Arc<Msg>,
}

impl RedisMsgDataSrc {
    pub fn new(msg: Msg) -> Self {
        Self { msg: Arc::new(msg) }
    }
}

impl DataSrc<RedisMsgDataConn> for RedisMsgDataSrc {
    fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    fn close(&mut self) {}

    fn create_data_conn(&mut self) -> errs::Result<Box<RedisMsgDataConn>> {
        let msg = Arc::clone(&self.msg);
        Ok(Box::new(RedisMsgDataConn::new(msg)))
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
            let mut ds = RedisMsgDataSrc::new(msg);
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
            let mut ds = RedisMsgDataSrc::new(msg);
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
        let mut ds = RedisMsgDataSrc::new(msg);
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
