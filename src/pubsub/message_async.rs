// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use redis::Msg;
use sabi::tokio::{AsyncGroup, DataConn, DataSrc};
use std::sync::Arc;

pub struct RedisMsgDataConnAsync {
    msg: Arc<Msg>,
}

impl RedisMsgDataConnAsync {
    fn new(msg: Arc<Msg>) -> Self {
        Self { msg }
    }

    pub fn get_message(&self) -> &Msg {
        &self.msg
    }
}

impl DataConn for RedisMsgDataConnAsync {
    async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    async fn rollback_async(&mut self, _ag: &mut AsyncGroup) {}
    fn close(&mut self) {}

    fn should_force_back(&self) -> bool {
        true
    }
}

pub struct RedisMsgDataSrcAsync {
    msg: Arc<Msg>,
}

impl RedisMsgDataSrcAsync {
    pub fn new(msg: Msg) -> Self {
        Self { msg: Arc::new(msg) }
    }
}

impl DataSrc<RedisMsgDataConnAsync> for RedisMsgDataSrcAsync {
    async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    fn close(&mut self) {}

    async fn create_data_conn_async(&mut self) -> errs::Result<Box<RedisMsgDataConnAsync>> {
        let msg = Arc::clone(&self.msg);
        Ok(Box::new(RedisMsgDataConnAsync::new(msg)))
    }
}

#[cfg(test)]
mod unit_tests_of_data_src {
    use super::*;
    use redis::{PushInfo, PushKind, Value};

    mod test_new {
        use super::*;

        #[tokio::test]
        async fn ok() {
            let pi = PushInfo {
                kind: PushKind::Message,
                data: vec![Value::Int(123i64), Value::SimpleString("hello".to_string())],
            };
            let msg = Msg::from_push_info(pi).unwrap();
            let mut ds = RedisMsgDataSrcAsync::new(msg);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            let Ok(data_conn) = ds.create_data_conn_async().await else {
                panic!("fail to create data_conn");
            };
            let msg = data_conn.get_message();
            assert_eq!(msg.get_channel::<i64>().unwrap(), 123i64);
            assert_eq!(msg.get_payload::<String>().unwrap(), "hello");

            ds.close();
        }
    }

    mod test_create_data_conn {
        use super::*;

        #[tokio::test]
        async fn ok() {
            let pi = PushInfo {
                kind: PushKind::Message,
                data: vec![Value::Int(123i64), Value::SimpleString("hello".to_string())],
            };
            let msg = Msg::from_push_info(pi).unwrap();
            let mut ds = RedisMsgDataSrcAsync::new(msg);
            let mut ag = AsyncGroup::new();
            if let Err(err) = ds.setup_async(&mut ag).await {
                panic!("{err:?}");
            }
            let errors = ag.join_async().await;
            assert!(errors.is_empty());

            let Ok(data_conn) = ds.create_data_conn_async().await else {
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

    #[tokio::test]
    async fn test() {
        let pi = PushInfo {
            kind: PushKind::Message,
            data: vec![Value::Int(123i64), Value::SimpleString("hello".to_string())],
        };
        let msg = Msg::from_push_info(pi).unwrap();
        let mut ds = RedisMsgDataSrcAsync::new(msg);
        let mut ag = AsyncGroup::new();
        if let Err(err) = ds.setup_async(&mut ag).await {
            panic!("{err:?}");
        }
        let errors = ag.join_async().await;
        assert!(errors.is_empty());

        let Ok(mut data_conn) = ds.create_data_conn_async().await else {
            panic!("fail to create data_conn");
        };
        let msg = data_conn.get_message();
        assert_eq!(msg.get_channel::<i64>().unwrap(), 123i64);
        assert_eq!(msg.get_payload::<String>().unwrap(), "hello");

        let mut ag = AsyncGroup::new();
        data_conn.pre_commit_async(&mut ag).await.unwrap();

        let mut ag = AsyncGroup::new();
        data_conn.commit_async(&mut ag).await.unwrap();

        let mut ag = AsyncGroup::new();
        data_conn.post_commit_async(&mut ag).await;

        let mut ag = AsyncGroup::new();
        data_conn.rollback_async(&mut ag).await;

        let mut ag = AsyncGroup::new();
        data_conn.force_back_async(&mut ag).await;

        assert!(data_conn.should_force_back());

        ds.close();
    }
}
