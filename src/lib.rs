// Copyright (C) 2025-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "standalone")]
mod standalone_std;

#[cfg(feature = "standalone")]
#[cfg_attr(docsrs, doc(cfg(feature = "standalone")))]
pub use standalone_std::{
    RedisDataConn, RedisDataSrc, RedisError, RedisPubSubSubscriber, RedisPubSubSubscriberError,
};

#[cfg(feature = "standalone-async")]
mod standalone_tokio;

#[cfg(feature = "standalone-async")]
#[cfg_attr(docsrs, doc(cfg(feature = "standalone-async")))]
pub use standalone_tokio::{
    RedisDataConnAsync, RedisDataSrcAsync, RedisErrorAsync, RedisPubSubSubscriberAsync,
    RedisPubSubSubscriberErrorAsync,
};

#[cfg(feature = "sentinel")]
mod sentinel_std;

#[cfg(feature = "sentinel-async")]
mod sentinel_tokio;

#[cfg(any(feature = "sentinel", feature = "sentinel-async"))]
pub mod sentinel {
    #[cfg(feature = "sentinel")]
    #[cfg_attr(docsrs, doc(cfg(feature = "sentinel")))]
    pub use crate::sentinel_std::{
        RedisDataConn, RedisDataSrc, RedisError, RedisPubSubSubscriber, RedisPubSubSubscriberError,
    };

    #[cfg(feature = "sentinel-async")]
    #[cfg_attr(docsrs, doc(cfg(feature = "sentinel-async")))]
    pub use crate::sentinel_tokio::{
        RedisDataConnAsync, RedisDataSrcAsync, RedisErrorAsync, RedisPubSubSubscriberAsync,
        RedisPubSubSubscriberErrorAsync,
    };
}

#[cfg(feature = "cluster")]
mod cluster_std;

#[cfg(feature = "cluster-async")]
mod cluster_tokio;

#[cfg(any(feature = "cluster", feature = "cluster-async"))]
pub mod cluster {
    #[cfg(feature = "cluster")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cluster")))]
    pub use crate::cluster_std::{
        RedisDataConn, RedisDataSrc, RedisError, RedisPubSubSubscriber, RedisPubSubSubscriberError,
    };

    #[cfg(feature = "cluster-async")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cluster-async")))]
    pub use crate::cluster_tokio::{
        RedisDataConnAsync, RedisDataSrcAsync, RedisErrorAsync, RedisPubSubSubscriberAsync,
        RedisPubSubSubscriberErrorAsync,
    };
}

#[cfg(any(feature = "standalone", feature = "sentinel", feature = "cluster"))]
mod pubsub_msg_std;

#[cfg(any(feature = "standalone", feature = "sentinel", feature = "cluster"))]
pub use pubsub_msg_std::{RedisPubSubMsgDataConn, RedisPubSubMsgDataSrc};

#[cfg(any(
    feature = "standalone-async",
    feature = "sentinel-async",
    feature = "cluster-async"
))]
mod pubsub_msg_tokio;

#[cfg(any(
    feature = "standalone-async",
    feature = "sentinel-async",
    feature = "cluster-async"
))]
pub use pubsub_msg_tokio::{RedisPubSubMsgDataConnAsync, RedisPubSubMsgDataSrcAsync};

#[cfg(any(feature = "standalone", feature = "sentinel", feature = "cluster"))]
mod retry;

#[cfg(any(
    feature = "standalone-async",
    feature = "sentinel-async",
    feature = "cluster-async"
))]
mod retry_async;
