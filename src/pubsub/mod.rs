// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

#[cfg(any(feature = "standalone", feature = "sentinel", feature = "cluster"))]
mod message;

#[cfg(any(feature = "standalone", feature = "sentinel", feature = "cluster"))]
pub use message::{RedisMsgDataConn, RedisMsgDataSrc};

#[cfg(any(
    feature = "standalone-async",
    feature = "sentinel-async",
    feature = "cluster-async"
))]
mod message_async;

#[cfg(any(
    feature = "standalone-async",
    feature = "sentinel-async",
    feature = "cluster-async"
))]
pub use message_async::{RedisMsgDataConnAsync, RedisMsgDataSrcAsync};

#[cfg(feature = "standalone")]
mod standalone;

#[cfg(feature = "standalone")]
pub use standalone::{RedisSubscriber, RedisSubscriberError};

#[cfg(feature = "standalone-async")]
mod standalone_async;

#[cfg(feature = "standalone-async")]
pub use standalone_async::{RedisSubscriberAsync, RedisSubscriberErrorAsync};

#[cfg(feature = "sentinel")]
mod sentinel;

#[cfg(feature = "sentinel")]
pub use sentinel::{RedisSentinelSubscriber, RedisSentinelSubscriberError};

#[cfg(feature = "sentinel-async")]
mod sentinel_async;

#[cfg(feature = "sentinel-async")]
pub use sentinel_async::{RedisSentinelSubscriberAsync, RedisSentinelSubscriberErrorAsync};

#[cfg(feature = "cluster")]
mod cluster;

#[cfg(feature = "cluster")]
pub use cluster::{RedisClusterSubscriber, RedisClusterSubscriberError};

#[cfg(feature = "cluster-async")]
mod cluster_async;

#[cfg(feature = "cluster-async")]
pub use cluster_async::{RedisClusterSubscriberAsync, RedisClusterSubscriberErrorAsync};
