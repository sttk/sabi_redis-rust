// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

#[cfg(feature = "sentinel-sync")]
mod sentinel_sync;

#[cfg(feature = "sentinel-sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sentinel-sync")))]
pub use sentinel_sync::{RedisSentinelDataConn, RedisSentinelDataSrc, RedisSentinelSyncError};

#[cfg(feature = "sentinel-async")]
mod sentinel_async;

#[cfg(feature = "sentinel-async")]
#[cfg_attr(docsrs, doc(cfg(feature = "sentinel-async")))]
pub use sentinel_async::{
    RedisSentinelAsyncDataConn, RedisSentinelAsyncDataSrc, RedisSentinelAsyncError,
};
