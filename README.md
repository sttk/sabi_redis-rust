# [sabi_redis for Rust][repo-url] [![crates.io][cratesio-img]][cratesio-url] [![doc.rs][docrs-img]][docrs-url] [![CI Status][ci-img]][ci-url] [![MIT License][mit-img]][mit-url]

**sabi-redis** is a Redis data source implementation for [sabi](https://crates.io/crates/sabi), a lightweight data access framework for Rust. It enables seamless integration of Redis access into `sabi`'s transaction management system.

## Key Features

- **Multiple Configurations**: Fully supports Standalone, Sentinel, and Cluster configurations.
- **Sync & Async Support**: Provides both synchronous and asynchronous (Tokio-based) APIs.
- **Pub/Sub Integration**: Includes specialized structures to receive Redis Pub/Sub messages and process them as `sabi` data sources.
- **Efficient Connection Pooling**: Manages connections using established pooling libraries (`r2d2` for synchronous and `deadpool-redis` for asynchronous).
- **Transaction Management**: Leverages `sabi`'s transaction lifecycle to handle commits and rollbacks (force-back) logic consistently.

## Installation

In Cargo.toml, write this crate as a dependency:

```toml
[dependencies]
sabi_redis = "0.7.0" # `standalone` feature is enabled by default.

# If you want to use the `standalone-async` feature:
# sabi_redis = { version = "0.7.0", default-features = false, features = ["standalone-async"] }

# If you want to use the `sentinel` feature:
# sabi_redis = { version = "0.7.0", default-features = false, features = ["sentinel"] }

# If you want to use the `sentinel-async` feature:
# sabi_redis = { version = "0.7.0", default-features = false, features = ["sentinel-async"] }

# If you want to use the `cluster` feature:
# sabi_redis = { version = "0.7.0", default-features = false, features = ["cluster"] }

# If you want to use the `cluster-async` feature:
# sabi_redis = { version = "0.7.0", default-features = false, features = ["cluster-async"] }
```

## Usage

### For Standalone Server And Synchronous Commands
> The `standalone` feature is required for this functionality, and it is enabled by default.

```rust
use sabi::{setup, uses, DataHub, DataAcc};
use sabi_redis::{RedisDataSrc, RedisDataConn};
use redis::TypedCommands;
use override_macro::{overridable, override_with};

// Register the data source in the global scope
uses!("redis", RedisDataSrc::new("redis://127.0.0.1:6379/0"));

// Define a trait for logic
#[overridable]
trait MyData {
    fn say_greeting(&mut self, greeting: &str) -> errs::Result<()>;
}

// Define a trait for data access
#[overridable]
trait RedisDataAcc: DataAcc {
    fn say_greeting(&mut self, greeting: &str) -> errs::Result<()> {
        let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
        let redis_conn = data_conn.get_connection();
        redis_conn.set("greeting", greeting).map_err(|e| errs::Err::with_source("fail", e))?;
        Ok(())
    }
}

// Integrate the data access trait into DataHub
impl RedisDataAcc for DataHub {}

// Override the logic trait with the data access implementation
#[override_with(RedisDataAcc)]
impl MyData for DataHub {}

// Logic function takes the trait
fn my_logic(data: &mut impl MyData) -> errs::Result<()> {
    data.say_greeting("Hello!")
}

fn my_app() -> errs::Result<()> {
    let mut hub = DataHub::new();
    hub.txn(my_logic)
}

fn main() -> errs::Result<()> {
    let _auto_shutdown = setup()?;
    my_app()
}
```

#### For Pub/Sub Subscribers And Synchronous Messages

```rust
use sabi::{setup, DataHub, DataAcc};
use sabi_redis::{RedisPubSubSubscriber, RedisPubSubMsgDataSrc, RedisPubSubMsgDataConn};
use redis::ControlFlow;
use override_macro::{overridable, override_with};

#[overridable]
trait MyMsgData {
    fn get_payload(&mut self) -> errs::Result<String>;
}

#[overridable]
trait RedisPubSubDataAcc: DataAcc {
    fn get_payload(&mut self) -> errs::Result<String> {
        let data_conn = self.get_data_conn::<RedisPubSubMsgDataConn>("msg")?;
        let msg = data_conn.get_message();
        msg.get_payload::<String>().map_err(|e| errs::Err::with_source("fail", e))
    }
}

impl RedisPubSubDataAcc for DataHub {}

#[override_with(RedisPubSubDataAcc)]
impl MyMsgData for DataHub {}

fn receive_logic(data: &mut impl MyMsgData) -> errs::Result<()> {
    let payload = data.get_payload()?;
    println!("Received: {}", payload);
    Ok(())
}

fn main() -> errs::Result<()> {
    let _auto_shutdown = setup()?;

    let mut subscriber = RedisPubSubSubscriber::new("redis://127.0.0.1:6379/0");
    subscriber.subscribe("channel-1");
    subscriber.receive(|msg| {
        let mut hub = DataHub::new();
        hub.uses("msg", RedisPubSubMsgDataSrc::new(msg));
        hub.txn(receive_logic).unwrap();
        ControlFlow::Continue
    })
}
```

### For Standalone Server And Asynchronous Commands
> The `standalone-async` feature is required for this functionality.

```rust
use sabi::tokio::{logic, setup_async, uses, DataHub, DataAcc};
use sabi_redis::{RedisDataSrcAsync, RedisDataConnAsync};
use redis::AsyncTypedCommands;
use override_macro::{overridable, override_with};

uses!("redis", RedisDataSrcAsync::new("redis://127.0.0.1:6379/0"));

#[overridable]
trait MyDataAsync {
    async fn say_greeting_async(&mut self, greeting: &str) -> errs::Result<()>;
}

#[overridable]
trait RedisDataAccAsync: DataAcc {
    async fn say_greeting_async(&mut self, greeting: &str) -> errs::Result<()> {
        let data_conn = self.get_data_conn_async::<RedisDataConnAsync>("redis").await?;
        let redis_conn = data_conn.get_connection();
        redis_conn.set("greeting", greeting).await.map_err(|e| errs::Err::with_source("fail", e))?;
        Ok(())
    }
}

impl RedisDataAccAsync for DataHub {}

#[override_with(RedisDataAccAsync)]
impl MyDataAsync for DataHub {}

async fn my_logic_async(data: &mut impl MyDataAsync) -> errs::Result<()> {
    data.say_greeting_async("Hello!").await
}

async fn my_app_async() -> errs::Result<()> {
    let mut hub = DataHub::new();
    hub.txn_async(logic!(my_logic_async)).await
}

#[tokio::main]
async fn main() -> errs::Result<()> {
    let _auto_shutdown = setup_async().await?;
    my_app_async().await
}
```

#### For Pub/Sub Subscribers And Asynchronous Messages

```rust
use sabi::tokio::{logic, setup_async, DataHub, DataAcc};
use sabi_redis::{RedisPubSubSubscriberAsync, RedisPubSubMsgDataSrcAsync, RedisPubSubMsgDataConnAsync};
use redis::ControlFlow;
use override_macro::{overridable, override_with};

#[overridable]
trait MyMsgDataAsync {
    async fn get_payload_async(&mut self) -> errs::Result<String>;
}

#[overridable]
trait RedisPubSubDataAccAsync: DataAcc {
    async fn get_payload_async(&mut self) -> errs::Result<String> {
        let data_conn = self.get_data_conn_async::<RedisPubSubMsgDataConnAsync>("msg").await?;
        let msg = data_conn.get_message();
        msg.get_payload::<String>().map_err(|e| errs::Err::with_source("fail", e))
    }
}

impl RedisPubSubDataAccAsync for DataHub {}

#[override_with(RedisPubSubDataAccAsync)]
impl MyMsgDataAsync for DataHub {}

async fn receive_logic_async(data: &mut impl MyMsgDataAsync) -> errs::Result<()> {
    let payload = data.get_payload_async().await?;
    println!("Received: {}", payload);
    Ok(())
}

#[tokio::main]
async fn main() -> errs::Result<()> {
    let _auto_shutdown = setup_async().await?;

    let mut subscriber = RedisPubSubSubscriberAsync::new("redis://127.0.0.1:6379/0");
    subscriber.subscribe("channel-1");
    subscriber.receive_async(async |msg| {
        let mut hub = DataHub::new();
        hub.uses("msg", RedisPubSubMsgDataSrcAsync::new(msg));
        hub.txn_async(logic!(receive_logic_async)).await.unwrap();
        ControlFlow::Continue
    }).await
}
```

### For Sentinel Configuration And Synchronous Commands
> The `sentinel` feature is required for this functionality.

```rust
use sabi::{setup, uses, DataHub, DataAcc};
use sabi_redis::sentinel::{RedisDataSrc, RedisDataConn};
use redis::sentinel::SentinelServerType;
use redis::TypedCommands;
use override_macro::{overridable, override_with};

uses!(
    "redis",
    RedisDataSrc::new(
        vec!["redis://127.0.0.1:26379/", "redis://127.0.0.1:26380/", "redis://127.0.0.1:26381/"],
        "mymaster",
        SentinelServerType::Master,
    )
);

#[overridable]
trait MyData {
    fn say_greeting(&mut self, greeting: &str) -> errs::Result<()>;
}

#[overridable]
trait RedisDataAcc: DataAcc {
    fn say_greeting(&mut self, greeting: &str) -> errs::Result<()> {
        let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
        let redis_conn = data_conn.get_connection();
        redis_conn.set("greeting", greeting).map_err(|e| errs::Err::with_source("fail", e))?;
        Ok(())
    }
}

impl RedisDataAcc for DataHub {}

#[override_with(RedisDataAcc)]
impl MyData for DataHub {}

fn my_logic(data: &mut impl MyData) -> errs::Result<()> {
    data.say_greeting("Hello!")
}

fn my_app() -> errs::Result<()> {
    let mut hub = DataHub::new();
    hub.txn(my_logic)
}

fn main() -> errs::Result<()> {
    let _auto_shutdown = setup()?;
    my_app()
}
```

#### For Pub/Sub Subscribers And Synchronous Messages

```rust
use sabi::{setup, DataHub, DataAcc};
use sabi_redis::sentinel::{RedisPubSubSubscriber, RedisPubSubMsgDataSrc, RedisPubSubMsgDataConn};
use redis::sentinel::SentinelServerType;
use redis::ControlFlow;
use override_macro::{overridable, override_with};

#[overridable]
trait MyMsgData {
    fn get_payload(&mut self) -> errs::Result<String>;
}

#[overridable]
trait RedisPubSubDataAcc: DataAcc {
    fn get_payload(&mut self) -> errs::Result<String> {
        let data_conn = self.get_data_conn::<RedisPubSubMsgDataConn>("msg")?;
        let msg = data_conn.get_message();
        msg.get_payload::<String>().map_err(|e| errs::Err::with_source("fail", e))
    }
}

impl RedisPubSubDataAcc for DataHub {}

#[override_with(RedisPubSubDataAcc)]
impl MyMsgData for DataHub {}

fn receive_logic(data: &mut impl MyMsgData) -> errs::Result<()> {
    let payload = data.get_payload()?;
    println!("Received: {}", payload);
    Ok(())
}

fn main() -> errs::Result<()> {
    let _auto_shutdown = setup()?;

    let mut subscriber = RedisPubSubSubscriber::new(
        vec!["redis://127.0.0.1:26379/", "redis://127.0.0.1:26380/", "redis://127.0.0.1:26381/"],
        "mymaster",
        SentinelServerType::Master,
    );
    subscriber.subscribe("channel-1");
    subscriber.receive(|msg| {
        let mut hub = DataHub::new();
        hub.uses("msg", RedisPubSubMsgDataSrc::new(msg));
        hub.txn(receive_logic).unwrap();
        ControlFlow::Continue
    })
}
```

### For Sentinel Configuration And Asynchronous Commands
> The `sentinel-async` feature is required for this functionality.

```rust
use sabi::tokio::{logic, setup_async, uses, DataHub, DataAcc};
use sabi_redis::sentinel::{RedisDataSrcAsync, RedisDataConnAsync};
use redis::sentinel::SentinelServerType;
use redis::AsyncTypedCommands;
use override_macro::{overridable, override_with};

uses!(
    "redis",
    RedisDataSrcAsync::new(
        vec!["redis://127.0.0.1:26379/", "redis://127.0.0.1:26380/", "redis://127.0.0.1:26381/"],
        "mymaster",
        SentinelServerType::Master,
    )
);

#[overridable]
trait MyDataAsync {
    async fn say_greeting_async(&mut self, greeting: &str) -> errs::Result<()>;
}

#[overridable]
trait RedisDataAccAsync: DataAcc {
    async fn say_greeting_async(&mut self, greeting: &str) -> errs::Result<()> {
        let data_conn = self.get_data_conn_async::<RedisDataConnAsync>("redis").await?;
        let redis_conn = data_conn.get_connection();
        redis_conn.set("greeting", greeting).await.map_err(|e| errs::Err::with_source("fail", e))?;
        Ok(())
    }
}

impl RedisDataAccAsync for DataHub {}

#[override_with(RedisDataAccAsync)]
impl MyDataAsync for DataHub {}

async fn my_logic_async(data: &mut impl MyDataAsync) -> errs::Result<()> {
    data.say_greeting_async("Hello!").await
}

async fn my_app_async() -> errs::Result<()> {
    let mut hub = DataHub::new();
    hub.txn_async(logic!(my_logic_async)).await
}

#[tokio::main]
async fn main() -> errs::Result<()> {
    let _auto_shutdown = setup_async().await?;
    my_app_async().await
}
```

#### For Pub/Sub Subscribers And Asynchronous Messages

```rust
use sabi::tokio::{logic, setup_async, DataHub, DataAcc};
use sabi_redis::sentinel::{RedisPubSubSubscriberAsync, RedisPubSubMsgDataSrcAsync, RedisPubSubMsgDataConnAsync};
use redis::sentinel::SentinelServerType;
use redis::ControlFlow;
use override_macro::{overridable, override_with};

#[overridable]
trait MyMsgDataAsync {
    async fn get_payload_async(&mut self) -> errs::Result<String>;
}

#[overridable]
trait RedisPubSubDataAccAsync: DataAcc {
    async fn get_payload_async(&mut self) -> errs::Result<String> {
        let data_conn = self.get_data_conn_async::<RedisPubSubMsgDataConnAsync>("msg").await?;
        let msg = data_conn.get_message();
        msg.get_payload::<String>().map_err(|e| errs::Err::with_source("fail", e))
    }
}

impl RedisPubSubDataAccAsync for DataHub {}

#[override_with(RedisPubSubDataAccAsync)]
impl MyMsgDataAsync for DataHub {}

async fn receive_logic_async(data: &mut impl MyMsgDataAsync) -> errs::Result<()> {
    let payload = data.get_payload_async().await?;
    println!("Received: {}", payload);
    Ok(())
}

#[tokio::main]
async fn main() -> errs::Result<()> {
    let _auto_shutdown = setup_async().await?;

    let mut subscriber = RedisPubSubSubscriberAsync::new(
        vec!["redis://127.0.0.1:26379/", "redis://127.0.0.1:26380/", "redis://127.0.0.1:26381/"],
        "mymaster",
        SentinelServerType::Master,
    );
    subscriber.subscribe("channel-1");
    subscriber.receive_async(async |msg| {
        let mut hub = DataHub::new();
        hub.uses("msg", RedisPubSubMsgDataSrcAsync::new(msg));
        hub.txn_async(logic!(receive_logic_async)).await.unwrap();
        ControlFlow::Continue
    }).await
}
```

### For Cluster Configuration And Synchronous Commands
> The `cluster` feature is required for this functionality.

```rust
use sabi::{setup, uses, DataHub, DataAcc};
use sabi_redis::cluster::{RedisDataSrc, RedisDataConn};
use redis::TypedCommands;
use override_macro::{overridable, override_with};

uses!(
    "redis",
    RedisDataSrc::new(vec![
        "redis://127.0.0.1:7000/",
        "redis://127.0.0.1:7001/",
        "redis://127.0.0.1:7002/",
    ])
);

#[overridable]
trait MyData {
    fn say_greeting(&mut self, greeting: &str) -> errs::Result<()>;
}

#[overridable]
trait RedisDataAcc: DataAcc {
    fn say_greeting(&mut self, greeting: &str) -> errs::Result<()> {
        let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
        let redis_conn = data_conn.get_connection();
        redis_conn.set("greeting", greeting).map_err(|e| errs::Err::with_source("fail", e))?;
        Ok(())
    }
}

impl RedisDataAcc for DataHub {}

#[override_with(RedisDataAcc)]
impl MyData for DataHub {}

fn my_logic(data: &mut impl MyData) -> errs::Result<()> {
    data.say_greeting("Hello!")
}

fn my_app() -> errs::Result<()> {
    let mut hub = DataHub::new();
    hub.txn(my_logic)
}

fn main() -> errs::Result<()> {
    let _auto_shutdown = setup()?;
    my_app()
}
```

#### For Pub/Sub Subscribers And Synchronous Messages

```rust
use sabi::{setup, DataHub, DataAcc};
use sabi_redis::cluster::{RedisPubSubSubscriber, RedisPubSubMsgDataSrc, RedisPubSubMsgDataConn};
use redis::ControlFlow;
use override_macro::{overridable, override_with};

#[overridable]
trait MyMsgData {
    fn get_payload(&mut self) -> errs::Result<String>;
}

#[overridable]
trait RedisPubSubDataAcc: DataAcc {
    fn get_payload(&mut self) -> errs::Result<String> {
        let data_conn = self.get_data_conn::<RedisPubSubMsgDataConn>("msg")?;
        let msg = data_conn.get_message();
        msg.get_payload::<String>().map_err(|e| errs::Err::with_source("fail", e))
    }
}

impl RedisPubSubDataAcc for DataHub {}

#[override_with(RedisPubSubDataAcc)]
impl MyMsgData for DataHub {}

fn receive_logic(data: &mut impl MyMsgData) -> errs::Result<()> {
    let payload = data.get_payload()?;
    println!("Received: {}", payload);
    Ok(())
}

fn main() -> errs::Result<()> {
    let _auto_shutdown = setup()?;

    let mut subscriber = RedisPubSubSubscriber::new(vec![
        "redis://127.0.0.1:7000/",
        "redis://127.0.0.1:7001/",
        "redis://127.0.0.1:7002/",
    ]);
    subscriber.subscribe("channel-1");
    subscriber.receive(|msg| {
        let mut hub = DataHub::new();
        hub.uses("msg", RedisPubSubMsgDataSrc::new(msg));
        hub.txn(receive_logic).unwrap();
        ControlFlow::Continue
    })
}
```

### For Cluster Configuration And Asynchronous Commands
> The `cluster-async` feature is required for this functionality.

```rust
use sabi::tokio::{logic, setup_async, uses, DataHub, DataAcc};
use sabi_redis::cluster::{RedisDataSrcAsync, RedisDataConnAsync};
use redis::AsyncTypedCommands;
use override_macro::{overridable, override_with};

uses!(
    "redis",
    RedisDataSrcAsync::new(vec![
        "redis://127.0.0.1:7000/",
        "redis://127.0.0.1:7001/",
        "redis://127.0.0.1:7002/",
    ])
);

#[overridable]
trait MyDataAsync {
    async fn say_greeting_async(&mut self, greeting: &str) -> errs::Result<()>;
}

#[overridable]
trait RedisDataAccAsync: DataAcc {
    async fn say_greeting_async(&mut self, greeting: &str) -> errs::Result<()> {
        let data_conn = self.get_data_conn_async::<RedisDataConnAsync>("redis").await?;
        let redis_conn = data_conn.get_connection();
        redis_conn.set("greeting", greeting).await.map_err(|e| errs::Err::with_source("fail", e))?;
        Ok(())
    }
}

impl RedisDataAccAsync for DataHub {}

#[override_with(RedisDataAccAsync)]
impl MyDataAsync for DataHub {}

async fn my_logic_async(data: &mut impl MyDataAsync) -> errs::Result<()> {
    data.say_greeting_async("Hello!").await
}

async fn my_app_async() -> errs::Result<()> {
    let mut hub = DataHub::new();
    hub.txn_async(logic!(my_logic_async)).await
}

#[tokio::main]
async fn main() -> errs::Result<()> {
    let _auto_shutdown = setup_async().await?;
    my_app_async().await
}
```

#### For Pub/Sub Subscribers And Asynchronous Messages

```rust
use sabi::tokio::{logic, setup_async, DataHub, DataAcc};
use sabi_redis::cluster::{RedisPubSubSubscriberAsync, RedisPubSubMsgDataSrcAsync, RedisPubSubMsgDataConnAsync};
use redis::ControlFlow;
use override_macro::{overridable, override_with};

#[overridable]
trait MyMsgDataAsync {
    async fn get_payload_async(&mut self) -> errs::Result<String>;
}

#[overridable]
trait RedisPubSubDataAccAsync: DataAcc {
    async fn get_payload_async(&mut self) -> errs::Result<String> {
        let data_conn = self.get_data_conn_async::<RedisPubSubMsgDataConnAsync>("msg").await?;
        let msg = data_conn.get_message();
        msg.get_payload::<String>().map_err(|e| errs::Err::with_source("fail", e))
    }
}

impl RedisPubSubDataAccAsync for DataHub {}

#[override_with(RedisPubSubDataAccAsync)]
impl MyMsgDataAsync for DataHub {}

async fn receive_logic_async(data: &mut impl MyMsgDataAsync) -> errs::Result<()> {
    let payload = data.get_payload_async().await?;
    println!("Received: {}", payload);
    Ok(())
}

#[tokio::main]
async fn main() -> errs::Result<()> {
    let _auto_shutdown = setup_async().await?;

    let mut subscriber = RedisPubSubSubscriberAsync::new(vec![
        "redis://127.0.0.1:7000/",
        "redis://127.0.0.1:7001/",
        "redis://127.0.0.1:7002/",
    ]);
    subscriber.subscribe("channel-1");
    subscriber.receive_async(async |msg| {
        let mut hub = DataHub::new();
        hub.uses("msg", RedisPubSubMsgDataSrcAsync::new(msg));
        hub.txn_async(logic!(receive_logic_async)).await.unwrap();
        ControlFlow::Continue
    }).await
}
```

## Supported Rust versions

This crate supports Rust 1.87.0 or later.

```sh
% ./build.sh msrv
  [Meta]   cargo-msrv 0.18.4

Compatibility Check #1: Rust 1.75.0
  [FAIL]   Is incompatible

Compatibility Check #2: Rust 1.85.1
  [FAIL]   Is incompatible

Compatibility Check #3: Rust 1.90.0
  [OK]     Is compatible

Compatibility Check #4: Rust 1.87.0
  [OK]     Is compatible

Compatibility Check #5: Rust 1.86.0
  [FAIL]   Is incompatible

Result:
   Considered (min … max):   Rust 1.56.1 … Rust 1.94.1
   Search method:            bisect
   MSRV:                     1.87.0
   Target:                   x86_64-apple-darwin
```

## License

Copyright (C) 2025-2026 Takayuki Sato

This program is free software under MIT License.<br>
See the file LICENSE in this distribution for more details.


[repo-url]: https://github.com/sttk/sabi_redis-rust
[cratesio-img]: https://img.shields.io/badge/crates.io-ver.0.7.0-fc8d62?logo=rust
[cratesio-url]: https://crates.io/crates/sabi_redis
[docrs-img]: https://img.shields.io/badge/doc.rs-sabi_redis-66c2a5?logo=docs.rs
[docrs-url]: https://docs.rs/sabi_redis
[ci-img]: https://github.com/sttk/sabi_redis-rust/actions/workflows/rust.yml/badge.svg?branch=main
[ci-url]: https://github.com/sttk/sabi_redis-rust/actions?query=branch%3Amain
[mit-img]: https://img.shields.io/badge/license-MIT-green.svg
[mit-url]: https://opensource.org/licenses/MIT
