# [sabi_redis for Rust][repo-url] [![crates.io][cratesio-img]][cratesio-url] [![doc.rs][docrs-img]][docrs-url] [![CI Status][ci-img]][ci-url] [![MIT License][mit-img]][mit-url]

The sabi data access library for Redis in Rust.

`sabi_redis` is a Rust crate that provides a streamlined way to access various Redis configurations
within the sabi framework. It includes `DataSrc` and `DataConn` derived classes designed to make
your development process more efficient

`RedisDataSrc` and `RedisDataConn` are designed for a standalone Redis server and provide
synchronous connections for processing Redis commands.

Unlike relational databases, Redis does not support data rollbacks. This can lead to data
inconsistency if a transaction involving both Redis and another database fails mid-process.
To address this, `sabi_redis` offers three unique features to help developers manage Redis updates
and revert changes when necessary: *"force back"*, *"pre-commit"*, and *"post-commit"*.

## Installation

In Cargo.toml, write this crate as a dependency:

```toml
[dependencies]
sabi_redis = "0.0.0"
```

## Usage

### For Standalone Server And Synchronous Commands

Here is an example of how to use `RedisDataSrc` and `RedisDataConn` to connect to Redis and
execute a simple command.

```rust
use errs;
use override_macro::{overridable, override_with};
use redis::TypedCommands;
use sabi;
use sabi_redis::{RedisDataSrc, RedisDataConn};

fn main() -> Result<(), errs::Err> {
    // Register a `RedisDataSrc` instance to connect to a Redis server with the key "redis".
    sabi::uses("redis", RedisDataSrc::new("redis://127.0.0.1:6379/0"));

    // In this setup process, the registered `RedisDataSrc` instance connects to a Redis server.
    let _auto_shutdown = sabi::setup()?;

    my_app()
}

fn my_app() -> Result<(), errs::Err> {
    let mut data = sabi::DataHub::new();
    sabi::txn!(my_logic, data)
}

fn my_logic(data: &mut impl MyData) -> Result<(), errs::Err> {
    let greeting = data.get_greeting()?;
    data.say_greeting(&greeting)
}

#[overridable]
trait MyData {
    fn get_greeting(&mut self) -> Result<String, errs::Err>;
    fn say_greeting(&mut self, greeting: &str) -> Result<(), errs::Err>;
}

#[overridable]
trait GettingDataAcc: sabi::DataAcc {
    fn get_greeting(&mut self) -> Result<String, errs::Err> {
        Ok("Hello!".to_string())
    }
}

#[overridable]
trait RedisSayingDataAcc: sabi::DataAcc {
    fn say_greeting(&mut self, greeting: &str) -> Result<(), errs::Err> {
        // Retrieve a `RedisDataConn` instance by the key "redis".
        let data_conn = self.get_data_conn::<RedisDataConn>("redis")?;

        // Get a Redis connection to execute Redis synchronous commands.
        let mut redis_conn = data_conn.get_connection()?;

        if let Err(e) = redis_conn.set("greeting", greeting) {
            return Err(errs::Err::with_source("fail to set greeting", e));
        }

        // Register a force back process to revert updates to Redis when an error occurs.
        data_conn.add_force_back(|redis_conn| {
            let result = redis_conn.del("greeting");
            if let Err(e) = result {
                return Err(errs::Err::with_source("fail to force back", e));
            }
            Ok(())
        });

        Ok(())
    }
}

impl GettingDataAcc for sabi::DataHub {}
impl RedisSayingDataAcc for sabi::DataHub {}

#[override_with(GettingDataAcc, RedisSayingDataAcc)]
impl MyData for sabi::DataHub {}
```


## Supported Rust versions

This crate supports Rust 1.85.1 or later.

```sh
% ./build.sh msrv
  [Meta]   cargo-msrv 0.18.4

Compatibility Check #1: Rust 1.73.0
  [FAIL]   Is incompatible

Compatibility Check #2: Rust 1.81.0
  [FAIL]   Is incompatible

Compatibility Check #3: Rust 1.85.1
  [OK]     Is compatible

Compatibility Check #4: Rust 1.83.0
  [FAIL]   Is incompatible

Compatibility Check #5: Rust 1.84.1
  [FAIL]   Is incompatible

Result:
   Considered (min … max):   Rust 1.56.1 … Rust 1.89.0
   Search method:            bisect
   MSRV:                     1.85.1
   Target:                   x86_64-apple-darwin
```

## License

Copyright (C) 2025 Takayuki Sato

This program is free software under MIT License.<br>
See the file LICENSE in this distribution for more details.


[repo-url]: https://github.com/sttk/sabi_redis-rust
[cratesio-img]: https://img.shields.io/badge/crates.io-ver.0.0.0-fc8d62?logo=rust
[cratesio-url]: https://crates.io/crates/sabi_redis
[docrs-img]: https://img.shields.io/badge/doc.rs-sabi_redis-66c2a5?logo=docs.rs
[docrs-url]: https://docs.rs/sabi_redis
[ci-img]: https://github.com/sttk/sabi_redis-rust/actions/workflows/rust.yml/badge.svg?branch=main
[ci-url]: https://github.com/sttk/sabi_redis-rust/actions?query=branch%3Amain
[mit-img]: https://img.shields.io/badge/license-MIT-green.svg
[mit-url]: https://opensource.org/licenses/MIT
