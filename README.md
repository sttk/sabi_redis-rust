# [sabi_redis for Rust][repo-url] [![crates.io][cratesio-img]][cratesio-url] [![doc.rs][docrs-img]][docrs-url] [![CI Status][ci-img]][ci-url] [![MIT License][mit-img]][mit-url]

The sabi data access library for Redis in Rust.

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
