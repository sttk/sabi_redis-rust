// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::{thread, time};

pub(crate) struct Retry {
    max_count: u32,
    init_delay_ms: u64,
    max_delay_ms: u64,
    current_count: u32,
}

impl Retry {
    pub(crate) fn new() -> Self {
        Self {
            max_count: 30,
            init_delay_ms: 1000,
            max_delay_ms: 5000,
            current_count: 0,
        }
    }

    pub(crate) fn with_params(max_count: u32, init_delay_ms: u64, max_delay_ms: u64) -> Self {
        Self {
            max_count,
            init_delay_ms,
            max_delay_ms,
            current_count: 0,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.current_count = 0;
    }

    pub(crate) fn wait_with_backoff(&mut self) -> bool {
        if self.current_count >= self.max_count {
            return false;
        }
        let count = self.current_count + 1;
        let wait_ms = self
            .init_delay_ms
            .saturating_mul(2u64.saturating_pow(self.current_count))
            .min(self.max_delay_ms);
        thread::sleep(time::Duration::from_millis(wait_ms));
        self.current_count = count;
        true
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_new() {
        let retry = Retry::new();
        assert_eq!(retry.max_count, 30);
        assert_eq!(retry.init_delay_ms, 1000);
        assert_eq!(retry.max_delay_ms, 5000);
        assert_eq!(retry.current_count, 0);
    }

    #[test]
    fn test_with_params() {
        let retry = Retry::with_params(5, 100, 1000);
        assert_eq!(retry.max_count, 5);
        assert_eq!(retry.init_delay_ms, 100);
        assert_eq!(retry.max_delay_ms, 1000);
        assert_eq!(retry.current_count, 0);
    }

    #[test]
    fn test_wait_with_backoff() {
        let mut retry = Retry::with_params(6, 100, 1000);

        let tm = Instant::now();
        assert!(retry.wait_with_backoff());
        let d = tm.elapsed().as_millis();
        assert!(d > 90);
        assert!(d < 200);

        let tm = Instant::now();
        assert!(retry.wait_with_backoff());
        let d = tm.elapsed().as_millis();
        assert!(d > 190);
        assert!(d < 300);

        let tm = Instant::now();
        assert!(retry.wait_with_backoff());
        let d = tm.elapsed().as_millis();
        assert!(d > 390);
        assert!(d < 500);

        let tm = Instant::now();
        assert!(retry.wait_with_backoff());
        let d = tm.elapsed().as_millis();
        assert!(d > 790);
        assert!(d < 900);

        let tm = Instant::now();
        assert!(retry.wait_with_backoff());
        let d = tm.elapsed().as_millis();
        assert!(d > 990);
        assert!(d < 1100);

        let tm = Instant::now();
        assert!(retry.wait_with_backoff());
        let d = tm.elapsed().as_millis();
        assert!(d > 990);
        assert!(d < 1100);

        assert!(!retry.wait_with_backoff());
    }
}
