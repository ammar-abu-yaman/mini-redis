use std::time::{Duration, Instant};

#[derive(PartialEq, Clone)]
pub enum DataFrame<T> {
    Empty,
    Plain(T),
    Expiring {
        data: T,
        expiration: Duration,
        timestamp: Instant,
    },
}

impl<T> Default for DataFrame<T> {
    fn default() -> Self {
        DataFrame::Empty
    }
}

impl<T> DataFrame<T> {
    pub fn plain(data: T) -> Self {
        Self::Plain(data)
    }

    pub fn with_expiration(data: T, expiration: Duration) -> Self {
        Self::Expiring {
            data,
            expiration,
            timestamp: Instant::now(),
        }
    }
}

impl<T> DataFrame<T> {
    pub fn has_expired(&self) -> bool {
        if let Self::Expiring {
            data: _,
            expiration,
            timestamp,
        } = &self
        {
            if &timestamp.elapsed() >= expiration {
                true
            } else {
                false
            }
        } else {
            false
        }
    }
}
