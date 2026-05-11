#![allow(clippy::redundant_pub_crate)]

use std::time::{SystemTime, UNIX_EPOCH};

use crate::types::EpochMillis;

pub(crate) fn current_epoch_ms() -> EpochMillis {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            EpochMillis::try_from(duration.as_millis()).unwrap_or(EpochMillis::MAX)
        })
}
