use std::sync::OnceLock;

use tracing_subscriber::{EnvFilter, fmt};

pub fn init_tracing() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let _ = fmt().with_env_filter(filter).with_test_writer().try_init();
    });
}
