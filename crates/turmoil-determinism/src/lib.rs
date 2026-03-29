pub mod rand;
pub mod time;

use std::time::{Duration, SystemTime};

use ::rand::rngs::StdRng;

pub struct Builder {
    inner: turmoil::Builder,
    seed: u64,
}

impl Builder {
    pub fn new(seed: u64) -> Self {
        let mut inner = turmoil::Builder::new();
        inner.rng_seed(seed);
        Self { inner, seed }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn std_rng(&self) -> StdRng {
        rand::seeded_std_rng(self.seed)
    }

    pub fn inner_mut(&mut self) -> &mut turmoil::Builder {
        &mut self.inner
    }

    pub fn epoch(&mut self, value: SystemTime) -> &mut Self {
        self.inner.epoch(value);
        self
    }

    pub fn simulation_duration(&mut self, value: Duration) -> &mut Self {
        self.inner.simulation_duration(value);
        self
    }

    pub fn tick_duration(&mut self, value: Duration) -> &mut Self {
        self.inner.tick_duration(value);
        self
    }

    pub fn min_message_latency(&mut self, value: Duration) -> &mut Self {
        self.inner.min_message_latency(value);
        self
    }

    pub fn max_message_latency(&mut self, value: Duration) -> &mut Self {
        self.inner.max_message_latency(value);
        self
    }

    pub fn enable_random_order(&mut self) -> &mut Self {
        self.inner.enable_random_order();
        self
    }

    pub fn enable_tokio_io(&mut self) -> &mut Self {
        self.inner.enable_tokio_io();
        self
    }

    pub fn build<'a>(&self) -> turmoil::Sim<'a> {
        self.inner.build()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ::rand::Rng;

    #[test]
    fn seeded_std_rng_is_reproducible() {
        let mut left = crate::rand::seeded_std_rng(7);
        let mut right = crate::rand::seeded_std_rng(7);
        assert_eq!(left.next_u64(), right.next_u64());
        assert_eq!(left.next_u64(), right.next_u64());
    }

    #[test]
    fn builder_reuses_seed_across_builds() {
        let mut builder = crate::Builder::new(0x5151);
        builder
            .simulation_duration(Duration::from_millis(5))
            .tick_duration(Duration::from_millis(1));

        let mut first = builder.build();
        let mut second = builder.build();

        first.client("left", async move { Ok(()) });
        second.client("right", async move { Ok(()) });

        first.run().expect("first build should run");
        second.run().expect("second build should run");
    }
}
