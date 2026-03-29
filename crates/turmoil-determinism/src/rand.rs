use ::rand::{Rng, SeedableRng, rngs::StdRng};

pub fn seeded_std_rng(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

pub fn seeded_rng<T>(seed: u64) -> T
where
    T: SeedableRng,
{
    T::seed_from_u64(seed)
}

pub fn fill_seeded_bytes(seed: u64, dest: &mut [u8]) {
    seeded_std_rng(seed).fill_bytes(dest);
}
