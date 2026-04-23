pub trait Entropy {
    fn next_u64(&mut self) -> u64 {
        panic!("Entropy::next_u64 stub")
    }

    fn fill_bytes(&mut self, _buf: &mut [u8]) {
        panic!("Entropy::fill_bytes stub")
    }
}
