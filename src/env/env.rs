use super::{Clock, Entropy, Fs, Net, ObjectStore, Observability, Timers};

pub trait Env: Send {
    fn observability(&mut self) -> &mut dyn Observability {
        panic!("Env::observability stub")
    }

    fn clock(&mut self) -> &mut dyn Clock {
        panic!("Env::clock stub")
    }

    fn entropy(&mut self) -> &mut dyn Entropy {
        panic!("Env::entropy stub")
    }

    fn timers(&mut self) -> &mut dyn Timers {
        panic!("Env::timers stub")
    }

    fn fs(&mut self) -> &mut dyn Fs {
        panic!("Env::fs stub")
    }

    fn net(&mut self) -> &mut dyn Net {
        panic!("Env::net stub")
    }

    fn object_store(&mut self) -> &mut dyn ObjectStore {
        panic!("Env::object_store stub")
    }
}
