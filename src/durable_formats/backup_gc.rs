mod generated {
    #![allow(clippy::all)]

    include!("backup_gc_generated.rs");
}

pub(crate) use generated::terracedb::durable::backup_gc::*;

pub(crate) const FILE_IDENTIFIER: &str = "TDBG";
