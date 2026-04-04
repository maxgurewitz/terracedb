mod generated {
    #![allow(clippy::all)]

    include!("remote_cache_generated.rs");
}

pub(crate) use generated::terracedb::durable::remote_cache::*;

pub(crate) const FILE_IDENTIFIER: &str = "TDRC";
