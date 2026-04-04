mod generated {
    #![allow(clippy::all)]

    include!("catalog_generated.rs");
}

pub(crate) use generated::terracedb::durable::catalog::*;

pub(crate) const FILE_IDENTIFIER: &str = "TDBC";
