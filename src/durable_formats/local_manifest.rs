mod generated {
    #![allow(clippy::all)]

    include!("local_manifest_generated.rs");
}

pub(crate) use generated::terracedb::durable::local_manifest::*;

pub(crate) const BODY_IDENTIFIER: &str = "TDMB";
pub(crate) const FILE_IDENTIFIER: &str = "TDMF";
