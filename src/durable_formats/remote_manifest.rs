mod generated {
    #![allow(clippy::all)]

    include!("remote_manifest_generated.rs");
}

pub(crate) use generated::terracedb::durable::remote_manifest::*;

pub(crate) const BODY_IDENTIFIER: &str = "TDRB";
pub(crate) const FILE_IDENTIFIER: &str = "TDRF";
