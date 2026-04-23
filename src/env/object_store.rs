use std::time::Instant;

use super::{Bytes, CompletionTarget};

pub trait ObjectStore {
    fn put_opts(
        &mut self,
        _key: ObjectKey,
        _payload: PutPayload,
        _opts: PutOptions,
        _target: CompletionTarget,
    ) -> ObjectOpId {
        panic!("ObjectStore::put_opts stub")
    }

    fn get_opts(
        &mut self,
        _key: ObjectKey,
        _opts: GetOptions,
        _target: CompletionTarget,
    ) -> ObjectOpId {
        panic!("ObjectStore::get_opts stub")
    }

    fn get_ranges(
        &mut self,
        _key: ObjectKey,
        _ranges: Vec<ByteRange>,
        _target: CompletionTarget,
    ) -> ObjectOpId {
        panic!("ObjectStore::get_ranges stub")
    }

    fn start_multipart_put(
        &mut self,
        _key: ObjectKey,
        _opts: PutMultipartOptions,
        _target: CompletionTarget,
    ) -> ObjectOpId {
        panic!("ObjectStore::start_multipart_put stub")
    }

    fn upload_part(
        &mut self,
        _upload: MultipartUploadId,
        _part_number: PartNumber,
        _payload: PutPayload,
        _target: CompletionTarget,
    ) -> ObjectOpId {
        panic!("ObjectStore::upload_part stub")
    }

    fn complete_multipart_put(
        &mut self,
        _upload: MultipartUploadId,
        _parts: Vec<UploadedPart>,
        _target: CompletionTarget,
    ) -> ObjectOpId {
        panic!("ObjectStore::complete_multipart_put stub")
    }

    fn abort_multipart_put(
        &mut self,
        _upload: MultipartUploadId,
        _target: CompletionTarget,
    ) -> ObjectOpId {
        panic!("ObjectStore::abort_multipart_put stub")
    }

    fn delete(&mut self, _key: ObjectKey, _target: CompletionTarget) -> ObjectOpId {
        panic!("ObjectStore::delete stub")
    }

    fn list(&mut self, _prefix: Option<ObjectPrefix>, _target: CompletionTarget) -> ObjectOpId {
        panic!("ObjectStore::list stub")
    }

    fn list_with_delimiter(
        &mut self,
        _prefix: Option<ObjectPrefix>,
        _target: CompletionTarget,
    ) -> ObjectOpId {
        panic!("ObjectStore::list_with_delimiter stub")
    }

    fn copy_opts(
        &mut self,
        _from: ObjectKey,
        _to: ObjectKey,
        _opts: CopyOptions,
        _target: CompletionTarget,
    ) -> ObjectOpId {
        panic!("ObjectStore::copy_opts stub")
    }

    fn rename_opts(
        &mut self,
        _from: ObjectKey,
        _to: ObjectKey,
        _opts: RenameOptions,
        _target: CompletionTarget,
    ) -> ObjectOpId {
        panic!("ObjectStore::rename_opts stub")
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObjectOpId;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ObjectKey;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ObjectPrefix;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ETag;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ObjectTag;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectAttributes;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PutPayload {
    pub bytes: Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PutOptions {
    pub mode: PutMode,
    pub tags: Vec<ObjectTag>,
    pub attributes: ObjectAttributes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PutMode {
    Create,
    Overwrite,
    UpdateIfMatches { etag: ETag },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetOptions {
    pub range: Option<ByteRange>,
    pub if_match: Option<ETag>,
    pub if_none_match: Option<ETag>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PutMultipartOptions {
    pub part_size_hint: Option<usize>,
    pub attributes: ObjectAttributes,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ByteRange {
    pub start: u64,
    pub end: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartNumber(pub u32);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MultipartUploadId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UploadedPart {
    pub part_number: PartNumber,
    pub etag: ETag,
    pub size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectMeta {
    pub key: ObjectKey,
    pub size: u64,
    pub etag: Option<ETag>,
    pub last_modified: Option<Instant>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyOptions;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenameOptions;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PutResult;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetResult;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListResult;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObjectCompletion {
    Put(Result<PutResult, ObjectError>),
    Get(Result<GetResult, ObjectError>),
    GetRanges(Result<Vec<Bytes>, ObjectError>),
    MultipartStarted(Result<MultipartUploadId, ObjectError>),
    PartUploaded(Result<UploadedPart, ObjectError>),
    MultipartCompleted(Result<PutResult, ObjectError>),
    MultipartAborted(Result<(), ObjectError>),
    Deleted(Result<(), ObjectError>),
    Listed(Result<Vec<ObjectMeta>, ObjectError>),
    ListedWithDelimiter(Result<ListResult, ObjectError>),
    Copied(Result<(), ObjectError>),
    Renamed(Result<(), ObjectError>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectError;
