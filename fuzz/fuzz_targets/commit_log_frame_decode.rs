#![no_main]

use libfuzzer_sys::fuzz_target;
use terracedb::engine::commit_log::CommitRecord;

fuzz_target!(|data: &[u8]| {
    let _ = CommitRecord::decode_frame(data);
});
