#![no_main]

use libfuzzer_sys::fuzz_target;
use terracedb::engine::commit_log::SegmentFooter;

fuzz_target!(|data: &[u8]| {
    let _ = SegmentFooter::decode(data);
});
