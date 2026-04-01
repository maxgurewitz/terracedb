#![no_main]

use libfuzzer_sys::fuzz_target;
use terracedb_debezium::DebeziumPrimaryKey;

fuzz_target!(|data: &[u8]| {
    let _ = DebeziumPrimaryKey::decode(data);
});
