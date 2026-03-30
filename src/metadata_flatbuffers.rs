use flatbuffers::{
    FlatBufferBuilder, Follow, ForwardsUOffset, InvalidFlatbuffer, Table, TableUnfinishedWIPOffset,
    VOffsetT, Vector, WIPOffset,
};

pub(crate) const CATALOG_IDENTIFIER: &str = "TDBC";
pub(crate) const MANIFEST_BODY_IDENTIFIER: &str = "TDMB";
pub(crate) const MANIFEST_FILE_IDENTIFIER: &str = "TDMF";
pub(crate) const REMOTE_MANIFEST_BODY_IDENTIFIER: &str = "TDRB";
pub(crate) const REMOTE_MANIFEST_FILE_IDENTIFIER: &str = "TDRF";
pub(crate) const REMOTE_CACHE_IDENTIFIER: &str = "TDRC";
pub(crate) const BACKUP_GC_IDENTIFIER: &str = "TDBG";

fn finish_table<'fbb, T>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    start: WIPOffset<TableUnfinishedWIPOffset>,
) -> WIPOffset<T> {
    let offset = fbb.end_table(start);
    WIPOffset::new(offset.value())
}

pub(crate) fn root_with_identifier<'a, T>(
    bytes: &'a [u8],
    ident: &str,
    name: &str,
) -> Result<T::Inner, String>
where
    T: Follow<'a> + flatbuffers::Verifiable + 'a,
{
    if !flatbuffers::buffer_has_identifier(bytes, ident, false) {
        return Err(format!("{name} file identifier mismatch"));
    }

    flatbuffers::root::<T>(bytes)
        .map_err(|error: InvalidFlatbuffer| format!("invalid {name} flatbuffer: {error}"))
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct MetadataEntry<'a> {
    pub(crate) _tab: Table<'a>,
}

impl<'a> Follow<'a> for MetadataEntry<'a> {
    type Inner = MetadataEntry<'a>;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

impl<'a> MetadataEntry<'a> {
    pub(crate) const VT_KEY: VOffsetT = 4;
    pub(crate) const VT_VALUE_JSON: VOffsetT = 6;

    pub(crate) fn key(&self) -> &'a str {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(Self::VT_KEY, None)
                .unwrap()
        }
    }

    pub(crate) fn value_json(&self) -> &'a str {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(Self::VT_VALUE_JSON, None)
                .unwrap()
        }
    }
}

impl flatbuffers::Verifiable for MetadataEntry<'_> {
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<ForwardsUOffset<&str>>("key", Self::VT_KEY, true)?
            .visit_field::<ForwardsUOffset<&str>>("value_json", Self::VT_VALUE_JSON, true)?
            .finish();
        Ok(())
    }
}

pub(crate) fn create_metadata_entry<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    key: WIPOffset<&'fbb str>,
    value_json: WIPOffset<&'fbb str>,
) -> WIPOffset<MetadataEntry<'fbb>> {
    let start = fbb.start_table();
    fbb.push_slot_always::<WIPOffset<_>>(MetadataEntry::VT_KEY, key);
    fbb.push_slot_always::<WIPOffset<_>>(MetadataEntry::VT_VALUE_JSON, value_json);
    finish_table(fbb, start)
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct FieldValue<'a> {
    pub(crate) _tab: Table<'a>,
}

impl<'a> Follow<'a> for FieldValue<'a> {
    type Inner = FieldValue<'a>;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

impl<'a> FieldValue<'a> {
    pub(crate) const VT_KIND: VOffsetT = 4;
    pub(crate) const VT_I64_VALUE: VOffsetT = 6;
    pub(crate) const VT_F64_VALUE: VOffsetT = 8;
    pub(crate) const VT_STRING_VALUE: VOffsetT = 10;
    pub(crate) const VT_BYTES_VALUE: VOffsetT = 12;
    pub(crate) const VT_BOOL_VALUE: VOffsetT = 14;

    pub(crate) fn kind(&self) -> u8 {
        unsafe { self._tab.get::<u8>(Self::VT_KIND, Some(0)).unwrap() }
    }

    pub(crate) fn i64_value(&self) -> i64 {
        unsafe { self._tab.get::<i64>(Self::VT_I64_VALUE, Some(0)).unwrap() }
    }

    pub(crate) fn f64_value(&self) -> f64 {
        unsafe { self._tab.get::<f64>(Self::VT_F64_VALUE, Some(0.0)).unwrap() }
    }

    pub(crate) fn string_value(&self) -> Option<&'a str> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(Self::VT_STRING_VALUE, None)
        }
    }

    pub(crate) fn bytes_value(&self) -> Option<Vector<'a, u8>> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<Vector<'a, u8>>>(Self::VT_BYTES_VALUE, None)
        }
    }

    pub(crate) fn bool_value(&self) -> bool {
        unsafe {
            self._tab
                .get::<bool>(Self::VT_BOOL_VALUE, Some(false))
                .unwrap()
        }
    }
}

impl flatbuffers::Verifiable for FieldValue<'_> {
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<u8>("kind", Self::VT_KIND, false)?
            .visit_field::<i64>("i64_value", Self::VT_I64_VALUE, false)?
            .visit_field::<f64>("f64_value", Self::VT_F64_VALUE, false)?
            .visit_field::<ForwardsUOffset<&str>>("string_value", Self::VT_STRING_VALUE, false)?
            .visit_field::<ForwardsUOffset<Vector<'_, u8>>>(
                "bytes_value",
                Self::VT_BYTES_VALUE,
                false,
            )?
            .visit_field::<bool>("bool_value", Self::VT_BOOL_VALUE, false)?
            .finish();
        Ok(())
    }
}

pub(crate) fn create_field_value<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    kind: u8,
    i64_value: i64,
    f64_value: f64,
    string_value: Option<WIPOffset<&'fbb str>>,
    bytes_value: Option<WIPOffset<Vector<'fbb, u8>>>,
    bool_value: bool,
) -> WIPOffset<FieldValue<'fbb>> {
    let start = fbb.start_table();
    fbb.push_slot::<u8>(FieldValue::VT_KIND, kind, 0);
    fbb.push_slot::<i64>(FieldValue::VT_I64_VALUE, i64_value, 0);
    fbb.push_slot::<f64>(FieldValue::VT_F64_VALUE, f64_value, 0.0);
    if let Some(string_value) = string_value {
        fbb.push_slot_always::<WIPOffset<_>>(FieldValue::VT_STRING_VALUE, string_value);
    }
    if let Some(bytes_value) = bytes_value {
        fbb.push_slot_always::<WIPOffset<_>>(FieldValue::VT_BYTES_VALUE, bytes_value);
    }
    fbb.push_slot::<bool>(FieldValue::VT_BOOL_VALUE, bool_value, false);
    finish_table(fbb, start)
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct FieldDefinition<'a> {
    pub(crate) _tab: Table<'a>,
}

impl<'a> Follow<'a> for FieldDefinition<'a> {
    type Inner = FieldDefinition<'a>;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

impl<'a> FieldDefinition<'a> {
    pub(crate) const VT_ID: VOffsetT = 4;
    pub(crate) const VT_NAME: VOffsetT = 6;
    pub(crate) const VT_FIELD_TYPE: VOffsetT = 8;
    pub(crate) const VT_NULLABLE: VOffsetT = 10;
    pub(crate) const VT_DEFAULT_VALUE: VOffsetT = 12;

    pub(crate) fn id(&self) -> u32 {
        unsafe { self._tab.get::<u32>(Self::VT_ID, Some(0)).unwrap() }
    }

    pub(crate) fn name(&self) -> &'a str {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(Self::VT_NAME, None)
                .unwrap()
        }
    }

    pub(crate) fn field_type(&self) -> u8 {
        unsafe { self._tab.get::<u8>(Self::VT_FIELD_TYPE, Some(0)).unwrap() }
    }

    pub(crate) fn nullable(&self) -> bool {
        unsafe {
            self._tab
                .get::<bool>(Self::VT_NULLABLE, Some(false))
                .unwrap()
        }
    }

    pub(crate) fn default_value(&self) -> Option<FieldValue<'a>> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<FieldValue<'a>>>(Self::VT_DEFAULT_VALUE, None)
        }
    }
}

impl flatbuffers::Verifiable for FieldDefinition<'_> {
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<u32>("id", Self::VT_ID, false)?
            .visit_field::<ForwardsUOffset<&str>>("name", Self::VT_NAME, true)?
            .visit_field::<u8>("field_type", Self::VT_FIELD_TYPE, false)?
            .visit_field::<bool>("nullable", Self::VT_NULLABLE, false)?
            .visit_field::<ForwardsUOffset<FieldValue<'_>>>(
                "default_value",
                Self::VT_DEFAULT_VALUE,
                false,
            )?
            .finish();
        Ok(())
    }
}

pub(crate) fn create_field_definition<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    id: u32,
    name: WIPOffset<&'fbb str>,
    field_type: u8,
    nullable: bool,
    default_value: Option<WIPOffset<FieldValue<'fbb>>>,
) -> WIPOffset<FieldDefinition<'fbb>> {
    let start = fbb.start_table();
    fbb.push_slot::<u32>(FieldDefinition::VT_ID, id, 0);
    fbb.push_slot_always::<WIPOffset<_>>(FieldDefinition::VT_NAME, name);
    fbb.push_slot::<u8>(FieldDefinition::VT_FIELD_TYPE, field_type, 0);
    fbb.push_slot::<bool>(FieldDefinition::VT_NULLABLE, nullable, false);
    if let Some(default_value) = default_value {
        fbb.push_slot_always::<WIPOffset<_>>(FieldDefinition::VT_DEFAULT_VALUE, default_value);
    }
    finish_table(fbb, start)
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct SchemaDefinition<'a> {
    pub(crate) _tab: Table<'a>,
}

impl<'a> Follow<'a> for SchemaDefinition<'a> {
    type Inner = SchemaDefinition<'a>;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

impl<'a> SchemaDefinition<'a> {
    pub(crate) const VT_VERSION: VOffsetT = 4;
    pub(crate) const VT_FIELDS: VOffsetT = 6;

    pub(crate) fn version(&self) -> u32 {
        unsafe { self._tab.get::<u32>(Self::VT_VERSION, Some(0)).unwrap() }
    }

    pub(crate) fn fields(&self) -> Vector<'a, ForwardsUOffset<FieldDefinition<'a>>> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<Vector<'a, ForwardsUOffset<FieldDefinition<'a>>>>>(
                    Self::VT_FIELDS,
                    None,
                )
                .unwrap()
        }
    }
}

impl flatbuffers::Verifiable for SchemaDefinition<'_> {
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<u32>("version", Self::VT_VERSION, false)?
            .visit_field::<ForwardsUOffset<Vector<'_, ForwardsUOffset<FieldDefinition<'_>>>>>(
                "fields",
                Self::VT_FIELDS,
                true,
            )?
            .finish();
        Ok(())
    }
}

pub(crate) fn create_schema_definition<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    version: u32,
    fields: WIPOffset<Vector<'fbb, ForwardsUOffset<FieldDefinition<'fbb>>>>,
) -> WIPOffset<SchemaDefinition<'fbb>> {
    let start = fbb.start_table();
    fbb.push_slot::<u32>(SchemaDefinition::VT_VERSION, version, 0);
    fbb.push_slot_always::<WIPOffset<_>>(SchemaDefinition::VT_FIELDS, fields);
    finish_table(fbb, start)
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct TableConfig<'a> {
    pub(crate) _tab: Table<'a>,
}

impl<'a> Follow<'a> for TableConfig<'a> {
    type Inner = TableConfig<'a>;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

impl<'a> TableConfig<'a> {
    pub(crate) const VT_NAME: VOffsetT = 4;
    pub(crate) const VT_FORMAT: VOffsetT = 6;
    pub(crate) const VT_HAS_MAX_MERGE_OPERAND_CHAIN_LENGTH: VOffsetT = 8;
    pub(crate) const VT_MAX_MERGE_OPERAND_CHAIN_LENGTH: VOffsetT = 10;
    pub(crate) const VT_HAS_BLOOM_FILTER_BITS_PER_KEY: VOffsetT = 12;
    pub(crate) const VT_BLOOM_FILTER_BITS_PER_KEY: VOffsetT = 14;
    pub(crate) const VT_HAS_HISTORY_RETENTION_SEQUENCES: VOffsetT = 16;
    pub(crate) const VT_HISTORY_RETENTION_SEQUENCES: VOffsetT = 18;
    pub(crate) const VT_COMPACTION_STRATEGY: VOffsetT = 20;
    pub(crate) const VT_SCHEMA: VOffsetT = 22;
    pub(crate) const VT_METADATA_ENTRIES: VOffsetT = 24;

    pub(crate) fn name(&self) -> &'a str {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(Self::VT_NAME, None)
                .unwrap()
        }
    }

    pub(crate) fn format(&self) -> u8 {
        unsafe { self._tab.get::<u8>(Self::VT_FORMAT, Some(0)).unwrap() }
    }

    pub(crate) fn has_max_merge_operand_chain_length(&self) -> bool {
        unsafe {
            self._tab
                .get::<bool>(Self::VT_HAS_MAX_MERGE_OPERAND_CHAIN_LENGTH, Some(false))
                .unwrap()
        }
    }

    pub(crate) fn max_merge_operand_chain_length(&self) -> u32 {
        unsafe {
            self._tab
                .get::<u32>(Self::VT_MAX_MERGE_OPERAND_CHAIN_LENGTH, Some(0))
                .unwrap()
        }
    }

    pub(crate) fn has_bloom_filter_bits_per_key(&self) -> bool {
        unsafe {
            self._tab
                .get::<bool>(Self::VT_HAS_BLOOM_FILTER_BITS_PER_KEY, Some(false))
                .unwrap()
        }
    }

    pub(crate) fn bloom_filter_bits_per_key(&self) -> u32 {
        unsafe {
            self._tab
                .get::<u32>(Self::VT_BLOOM_FILTER_BITS_PER_KEY, Some(0))
                .unwrap()
        }
    }

    pub(crate) fn has_history_retention_sequences(&self) -> bool {
        unsafe {
            self._tab
                .get::<bool>(Self::VT_HAS_HISTORY_RETENTION_SEQUENCES, Some(false))
                .unwrap()
        }
    }

    pub(crate) fn history_retention_sequences(&self) -> u64 {
        unsafe {
            self._tab
                .get::<u64>(Self::VT_HISTORY_RETENTION_SEQUENCES, Some(0))
                .unwrap()
        }
    }

    pub(crate) fn compaction_strategy(&self) -> u8 {
        unsafe {
            self._tab
                .get::<u8>(Self::VT_COMPACTION_STRATEGY, Some(0))
                .unwrap()
        }
    }

    pub(crate) fn schema(&self) -> Option<SchemaDefinition<'a>> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<SchemaDefinition<'a>>>(Self::VT_SCHEMA, None)
        }
    }

    pub(crate) fn metadata_entries(
        &self,
    ) -> Option<Vector<'a, ForwardsUOffset<MetadataEntry<'a>>>> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<Vector<'a, ForwardsUOffset<MetadataEntry<'a>>>>>(
                    Self::VT_METADATA_ENTRIES,
                    None,
                )
        }
    }
}

impl flatbuffers::Verifiable for TableConfig<'_> {
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<ForwardsUOffset<&str>>("name", Self::VT_NAME, true)?
            .visit_field::<u8>("format", Self::VT_FORMAT, false)?
            .visit_field::<bool>(
                "has_max_merge_operand_chain_length",
                Self::VT_HAS_MAX_MERGE_OPERAND_CHAIN_LENGTH,
                false,
            )?
            .visit_field::<u32>(
                "max_merge_operand_chain_length",
                Self::VT_MAX_MERGE_OPERAND_CHAIN_LENGTH,
                false,
            )?
            .visit_field::<bool>(
                "has_bloom_filter_bits_per_key",
                Self::VT_HAS_BLOOM_FILTER_BITS_PER_KEY,
                false,
            )?
            .visit_field::<u32>(
                "bloom_filter_bits_per_key",
                Self::VT_BLOOM_FILTER_BITS_PER_KEY,
                false,
            )?
            .visit_field::<bool>(
                "has_history_retention_sequences",
                Self::VT_HAS_HISTORY_RETENTION_SEQUENCES,
                false,
            )?
            .visit_field::<u64>(
                "history_retention_sequences",
                Self::VT_HISTORY_RETENTION_SEQUENCES,
                false,
            )?
            .visit_field::<u8>("compaction_strategy", Self::VT_COMPACTION_STRATEGY, false)?
            .visit_field::<ForwardsUOffset<SchemaDefinition<'_>>>("schema", Self::VT_SCHEMA, false)?
            .visit_field::<ForwardsUOffset<Vector<'_, ForwardsUOffset<MetadataEntry<'_>>>>>(
                "metadata_entries",
                Self::VT_METADATA_ENTRIES,
                true,
            )?
            .finish();
        Ok(())
    }
}

pub(crate) struct TableConfigCreateArgs<'fbb> {
    pub(crate) name: WIPOffset<&'fbb str>,
    pub(crate) format: u8,
    pub(crate) has_max_merge_operand_chain_length: bool,
    pub(crate) max_merge_operand_chain_length: u32,
    pub(crate) has_bloom_filter_bits_per_key: bool,
    pub(crate) bloom_filter_bits_per_key: u32,
    pub(crate) has_history_retention_sequences: bool,
    pub(crate) history_retention_sequences: u64,
    pub(crate) compaction_strategy: u8,
    pub(crate) schema: Option<WIPOffset<SchemaDefinition<'fbb>>>,
    pub(crate) metadata_entries: WIPOffset<Vector<'fbb, ForwardsUOffset<MetadataEntry<'fbb>>>>,
}

pub(crate) fn create_table_config<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    args: TableConfigCreateArgs<'fbb>,
) -> WIPOffset<TableConfig<'fbb>> {
    let start = fbb.start_table();
    fbb.push_slot_always::<WIPOffset<_>>(TableConfig::VT_NAME, args.name);
    fbb.push_slot::<u8>(TableConfig::VT_FORMAT, args.format, 0);
    fbb.push_slot::<bool>(
        TableConfig::VT_HAS_MAX_MERGE_OPERAND_CHAIN_LENGTH,
        args.has_max_merge_operand_chain_length,
        false,
    );
    fbb.push_slot::<u32>(
        TableConfig::VT_MAX_MERGE_OPERAND_CHAIN_LENGTH,
        args.max_merge_operand_chain_length,
        0,
    );
    fbb.push_slot::<bool>(
        TableConfig::VT_HAS_BLOOM_FILTER_BITS_PER_KEY,
        args.has_bloom_filter_bits_per_key,
        false,
    );
    fbb.push_slot::<u32>(
        TableConfig::VT_BLOOM_FILTER_BITS_PER_KEY,
        args.bloom_filter_bits_per_key,
        0,
    );
    fbb.push_slot::<bool>(
        TableConfig::VT_HAS_HISTORY_RETENTION_SEQUENCES,
        args.has_history_retention_sequences,
        false,
    );
    fbb.push_slot::<u64>(
        TableConfig::VT_HISTORY_RETENTION_SEQUENCES,
        args.history_retention_sequences,
        0,
    );
    fbb.push_slot::<u8>(
        TableConfig::VT_COMPACTION_STRATEGY,
        args.compaction_strategy,
        0,
    );
    if let Some(schema) = args.schema {
        fbb.push_slot_always::<WIPOffset<_>>(TableConfig::VT_SCHEMA, schema);
    }
    fbb.push_slot_always::<WIPOffset<_>>(TableConfig::VT_METADATA_ENTRIES, args.metadata_entries);
    finish_table(fbb, start)
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct CatalogEntry<'a> {
    pub(crate) _tab: Table<'a>,
}

impl<'a> Follow<'a> for CatalogEntry<'a> {
    type Inner = CatalogEntry<'a>;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

impl<'a> CatalogEntry<'a> {
    pub(crate) const VT_ID: VOffsetT = 4;
    pub(crate) const VT_CONFIG: VOffsetT = 6;

    pub(crate) fn id(&self) -> u32 {
        unsafe { self._tab.get::<u32>(Self::VT_ID, Some(0)).unwrap() }
    }

    pub(crate) fn config(&self) -> TableConfig<'a> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<TableConfig<'a>>>(Self::VT_CONFIG, None)
                .unwrap()
        }
    }
}

impl flatbuffers::Verifiable for CatalogEntry<'_> {
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<u32>("id", Self::VT_ID, false)?
            .visit_field::<ForwardsUOffset<TableConfig<'_>>>("config", Self::VT_CONFIG, true)?
            .finish();
        Ok(())
    }
}

pub(crate) fn create_catalog_entry<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    id: u32,
    config: WIPOffset<TableConfig<'fbb>>,
) -> WIPOffset<CatalogEntry<'fbb>> {
    let start = fbb.start_table();
    fbb.push_slot::<u32>(CatalogEntry::VT_ID, id, 0);
    fbb.push_slot_always::<WIPOffset<_>>(CatalogEntry::VT_CONFIG, config);
    finish_table(fbb, start)
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct Catalog<'a> {
    pub(crate) _tab: Table<'a>,
}

impl<'a> Follow<'a> for Catalog<'a> {
    type Inner = Catalog<'a>;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

impl<'a> Catalog<'a> {
    pub(crate) const VT_FORMAT_VERSION: VOffsetT = 4;
    pub(crate) const VT_TABLES: VOffsetT = 6;

    pub(crate) fn format_version(&self) -> u32 {
        unsafe {
            self._tab
                .get::<u32>(Self::VT_FORMAT_VERSION, Some(0))
                .unwrap()
        }
    }

    pub(crate) fn tables(&self) -> Vector<'a, ForwardsUOffset<CatalogEntry<'a>>> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<Vector<'a, ForwardsUOffset<CatalogEntry<'a>>>>>(
                    Self::VT_TABLES,
                    None,
                )
                .unwrap()
        }
    }
}

impl flatbuffers::Verifiable for Catalog<'_> {
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<u32>("format_version", Self::VT_FORMAT_VERSION, false)?
            .visit_field::<ForwardsUOffset<Vector<'_, ForwardsUOffset<CatalogEntry<'_>>>>>(
                "tables",
                Self::VT_TABLES,
                true,
            )?
            .finish();
        Ok(())
    }
}

pub(crate) fn create_catalog<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    format_version: u32,
    tables: WIPOffset<Vector<'fbb, ForwardsUOffset<CatalogEntry<'fbb>>>>,
) -> WIPOffset<Catalog<'fbb>> {
    let start = fbb.start_table();
    fbb.push_slot::<u32>(Catalog::VT_FORMAT_VERSION, format_version, 0);
    fbb.push_slot_always::<WIPOffset<_>>(Catalog::VT_TABLES, tables);
    finish_table(fbb, start)
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct ManifestSstable<'a> {
    pub(crate) _tab: Table<'a>,
}

impl<'a> Follow<'a> for ManifestSstable<'a> {
    type Inner = ManifestSstable<'a>;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

impl<'a> ManifestSstable<'a> {
    pub(crate) const VT_TABLE_ID: VOffsetT = 4;
    pub(crate) const VT_LEVEL: VOffsetT = 6;
    pub(crate) const VT_LOCAL_ID: VOffsetT = 8;
    pub(crate) const VT_FILE_PATH: VOffsetT = 10;
    pub(crate) const VT_REMOTE_KEY: VOffsetT = 12;
    pub(crate) const VT_LENGTH: VOffsetT = 14;
    pub(crate) const VT_CHECKSUM: VOffsetT = 16;
    pub(crate) const VT_DATA_CHECKSUM: VOffsetT = 18;
    pub(crate) const VT_MIN_KEY: VOffsetT = 20;
    pub(crate) const VT_MAX_KEY: VOffsetT = 22;
    pub(crate) const VT_MIN_SEQUENCE: VOffsetT = 24;
    pub(crate) const VT_MAX_SEQUENCE: VOffsetT = 26;
    pub(crate) const VT_HAS_SCHEMA_VERSION: VOffsetT = 28;
    pub(crate) const VT_SCHEMA_VERSION: VOffsetT = 30;

    pub(crate) fn table_id(&self) -> u32 {
        unsafe { self._tab.get::<u32>(Self::VT_TABLE_ID, Some(0)).unwrap() }
    }

    pub(crate) fn level(&self) -> u32 {
        unsafe { self._tab.get::<u32>(Self::VT_LEVEL, Some(0)).unwrap() }
    }

    pub(crate) fn local_id(&self) -> &'a str {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(Self::VT_LOCAL_ID, None)
                .unwrap()
        }
    }

    pub(crate) fn file_path(&self) -> Option<&'a str> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(Self::VT_FILE_PATH, None)
        }
    }

    pub(crate) fn remote_key(&self) -> Option<&'a str> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(Self::VT_REMOTE_KEY, None)
        }
    }

    pub(crate) fn length(&self) -> u64 {
        unsafe { self._tab.get::<u64>(Self::VT_LENGTH, Some(0)).unwrap() }
    }

    pub(crate) fn checksum(&self) -> u32 {
        unsafe { self._tab.get::<u32>(Self::VT_CHECKSUM, Some(0)).unwrap() }
    }

    pub(crate) fn data_checksum(&self) -> u32 {
        unsafe {
            self._tab
                .get::<u32>(Self::VT_DATA_CHECKSUM, Some(0))
                .unwrap()
        }
    }

    pub(crate) fn min_key(&self) -> Vector<'a, u8> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<Vector<'a, u8>>>(Self::VT_MIN_KEY, None)
                .unwrap()
        }
    }

    pub(crate) fn max_key(&self) -> Vector<'a, u8> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<Vector<'a, u8>>>(Self::VT_MAX_KEY, None)
                .unwrap()
        }
    }

    pub(crate) fn min_sequence(&self) -> u64 {
        unsafe {
            self._tab
                .get::<u64>(Self::VT_MIN_SEQUENCE, Some(0))
                .unwrap()
        }
    }

    pub(crate) fn max_sequence(&self) -> u64 {
        unsafe {
            self._tab
                .get::<u64>(Self::VT_MAX_SEQUENCE, Some(0))
                .unwrap()
        }
    }

    pub(crate) fn has_schema_version(&self) -> bool {
        unsafe {
            self._tab
                .get::<bool>(Self::VT_HAS_SCHEMA_VERSION, Some(false))
                .unwrap()
        }
    }

    pub(crate) fn schema_version(&self) -> u32 {
        unsafe {
            self._tab
                .get::<u32>(Self::VT_SCHEMA_VERSION, Some(0))
                .unwrap()
        }
    }
}

impl flatbuffers::Verifiable for ManifestSstable<'_> {
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<u32>("table_id", Self::VT_TABLE_ID, false)?
            .visit_field::<u32>("level", Self::VT_LEVEL, false)?
            .visit_field::<ForwardsUOffset<&str>>("local_id", Self::VT_LOCAL_ID, true)?
            .visit_field::<ForwardsUOffset<&str>>("file_path", Self::VT_FILE_PATH, false)?
            .visit_field::<ForwardsUOffset<&str>>("remote_key", Self::VT_REMOTE_KEY, false)?
            .visit_field::<u64>("length", Self::VT_LENGTH, false)?
            .visit_field::<u32>("checksum", Self::VT_CHECKSUM, false)?
            .visit_field::<u32>("data_checksum", Self::VT_DATA_CHECKSUM, false)?
            .visit_field::<ForwardsUOffset<Vector<'_, u8>>>("min_key", Self::VT_MIN_KEY, true)?
            .visit_field::<ForwardsUOffset<Vector<'_, u8>>>("max_key", Self::VT_MAX_KEY, true)?
            .visit_field::<u64>("min_sequence", Self::VT_MIN_SEQUENCE, false)?
            .visit_field::<u64>("max_sequence", Self::VT_MAX_SEQUENCE, false)?
            .visit_field::<bool>("has_schema_version", Self::VT_HAS_SCHEMA_VERSION, false)?
            .visit_field::<u32>("schema_version", Self::VT_SCHEMA_VERSION, false)?
            .finish();
        Ok(())
    }
}

pub(crate) struct ManifestSstableCreateArgs<'fbb> {
    pub(crate) table_id: u32,
    pub(crate) level: u32,
    pub(crate) local_id: WIPOffset<&'fbb str>,
    pub(crate) file_path: Option<WIPOffset<&'fbb str>>,
    pub(crate) remote_key: Option<WIPOffset<&'fbb str>>,
    pub(crate) length: u64,
    pub(crate) checksum: u32,
    pub(crate) data_checksum: u32,
    pub(crate) min_key: WIPOffset<Vector<'fbb, u8>>,
    pub(crate) max_key: WIPOffset<Vector<'fbb, u8>>,
    pub(crate) min_sequence: u64,
    pub(crate) max_sequence: u64,
    pub(crate) has_schema_version: bool,
    pub(crate) schema_version: u32,
}

pub(crate) fn create_manifest_sstable<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    args: ManifestSstableCreateArgs<'fbb>,
) -> WIPOffset<ManifestSstable<'fbb>> {
    let start = fbb.start_table();
    fbb.push_slot::<u32>(ManifestSstable::VT_TABLE_ID, args.table_id, 0);
    fbb.push_slot::<u32>(ManifestSstable::VT_LEVEL, args.level, 0);
    fbb.push_slot_always::<WIPOffset<_>>(ManifestSstable::VT_LOCAL_ID, args.local_id);
    if let Some(file_path) = args.file_path {
        fbb.push_slot_always::<WIPOffset<_>>(ManifestSstable::VT_FILE_PATH, file_path);
    }
    if let Some(remote_key) = args.remote_key {
        fbb.push_slot_always::<WIPOffset<_>>(ManifestSstable::VT_REMOTE_KEY, remote_key);
    }
    fbb.push_slot::<u64>(ManifestSstable::VT_LENGTH, args.length, 0);
    fbb.push_slot::<u32>(ManifestSstable::VT_CHECKSUM, args.checksum, 0);
    fbb.push_slot::<u32>(ManifestSstable::VT_DATA_CHECKSUM, args.data_checksum, 0);
    fbb.push_slot_always::<WIPOffset<_>>(ManifestSstable::VT_MIN_KEY, args.min_key);
    fbb.push_slot_always::<WIPOffset<_>>(ManifestSstable::VT_MAX_KEY, args.max_key);
    fbb.push_slot::<u64>(ManifestSstable::VT_MIN_SEQUENCE, args.min_sequence, 0);
    fbb.push_slot::<u64>(ManifestSstable::VT_MAX_SEQUENCE, args.max_sequence, 0);
    fbb.push_slot::<bool>(
        ManifestSstable::VT_HAS_SCHEMA_VERSION,
        args.has_schema_version,
        false,
    );
    fbb.push_slot::<u32>(ManifestSstable::VT_SCHEMA_VERSION, args.schema_version, 0);
    finish_table(fbb, start)
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct ManifestBody<'a> {
    pub(crate) _tab: Table<'a>,
}

impl<'a> Follow<'a> for ManifestBody<'a> {
    type Inner = ManifestBody<'a>;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

impl<'a> ManifestBody<'a> {
    pub(crate) const VT_FORMAT_VERSION: VOffsetT = 4;
    pub(crate) const VT_GENERATION: VOffsetT = 6;
    pub(crate) const VT_LAST_FLUSHED_SEQUENCE: VOffsetT = 8;
    pub(crate) const VT_SSTABLES: VOffsetT = 10;

    pub(crate) fn format_version(&self) -> u32 {
        unsafe {
            self._tab
                .get::<u32>(Self::VT_FORMAT_VERSION, Some(0))
                .unwrap()
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        unsafe { self._tab.get::<u64>(Self::VT_GENERATION, Some(0)).unwrap() }
    }

    pub(crate) fn last_flushed_sequence(&self) -> u64 {
        unsafe {
            self._tab
                .get::<u64>(Self::VT_LAST_FLUSHED_SEQUENCE, Some(0))
                .unwrap()
        }
    }

    pub(crate) fn sstables(&self) -> Vector<'a, ForwardsUOffset<ManifestSstable<'a>>> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<Vector<'a, ForwardsUOffset<ManifestSstable<'a>>>>>(
                    Self::VT_SSTABLES,
                    None,
                )
                .unwrap()
        }
    }
}

impl flatbuffers::Verifiable for ManifestBody<'_> {
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<u32>("format_version", Self::VT_FORMAT_VERSION, false)?
            .visit_field::<u64>("generation", Self::VT_GENERATION, false)?
            .visit_field::<u64>(
                "last_flushed_sequence",
                Self::VT_LAST_FLUSHED_SEQUENCE,
                false,
            )?
            .visit_field::<ForwardsUOffset<Vector<'_, ForwardsUOffset<ManifestSstable<'_>>>>>(
                "sstables",
                Self::VT_SSTABLES,
                true,
            )?
            .finish();
        Ok(())
    }
}

pub(crate) fn create_manifest_body<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    format_version: u32,
    generation: u64,
    last_flushed_sequence: u64,
    sstables: WIPOffset<Vector<'fbb, ForwardsUOffset<ManifestSstable<'fbb>>>>,
) -> WIPOffset<ManifestBody<'fbb>> {
    let start = fbb.start_table();
    fbb.push_slot::<u32>(ManifestBody::VT_FORMAT_VERSION, format_version, 0);
    fbb.push_slot::<u64>(ManifestBody::VT_GENERATION, generation, 0);
    fbb.push_slot::<u64>(
        ManifestBody::VT_LAST_FLUSHED_SEQUENCE,
        last_flushed_sequence,
        0,
    );
    fbb.push_slot_always::<WIPOffset<_>>(ManifestBody::VT_SSTABLES, sstables);
    finish_table(fbb, start)
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct ManifestFile<'a> {
    pub(crate) _tab: Table<'a>,
}

impl<'a> Follow<'a> for ManifestFile<'a> {
    type Inner = ManifestFile<'a>;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

impl<'a> ManifestFile<'a> {
    pub(crate) const VT_BODY_BYTES: VOffsetT = 4;
    pub(crate) const VT_CHECKSUM: VOffsetT = 6;

    pub(crate) fn body_bytes(&self) -> Vector<'a, u8> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<Vector<'a, u8>>>(Self::VT_BODY_BYTES, None)
                .unwrap()
        }
    }

    pub(crate) fn checksum(&self) -> u32 {
        unsafe { self._tab.get::<u32>(Self::VT_CHECKSUM, Some(0)).unwrap() }
    }
}

impl flatbuffers::Verifiable for ManifestFile<'_> {
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<ForwardsUOffset<Vector<'_, u8>>>(
                "body_bytes",
                Self::VT_BODY_BYTES,
                true,
            )?
            .visit_field::<u32>("checksum", Self::VT_CHECKSUM, false)?
            .finish();
        Ok(())
    }
}

pub(crate) fn create_manifest_file<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    body_bytes: WIPOffset<Vector<'fbb, u8>>,
    checksum: u32,
) -> WIPOffset<ManifestFile<'fbb>> {
    let start = fbb.start_table();
    fbb.push_slot_always::<WIPOffset<_>>(ManifestFile::VT_BODY_BYTES, body_bytes);
    fbb.push_slot::<u32>(ManifestFile::VT_CHECKSUM, checksum, 0);
    finish_table(fbb, start)
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct RemoteCommitLogSegment<'a> {
    pub(crate) _tab: Table<'a>,
}

impl<'a> Follow<'a> for RemoteCommitLogSegment<'a> {
    type Inner = RemoteCommitLogSegment<'a>;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

impl<'a> RemoteCommitLogSegment<'a> {
    pub(crate) const VT_OBJECT_KEY: VOffsetT = 4;
    pub(crate) const VT_FOOTER_BYTES: VOffsetT = 6;

    pub(crate) fn object_key(&self) -> &'a str {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(Self::VT_OBJECT_KEY, None)
                .unwrap()
        }
    }

    pub(crate) fn footer_bytes(&self) -> Vector<'a, u8> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<Vector<'a, u8>>>(Self::VT_FOOTER_BYTES, None)
                .unwrap()
        }
    }
}

impl flatbuffers::Verifiable for RemoteCommitLogSegment<'_> {
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<ForwardsUOffset<&str>>("object_key", Self::VT_OBJECT_KEY, true)?
            .visit_field::<ForwardsUOffset<Vector<'_, u8>>>(
                "footer_bytes",
                Self::VT_FOOTER_BYTES,
                true,
            )?
            .finish();
        Ok(())
    }
}

pub(crate) fn create_remote_commit_log_segment<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    object_key: WIPOffset<&'fbb str>,
    footer_bytes: WIPOffset<Vector<'fbb, u8>>,
) -> WIPOffset<RemoteCommitLogSegment<'fbb>> {
    let start = fbb.start_table();
    fbb.push_slot_always::<WIPOffset<_>>(RemoteCommitLogSegment::VT_OBJECT_KEY, object_key);
    fbb.push_slot_always::<WIPOffset<_>>(RemoteCommitLogSegment::VT_FOOTER_BYTES, footer_bytes);
    finish_table(fbb, start)
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct RemoteManifestBody<'a> {
    pub(crate) _tab: Table<'a>,
}

impl<'a> Follow<'a> for RemoteManifestBody<'a> {
    type Inner = RemoteManifestBody<'a>;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

impl<'a> RemoteManifestBody<'a> {
    pub(crate) const VT_FORMAT_VERSION: VOffsetT = 4;
    pub(crate) const VT_GENERATION: VOffsetT = 6;
    pub(crate) const VT_LAST_FLUSHED_SEQUENCE: VOffsetT = 8;
    pub(crate) const VT_SSTABLES: VOffsetT = 10;
    pub(crate) const VT_COMMIT_LOG_SEGMENTS: VOffsetT = 12;

    pub(crate) fn format_version(&self) -> u32 {
        unsafe {
            self._tab
                .get::<u32>(Self::VT_FORMAT_VERSION, Some(0))
                .unwrap()
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        unsafe { self._tab.get::<u64>(Self::VT_GENERATION, Some(0)).unwrap() }
    }

    pub(crate) fn last_flushed_sequence(&self) -> u64 {
        unsafe {
            self._tab
                .get::<u64>(Self::VT_LAST_FLUSHED_SEQUENCE, Some(0))
                .unwrap()
        }
    }

    pub(crate) fn sstables(&self) -> Vector<'a, ForwardsUOffset<ManifestSstable<'a>>> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<Vector<'a, ForwardsUOffset<ManifestSstable<'a>>>>>(
                    Self::VT_SSTABLES,
                    None,
                )
                .unwrap()
        }
    }

    pub(crate) fn commit_log_segments(
        &self,
    ) -> Vector<'a, ForwardsUOffset<RemoteCommitLogSegment<'a>>> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<Vector<'a, ForwardsUOffset<RemoteCommitLogSegment<'a>>>>>(
                    Self::VT_COMMIT_LOG_SEGMENTS,
                    None,
                )
                .unwrap()
        }
    }
}

impl flatbuffers::Verifiable for RemoteManifestBody<'_> {
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<u32>("format_version", Self::VT_FORMAT_VERSION, false)?
            .visit_field::<u64>("generation", Self::VT_GENERATION, false)?
            .visit_field::<u64>("last_flushed_sequence", Self::VT_LAST_FLUSHED_SEQUENCE, false)?
            .visit_field::<ForwardsUOffset<Vector<'_, ForwardsUOffset<ManifestSstable<'_>>>>>("sstables", Self::VT_SSTABLES, true)?
            .visit_field::<ForwardsUOffset<Vector<'_, ForwardsUOffset<RemoteCommitLogSegment<'_>>>>>("commit_log_segments", Self::VT_COMMIT_LOG_SEGMENTS, true)?
            .finish();
        Ok(())
    }
}

pub(crate) fn create_remote_manifest_body<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    format_version: u32,
    generation: u64,
    last_flushed_sequence: u64,
    sstables: WIPOffset<Vector<'fbb, ForwardsUOffset<ManifestSstable<'fbb>>>>,
    commit_log_segments: WIPOffset<Vector<'fbb, ForwardsUOffset<RemoteCommitLogSegment<'fbb>>>>,
) -> WIPOffset<RemoteManifestBody<'fbb>> {
    let start = fbb.start_table();
    fbb.push_slot::<u32>(RemoteManifestBody::VT_FORMAT_VERSION, format_version, 0);
    fbb.push_slot::<u64>(RemoteManifestBody::VT_GENERATION, generation, 0);
    fbb.push_slot::<u64>(
        RemoteManifestBody::VT_LAST_FLUSHED_SEQUENCE,
        last_flushed_sequence,
        0,
    );
    fbb.push_slot_always::<WIPOffset<_>>(RemoteManifestBody::VT_SSTABLES, sstables);
    fbb.push_slot_always::<WIPOffset<_>>(
        RemoteManifestBody::VT_COMMIT_LOG_SEGMENTS,
        commit_log_segments,
    );
    finish_table(fbb, start)
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct RemoteManifestFile<'a> {
    pub(crate) _tab: Table<'a>,
}

impl<'a> Follow<'a> for RemoteManifestFile<'a> {
    type Inner = RemoteManifestFile<'a>;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

impl<'a> RemoteManifestFile<'a> {
    pub(crate) const VT_BODY_BYTES: VOffsetT = 4;
    pub(crate) const VT_CHECKSUM: VOffsetT = 6;

    pub(crate) fn body_bytes(&self) -> Vector<'a, u8> {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<Vector<'a, u8>>>(Self::VT_BODY_BYTES, None)
                .unwrap()
        }
    }

    pub(crate) fn checksum(&self) -> u32 {
        unsafe { self._tab.get::<u32>(Self::VT_CHECKSUM, Some(0)).unwrap() }
    }
}

impl flatbuffers::Verifiable for RemoteManifestFile<'_> {
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<ForwardsUOffset<Vector<'_, u8>>>(
                "body_bytes",
                Self::VT_BODY_BYTES,
                true,
            )?
            .visit_field::<u32>("checksum", Self::VT_CHECKSUM, false)?
            .finish();
        Ok(())
    }
}

pub(crate) fn create_remote_manifest_file<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    body_bytes: WIPOffset<Vector<'fbb, u8>>,
    checksum: u32,
) -> WIPOffset<RemoteManifestFile<'fbb>> {
    let start = fbb.start_table();
    fbb.push_slot_always::<WIPOffset<_>>(RemoteManifestFile::VT_BODY_BYTES, body_bytes);
    fbb.push_slot::<u32>(RemoteManifestFile::VT_CHECKSUM, checksum, 0);
    finish_table(fbb, start)
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct RemoteCacheEntry<'a> {
    pub(crate) _tab: Table<'a>,
}

impl<'a> Follow<'a> for RemoteCacheEntry<'a> {
    type Inner = RemoteCacheEntry<'a>;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

impl<'a> RemoteCacheEntry<'a> {
    pub(crate) const VT_FORMAT_VERSION: VOffsetT = 4;
    pub(crate) const VT_OBJECT_KEY: VOffsetT = 6;
    pub(crate) const VT_SPAN_KIND: VOffsetT = 8;
    pub(crate) const VT_START: VOffsetT = 10;
    pub(crate) const VT_END: VOffsetT = 12;
    pub(crate) const VT_DATA_LEN: VOffsetT = 14;

    pub(crate) fn format_version(&self) -> u32 {
        unsafe {
            self._tab
                .get::<u32>(Self::VT_FORMAT_VERSION, Some(0))
                .unwrap()
        }
    }

    pub(crate) fn object_key(&self) -> &'a str {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(Self::VT_OBJECT_KEY, None)
                .unwrap()
        }
    }

    pub(crate) fn span_kind(&self) -> u8 {
        unsafe { self._tab.get::<u8>(Self::VT_SPAN_KIND, Some(0)).unwrap() }
    }

    pub(crate) fn start(&self) -> u64 {
        unsafe { self._tab.get::<u64>(Self::VT_START, Some(0)).unwrap() }
    }

    pub(crate) fn end(&self) -> u64 {
        unsafe { self._tab.get::<u64>(Self::VT_END, Some(0)).unwrap() }
    }

    pub(crate) fn data_len(&self) -> u64 {
        unsafe { self._tab.get::<u64>(Self::VT_DATA_LEN, Some(0)).unwrap() }
    }
}

impl flatbuffers::Verifiable for RemoteCacheEntry<'_> {
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<u32>("format_version", Self::VT_FORMAT_VERSION, false)?
            .visit_field::<ForwardsUOffset<&str>>("object_key", Self::VT_OBJECT_KEY, true)?
            .visit_field::<u8>("span_kind", Self::VT_SPAN_KIND, false)?
            .visit_field::<u64>("start", Self::VT_START, false)?
            .visit_field::<u64>("end", Self::VT_END, false)?
            .visit_field::<u64>("data_len", Self::VT_DATA_LEN, false)?
            .finish();
        Ok(())
    }
}

pub(crate) fn create_remote_cache_entry<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    format_version: u32,
    object_key: WIPOffset<&'fbb str>,
    span_kind: u8,
    start: u64,
    end: u64,
    data_len: u64,
) -> WIPOffset<RemoteCacheEntry<'fbb>> {
    let table_start = fbb.start_table();
    fbb.push_slot::<u32>(RemoteCacheEntry::VT_FORMAT_VERSION, format_version, 0);
    fbb.push_slot_always::<WIPOffset<_>>(RemoteCacheEntry::VT_OBJECT_KEY, object_key);
    fbb.push_slot::<u8>(RemoteCacheEntry::VT_SPAN_KIND, span_kind, 0);
    fbb.push_slot::<u64>(RemoteCacheEntry::VT_START, start, 0);
    fbb.push_slot::<u64>(RemoteCacheEntry::VT_END, end, 0);
    fbb.push_slot::<u64>(RemoteCacheEntry::VT_DATA_LEN, data_len, 0);
    finish_table(fbb, table_start)
}

#[derive(Copy, Clone, PartialEq)]
pub(crate) struct BackupGcBirthRecord<'a> {
    pub(crate) _tab: Table<'a>,
}

impl<'a> Follow<'a> for BackupGcBirthRecord<'a> {
    type Inner = BackupGcBirthRecord<'a>;

    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: unsafe { Table::new(buf, loc) },
        }
    }
}

impl<'a> BackupGcBirthRecord<'a> {
    pub(crate) const VT_FORMAT_VERSION: VOffsetT = 4;
    pub(crate) const VT_OBJECT_KEY: VOffsetT = 6;
    pub(crate) const VT_FIRST_UPLOADED_AT_MILLIS: VOffsetT = 8;

    pub(crate) fn format_version(&self) -> u32 {
        unsafe {
            self._tab
                .get::<u32>(Self::VT_FORMAT_VERSION, Some(0))
                .unwrap()
        }
    }

    pub(crate) fn object_key(&self) -> &'a str {
        unsafe {
            self._tab
                .get::<ForwardsUOffset<&str>>(Self::VT_OBJECT_KEY, None)
                .unwrap()
        }
    }

    pub(crate) fn first_uploaded_at_millis(&self) -> u64 {
        unsafe {
            self._tab
                .get::<u64>(Self::VT_FIRST_UPLOADED_AT_MILLIS, Some(0))
                .unwrap()
        }
    }
}

impl flatbuffers::Verifiable for BackupGcBirthRecord<'_> {
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<u32>("format_version", Self::VT_FORMAT_VERSION, false)?
            .visit_field::<ForwardsUOffset<&str>>("object_key", Self::VT_OBJECT_KEY, true)?
            .visit_field::<u64>(
                "first_uploaded_at_millis",
                Self::VT_FIRST_UPLOADED_AT_MILLIS,
                false,
            )?
            .finish();
        Ok(())
    }
}

pub(crate) fn create_backup_gc_birth_record<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    format_version: u32,
    object_key: WIPOffset<&'fbb str>,
    first_uploaded_at_millis: u64,
) -> WIPOffset<BackupGcBirthRecord<'fbb>> {
    let table_start = fbb.start_table();
    fbb.push_slot::<u32>(BackupGcBirthRecord::VT_FORMAT_VERSION, format_version, 0);
    fbb.push_slot_always::<WIPOffset<_>>(BackupGcBirthRecord::VT_OBJECT_KEY, object_key);
    fbb.push_slot::<u64>(
        BackupGcBirthRecord::VT_FIRST_UPLOADED_AT_MILLIS,
        first_uploaded_at_millis,
        0,
    );
    finish_table(fbb, table_start)
}
