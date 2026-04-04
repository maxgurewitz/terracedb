use flatbuffers::{Follow, InvalidFlatbuffer};

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
