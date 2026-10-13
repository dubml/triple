use crate::{Error, Result};

/// Decoded application value; a binding handles any Base64 representation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MetadataValue {
    /// Printable ASCII text.
    Ascii(String),
    /// Opaque bytes, including empty values.
    Binary(Vec<u8>),
}

/// Application metadata preserving duplicate values in insertion order.
///
/// Protocol header filtering and wire encoding belong to each binding.
///
/// ```
/// let mut metadata = triple_core::Metadata::new();
/// metadata.append_ascii("trace-id", "first")?;
/// metadata.append_ascii("trace-id", "second")?;
/// metadata.append_binary("trace-bin", &[0, 255])?;
/// assert_eq!(metadata.ascii_values("trace-id").collect::<Vec<_>>(), ["first", "second"]);
/// assert_eq!(metadata.binary_values("trace-bin").collect::<Vec<_>>(), [&[0, 255][..]]);
/// # Ok::<(), triple_core::Error>(())
/// ```
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct Metadata {
    entries: Vec<(String, MetadataValue)>,
}

impl Metadata {
    /// Creates empty metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends text without replacing an existing value with the same name.
    pub fn append_ascii(&mut self, name: &str, value: &str) -> Result<()> {
        validate_name(name)?;
        if name.ends_with("-bin") || !value.bytes().all(|byte| (0x20..=0x7e).contains(&byte)) {
            return Err(Error::InvalidAsciiValue);
        }
        self.entries
            .push((name.to_owned(), MetadataValue::Ascii(value.to_owned())));
        Ok(())
    }

    /// Appends raw binary bytes without performing wire encoding.
    pub fn append_binary(&mut self, name: &str, value: &[u8]) -> Result<()> {
        validate_name(name)?;
        if !name.ends_with("-bin") {
            return Err(Error::InvalidBinaryName);
        }
        self.entries
            .push((name.to_owned(), MetadataValue::Binary(value.to_vec())));
        Ok(())
    }

    /// Borrows each entry in insertion order, including duplicate names.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &MetadataValue)> {
        self.entries
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    /// Borrows text values for one exact metadata name.
    pub fn ascii_values<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a str> {
        self.iter().filter_map(move |(key, value)| {
            if key != name {
                return None;
            }
            match value {
                MetadataValue::Ascii(value) => Some(value.as_str()),
                MetadataValue::Binary(_) => None,
            }
        })
    }

    /// Borrows raw binary values without decoding or copying them.
    pub fn binary_values<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a [u8]> {
        self.iter().filter_map(move |(key, value)| {
            if key != name {
                return None;
            }
            match value {
                MetadataValue::Binary(value) => Some(value.as_slice()),
                MetadataValue::Ascii(_) => None,
            }
        })
    }
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty()
        || !name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-.".contains(&byte)
        })
    {
        return Err(Error::InvalidMetadataName);
    }
    Ok(())
}
