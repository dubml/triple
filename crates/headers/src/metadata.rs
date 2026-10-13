// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use base64::{
    Engine,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
};
use http::{HeaderMap, HeaderValue, header::HeaderName};

use crate::{Error, Result};

/// Application metadata preserving duplicate header values.
///
/// ```
/// use headers::Metadata;
/// let mut metadata = Metadata::new();
/// metadata.append_ascii("trace-id", "first")?;
/// metadata.append_ascii("trace-id", "second")?;
/// metadata.append_binary("trace-bin", &[0, 255])?;
/// assert_eq!(metadata.ascii_values("trace-id").collect::<Vec<_>>(), ["first", "second"]);
/// assert_eq!(metadata.binary_values("trace-bin")?, [vec![0, 255]]);
/// # Ok::<(), headers::Error>(())
/// ```
#[derive(Debug, Default, Clone)]
pub struct Metadata {
    semantic: triple_core::Metadata,
    discarded_ascii_names: Vec<String>,
}

impl Metadata {
    /// Creates empty metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends an ASCII value without replacing existing values.
    pub fn append_ascii(&mut self, name: &str, value: &str) -> Result<()> {
        metadata_name(name)?;
        self.semantic
            .append_ascii(name, value)
            .map_err(|_| Error::InvalidHeader("ASCII metadata"))
    }

    /// Appends binary metadata using unpadded Base64 on the wire.
    pub fn append_binary(&mut self, name: &str, value: &[u8]) -> Result<()> {
        metadata_name(name)?;
        self.semantic
            .append_binary(name, value)
            .map_err(|_| Error::InvalidHeader("binary metadata name"))
    }

    /// Borrows all ASCII values for one metadata name in their received order.
    pub fn ascii_values<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a str> {
        self.semantic.ascii_values(name)
    }

    /// Decodes binary values, including comma-joined and padded peer values.
    pub fn binary_values(&self, name: &str) -> Result<Vec<Vec<u8>>> {
        if !name.ends_with("-bin") {
            return Err(Error::InvalidHeader("binary metadata name"));
        }
        Ok(self
            .semantic
            .binary_values(name)
            .map(<[u8]>::to_vec)
            .collect())
    }

    /// Parses application metadata using the legacy gRPC reserved-header policy.
    pub fn from_headers(headers: &HeaderMap) -> Result<Self> {
        Self::from_headers_with_reserved(headers, is_grpc_reserved_header)
    }

    /// Parses application metadata with the caller's reserved-header predicate.
    pub fn from_headers_with_reserved(
        headers: &HeaderMap,
        is_reserved: impl Fn(&str) -> bool,
    ) -> Result<Self> {
        let mut metadata = Self::new();
        for (name, value) in headers {
            if is_reserved(name.as_str()) {
                continue;
            }
            validate_metadata_name(name.as_str())?;
            if name.as_str().ends_with("-bin") {
                for part in value.as_bytes().split(|b| *b == b',') {
                    // TODO(spec): gRPC does not define invalid Base64 handling.
                    // Preserve the existing failure of the whole metadata parse.
                    let decoded = decode_binary(part.trim_ascii())?;
                    metadata
                        .semantic
                        .append_binary(name.as_str(), &decoded)
                        .map_err(|_| Error::InvalidHeader("binary metadata name"))?;
                }
            } else {
                // gRPC Requests permits discarding an invalid ASCII value that is
                // valid HTTP. Keep a per-message diagnostic without failing the call.
                let accepted = value.to_str().ok().is_some_and(|value| {
                    metadata.semantic.append_ascii(name.as_str(), value).is_ok()
                });
                if !accepted {
                    metadata
                        .discarded_ascii_names
                        .push(name.as_str().to_owned());
                }
            }
        }
        Ok(metadata)
    }

    /// Borrows the names of discarded ASCII values, one record per dropped entry.
    ///
    /// Diagnostics stay in the binding and are never serialized as metadata.
    pub fn discarded_ascii_names(&self) -> impl Iterator<Item = &str> {
        self.discarded_ascii_names.iter().map(String::as_str)
    }

    pub(crate) fn to_headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        for (name, value) in self.semantic.iter() {
            let name = metadata_name(name)?;
            let value = match value {
                triple_core::MetadataValue::Ascii(value) => HeaderValue::from_str(value),
                triple_core::MetadataValue::Binary(value) => {
                    HeaderValue::from_str(&STANDARD_NO_PAD.encode(value))
                }
            }
            .map_err(|_| Error::InvalidHeader("metadata value"))?;
            headers.append(name, value);
        }
        Ok(headers)
    }
}

impl AsRef<triple_core::Metadata> for Metadata {
    fn as_ref(&self) -> &triple_core::Metadata {
        &self.semantic
    }
}

impl From<triple_core::Metadata> for Metadata {
    fn from(semantic: triple_core::Metadata) -> Self {
        Self {
            semantic,
            discarded_ascii_names: Vec::new(),
        }
    }
}

pub(crate) fn decode_binary(value: &[u8]) -> Result<Vec<u8>> {
    STANDARD_NO_PAD
        .decode(value)
        .or_else(|_| STANDARD.decode(value))
        .map_err(|_| Error::InvalidHeader("binary metadata Base64"))
}

fn metadata_name(name: &str) -> Result<HeaderName> {
    if is_grpc_reserved_header(name) {
        return Err(Error::InvalidHeader("metadata name"));
    }
    validate_metadata_name(name)
}

fn validate_metadata_name(name: &str) -> Result<HeaderName> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_-.".contains(&b))
    {
        return Err(Error::InvalidHeader("metadata name"));
    }
    HeaderName::from_bytes(name.as_bytes()).map_err(|_| Error::InvalidHeader("metadata name"))
}

/// The legacy gRPC protocol fields excluded from custom metadata.
pub fn is_grpc_reserved_header(name: &str) -> bool {
    name.starts_with("grpc-")
        || name.starts_with(':')
        || matches!(
            name,
            "content-type" | "te" | "user-agent" | "content-length" | "host"
        )
}
