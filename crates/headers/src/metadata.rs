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
    pub(crate) headers: HeaderMap,
}

impl Metadata {
    /// Creates empty metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends an ASCII value without replacing existing values.
    pub fn append_ascii(&mut self, name: &str, value: &str) -> Result<()> {
        let name = metadata_name(name)?;
        if name.as_str().ends_with("-bin") || !value.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
            return Err(Error::InvalidHeader("ASCII metadata"));
        }
        let value =
            HeaderValue::from_str(value).map_err(|_| Error::InvalidHeader("metadata value"))?;
        self.headers.append(name, value);
        Ok(())
    }

    /// Appends binary metadata using unpadded Base64 on the wire.
    pub fn append_binary(&mut self, name: &str, value: &[u8]) -> Result<()> {
        let name = metadata_name(name)?;
        if !name.as_str().ends_with("-bin") {
            return Err(Error::InvalidHeader("binary metadata name"));
        }
        let value = HeaderValue::from_str(&STANDARD_NO_PAD.encode(value))
            .map_err(|_| Error::InvalidHeader("binary metadata value"))?;
        self.headers.append(name, value);
        Ok(())
    }

    /// Borrows all ASCII values for one metadata name in their received order.
    pub fn ascii_values<'a>(&'a self, name: &str) -> impl Iterator<Item = &'a str> {
        self.headers
            .get_all(name)
            .iter()
            .filter_map(|value| value.to_str().ok())
    }

    /// Decodes binary values, including comma-joined and padded peer values.
    pub fn binary_values(&self, name: &str) -> Result<Vec<Vec<u8>>> {
        if !name.ends_with("-bin") {
            return Err(Error::InvalidHeader("binary metadata name"));
        }
        self.headers
            .get_all(name)
            .iter()
            .flat_map(|value| value.as_bytes().split(|b| *b == b','))
            .map(|value| decode_binary(value.trim_ascii()))
            .collect()
    }

    /// Parses application metadata, excluding reserved protocol fields.
    pub fn from_headers(headers: &HeaderMap) -> Result<Self> {
        let mut metadata = Self::new();
        for (name, value) in headers {
            if is_protocol_header(name.as_str()) {
                continue;
            }
            metadata_name(name.as_str())?;
            if name.as_str().ends_with("-bin") {
                for part in value.as_bytes().split(|b| *b == b',') {
                    decode_binary(part.trim_ascii())?;
                }
            }
            // HeaderName and HeaderValue clones share immutable backing storage.
            metadata.headers.append(name.clone(), value.clone());
        }
        Ok(metadata)
    }
}

pub(crate) fn decode_binary(value: &[u8]) -> Result<Vec<u8>> {
    STANDARD_NO_PAD
        .decode(value)
        .or_else(|_| STANDARD.decode(value))
        .map_err(|_| Error::InvalidHeader("binary metadata Base64"))
}

fn metadata_name(name: &str) -> Result<HeaderName> {
    if name.is_empty()
        || is_protocol_header(name)
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_-.".contains(&b))
    {
        return Err(Error::InvalidHeader("metadata name"));
    }
    HeaderName::from_bytes(name.as_bytes()).map_err(|_| Error::InvalidHeader("metadata name"))
}

fn is_protocol_header(name: &str) -> bool {
    name.starts_with("grpc-")
        || name.starts_with(':')
        || matches!(
            name,
            "content-type" | "te" | "user-agent" | "content-length" | "host"
        )
}
