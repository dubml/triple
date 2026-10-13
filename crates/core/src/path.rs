use crate::{Error, Result};

/// Borrowed, case-sensitive `/Service/Method` identifier.
///
/// ```
/// let path = triple_core::Path::parse("/demo.GreetService/Greet")?;
/// assert_eq!(path.service(), "demo.GreetService");
/// assert_eq!(path.method(), "Greet");
/// # Ok::<(), triple_core::Error>(())
/// ```
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct Path<'a> {
    raw: &'a str,
    service: &'a str,
    method: &'a str,
}

impl<'a> Path<'a> {
    /// Parses exactly two nonempty segments, without normalizing their case.
    pub fn parse(path: &'a str) -> Result<Self> {
        let Some((service, method)) = path.strip_prefix('/').and_then(|p| p.split_once('/')) else {
            return Err(Error::InvalidPath);
        };
        if service.is_empty()
            || method.is_empty()
            || method.contains('/')
            || path.contains(['?', '#'])
        {
            return Err(Error::InvalidPath);
        }
        Ok(Self {
            raw: path,
            service,
            method,
        })
    }

    /// Returns the original path unchanged.
    pub fn as_str(self) -> &'a str {
        self.raw
    }

    /// Returns the service name without allocating.
    pub fn service(self) -> &'a str {
        self.service
    }

    /// Returns the method name without allocating.
    pub fn method(self) -> &'a str {
        self.method
    }
}
