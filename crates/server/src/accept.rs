use std::io::{Error, ErrorKind};

pub(super) fn is_transient(error: &Error) -> bool {
    if matches!(
        error.kind(),
        ErrorKind::WouldBlock | ErrorKind::ConnectionAborted
    ) {
        return true;
    }
    #[cfg(unix)]
    if matches!(
        error.raw_os_error(),
        Some(libc::EMFILE | libc::ENFILE | libc::ECONNABORTED | libc::ENOBUFS)
    ) {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_transient_errors_do_not_stop_the_server() {
        for (kind, expected) in [
            (ErrorKind::WouldBlock, true),
            (ErrorKind::ConnectionAborted, true),
            (ErrorKind::PermissionDenied, false),
            (ErrorKind::InvalidInput, false),
            (ErrorKind::Interrupted, false),
            (ErrorKind::Other, false),
        ] {
            assert_eq!(is_transient(&Error::from(kind)), expected, "{kind:?}");
        }
        #[cfg(unix)]
        for (errno, expected) in [
            (libc::EMFILE, true),
            (libc::ENFILE, true),
            (libc::ECONNABORTED, true),
            (libc::ENOBUFS, true),
            (libc::EACCES, false),
            (libc::EINVAL, false),
            (libc::EBADF, false),
        ] {
            assert_eq!(
                is_transient(&Error::from_raw_os_error(errno)),
                expected,
                "errno={errno}"
            );
        }
    }
}
