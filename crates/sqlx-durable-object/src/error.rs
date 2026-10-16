use sqlx_core::error::Error;

/// The error for something sqlx asks of a driver that Durable Object storage
/// cannot do.
///
/// `Protocol` is the nearest of sqlx's variants: the request is well-formed,
/// the backend just has no way to answer it. Kept in one place so the choice
/// can change without hunting down every call site.
pub(crate) fn unsupported(message: &str) -> Error {
    Error::Protocol(format!("Durable Object storage: {message}"))
}
