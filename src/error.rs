use core::fmt;
use std::panic::Location;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use tracing::debug;

pub struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error> + fmt::Debug,
{
    #[track_caller]
    fn from(error: E) -> Self {
        debug!(location=?Location::caller(), ?error);
        Self(error.into())
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
