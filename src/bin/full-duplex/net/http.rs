use axum::{
    Router,
    response::{IntoResponse, Response},
};
use reqwest::StatusCode;

pub fn app() -> Router {
    Router::new()
}

struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;
    use reqwest::Client;
    use tokio::{net::TcpListener, spawn};
    use url::Url;

    async fn serve() -> Result<(Client, Url)> {
        let listener = TcpListener::bind("0.0.0.0:0").await?;
        let addr = listener.local_addr()?;

        let client = Client::new();
        let url = Url::parse(&format!("http://{addr}"))?;

        spawn(async move {
            if let Err(e) = axum::serve(listener, app()).await {
                eprintln!("server error: {e}");
            }
        });

        Ok((client, url))
    }
}
