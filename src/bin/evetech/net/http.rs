use crate::order::{Client, Handler, Order};
use anyhow::Context as _;
use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{Stream, TryStreamExt as _};
use http_json_stream::{JsonPart, JsonStream};
use reqwest::{StatusCode, header};
use serde::Deserialize;
use url::Url;

// trait alias (not to confuse with unstable feature with the same name)
// trait CsvByteStreamHandle: CsvByteStream + Clone + Send + Sync + 'static {}
// impl<T: ?Sized + CsvByteStream + Clone + Send + Sync + 'static> CsvByteStreamHandle for T {}

#[derive(Debug, Clone)]
struct AppState<T: Client + Clone + Send + Sync + 'static>(Handler<T>); // Handler now needs to be generic

fn app<T: Client + Clone + Send + Sync + 'static>(handler: Handler<T>) -> Router {
    Router::new()
        .route("/", get(handle_csv))
        .with_state(AppState(handler))
}

#[derive(Debug, Clone, Default)]
pub struct AppClient(reqwest::Client);

#[async_trait]
impl Client for AppClient {
    async fn regions(
        &self,
        url: Url,
    ) -> Result<impl Stream<Item = Result<u32, anyhow::Error>> + Send + 'static, anyhow::Error>
    {
        let response = self
            .0
            .get(url.join("/v1/universe/regions")?)
            .send()
            .await?
            .error_for_status()?;
        let stream = JsonStream::<_, _, u32>::process(response, JsonPart::level(1))
            .map_err(|e| anyhow::anyhow!(e.to_string()));
        Ok(stream) // map the error here
    }

    async fn max_pages(&self, url: Url, region: u32) -> Result<u32, anyhow::Error> {
        Ok(self
            .0
            .head(url.join(&format!("/v1/markets/{region}/orders"))?)
            .send()
            .await?
            .error_for_status()?
            .headers()
            .get("x-pages")
            .context("Missing x-pages header")?
            .to_str()?
            .parse::<u32>()?)
    }

    async fn orders(
        &self,
        url: Url,
        region: u32,
        page: u32,
    ) -> Result<impl Stream<Item = Result<Order, anyhow::Error>> + Send + 'static, anyhow::Error>
    {
        let response = self
            .0
            .get(url.join(&format!("/v1/markets/{region}/orders?page={page}"))?)
            .send()
            .await?
            .error_for_status()?;
        let stream = JsonStream::<_, _, Order>::process(response, JsonPart::level(1))
            .map_err(|e| anyhow::anyhow!(e.to_string()));
        Ok(stream) // map the error here
    }
}

#[derive(Deserialize)]
struct CsvParams {
    base_url: Url,
    has_header: Option<bool>,
}

async fn handle_csv<T: Client + Clone + Send + Sync + 'static>(
    State(AppState(handler)): State<AppState<T>>,
    Query(CsvParams {
        base_url,
        has_header,
    }): Query<CsvParams>,
) -> Result<Response, AppError> {
    let has_header = has_header.unwrap_or_default();
    let stream = handler.order_stream(base_url, has_header).await;
    let response = Response::builder()
        .header(header::CONTENT_TYPE, mime::TEXT_CSV.essence_str())
        .header(
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"evetech.csv\"",
        )
        .status(StatusCode::OK)
        .body(Body::from_stream(stream))
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    Ok(response)
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
    use crate::order::Order;
    use anyhow::Context;
    use axum::{
        Json, Router,
        extract::Path,
        http::Response,
        routing::{get, head},
    };
    use csv_async::AsyncReaderBuilder;
    use dial9_tokio_telemetry::telemetry::{RotatingWriter, TracedRuntime};
    use futures_util::TryStreamExt;
    use reqwest::{Client, StatusCode, header};
    use std::io;
    use tokio::{net::TcpListener, spawn};
    use tokio_stream::StreamExt;
    use tokio_util::io::StreamReader;
    use url::Url;

    // #[tokio::test]
    #[test]
    fn test_csv_ok() -> anyhow::Result<()> {
        // https://docs.rs/dial9-tokio-telemetry/latest/dial9_tokio_telemetry/#quick-start
        // https://dial9-tokio-telemetry.russell-r-cohen.workers.dev/
        let trace_path = "./trace.bin";
        let writer = RotatingWriter::single_file(trace_path)?;

        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all(); // builder.worker_threads(4).enable_all();

        let (runtime, _guard) = TracedRuntime::builder()
            .with_task_tracking(true)
            .with_trace_path(trace_path)
            .build_and_start(builder, writer)?;

        runtime.block_on(async {
            let num_regions = 1 << 2;
            let num_pages = 1 << 2;
            let num_orders = 1 << 2;

            let has_header = false;

            let (client, api_url) = api_serve(num_regions, num_pages, num_orders).await?;
            let (client, mut url) = serve(client).await?;

            url.query_pairs_mut()
                .append_pair("base_url", api_url.as_str());

            let response = client.get(url).send().await?;
            assert_eq!(response.status(), StatusCode::OK);
            let headers = response.headers();
            let content_type = headers
                .get(header::CONTENT_TYPE)
                .context("Missing content-type header")?;
            assert_eq!(content_type, mime::TEXT_CSV.as_ref());
            // check content-disposition

            // include this info in the headers of the request
            // or the query, so that we can use that in our reader
            let mut rdr = AsyncReaderBuilder::new()
                .has_headers(has_header)
                .buffer_capacity(4 << 10) // not my concern?
                .create_reader(StreamReader::new(
                    response.bytes_stream().map_err(io::Error::other),
                ));
            let mut records = rdr.records();
            let mut num_records = 0;
            while let Some(record) = records.next().await {
                let record = record?;
                assert_eq!(record.len(), 12);
                num_records += 1;
            }
            assert_eq!(num_records, num_regions * num_pages * num_orders);

            Ok::<_, anyhow::Error>(())
        })?;

        Ok(())
    }

    async fn api_serve(
        num_regions: usize,
        num_pages: usize,
        num_orders: usize,
    ) -> anyhow::Result<(Client, Url)> {
        let listener = TcpListener::bind("0.0.0.0:0").await?;
        let addr = listener.local_addr()?;

        let client = Client::builder().build()?; // modify the client
        let url = Url::parse(&format!("http://{addr}"))?;

        // a vector of ids (stargeting at 10000)
        // a vector of orders
        let regions = (1..=num_regions).map(|n| 10000 + n).collect::<Vec<_>>();
        let orders = (1..=num_orders)
            .map(|_| Order::default())
            .collect::<Vec<_>>();

        let app = Router::new()
            // GET "/v1/universe/regions"
            .route("/v1/universe/regions", get(async move |()| Json(regions)))
            // HEAD "/v1/markets/{region}/orders"
            .route(
                "/v1/markets/{region}/orders",
                head(async move |Path(_): Path<usize>| {
                    Response::builder()
                        .header("x-pages", num_pages)
                        .body(Body::empty())
                        .unwrap()
                }),
            )
            // GET "/v1/markets/{region}/orders?page={page}"
            .route(
                "/v1/markets/{region}/orders",
                get(async move |Path(_): Path<usize>| Json(orders)),
            );

        spawn(async move {
            if let Err(err) = axum::serve(listener, app).await {
                eprintln!("server error: {err}");
            }
        });

        Ok((client, url))
    }

    async fn serve(api_client: Client) -> anyhow::Result<(Client, Url)> {
        let listener = TcpListener::bind("0.0.0.0:0").await?;
        let addr = listener.local_addr()?;

        let client = Client::new();
        let url = Url::parse(&format!("http://{addr}"))?;

        let handler = Handler::new(AppClient(api_client));

        // spawn new process, how would i close it?
        spawn(async move {
            if let Err(e) = axum::serve(listener, app(handler)).await {
                eprintln!("server error: {e}");
            }
        });

        Ok((client, url))
    }
}
