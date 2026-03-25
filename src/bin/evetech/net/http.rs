use anyhow::Context;
use async_stream::try_stream;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{Query, State},
    http::Response,
    routing::get,
};
use csv_async::{AsyncWriterBuilder, Terminator};
use futures_util::{Stream, StreamExt, TryStreamExt};
use http_json_stream::{JsonPart, JsonStream};
use reqwest::{Client, StatusCode, header};
use serde::Deserialize;
use std::io;
use std::marker::Send;
use tokio::{io::duplex, sync::mpsc, task::JoinSet};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::ReaderStream;
use tracing::{Instrument, trace_span};
use url::Url;

use crate::order::Order;

const DEFAULT_BUF_SIZE: usize = 1;
const DEFAULT_LIMIT: usize = 1 << 0;

pub fn app(client: Client) -> Router {
    Router::new()
        .route("/", get(handle_csv))
        .with_state(AppState(client))
}

#[derive(Debug, Clone)]
struct AppState(Client);

async fn csv_stream(
    client: Client,
    base_url: Url,
    has_header: bool, // better to be a config map
) -> impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static {
    let mut set = JoinSet::new();

    let (tx, regions) = mpsc::channel(DEFAULT_BUF_SIZE);
    set.spawn({
        let client = client.clone();
        let base_url = base_url.clone();
        async move {
            let response = client
                .get(base_url.join("/v1/universe/regions")?)
                .send()
                .await?
                .error_for_status()?;
            // check the content-type & and content-length
            let mut stream = JsonStream::<_, _, u32>::process(response, JsonPart::level(1));

            while let Some(id) = stream
                .try_next()
                .await
                .map_err(|e| anyhow::anyhow!(e.to_string()))?
            {
                tx.send(id).await?;
            }
            Ok::<_, anyhow::Error>(())
        }
        .instrument(trace_span!("regions"))
    });

    let (tx, queries) = mpsc::channel(DEFAULT_BUF_SIZE);
    set.spawn({
        let client = client.clone();
        let base_url = base_url.clone();
        async move {
            ReceiverStream::new(regions)
                .map(Ok::<_, anyhow::Error>)
                .try_for_each_concurrent(DEFAULT_LIMIT, |region| {
                    let tx = tx.clone();
                    let client = client.clone();
                    let base_url = base_url.clone();
                    async move {
                        let last = client
                            .head(base_url.join(&format!("/v1/markets/{region}/orders"))?)
                            .send()
                            .await?
                            .error_for_status()?
                            .headers()
                            .get("x-pages")
                            .context("Missing x-pages header")?
                            .to_str()?
                            .parse::<u32>()?;

                        for page in 1..=last {
                            tx.send((region, page)).await?;
                        }
                        Ok::<_, anyhow::Error>(())
                    }
                    .instrument(trace_span!("pages"))
                })
                .await?;
            Ok::<_, anyhow::Error>(())
        }
    });

    let (tx, mut orders) = mpsc::channel(DEFAULT_BUF_SIZE);
    set.spawn({
        let client = client.clone();
        let base_url = base_url.clone();
        async move {
            ReceiverStream::new(queries)
                .map(Ok::<_, anyhow::Error>)
                .try_for_each_concurrent(DEFAULT_LIMIT, |(region, page)| {
                    let tx = tx.clone();
                    let client = client.clone();
                    let base_url = base_url.clone();
                    async move {
                        let response = client
                            .get(
                                base_url
                                    .join(&format!("/v1/markets/{region}/orders?page={page}"))?,
                            )
                            .send()
                            .await?
                            .error_for_status()?;
                        // support ndjson &/or jsonl
                        // check the content-type & and content-length
                        let mut stream =
                            JsonStream::<_, _, Order>::process(response, JsonPart::level(1));

                        while let Some(order) = stream
                            .try_next()
                            .await
                            .map_err(|e| anyhow::anyhow!(e.to_string()))?
                        {
                            tx.send(order).await?;
                        }
                        Ok::<_, anyhow::Error>(())
                    }
                    .instrument(trace_span!("orders"))
                })
                .await?;
            Ok::<_, anyhow::Error>(())
        }
    });

    let (rx, tx) = duplex(4 << 10);
    set.spawn({
        async move {
            let mut wri = AsyncWriterBuilder::new()
                .has_headers(has_header) // no header
                .buffer_capacity(4 << 10)
                .create_serializer(tx);
            while let Some(order) = orders.recv().await {
                wri.serialize(&order).await?;
            }
            wri.flush().await?;
            Ok::<_, anyhow::Error>(())
        }
        .instrument(trace_span!("csv"))
    });

    let mut stream = ReaderStream::new(rx);
    try_stream! {
        while let Some(msg) = stream.next().await {
            let msg = msg?; // Result<Bytes, Error>
            yield msg
        }
        for res in set.join_all().await {
            res.map_err(io::Error::other)?;
        }
    }
}

#[derive(Deserialize)]
struct CsvParams {
    base_url: Url,
    has_header: Option<bool>,
}

async fn handle_csv(
    State(AppState(client)): State<AppState>,
    Query(CsvParams {
        base_url,
        has_header,
    }): Query<CsvParams>,
) -> Response<Body> {
    let has_header = has_header.unwrap_or_default();
    let stream = csv_stream(client, base_url, has_header).await;
    Response::builder()
        .header(header::CONTENT_TYPE, mime::TEXT_CSV.essence_str())
        .header(
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"evetech.csv\"",
        )
        .status(StatusCode::OK)
        .body(Body::from_stream(stream))
        .unwrap()
}

#[cfg(test)]
mod test {
    use super::*;
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

        let client = Client::new();
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

        // spawn new process, how would i close it?
        spawn(async move {
            if let Err(err) = axum::serve(listener, app(api_client)).await {
                eprintln!("server error: {err}");
            }
        });

        Ok((client, url))
    }
}
