use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use csv_async::AsyncWriterBuilder;
use futures_util::{Stream, StreamExt as _, TryStreamExt as _};
use serde::{Deserialize, Serialize};
use std::io;
use tokio::{io::duplex, pin, sync::mpsc, task::JoinSet};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::ReaderStream;
use tracing::{Instrument as _, trace_span};
use url::Url;

const DEFAULT_BUF_SIZE: usize = 1;
const DEFAULT_LIMIT: usize = 1 << 0;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Order {
    duration: i64,
    is_buy_order: bool,
    issued: String,
    location_id: i64,
    min_volume: i64,
    order_id: i64,
    price: f64,
    range: String,
    system_id: i64,
    type_id: i64,
    volume_remain: i64,
    volume_total: i64,
}

#[async_trait]
pub trait Client {
    async fn regions(
        &self,
        url: Url,
    ) -> Result<impl Stream<Item = Result<u32, anyhow::Error>> + Send + 'static, anyhow::Error>;
    async fn max_pages(&self, url: Url, region: u32) -> Result<u32, anyhow::Error>;
    async fn orders(
        &self,
        url: Url,
        region: u32,
        page: u32,
    ) -> Result<impl Stream<Item = Result<Order, anyhow::Error>> + Send + 'static, anyhow::Error>;
}

#[derive(Debug, Clone)]
pub struct Handler<C: Client + Clone + Send + Sync + 'static> {
    client: C,
}

impl<C: Client + Clone + Send + Sync + 'static> Handler<C> {
    pub async fn order_stream(
        &self,
        base_url: Url,
        has_header: bool,
    ) -> impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static {
        let mut set = JoinSet::new();

        let (tx, regions) = mpsc::channel::<u32>(DEFAULT_BUF_SIZE);
        set.spawn({
            let client = self.client.clone();
            let base_url = base_url.clone();
            async move {
                let stream = client.regions(base_url.clone()).await?;
                pin!(stream);
                while let Some(id) = stream.try_next().await? {
                    tx.send(id).await?;
                }
                Ok::<_, anyhow::Error>(())
            }
            .instrument(trace_span!("regions"))
        });

        let (tx, queries) = mpsc::channel::<(u32, u32)>(DEFAULT_BUF_SIZE);
        set.spawn({
            let client = self.client.clone();
            let base_url = base_url.clone();
            async move {
                ReceiverStream::new(regions)
                    .map(Ok::<_, anyhow::Error>)
                    .try_for_each_concurrent(DEFAULT_LIMIT, |region| {
                        let tx = tx.clone();
                        let client = client.clone();
                        let base_url = base_url.clone();
                        async move {
                            let last = client.max_pages(base_url, region).await?;

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

        let (tx, mut orders) = mpsc::channel::<Order>(DEFAULT_BUF_SIZE);
        set.spawn({
            let client = self.client.clone();
            let base_url = base_url.clone();
            async move {
                ReceiverStream::new(queries)
                    .map(Ok::<_, anyhow::Error>)
                    .try_for_each_concurrent(DEFAULT_LIMIT, |(region, page)| {
                        let tx = tx.clone();
                        let client = client.clone();
                        let base_url = base_url.clone(); // cloning twice?
                        async move {
                            let stream = client.orders(base_url.clone(), region, page).await?;
                            pin!(stream);
                            while let Some(order) = stream.try_next().await? {
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
                let msg = msg?;
                yield msg
            }
            for res in set.join_all().await {
                res.map_err(io::Error::other)?;
            }
        }
    }
}
