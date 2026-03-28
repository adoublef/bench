use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use csv_async::AsyncWriterBuilder;
use futures_util::{Stream, StreamExt as _, TryStreamExt as _};
use serde::{Deserialize, Serialize};
use std::io;
use tokio::{io::duplex, sync::mpsc, task::JoinSet};
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

// NOTE, due to defining an error here, we would need to define the
// HttpClient here too else I don't really know how we can define
// a module level error properly

#[async_trait]
pub trait Client: Clone + Send + Sync + 'static {
    // type Error;

    async fn regions(
        &self,
        url: &Url,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<u32>> + Send + Unpin + 'static>;
    async fn max_pages(&self, url: &Url, region: u32) -> anyhow::Result<u32>;
    async fn orders(
        &self,
        url: &Url,
        region: u32,
        page: u32,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<Order>> + Send + Unpin + 'static>;
}

// From anyhow?

#[derive(Debug, Clone)]
pub struct Handler<C> {
    client: C,
}

impl<C> Handler<C>
where
    C: Client,
{
    pub fn new(client: C) -> Self {
        Self { client }
    }

    pub fn order_stream(
        &self,
        base_url: Url,
        has_header: bool,
    ) -> impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static {
        let mut set = JoinSet::new();

        let (tx, regions) = mpsc::channel::<u32>(DEFAULT_BUF_SIZE);
        set.spawn({
            let client = self.client.clone();
            let base_url = base_url.clone(); // we form the string here?
            async move {
                let mut stream = client.regions(&base_url).await?;
                while let Some(id) = stream.try_next().await? {
                    tx.send(id).await?;
                }
                anyhow::Ok(())
            }
            .instrument(trace_span!("regions"))
        });

        let (tx, queries) = mpsc::channel::<(u32, u32)>(DEFAULT_BUF_SIZE);
        set.spawn({
            let client = self.client.clone();
            let base_url = base_url.clone();
            async move {
                ReceiverStream::new(regions)
                    .map(anyhow::Ok)
                    .try_for_each_concurrent(DEFAULT_LIMIT, async |region| {
                        async {
                            let last = client.max_pages(&base_url, region).await?;
                            for page in 1..=last {
                                tx.send((region, page)).await?;
                            }
                            anyhow::Ok(())
                        }
                        .instrument(trace_span!("pages", region))
                        .await
                    })
                    .await?;
                anyhow::Ok(())
            }
        });

        let (tx, mut orders) = mpsc::channel::<Order>(DEFAULT_BUF_SIZE);
        set.spawn({
            let client = self.client.clone();
            let base_url = base_url.clone();
            async move {
                ReceiverStream::new(queries)
                    .map(anyhow::Ok)
                    .try_for_each_concurrent(DEFAULT_LIMIT, async |(region, page)| {
                        async {
                            let mut stream = client.orders(&base_url, region, page).await?;
                            while let Some(order) = stream.try_next().await? {
                                tx.send(order).await?;
                            }
                            anyhow::Ok(())
                        }
                        .instrument(trace_span!("orders", region, page))
                        .await
                    })
                    .await?;
                anyhow::Ok(())
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
                anyhow::Ok(())
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
