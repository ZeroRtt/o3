//! Metrics utilities for o3 projects.

use std::{
    collections::{BinaryHeap, HashMap},
    io::Result,
    net::SocketAddr,
    task::Poll,
    thread::sleep,
    time::Duration,
};

use metricrs::{Counter, Token, global::get_global_registry};
use metricrs_protobuf::{
    fetch::Fetch,
    protos::memory::{Metadata, Query},
};
use pin_project::pin_project;
use tokio::io::AsyncWrite;

/// Add metrics functions for [`AsyncWrite`]
#[pin_project]
pub struct AsyncMetricWrite<W> {
    #[pin]
    write: W,
    counter: Option<Counter>,
}

impl<W> AsyncMetricWrite<W> {
    /// Creata a new [`AsyncMetricWrite`]
    pub fn new(write: W, name: &str, labels: &[(&str, &str)]) -> Self {
        Self {
            write,
            counter: get_global_registry()
                .map(|registry| registry.counter(Token::new(name, labels))),
        }
    }
}

impl<W> AsyncWrite for AsyncMetricWrite<W>
where
    W: AsyncWrite,
{
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize>> {
        let this = self.project();

        match this.write.poll_write(cx, buf) {
            std::task::Poll::Ready(Ok(send_size)) => {
                // update metrics instrument `COUNTER`.
                if let Some(counter) = &this.counter {
                    counter.increment(send_size as u64);
                }

                return Poll::Ready(Ok(send_size));
            }
            poll => poll,
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        self.project().write.poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        self.project().write.poll_shutdown(cx)
    }
}

pub struct MetricsPrint {
    fetch: Fetch,
    verson: u64,
    metadatas: HashMap<u64, Metadata>,
}

impl MetricsPrint {
    pub fn connect(remote: SocketAddr) -> Result<Self> {
        Ok(Self {
            fetch: Fetch::connect(remote)?,
            verson: 0,
            metadatas: Default::default(),
        })
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            self.fetch_once()?;
            sleep(Duration::from_secs(10));
        }
    }

    fn fetch_once(&mut self) -> Result<()> {
        let query_result = self.fetch.query(Query {
            version: self.verson,
            ..Default::default()
        })?;

        if query_result.version > self.verson {
            log::trace!("update metrics metadata: {:?}", query_result.metadatas);

            self.verson = query_result.version;

            self.metadatas.clear();

            for metadata in query_result.metadatas {
                self.metadatas.insert(metadata.hash, metadata);
            }
        }

        let mut metrics = BinaryHeap::new();

        for value in query_result.values {
            if let Some(metadata) = self.metadatas.get(&value.hash) {
                let labels = metadata
                    .labels
                    .iter()
                    .map(|label| format!("{}={}", label.key, label.value))
                    .collect::<Vec<_>>()
                    .join(",");

                match metadata.instrument.enum_value_or_default() {
                    metricrs_protobuf::protos::memory::Instrument::COUNTER => {
                        metrics.push(format!(
                            "{}, {}, value={}",
                            metadata.name, labels, value.value
                        ));
                    }
                    _ => {
                        metrics.push(format!(
                            "{}, {}, value={}",
                            metadata.name,
                            labels,
                            f64::from_bits(value.value)
                        ));
                    }
                };
            } else {
                metrics.push(format!(
                    "UNKNOWN, hash={}, value={}",
                    value.hash, value.value
                ));
            }
        }

        if !metrics.is_empty() {
            log::info!(target: "metrics", "\n\t{}", metrics.into_vec().join("\n\t"));
        }

        Ok(())
    }
}
