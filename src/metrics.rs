//! Metrics utilities for o3 projects.

use std::{
    collections::HashMap, io::Result, net::SocketAddr, task::Poll, thread::sleep, time::Duration,
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
    print_interval: Duration,
}

impl MetricsPrint {
    pub fn connect(remote: SocketAddr, print_interval: Duration) -> Result<Self> {
        Ok(Self {
            fetch: Fetch::connect(remote)?,
            verson: 0,
            metadatas: Default::default(),
            print_interval,
        })
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            self.fetch_once()?;
            sleep(self.print_interval);
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

        let mut pipelines_forward = None;
        let mut pipelines_backward = None;
        let mut conns = None;
        let mut forwards = vec![];
        let mut backwards = vec![];
        let mut quic_socket_send = vec![];
        let mut quic_socket_recv = vec![];

        for value in query_result.values {
            if let Some(metadata) = self.metadatas.get(&value.hash) {
                match metadata.name.as_str() {
                    "pipeline.forward" => {
                        pipelines_forward = Some(f64::from_bits(value.value));
                    }
                    "forward" => {
                        let labels = metadata
                            .labels
                            .iter()
                            .map(|label| format!("{}={}", label.key, label.value))
                            .collect::<Vec<_>>()
                            .join(",");

                        forwards.push(format!("{}, value={}", labels, value.value));
                    }
                    "pipeline.backward" => {
                        pipelines_backward = Some(f64::from_bits(value.value));
                    }
                    "backward" => {
                        let labels = metadata
                            .labels
                            .iter()
                            .map(|label| format!("{}={}", label.key, label.value))
                            .collect::<Vec<_>>()
                            .join(",");

                        backwards.push(format!("{}, value={}", labels, value.value));
                    }
                    "conns" => {
                        conns = Some(f64::from_bits(value.value));
                    }
                    "quic.socket.send" => {
                        let labels = metadata
                            .labels
                            .iter()
                            .map(|label| format!("{}={}", label.key, label.value))
                            .collect::<Vec<_>>()
                            .join(",");

                        quic_socket_send.push(format!("{}, value={}", labels, value.value));
                    }
                    "quic.socket.recv" => {
                        let labels = metadata
                            .labels
                            .iter()
                            .map(|label| format!("{}={}", label.key, label.value))
                            .collect::<Vec<_>>()
                            .join(",");

                        quic_socket_recv.push(format!("{}, value={}", labels, value.value));
                    }
                    _ => {}
                }
            }
        }

        log::info!(
            target: "metrics",
            "conns={}, pipeline.forward={}, pipeline.backward={}",
            conns.unwrap_or(0.0),
            pipelines_forward.unwrap_or(0.0),
            pipelines_backward.unwrap_or(0.0)
        );

        log::info!(target: "metrics", "forward:");

        for forward in forwards {
            log::info!(target: "metrics", "{}",forward);
        }

        log::info!(target: "metrics", "backward:");

        for backward in backwards {
            log::info!(target: "metrics", "{}",backward);
        }

        log::info!(target: "metrics", "socket send:");

        for v in quic_socket_send {
            log::info!(target: "metrics", "{}",v);
        }

        log::info!(target: "metrics", "socket recv:");

        for v in quic_socket_recv {
            log::info!(target: "metrics", "{}",v);
        }

        Ok(())
    }
}
