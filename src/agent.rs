//! client-side tunnel protocol implementation

use std::{
    collections::HashMap,
    io::Result,
    net::{Ipv6Addr, SocketAddr},
    sync::Arc,
};

use parking_lot::{Mutex, RwLock};
use rand::seq::SliceRandom;
use tokio::{
    io::{AsyncWriteExt, copy},
    net::{TcpListener, TcpStream},
};
use zerortt::{
    futures::{Group, QuicConn, QuicConnector, QuicStream},
    poll::{StreamKind, Token},
    quiche,
};

use crate::metrics::AsyncMetricWrite;

/// Forward agent, passes tcp traffic through quic tunnel.
pub struct Agent {
    /// Listen incoming tcp streams.
    listener: TcpListener,
    /// Pooled connector for `O3` servers.
    connector: Arc<PoolConnector>,
}

impl Agent {
    /// Create a new agent.
    pub async fn new(
        local: SocketAddr,
        remote: Vec<SocketAddr>,
        config: quiche::Config,
    ) -> Result<Self> {
        let group = Group::bind((Ipv6Addr::UNSPECIFIED, 0), None)?;

        let local_addr = group.local_addrs().copied().next().unwrap();

        let background = group.clone();

        std::thread::spawn(|| {
            if let Err(err) = background.run() {
                log::error!("quic background worker is stopped, {}", err);
            } else {
                log::warn!("quic background worker is stopped");
            }
        });

        Ok(Self {
            listener: TcpListener::bind(local).await?,
            connector: Arc::new(PoolConnector {
                config: Mutex::new(config),
                local_addr,
                remote_server_addrs: remote,
                connector: QuicConnector::from(group),
                active_conns: Default::default(),
            }),
        })
    }

    /// Run the main event loop of this `Agent`.
    pub async fn run(self) -> Result<()> {
        loop {
            let (stream, raddr) = self.listener.accept().await?;

            let connector = self.connector.clone();
            // Handle incoming TCP streams
            tokio::spawn(async move {
                log::info!("handle incoming, from={}", raddr);

                if let Err(err) = connector.handle_incoming_tcp_stream(stream, raddr).await {
                    log::error!("handle incoming, from={}, err={}", raddr, err);
                }
            });
        }
    }
}

struct PoolConnector {
    /// local address the `QUIC` socket bound to.
    local_addr: SocketAddr,
    /// `O3` server addresses.
    remote_server_addrs: Vec<SocketAddr>,
    /// Shared `QUIC` configuration.
    config: Mutex<quiche::Config>,
    /// Group of quic connections.
    connector: QuicConnector,
    /// Active `QUIC` connections.
    active_conns: RwLock<HashMap<Token, Arc<QuicConn>>>,
}

impl PoolConnector {
    async fn handle_incoming_tcp_stream(
        self: Arc<Self>,
        input: TcpStream,
        raddr: SocketAddr,
    ) -> Result<()> {
        let oread = Arc::new(self.open_stream().await?);

        let owrite = oread.clone();

        let (mut iread, iwrite) = input.into_split();

        static GAUGE_FORWARD: std::sync::LazyLock<Option<metricrs::Gauge>> =
            std::sync::LazyLock::new(|| {
                metricrs::global::get_global_registry().map(|registry| {
                    use metricrs::*;
                    registry.gauge(Token::new("pipelines.forward", &[]))
                })
            });

        static GAUGE_BACKWARD: std::sync::LazyLock<Option<metricrs::Gauge>> =
            std::sync::LazyLock::new(|| {
                metricrs::global::get_global_registry().map(|registry| {
                    use metricrs::*;
                    registry.gauge(Token::new("pipelines.backward", &[]))
                })
            });

        tokio::spawn(async move {
            log::trace!("create pipeline, from={}, to={}", raddr, owrite);

            if let Some(gauge) = GAUGE_FORWARD.as_ref() {
                gauge.increment(1.0);
            }

            let mut write = AsyncMetricWrite::new(
                owrite.as_ref(),
                "agent.forward",
                &[("id", &format!("{} => {}", raddr, owrite))],
            );

            match copy(&mut iread, &mut write).await {
                Ok(data) => {
                    log::trace!(
                        "pipeline is closed, from={}, to={}, len={}",
                        raddr,
                        owrite,
                        data
                    );
                }
                Err(err) => {
                    log::trace!(
                        "pipeline is closed, from={}, to={}, err={}",
                        raddr,
                        owrite,
                        err
                    );
                }
            }

            if let Some(gauge) = GAUGE_FORWARD.as_ref() {
                gauge.decrement(1.0);
            }

            _ = owrite.as_ref().shutdown().await;
        });

        tokio::spawn(async move {
            log::trace!("create pipeline, from={}, to={}", oread, raddr);

            if let Some(gauge) = GAUGE_BACKWARD.as_ref() {
                gauge.increment(1.0);
            }

            let mut iwrite = AsyncMetricWrite::new(
                iwrite,
                "agent.backward",
                &[("id", &format!("{} => {}", oread, raddr))],
            );
            match copy(&mut oread.as_ref(), &mut iwrite).await {
                Ok(data) => {
                    log::trace!(
                        "pipeline is closed, from={}, to={}, len={}",
                        oread,
                        raddr,
                        data
                    );
                }
                Err(err) => {
                    log::trace!(
                        "pipeline is closed, from={}, to={}, err={}",
                        oread,
                        raddr,
                        err
                    );
                }
            }

            if let Some(gauge) = GAUGE_BACKWARD.as_ref() {
                gauge.decrement(1.0);
            }

            _ = iwrite.shutdown().await;
        });

        Ok(())
    }

    async fn open_stream(self: &Arc<Self>) -> Result<QuicStream> {
        loop {
            let mut invalid_conns = vec![];

            let mut stream = None;

            let active_conns = self
                .active_conns
                .read()
                .values()
                .cloned()
                .collect::<Vec<_>>();

            for conn in active_conns {
                match conn.open(StreamKind::Bidi, true).await {
                    Ok(opened) => {
                        stream = Some(opened);
                        break;
                    }
                    Err(_) => {
                        invalid_conns.push(conn.token());
                    }
                }
            }

            {
                let mut active_conns = self.active_conns.write();

                for token in invalid_conns {
                    log::info!("remove connection, {:?}", token);
                    active_conns.remove(&token);
                }
            }

            // Successfully opened a new outbound stream !!
            if let Some(stream) = stream {
                return Ok(stream);
            }

            let mut remote_server_addrs = self.remote_server_addrs.clone();
            remote_server_addrs.shuffle(&mut rand::rng());

            let conn = self.clone().connector.create(
                None,
                self.local_addr,
                remote_server_addrs[0],
                // blocking other `open_stream` ops.
                &mut self.config.lock(),
            )?;

            conn.is_established().await?;

            self.active_conns
                .write()
                .insert(conn.token(), Arc::new(conn));
        }
    }
}
