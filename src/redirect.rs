//! server-side tunnel protocol implementation

use std::{io::Result, net::SocketAddr, sync::Arc};

use metricrs::instrument;
use tokio::{
    io::{AsyncWriteExt, copy},
    net::TcpStream,
};
use zrquic::{
    futures::{QuicConn, QuicListener, QuicStream},
    poll::server::Acceptor,
};

use crate::metrics::AsyncMetricWrite;

/// Reverse proxy based on `QUIC` protocol
pub struct Redirect {
    /// Redirect to server address.
    redirect_to: SocketAddr,
    /// socket to accept incoming `QUIC` connection.
    listener: QuicListener,
}

impl Redirect {
    /// Create a new `Redirect` service.
    pub fn new(
        laddrs: Vec<SocketAddr>,
        redirect_to: SocketAddr,
        acceptor: Acceptor,
    ) -> Result<Self> {
        let listener = QuicListener::bind(laddrs.as_slice(), acceptor)?;
        Ok(Self {
            redirect_to,
            listener,
        })
    }

    /// Run the main event loop of this `Redirect` service.
    pub async fn run(self) -> Result<()> {
        loop {
            let conn = self.listener.accept().await?;

            // Handle incoming `QUIC` connection.
            tokio::spawn(async move {
                let token = conn.token();
                log::info!("incoming quic conn, token={:?}", token);
                if let Err(err) = Self::handle_quic_conn(conn, self.redirect_to).await {
                    log::error!("handle quic conn, token={:?}, err={}", token, err);
                } else {
                    log::info!("incoming quic conn, token={:?}, closed", token);
                }
            });
        }
    }

    #[instrument(kind = Gauge, name = "pipelines")]
    async fn handle_quic_conn(conn: QuicConn, redirect_to: SocketAddr) -> Result<()> {
        loop {
            log::trace!("handle quic conn, token={:?}", conn.token());

            let stream = conn.accept().await?;

            tokio::spawn(async move {
                let from = stream.to_string();

                if let Err(err) = Self::handle_quic_stream(stream, redirect_to).await {
                    log::error!(
                        "create pipeline, from={}, to={}, err={}",
                        from,
                        redirect_to,
                        err
                    );
                }
            });
        }
    }

    async fn handle_quic_stream(quic_stream: QuicStream, redirect_to: SocketAddr) -> Result<()> {
        let tcp_stream = TcpStream::connect(redirect_to).await?;
        let (mut tcp_stream_read, tcp_stream_send) = tcp_stream.into_split();
        let quic_stream_send = Arc::new(quic_stream);
        let quic_stream_read = quic_stream_send.clone();

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
            log::trace!("forward, from={}, to={}", quic_stream_read, redirect_to);

            if let Some(gauge) = GAUGE_FORWARD.as_ref() {
                gauge.increment(1.0);
            }

            let mut metrics_write = AsyncMetricWrite::new(
                tcp_stream_send,
                "o3.forward",
                &[("id", &format!("{} => {}", quic_stream_read, redirect_to))],
            );

            match copy(&mut quic_stream_read.as_ref(), &mut metrics_write).await {
                Ok(data) => {
                    log::trace!(
                        "pipeline is closed, from={}, to={}, len={}",
                        quic_stream_read,
                        redirect_to,
                        data
                    );
                }
                Err(err) => {
                    log::trace!(
                        "pipeline is closed, from={}, to={}, err={}",
                        quic_stream_read,
                        redirect_to,
                        err
                    );
                }
            }

            if let Some(gauge) = GAUGE_FORWARD.as_ref() {
                gauge.decrement(1.0);
            }

            _ = metrics_write.shutdown();
        });

        tokio::spawn(async move {
            log::trace!("backward, from={}, to={}", redirect_to, quic_stream_send);

            if let Some(gauge) = GAUGE_BACKWARD.as_ref() {
                gauge.increment(1.0);
            }

            let mut metrics_write = AsyncMetricWrite::new(
                quic_stream_send.as_ref(),
                "o3.backward",
                &[("id", &format!("{} => {}", redirect_to, quic_stream_send))],
            );

            match copy(&mut tcp_stream_read, &mut metrics_write).await {
                Ok(data) => {
                    log::trace!(
                        "pipeline is closed, from={}, to={}, len={}",
                        redirect_to,
                        quic_stream_send,
                        data
                    );
                }
                Err(err) => {
                    log::trace!(
                        "pipeline is closed, from={}, to={}, err={}",
                        redirect_to,
                        quic_stream_send,
                        err
                    );
                }
            }

            if let Some(gauge) = GAUGE_BACKWARD.as_ref() {
                gauge.decrement(1.0);
            }

            _ = metrics_write.shutdown();
        });

        Ok(())
    }
}
