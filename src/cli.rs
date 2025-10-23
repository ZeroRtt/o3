use std::{
    io::{Error, ErrorKind, Result},
    net::{IpAddr, SocketAddr},
    ops::Range,
    path::PathBuf,
};

use clap::{Parser, Subcommand};
use zrquic::quiche;

fn parse_port_range(arg: &str) -> std::result::Result<Range<u16>, String> {
    let parts = arg.split(":").collect::<Vec<_>>();

    match parts.len() {
        1 => {
            let port = parts[0]
                .parse::<u16>()
                .map_err(|err| format!("failed to parse port: {}", err.to_string()))?;

            return Ok(port..port + 1);
        }
        2 => {
            let from = parts[0]
                .parse::<u16>()
                .map_err(|err| format!("failed to parse port(from): {}", err.to_string()))?;

            let to = parts[1]
                .parse::<u16>()
                .map_err(|err| format!("failed to parse port(to): {}", err.to_string()))?;

            if !(to > from) {
                return Err(format!("failed to parse port range: ensure `to > from`"));
            }

            return Ok(from..to);
        }
        _ => {
            return Err("Invalid port range, valid syntax: `xxx:xxx` or `xxx`".to_owned());
        }
    }
}

/// Cli parser for apps.
#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// Set agent proto list.
    #[arg(long, value_name = "PROTO_LIST", default_values_t = ["rproxy".to_string()])]
    pub protos: Vec<String>,

    #[cfg(feature = "agent")]
    /// Specify redirect server listening address.
    #[arg(short = 's', long = "server", value_name = "ADDR")]
    pub redirect_server_ip: IpAddr,

    #[cfg(feature = "agent")]
    /// Specify the redirect server listening port range.
    #[arg(short = 'p', long = "server-ports", value_name = "PORT", value_parser=parse_port_range)]
    pub redirect_server_port_range: Range<u16>,

    #[cfg(feature = "o3")]
    /// Specify redirect server listening address list.
    #[arg(short = 'l', long = "listen", value_name = "ADDR")]
    pub redirect_ip: Option<Vec<IpAddr>>,

    #[cfg(feature = "o3")]
    /// Specify the redirect server listening port range.
    #[arg(short = 'p', long = "port", value_name = "PORT", value_parser=parse_port_range)]
    pub redirect_port_range: Range<u16>,

    /// Configure the certificate chain file(PEM).
    #[arg(short, long, value_name = "PEM_FILE")]
    pub cert: Option<PathBuf>,

    /// Configure the private chain file(PEM).
    #[arg(short, long, value_name = "PEM_FILE", default_value = "redirect.key")]
    pub key: PathBuf,

    #[cfg(feature = "o3")]
    /// Specifies a file where trusted CA certificates are stored for the purposes of peer's certificate verification.
    #[arg(short, long, value_name = "PEM_FILE")]
    verify_peer: Option<PathBuf>,

    /// set maximum transmission unit for UDP packets
    #[arg(long, value_name = "STREAMS", default_value_t = 1200)]
    pub mtu: usize,

    /// set the congestion control algorithm. availables: `cubic`, `reno`, `bbr`, `bbr2` and `bbr2_gcongestion`
    #[arg(long, value_name = "algorithem", default_value = "bbr")]
    pub cc: String,

    /// Sets the initial_max_stream_data_bidi_remote transport parameter.
    ///
    /// When set to a non-zero value quiche will only allow at most v bytes of incoming stream data
    /// to be buffered for each locally-initiated bidirectional stream (that is, data that is not
    /// yet read by the application) and will allow more data to be received as the buffer is
    /// consumed by the application.
    ///
    /// When set to zero, either explicitly or via the default, quiche will not give any flow control
    /// to the peer, preventing it from sending any stream data.
    #[arg(long, value_name = "SIZE", default_value_t = 1024 * 1024 * 4)]
    pub initial_max_stream_data: u64,

    /// Sets the max_idle_timeout transport parameter, in milliseconds.
    #[arg(long, value_name = "SIZE", default_value_t = 60 * 1000)]
    pub max_idle_timeout: u64,

    /// Sets the `max_ack_delay` transport parameter, in milliseconds.
    #[arg(long, value_name = "SIZE", default_value_t = 30)]
    pub max_ack_delay: u64,

    /// Sets the ack_delay_exponent transport parameter.
    #[arg(long, value_name = "SIZE", default_value_t = 3)]
    pub ack_frequency_exponent: u64,

    /// Sets the quiche `initial_max_streams_bidi` transport parameter.
    ///
    /// When set to a non-zero value quiche will only allow v number of concurrent remotely-initiated bidirectional
    /// streams to be open at any given time and will increase the limit automatically as streams are completed.
    #[arg(long, value_name = "STREAMS", default_value_t = 100)]
    pub initial_max_streams: u64,

    /// Set the ring buffer size for channel data copying
    #[arg(long, value_name = "SIZE", default_value_t = 10240)]
    pub ring_buffer_size: usize,

    /// Set maximum incoming queue
    #[arg(long, value_name = "SIZE", default_value_t = 30)]
    pub pairing_stream_limits: usize,

    /// Debug mode, print verbose output informations.
    #[arg(short, long, default_value_t = false, action)]
    pub debug: bool,

    /// Collect metrics, and publish on `ADDR`.
    #[arg(short, long, value_name = "ADDR")]
    pub metrics: Option<SocketAddr>,

    #[command(subcommand)]
    pub commands: Commands,
}

impl Cli {
    #[cfg(feature = "agent")]
    pub fn parse_o3_server_addrs(&self) -> Result<Vec<SocketAddr>> {
        let mut laddrs: Vec<SocketAddr> = vec![];

        let redirect_server_ip = if let IpAddr::V4(v4) = self.redirect_server_ip {
            v4.to_ipv6_mapped().into()
        } else {
            self.redirect_server_ip
        };

        for port in self.redirect_server_port_range.clone() {
            laddrs.push(SocketAddr::new(redirect_server_ip, port));
        }

        Ok(laddrs)
    }

    #[cfg(feature = "o3")]
    pub fn parse_redirect_listen_addrs(&self) -> Result<Vec<SocketAddr>> {
        let ips = if let Some(interfaces) = self.redirect_ip.clone() {
            interfaces
        } else {
            use std::net::Ipv6Addr;

            vec![IpAddr::V6(Ipv6Addr::UNSPECIFIED)]
        };
        let mut laddrs: Vec<SocketAddr> = vec![];

        for port in self.redirect_port_range.clone() {
            for ip in &ips {
                laddrs.push(SocketAddr::new(*ip, port));
            }
        }

        Ok(laddrs)
    }

    /// Update quiche `Config` by cli params.
    pub fn quiche_config(&self, config: &mut quiche::Config) -> Result<()> {
        let protos = self
            .protos
            .iter()
            .map(|proto| proto.as_bytes())
            .collect::<Vec<_>>();

        config.set_initial_max_data(self.initial_max_stream_data * self.initial_max_streams);
        config.set_initial_max_stream_data_bidi_local(self.initial_max_stream_data);
        config.set_initial_max_stream_data_bidi_remote(self.initial_max_stream_data);
        config.set_initial_max_stream_data_uni(self.initial_max_stream_data);
        config.set_initial_max_streams_bidi(self.initial_max_streams);
        config.set_initial_max_streams_uni(self.initial_max_streams);
        config.set_max_idle_timeout(self.max_idle_timeout);
        config.set_max_ack_delay(self.max_ack_delay);
        config.set_ack_delay_exponent(self.ack_frequency_exponent);
        config.set_max_send_udp_payload_size(self.mtu);
        config
            .set_cc_algorithm_name(&self.cc)
            .map_err(Error::other)?;

        if let Some(cert) = &self.cert {
            config
                .load_cert_chain_from_pem_file(cert.to_str().unwrap())
                .map_err(|err| {
                    Error::new(
                        ErrorKind::NotFound,
                        format!(
                            "Unable to load certificate chain file {:?}, {}",
                            self.cert, err
                        ),
                    )
                })?;
        }

        config
            .load_priv_key_from_pem_file(self.key.to_str().unwrap())
            .map_err(|err| {
                Error::new(
                    ErrorKind::NotFound,
                    format!("Unable to load key file {:?}, {}", self.key, err),
                )
            })?;

        #[cfg(feature = "o3")]
        if let Some(ca) = &self.verify_peer {
            config
                .load_verify_locations_from_file(ca.to_str().unwrap())
                .map_err(|err| {
                    Error::new(
                        ErrorKind::NotFound,
                        format!("Unable to trusted CA file {:?}, {}", self.cert, err),
                    )
                })?;
        }

        config.set_application_protos(&protos).map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "failed to set application protos as {:?}, {}",
                    self.protos, err
                ),
            )
        })?;

        Ok(())
    }
}

/// Subcommand for redirect apps.
#[derive(Subcommand)]
pub enum Commands {
    #[cfg(feature = "agent")]
    /// Start a agent service.
    Listen {
        /// Specify the redirect target address
        on: Option<SocketAddr>,
    },

    #[cfg(feature = "o3")]
    /// Start a rproxy service.
    Redirect {
        /// Specify the redirect target address
        target: SocketAddr,
    },
}
