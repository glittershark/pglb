use std::borrow::Cow;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};

use clap::{Parser, ValueEnum};
use color_eyre::eyre::{Context, Result};
use futures::future::{try_select, Either};
use message::BackendMessageType;
use num_traits::FromPrimitive;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{self, TcpListener, TcpStream};
use tracing::{debug, error, info, trace, trace_span, Instrument};

use crate::message::FrontendMessageType;

mod message;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LogFormat {
    /// Corresponds to [`tracing_subscriber::fmt::format::Compact`]
    Compact,

    /// Corresponds to [`tracing_subscriber::fmt::format::Full`]
    Full,

    /// Corresponds to [`tracing_subscriber::fmt::format::Pretty`]
    Pretty,

    /// Corresponds to [`tracing_subscriber::fmt::format::Json`]
    Json,
}

#[derive(Debug, Error)]
#[error("Invalid log format '{0}'; expected one of 'compact', 'full', 'pretty', or 'json'")]
pub struct InvalidLogFormat(String);

impl FromStr for LogFormat {
    type Err = InvalidLogFormat;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "compact" => Ok(Self::Compact),
            "full" => Ok(Self::Full),
            "pretty" => Ok(Self::Pretty),
            "json" => Ok(Self::Json),
            _ => Err(InvalidLogFormat(s.to_owned())),
        }
    }
}

/// a better, faster postgres connection pool
#[derive(Parser)]
pub struct Options {
    /// TCP address to bind to
    #[clap(long, short = 'b', default_value = "0.0.0.0:5433")]
    pub bind: SocketAddr,

    /// TCP address (host:port) of upstream PostgreSQL server(s) to connect to
    #[clap(long, short = 'u', required = true)]
    pub upstream: Vec<String>,

    /// Format to use for log events.
    #[clap(long, default_value = "full", value_enum)]
    pub log_format: LogFormat,

    /// Disable colors in all log output
    #[clap(long)]
    pub no_color: bool,

    /// Log level filter for spans and events.
    #[clap(long, default_value = "info")]
    pub log_level: String,
}

struct Upstream {
    addrs: &'static [SocketAddr],
    next_addr: AtomicUsize,
}

impl Upstream {
    pub(crate) fn new(addrs: Vec<SocketAddr>) -> &'static Self {
        Box::leak(Box::new(Upstream {
            addrs: Box::leak(addrs.into_boxed_slice()),
            next_addr: AtomicUsize::new(0),
        }))
    }

    fn next_addr(&self) -> SocketAddr {
        self.addrs[self.next_addr.fetch_add(1, Ordering::Relaxed) % self.addrs.len()]
    }

    pub(crate) async fn connection(&self) -> Result<TcpStream> {
        let upstream_addr = self.next_addr();
        trace!(%upstream_addr);
        Ok(TcpStream::connect(upstream_addr).await?)
    }
}

const BUFFER_SIZE: usize = 16 * 1024;

#[derive(Debug)]
struct Packet<'a> {
    msg_type: u8,
    contents: Vec<Cow<'a, [u8]>>,
}

impl<'a> Packet<'a> {
    #[allow(dead_code)]
    fn into_owned(self) -> Packet<'static> {
        Packet {
            msg_type: self.msg_type,
            contents: self
                .contents
                .into_iter()
                .map(|b| Cow::Owned(b.into_owned()))
                .collect(),
        }
    }

    fn frontend_message_type(&self) -> FrontendMessageType {
        FrontendMessageType::from_u8(self.msg_type)
            .unwrap_or_else(|| panic!("Unknown frontend message type {}", self.msg_type))
    }

    fn backend_message_type(&self) -> BackendMessageType {
        BackendMessageType::from_u8(self.msg_type)
            .unwrap_or_else(|| panic!("Unknown backend message type {}", self.msg_type))
    }

    fn append(&mut self, buf: &[u8]) {
        match self.contents.last_mut() {
            Some(contents) => contents.to_mut().extend_from_slice(buf),
            None => todo!(),
        }
    }
}

struct Packets<'sniffer, 'buf> {
    sniffer: &'sniffer mut MessageSniffer,
    buf: &'buf [u8],
}

impl<'sniffer, 'buf> Iterator for Packets<'sniffer, 'buf> {
    type Item = Packet<'buf>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            return None;
        }

        let msg_type = if self.sniffer.next_msg == u64::MAX {
            FrontendMessageType::Startup as u8
        } else if let Some(msg_type) = self.buf.get(self.sniffer.next_msg as usize) {
            self.buf = &self.buf[(self.sniffer.next_msg as usize + 1)..];
            *msg_type
        } else {
            self.sniffer.next_msg -= self.buf.len() as u64;
            match &mut self.sniffer.unfinished_packet {
                Some(packet) => {
                    packet.append(self.buf);
                }
                None => {
                    panic!(
                        "Internal invariant failed: next_msg off end of buffer, but no \
                         unfinished packet"
                    );
                }
            }
            return None;
        };

        if self.buf.len() < 4 {
            // Looks like the PostgreSQL server sends these empty 'N' packets on startup; idk why
            // (it's not documented)
            debug_assert!(msg_type == b'N');
            return None;
        }

        let len = u32::from_be_bytes(
            // TODO: what if this crosses packet boundaries? O.o
            self.buf[0..4].try_into().expect("4 bytes read"),
        );

        if (len as usize) > self.buf.len() {
            self.sniffer.unfinished_packet = Some(Packet {
                msg_type,
                contents: vec![Cow::Owned(self.buf.into())],
            });
            None
        } else {
            let contents = vec![Cow::Borrowed(&self.buf[..(len as _)])];
            self.buf = &self.buf[(len as usize)..];
            self.sniffer.next_msg = 0;
            Some(Packet { msg_type, contents })
        }
    }
}

#[derive(Debug)]
struct MessageSniffer {
    next_msg: u64,
    unfinished_packet: Option<Packet<'static>>,
}

impl MessageSniffer {
    pub(crate) fn downstream() -> Self {
        Self {
            next_msg: u64::MAX,
            unfinished_packet: None,
        }
    }

    pub(crate) fn upstream() -> Self {
        Self {
            next_msg: 0,
            unfinished_packet: None,
        }
    }

    fn packets<'sniffer, 'buf>(&'sniffer mut self, buf: &'buf [u8]) -> Packets<'sniffer, 'buf> {
        Packets { sniffer: self, buf }
    }
}

#[derive(Debug)]
struct Backend {
    upstream_sniffer: MessageSniffer,
    downstream_sniffer: MessageSniffer,
}

impl Backend {
    fn new() -> Self {
        Self {
            upstream_sniffer: MessageSniffer::upstream(),
            downstream_sniffer: MessageSniffer::downstream(),
        }
    }

    async fn run(mut self, mut downstream: TcpStream, upstream: &'static Upstream) -> Result<()> {
        let mut upstream = upstream.connection().await?;

        let mut upstream_buf = Box::new([0; BUFFER_SIZE]);
        let mut downstream_buf = Box::new([0; BUFFER_SIZE]);

        loop {
            match try_select(
                Box::pin(upstream.read(upstream_buf.as_mut())),
                Box::pin(downstream.read(downstream_buf.as_mut())),
            )
            .await
            .map_err(|e| e.factor_first().0)?
            {
                Either::Left((n, _)) if n > 0 => {
                    trace!(read_upstream_bytes = n);
                    let buf = &upstream_buf[..n];
                    for packet in self.upstream_sniffer.packets(buf) {
                        trace!(backend_message = ?packet.backend_message_type());
                    }
                    downstream.write_all(buf).await?;
                }
                Either::Right((n, _)) if n > 0 => {
                    trace!(read_downstream_bytes = n);
                    let buf = &downstream_buf[..n];
                    for packet in self.downstream_sniffer.packets(buf) {
                        trace!(frontend_message = ?packet.frontend_message_type());
                    }
                    upstream.write_all(buf).await?;
                }
                _ => {}
            }
        }
    }
}

pub async fn run(opts: Options) -> Result<()> {
    let Options { bind, upstream, .. } = opts;

    let mut upstream_addrs = Vec::with_capacity(upstream.len());
    for host in upstream {
        for addr in net::lookup_host(&host)
            .await
            .wrap_err_with(|| format!("Failed to lookup host for upstream address {host}"))?
        {
            debug!(%addr, "Resolved upstream addr");
            upstream_addrs.push(addr);
        }
    }
    let upstream = Upstream::new(upstream_addrs);

    let listener = TcpListener::bind(bind).await?;
    info!(%bind);
    loop {
        let (socket, addr) = listener.accept().await?;
        let span = trace_span!("connection", %addr);
        tokio::spawn(
            async move {
                trace!("Accepted connection");
                if let Err(error) = Backend::new().run(socket, upstream).await {
                    error!(%error);
                }
            }
            .instrument(span),
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    use tracing_subscriber::prelude::*;

    color_eyre::install()?;
    let opts = Options::parse();

    let subscriber = tracing_subscriber::Registry::default()
        .with(tracing_subscriber::EnvFilter::new(&opts.log_level))
        .with({
            let layer = tracing_subscriber::fmt::layer();
            match opts.log_format {
                LogFormat::Compact => layer.compact().boxed(),
                LogFormat::Full => layer.boxed(),
                LogFormat::Pretty => layer.pretty().boxed(),
                LogFormat::Json => layer.json().with_current_span(true).boxed(),
            }
        })
        .with(tracing_error::ErrorLayer::default());
    tracing::subscriber::set_global_default(subscriber)?;

    run(opts).await
}
