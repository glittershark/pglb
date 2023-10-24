use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};

use clap::Parser;
use color_eyre::eyre::Result;
use futures::future::{try_select, Either};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{self, TcpListener, TcpStream};
use tracing::{debug, error, info, trace, trace_span, Instrument};

/// a better, faster postgres connection pool
#[derive(Parser)]
pub struct Options {
    /// TCP address to bind to
    #[clap(long, short = 'b', default_value = "0.0.0.0:5433")]
    pub bind: SocketAddr,

    /// TCP address (host:port) of upstream PostgreSQL server(s) to connect to
    #[clap(long, short = 'u', required = true)]
    pub upstream: Vec<String>,
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

async fn handle_client(mut downstream: TcpStream, upstream: &'static Upstream) -> Result<()> {
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
                downstream.write_all(&upstream_buf[..n]).await?;
            }
            Either::Right((n, _)) if n > 0 => {
                upstream.write_all(&downstream_buf[..n]).await?;
            }
            _ => {}
        }
    }
}

pub async fn run(opts: Options) -> Result<()> {
    let Options { bind, upstream } = opts;

    let mut upstream_addrs = Vec::with_capacity(upstream.len());
    for host in upstream {
        for addr in net::lookup_host(host).await? {
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
                if let Err(error) = handle_client(socket, upstream).await {
                    error!(%error);
                }
            }
            .instrument(span),
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    run(Options::parse()).await
}
