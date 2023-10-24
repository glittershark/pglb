use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, Barrier};

use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, Bencher, BenchmarkGroup, Criterion};
use postgres::{Client, NoTls};

mod tokio {
    use std::net::SocketAddr;

    use futures::future::{try_select, Either};
    use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
    use tokio::task::JoinHandle;

    const BUFFER_SIZE: usize = 16 * 1024 * 1024;

    async fn proxy_pipe(mut upstream: TcpStream, mut downstream: TcpStream) -> io::Result<()> {
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

    pub(super) async fn proxy<A>(listen: A, upstream: SocketAddr) -> io::Result<JoinHandle<()>>
    where
        A: ToSocketAddrs,
    {
        let listener = TcpListener::bind(listen).await?;
        Ok(tokio::spawn(async move {
            loop {
                let (sock, _addr) = listener.accept().await.unwrap();
                sock.set_nodelay(true).unwrap();

                let upstream = TcpStream::connect(upstream).await.unwrap();
                upstream.set_nodelay(true).unwrap();

                tokio::spawn(proxy_pipe(sock, upstream));
            }
        }))
    }
}

mod tokio_uring {
    use std::net::SocketAddr;
    use std::rc::Rc;

    use tokio::task::JoinHandle;
    use tokio::{io, try_join};
    use tokio_uring::buf::BoundedBuf;
    use tokio_uring::net::{TcpListener, TcpStream};

    const BUFFER_SIZE: usize = 16 * 1024 * 1024;

    async fn copy(from: Rc<TcpStream>, to: Rc<TcpStream>) -> io::Result<()> {
        let mut buf = vec![0; BUFFER_SIZE];

        loop {
            let (res, buf_read) = from.read(buf).await;
            let n = res?;
            if n == 0 {
                return Ok(());
            }
            let (res, write_buf) = to.write_all(buf_read.slice(..n)).await;
            res?;
            buf = write_buf.into_inner();
        }
    }

    pub(super) async fn proxy(
        listen: SocketAddr,
        upstream: SocketAddr,
    ) -> io::Result<JoinHandle<()>> {
        let listener = TcpListener::bind(listen)?;
        Ok(tokio_uring::spawn(async move {
            loop {
                let (sock, _addr) = listener.accept().await.unwrap();
                sock.set_nodelay(true).unwrap();
                tokio_uring::spawn(async move {
                    let upstream = Rc::new(TcpStream::connect(upstream).await.unwrap());
                    let downstream = Rc::new(sock);
                    upstream.set_nodelay(true).unwrap();

                    let mut upstream_downstream =
                        tokio_uring::spawn(copy(upstream.clone(), downstream.clone()));
                    let mut downstream_upstream =
                        tokio_uring::spawn(copy(downstream.clone(), upstream.clone()));

                    let _ = try_join!(&mut upstream_downstream, &mut downstream_upstream);
                    upstream_downstream.abort();
                    downstream_upstream.abort();
                });
            }
        }))
    }
}

mod glommio {
    use std::io;
    use std::net::SocketAddr;

    use futures::future::{try_select, Either};
    use futures::{AsyncReadExt, AsyncWriteExt};
    use glommio::executor;
    use glommio::net::{TcpListener, TcpStream};

    const BUFFER_SIZE: usize = 16 * 1024 * 1024;

    async fn proxy_pipe(mut upstream: TcpStream, mut downstream: TcpStream) -> io::Result<()> {
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

    pub(super) async fn proxy(listen: SocketAddr, upstream: SocketAddr) -> io::Result<()> {
        let listener = TcpListener::bind(listen)?;
        loop {
            let sock = listener.accept().await.unwrap();
            sock.set_nodelay(true).unwrap();

            let upstream = TcpStream::connect(upstream).await.unwrap();
            upstream.set_nodelay(true).unwrap();

            executor().spawn_local(proxy_pipe(sock, upstream)).detach();
        }
    }
}

const NUM_ROWS: usize = 10_000;

fn setup_fixture_data() {
    if env::var("NO_SEED").is_ok() {
        eprintln!("Skipping setting up fixture data");
        return;
    }

    eprint!("Setting up fixture data...");
    let mut client = Client::connect("postgresql://postgres:noria@127.0.0.1/noria", NoTls).unwrap();
    client
        .simple_query("DROP TABLE IF EXISTS benchmark_fixture_data;")
        .unwrap();
    client
        .simple_query("CREATE TABLE benchmark_fixture_data (x TEXT);")
        .unwrap();

    for _ in 0..NUM_ROWS {
        client
            .query(
                "INSERT INTO benchmark_fixture_data (x) VALUES ($1);",
                &[&"a"],
            )
            .unwrap();
    }
    eprintln!("Done.")
}

fn benchmark_one_client(query: &str, mut group: BenchmarkGroup<WallTime>) {
    let mut port = 8887u16;
    let mut next_port = || {
        port += 1;
        port
    };

    let do_bench = |b: &mut Bencher, port| {
        let mut client = loop {
            if let Ok(c) = Client::connect(
                &format!("postgresql://postgres:noria@127.0.0.1:{port}/noria"),
                NoTls,
            ) {
                break c;
            }
        };
        b.iter(|| {
            client.simple_query(query).unwrap();
        });
    };

    group.bench_function("no proxy (control)", |b| {
        do_bench(b, 5432);
    });

    group.bench_function("tokio proxy", |b| {
        let port = next_port();
        let rt = ::tokio::runtime::Runtime::new().unwrap();
        let _proxy = rt
            .block_on(tokio::proxy(
                SocketAddr::new("0.0.0.0".parse().unwrap(), port),
                "127.0.0.1:5432".parse().unwrap(),
            ))
            .unwrap();

        do_bench(b, port);
    });

    group.bench_function("tokio_uring proxy", |b| {
        let port = next_port();
        let started = Arc::new(Barrier::new(2));
        std::thread::spawn({
            let started = started.clone();
            move || {
                let rt = ::tokio_uring::Runtime::new(::tokio_uring::builder().entries(64)).unwrap();
                let proxy = rt
                    .block_on(tokio_uring::proxy(
                        SocketAddr::new("0.0.0.0".parse().unwrap(), port),
                        "127.0.0.1:5432".parse().unwrap(),
                    ))
                    .unwrap();
                started.wait();
                let _ = rt.block_on(proxy);
            }
        });

        started.wait();

        do_bench(b, port);
    });

    group.bench_function("glommio proxy", |b| {
        let port = next_port();
        let _proxy = ::glommio::LocalExecutorBuilder::default()
            .spawn(move || {
                glommio::proxy(
                    SocketAddr::new("0.0.0.0".parse().unwrap(), port),
                    "127.0.0.1:5432".parse().unwrap(),
                )
            })
            .unwrap();

        do_bench(b, port);
    });
}

fn benchmark_tcp_proxies(c: &mut Criterion) {
    setup_fixture_data();

    for (query, label) in [
        ("SELECT 1", "one value"),
        ("SELECT x FROM benchmark_fixture_data", "many values"),
    ] {
        let group = c.benchmark_group(format!("proxy to postgres/one client/{}", label));
        benchmark_one_client(query, group);
    }
}

criterion_group!(benches, benchmark_tcp_proxies);
criterion_main!(benches);
