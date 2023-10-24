use std::{
    env,
    net::TcpStream,
    sync::atomic::{AtomicU16, Ordering},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use assert_cmd::Command;
use postgres::{Client, NoTls};

static PORT: AtomicU16 = AtomicU16::new(10000);
fn next_port() -> u16 {
    PORT.fetch_add(1, Ordering::Relaxed)
}

struct Handle {
    port: u16,
    _thread: JoinHandle<()>,
    client: Option<Client>,
}

impl Handle {
    fn client(&mut self) -> &mut Client {
        if self.client.is_none() {
            let client = Client::connect(
                &format!(
                    "postgres://{}:{}@127.0.0.1:{}/{}",
                    env::var("PGUSER").unwrap_or_else(|_| "postgres".into()),
                    env::var("PGPASSWORD").unwrap_or_else(|_| "password".into()),
                    self.port,
                    env::var("PGDATABASE").unwrap_or_else(|_| "postgres".into()),
                ),
                NoTls,
            )
            .unwrap();
            self.client = Some(client);
        }

        self.client.as_mut().unwrap()
    }
}

fn run() -> Handle {
    let port = next_port();
    let mut cmd = Command::cargo_bin("pglb").unwrap();
    cmd.arg("--bind")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--upstream")
        .arg(format!(
            "{}:{}",
            env::var("PGHOST").unwrap_or_else(|_| "127.0.0.1".into()),
            env::var("PGPORT").unwrap_or_else(|_| "5432".into()),
        ));
    let thread = std::thread::spawn(move || {
        cmd.assert().success();
    });

    let start = Instant::now();
    while TcpStream::connect(format!("127.0.0.1:{port}")).is_err() {
        std::thread::sleep(Duration::from_millis(1));
        if start.elapsed() > Duration::from_secs(1) {
            panic!("Timed out trying to connect to pglb");
        }
    }

    Handle {
        port,
        _thread: thread,
        client: None,
    }
}

#[test]
fn single_client_query_version() {
    let mut h = run();
    h.client().simple_query("select version()").unwrap();
}
