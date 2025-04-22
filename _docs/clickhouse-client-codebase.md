# .docker/clickhouse/single_node/config.xml

```xml
<?xml version="1.0"?>
<clickhouse>

  <http_port>8123</http_port>
  <tcp_port>9000</tcp_port>

  <users_config>users.xml</users_config>
  <default_profile>default</default_profile>
  <default_database>default</default_database>

  <mark_cache_size>5368709120</mark_cache_size>

  <path>/var/lib/clickhouse/</path>
  <tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
  <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
  <access_control_path>/var/lib/clickhouse/access/</access_control_path>

  <logger>
    <level>debug</level>
    <log>/var/log/clickhouse-server/clickhouse-server.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
    <console>1</console>
  </logger>

  <query_log>
    <database>system</database>
    <table>query_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>1000</flush_interval_milliseconds>
  </query_log>
</clickhouse>

```

# .docker/clickhouse/users.xml

```xml
<?xml version="1.0"?>
<clickhouse>

  <profiles>
    <default>
      <load_balancing>random</load_balancing>
    </default>
  </profiles>

  <users>
    <default>
      <password></password>
      <networks>
        <ip>::/0</ip>
      </networks>
      <profile>default</profile>
      <quota>default</quota>
      <access_management>1</access_management>
    </default>
  </users>

  <quotas>
    <default>
      <interval>
        <duration>3600</duration>
        <queries>0</queries>
        <errors>0</errors>
        <result_rows>0</result_rows>
        <read_rows>0</read_rows>
        <execution_time>0</execution_time>
      </interval>
    </default>
  </quotas>
</clickhouse>

```

# .github/ISSUE_TEMPLATE/bug_report.md

```md
---
name: Bug report
about: Create a report to help us improve
title: ''
labels: bug
assignees: ''

---

<!-- delete unnecessary items -->
### Describe the bug

### Steps to reproduce
1.
2.
3.

### Expected behaviour

### Code example

### Error log

### Query log

### Configuration
#### Environment
* Client version:
* OS:

#### ClickHouse server
* ClickHouse Server version:
* ClickHouse Server non-default settings, if any:
* `CREATE TABLE` statements for tables involved:
* Sample data for all these tables, use [clickhouse-obfuscator](https://github.com/ClickHouse/ClickHouse/blob/master/programs/obfuscator/Obfuscator.cpp#L42-L80) if necessary

```

# .github/ISSUE_TEMPLATE/feature_request.md

```md
---
name: Feature request
about: Suggest an idea for the client
title: ''
labels: enhancement
assignees: ''

---

<!-- delete unnecessary items -->
### Use case

### Describe the solution you'd like

### Describe the alternatives you've considered

### Additional context

```

# .github/pull_request_template.md

```md
## Summary
A short description of the changes with a link to an open issue.

## Checklist
Delete items not relevant to your PR:
- [ ] Unit and integration tests covering the common scenarios were added
- [ ] A human-readable description of the changes was provided so that we can include it in CHANGELOG later
- [ ] For significant changes, documentation in README and https://github.com/ClickHouse/clickhouse-docs was updated with further explanations or tutorials

```

# .github/workflows/ci.yml

```yml
name: CI

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

env:
  CARGO_TERM_COLOR: always
  RUSTFLAGS: -Dwarnings
  RUSTDOCFLAGS: -Dwarnings
  RUST_BACKTRACE: 1
  MSRV: 1.73.0

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - run: rustup show active-toolchain -v
    - run: cargo build --all-targets
    - run: cargo build --all-targets --no-default-features
    - run: cargo build --all-targets --all-features

  msrv:
    runs-on: ubuntu-latest
    env:
      RUSTFLAGS: "" # remove -Dwarnings
    steps:
    - uses: actions/checkout@v4
    - run: rustup toolchain install ${{ env.MSRV }} --profile minimal
    - run: rustup override set ${{ env.MSRV }}
    - run: rustup show active-toolchain -v
    - run: cargo update -p native-tls --precise 0.2.13 # 0.2.14 requires rustc 1.80
    - run: cargo update -p litemap --precise 0.7.4 # 0.7.5 requires rustc 1.81
    - run: cargo update -p zerofrom --precise 0.1.5 # 0.1.6 requires rustc 1.81
    - run: cargo build
    - run: cargo build --no-default-features
    - run: cargo build --features uuid,time,chrono
    - run: cargo build --all-features

  rustfmt:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - run: rustup show active-toolchain -v
    - run: rustup component add rustfmt
    - run: cargo fmt --version
    - run: cargo fmt -- --check

  clippy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - run: rustup show active-toolchain -v
    - run: rustup component add clippy
    - run: cargo clippy --version
    - run: cargo clippy
    - run: cargo clippy --all-targets --no-default-features
    - run: cargo clippy --all-targets --all-features

    # TLS
    - run: cargo clippy --features native-tls
    - run: cargo clippy --features rustls-tls
    - run: cargo clippy --features rustls-tls-ring,rustls-tls-webpki-roots
    - run: cargo clippy --features rustls-tls-ring,rustls-tls-native-roots
    - run: cargo clippy --features rustls-tls-aws-lc,rustls-tls-webpki-roots
    - run: cargo clippy --features rustls-tls-aws-lc,rustls-tls-native-roots

  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - run: rustup show active-toolchain -v
    - run: cargo test
    - run: cargo test --no-default-features
    - run: cargo test --features uuid,time
    - run: cargo test --all-features

    services:
      clickhouse:
        image: clickhouse/clickhouse-server:24.10-alpine
        ports:
          - 8123:8123

  docs:
    needs: build
    runs-on: ubuntu-latest
    env:
      RUSTDOCFLAGS: -Dwarnings --cfg docsrs
    steps:
    - uses: actions/checkout@v4
    - run: rustup toolchain install nightly
    - run: rustup override set nightly
    - run: rustup show active-toolchain -v
    - run: cargo doc --all-features

```

# .gitignore

```
target
Cargo.lock

```

# benches/common.rs

```rs
#![allow(dead_code)] // typical for common test/bench modules :(

use std::{
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::atomic::{AtomicU32, Ordering},
    thread,
    time::Duration,
};

use bytes::Bytes;
use futures::stream::StreamExt;
use http_body_util::BodyExt;
use hyper::{
    body::{Body, Incoming},
    server::conn,
    service, Request, Response,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::{
    net::TcpListener,
    runtime,
    sync::{mpsc, oneshot},
};

use clickhouse::error::Result;

pub(crate) struct ServerHandle;

pub(crate) fn start_server<S, F, B>(addr: SocketAddr, serve: S) -> ServerHandle
where
    S: Fn(Request<Incoming>) -> F + Send + Sync + 'static,
    F: Future<Output = Response<B>> + Send,
    B: Body<Data = Bytes, Error = Infallible> + Send + 'static,
{
    let serving = async move {
        let listener = TcpListener::bind(addr).await.unwrap();

        loop {
            let (stream, _) = listener.accept().await.unwrap();

            let service =
                service::service_fn(|request| async { Ok::<_, Infallible>(serve(request).await) });

            // SELECT benchmark doesn't read the whole body, so ignore possible errors.
            let _ = conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .serve_connection(TokioIo::new(stream), service)
                .await;
        }
    };

    run_on_st_runtime("server", serving);
    ServerHandle
}

pub(crate) async fn skip_incoming(request: Request<Incoming>) {
    let mut body = request.into_body().into_data_stream();

    // Read and skip all frames.
    while let Some(result) = body.next().await {
        result.unwrap();
    }
}

pub(crate) struct RunnerHandle {
    tx: mpsc::UnboundedSender<Run>,
}

struct Run {
    future: Pin<Box<dyn Future<Output = Result<Duration>> + Send>>,
    callback: oneshot::Sender<Result<Duration>>,
}

impl RunnerHandle {
    pub(crate) fn run(
        &self,
        f: impl Future<Output = Result<Duration>> + Send + 'static,
    ) -> Duration {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(Run {
                future: Box::pin(f),
                callback: tx,
            })
            .unwrap();

        rx.blocking_recv().unwrap().unwrap()
    }
}

pub(crate) fn start_runner() -> RunnerHandle {
    let (tx, mut rx) = mpsc::unbounded_channel::<Run>();

    run_on_st_runtime("testee", async move {
        while let Some(run) = rx.recv().await {
            let result = run.future.await;
            let _ = run.callback.send(result);
        }
    });

    RunnerHandle { tx }
}

fn run_on_st_runtime(name: &str, f: impl Future + Send + 'static) {
    let name = name.to_string();
    thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            let no = AtomicU32::new(0);
            runtime::Builder::new_current_thread()
                .enable_all()
                .thread_name_fn(move || {
                    let no = no.fetch_add(1, Ordering::Relaxed);
                    format!("{name}-{no}")
                })
                .build()
                .unwrap()
                .block_on(f);
        })
        .unwrap();
}

```

# benches/insert.rs

```rs
use std::{
    future::Future,
    mem,
    time::{Duration, Instant},
};

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use http_body_util::Empty;
use hyper::{body::Incoming, Request, Response};
use serde::Serialize;

use clickhouse::{error::Result, Client, Compression, Row};

mod common;

async fn serve(request: Request<Incoming>) -> Response<Empty<Bytes>> {
    common::skip_incoming(request).await;
    Response::new(Empty::new())
}

#[derive(Row, Serialize)]
struct SomeRow {
    a: u64,
    b: i64,
    c: i32,
    d: u32,
    e: u64,
    f: u32,
    g: u64,
    h: i64,
}

impl SomeRow {
    fn sample() -> Self {
        black_box(Self {
            a: 42,
            b: 42,
            c: 42,
            d: 42,
            e: 42,
            f: 42,
            g: 42,
            h: 42,
        })
    }
}

async fn run_insert(client: Client, iters: u64) -> Result<Duration> {
    let start = Instant::now();
    let mut insert = client.insert("table")?;

    for _ in 0..iters {
        insert.write(&SomeRow::sample()).await?;
    }

    insert.end().await?;
    Ok(start.elapsed())
}

#[cfg(feature = "inserter")]
async fn run_inserter<const WITH_PERIOD: bool>(client: Client, iters: u64) -> Result<Duration> {
    let start = Instant::now();
    let mut inserter = client.inserter("table")?.with_max_rows(iters);

    if WITH_PERIOD {
        // Just to measure overhead, not to actually use it.
        inserter = inserter.with_period(Some(Duration::from_secs(1000)));
    }

    for _ in 0..iters {
        inserter.write(&SomeRow::sample())?;
        inserter.commit().await?;
    }

    inserter.end().await?;
    Ok(start.elapsed())
}

fn run<F>(c: &mut Criterion, name: &str, port: u16, f: impl Fn(Client, u64) -> F)
where
    F: Future<Output = Result<Duration>> + Send + 'static,
{
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    let _server = common::start_server(addr, serve);
    let runner = common::start_runner();

    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Bytes(mem::size_of::<SomeRow>() as u64));
    group.bench_function("no compression", |b| {
        b.iter_custom(|iters| {
            let client = Client::default()
                .with_url(format!("http://{addr}"))
                .with_compression(Compression::None);
            runner.run((f)(client, iters))
        })
    });
    #[cfg(feature = "lz4")]
    group.bench_function("lz4", |b| {
        b.iter_custom(|iters| {
            let client = Client::default()
                .with_url(format!("http://{addr}"))
                .with_compression(Compression::Lz4);
            runner.run((f)(client, iters))
        })
    });
    group.finish();
}

fn insert(c: &mut Criterion) {
    run(c, "insert", 6543, run_insert);
}

#[cfg(feature = "inserter")]
fn inserter(c: &mut Criterion) {
    run(c, "inserter", 6544, run_inserter::<false>);
    run(c, "inserter-period", 6545, run_inserter::<true>);
}

#[cfg(not(feature = "inserter"))]
criterion_group!(benches, insert);
#[cfg(feature = "inserter")]
criterion_group!(benches, insert, inserter);
criterion_main!(benches);

```

# benches/README.md

```md
# Benchmarks

All cases are run with `cargo bench --bench <case>`.

## With a mocked server

These benchmarks are run against a mocked server, which is a simple HTTP server that responds with a fixed response. This is useful to measure the overhead of the client itself:
* `select` checks throughput of `Client::query()`.
* `insert` checks throughput of `Client::insert()` and `Client::inserter()` (if the `inserter` features is enabled).

### How to collect perf data

The crate's code runs on the thread with the name `testee`:
\`\`\`bash
cargo bench --bench <name> &
perf record -p `ps -AT | grep testee | awk '{print $2}'` --call-graph dwarf,65528 --freq 5000 -g -- sleep 5
perf script > perf.script
\`\`\`

Then upload the `perf.script` file to [Firefox Profiler](https://profiler.firefox.com).

## With a running ClickHouse server

These benchmarks are run against a real ClickHouse server, so it must be started:
\`\`\`bash
docker compose up -d
cargo bench --bench <case>
\`\`\`

Cases:
* `select_numbers` measures time of running a big SELECT query to the `system.numbers_mt` table.

### How to collect perf data

\`\`\`bash
cargo bench --bench <name> &
perf record -p `ps -AT | grep <name> | awk '{print $2}'` --call-graph dwarf,65528 --freq 5000 -g -- sleep 5
perf script > perf.script
\`\`\`

Then upload the `perf.script` file to [Firefox Profiler](https://profiler.firefox.com).

```

# benches/select_numbers.rs

```rs
use serde::Deserialize;

use clickhouse::{Client, Compression, Row};

#[derive(Row, Deserialize)]
struct Data {
    no: u64,
}

async fn bench(name: &str, compression: Compression) {
    let start = std::time::Instant::now();
    let (sum, dec_mbytes, rec_mbytes) = tokio::spawn(do_bench(compression)).await.unwrap();
    assert_eq!(sum, 124999999750000000);
    let elapsed = start.elapsed();
    let throughput = dec_mbytes / elapsed.as_secs_f64();
    println!("{name:>8}  {elapsed:>7.3?}  {throughput:>4.0} MiB/s  {rec_mbytes:>4.0} MiB");
}

async fn do_bench(compression: Compression) -> (u64, f64, f64) {
    let client = Client::default()
        .with_compression(compression)
        .with_url("http://localhost:8123");

    let mut cursor = client
        .query("SELECT number FROM system.numbers_mt LIMIT 500000000")
        .fetch::<Data>()
        .unwrap();

    let mut sum = 0;
    while let Some(row) = cursor.next().await.unwrap() {
        sum += row.no;
    }

    let dec_bytes = cursor.decoded_bytes();
    let dec_mbytes = dec_bytes as f64 / 1024.0 / 1024.0;
    let recv_bytes = cursor.received_bytes();
    let recv_mbytes = recv_bytes as f64 / 1024.0 / 1024.0;
    (sum, dec_mbytes, recv_mbytes)
}

#[tokio::main]
async fn main() {
    println!("compress  elapsed  throughput  received");
    bench("none", Compression::None).await;
    #[cfg(feature = "lz4")]
    bench("lz4", Compression::Lz4).await;
}

```

# benches/select.rs

```rs
use std::{
    convert::Infallible,
    mem,
    time::{Duration, Instant},
};

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use futures::stream::{self, StreamExt as _};
use http_body_util::StreamBody;
use hyper::{
    body::{Body, Frame, Incoming},
    Request, Response,
};
use serde::Deserialize;

use clickhouse::{
    error::{Error, Result},
    Client, Compression, Row,
};

mod common;

async fn serve(
    request: Request<Incoming>,
    chunk: Bytes,
) -> Response<impl Body<Data = Bytes, Error = Infallible>> {
    common::skip_incoming(request).await;

    let stream = stream::repeat(chunk).map(|chunk| Ok(Frame::data(chunk)));
    Response::new(StreamBody::new(stream))
}

fn prepare_chunk() -> Bytes {
    use rand::{distributions::Standard, rngs::SmallRng, Rng, SeedableRng};

    // Generate random data to avoid _real_ compression.
    // TODO: It would be more useful to generate real data.
    let mut rng = SmallRng::seed_from_u64(0xBA5E_FEED);
    let raw: Vec<_> = (&mut rng).sample_iter(Standard).take(128 * 1024).collect();

    // If the feature is enabled, compress the data even if we use the `None`
    // compression. The compression ratio is low anyway due to random data.
    #[cfg(feature = "lz4")]
    let chunk = clickhouse::_priv::lz4_compress(&raw).unwrap();
    #[cfg(not(feature = "lz4"))]
    let chunk = Bytes::from(raw);

    chunk
}

fn select(c: &mut Criterion) {
    let addr = "127.0.0.1:6543".parse().unwrap();
    let chunk = prepare_chunk();
    let _server = common::start_server(addr, move |req| serve(req, chunk.clone()));
    let runner = common::start_runner();

    #[derive(Default, Debug, Row, Deserialize)]
    struct SomeRow {
        a: u64,
        b: i64,
        c: i32,
        d: u32,
    }

    async fn select_rows(client: Client, iters: u64) -> Result<Duration> {
        let mut sum = SomeRow::default();
        let start = Instant::now();
        let mut cursor = client
            .query("SELECT ?fields FROM some")
            .fetch::<SomeRow>()?;

        for _ in 0..iters {
            let Some(row) = cursor.next().await? else {
                return Err(Error::NotEnoughData);
            };
            sum.a = sum.a.wrapping_add(row.a);
            sum.b = sum.b.wrapping_add(row.b);
            sum.c = sum.c.wrapping_add(row.c);
            sum.d = sum.d.wrapping_add(row.d);
        }

        black_box(sum);
        Ok(start.elapsed())
    }

    async fn select_bytes(client: Client, min_size: u64) -> Result<Duration> {
        let start = Instant::now();
        let mut cursor = client
            .query("SELECT value FROM some")
            .fetch_bytes("RowBinary")?;

        let mut size = 0;
        while size < min_size {
            let buf = black_box(cursor.next().await?);
            size += buf.unwrap().len() as u64;
        }

        Ok(start.elapsed())
    }

    let mut group = c.benchmark_group("rows");
    group.throughput(Throughput::Bytes(mem::size_of::<SomeRow>() as u64));
    group.bench_function("uncompressed", |b| {
        b.iter_custom(|iters| {
            let client = Client::default()
                .with_url(format!("http://{addr}"))
                .with_compression(Compression::None);
            runner.run(select_rows(client, iters))
        })
    });
    #[cfg(feature = "lz4")]
    group.bench_function("lz4", |b| {
        b.iter_custom(|iters| {
            let client = Client::default()
                .with_url(format!("http://{addr}"))
                .with_compression(Compression::Lz4);
            runner.run(select_rows(client, iters))
        })
    });
    group.finish();

    const MIB: u64 = 1024 * 1024;
    let mut group = c.benchmark_group("mbytes");
    group.throughput(Throughput::Bytes(MIB));
    group.bench_function("uncompressed", |b| {
        b.iter_custom(|iters| {
            let client = Client::default()
                .with_url(format!("http://{addr}"))
                .with_compression(Compression::None);
            runner.run(select_bytes(client, iters * MIB))
        })
    });
    #[cfg(feature = "lz4")]
    group.bench_function("lz4", |b| {
        b.iter_custom(|iters| {
            let client = Client::default()
                .with_url(format!("http://{addr}"))
                .with_compression(Compression::Lz4);
            runner.run(select_bytes(client, iters * MIB))
        })
    });
    group.finish();
}

criterion_group!(benches, select);
criterion_main!(benches);

```

# Cargo.toml

```toml
[package]
name = "clickhouse"
version = "0.13.2"
description = "Official Rust client for ClickHouse DB"
keywords = ["clickhouse", "database", "driver", "tokio", "hyper"]
authors = ["ClickHouse Contributors", "Paul Loyd <pavelko95@gmail.com>"]
repository = "https://github.com/ClickHouse/clickhouse-rs"
homepage = "https://clickhouse.com"
license = "MIT OR Apache-2.0"
readme = "README.md"
edition = "2021"
# update `derive/Cargo.toml` and CI if changed
# TODO: after bumping to v1.80, remove `--precise` in the "msrv" CI job
rust-version = "1.73.0"

[lints.rust]
rust_2018_idioms = { level = "warn", priority = -1 }
unreachable_pub = "warn"
# TODO: missing_docs = "warn"
unexpected_cfgs = "allow" # for `docsrs`

[lints.clippy]
undocumented_unsafe_blocks = "warn"

[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]

[[bench]]
name = "select_numbers"
harness = false

[[bench]]
name = "insert"
harness = false

[[bench]]
name = "select"
harness = false

[[example]]
name = "inserter"
required-features = ["inserter"]

[[example]]
name = "mock"
required-features = ["test-util"]

[[example]]
name = "clickhouse_cloud"
required-features = ["rustls-tls"]

[[example]]
name = "data_types_derive_simple"
required-features = ["time", "uuid", "chrono"]

[[example]]
name = "data_types_variant"
required-features = ["time"]

[profile.release]
debug = true

[features]
default = ["lz4"]

test-util = ["hyper/server"]
inserter = ["dep:quanta"]
watch = ["dep:sha-1", "dep:serde_json", "serde/derive"]
uuid = ["dep:uuid"]
time = ["dep:time"]
lz4 = ["dep:lz4_flex", "dep:cityhash-rs"]
chrono = ["dep:chrono"]
futures03 = []

##  TLS
native-tls = ["dep:hyper-tls"]
# ext: native-tls-alpn
# ext: native-tls-vendored

rustls-tls = ["rustls-tls-aws-lc", "rustls-tls-webpki-roots"]
rustls-tls-aws-lc = [
    "dep:rustls",
    "dep:hyper-rustls",
    "hyper-rustls?/aws-lc-rs",
]
rustls-tls-ring = ["dep:rustls", "dep:hyper-rustls", "hyper-rustls?/ring"]
rustls-tls-webpki-roots = [
    "dep:rustls",
    "dep:hyper-rustls",
    "hyper-rustls?/webpki-tokio",
]
rustls-tls-native-roots = [
    "dep:rustls",
    "dep:hyper-rustls",
    "hyper-rustls?/native-tokio",
]

[dependencies]
clickhouse-derive = { version = "0.2.0", path = "derive" }

thiserror = "1.0.16"
serde = "1.0.106"
bytes = "1.5.0"
tokio = { version = "1.0.1", features = ["rt", "macros"] }
http-body-util = "0.1.2"
hyper = "1.4"
hyper-util = { version = "0.1.6", features = ["client-legacy", "http1"] }
hyper-tls = { version = "0.6.0", optional = true }
rustls = { version = "0.23", default-features = false, optional = true }
hyper-rustls = { version = "0.27.3", default-features = false, features = [
    "http1",
    "tls12",
], optional = true }
url = "2.1.1"
futures = "0.3.5"
futures-channel = "0.3.30"
static_assertions = "1.1"
sealed = "0.6"
sha-1 = { version = "0.10", optional = true }
serde_json = { version = "1.0.68", optional = true }
lz4_flex = { version = "0.11.3", default-features = false, features = [
    "std",
], optional = true }
cityhash-rs = { version = "=1.0.1", optional = true } # exact version for safety
uuid = { version = "1", optional = true }
time = { version = "0.3", optional = true }
chrono = { version = "0.4", optional = true, features = ["serde"] }
bstr = { version = "1.11.0", default-features = false }
quanta = { version = "0.12", optional = true }
replace_with = { version = "0.1.7" }

[dev-dependencies]
criterion = "0.5.0"
serde = { version = "1.0.106", features = ["derive"] }
tokio = { version = "1.0.1", features = ["full", "test-util"] }
hyper = { version = "1.1", features = ["server"] }
serde_bytes = "0.11.4"
serde_json = "1"
serde_repr = "0.1.7"
uuid = { version = "1", features = ["v4", "serde"] }
time = { version = "0.3.17", features = ["macros", "rand"] }
fixnum = { version = "0.9.2", features = ["serde", "i32", "i64", "i128"] }
rand = { version = "0.8.5", features = ["small_rng"] }

```

# CHANGELOG.md

```md
# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->

## [Unreleased] - ReleaseDate

## [0.13.2] - 2025-03-12
### Added
- query: added `Query::with_param` to support server-side parameters binding ([#159])
- derive: added [Variant data type](https://clickhouse.com/docs/en/sql-reference/data-types/variant) support ([#170]).
- query: added `Query::fetch_bytes` that allows streaming data in an arbitrary format ([#182])
- serde: added support for [chrono](https://docs.rs/chrono/latest/chrono/) ([#188])

### Changed
- MSRV is now 1.73 due to changes in `bstr` and `hyper-rustls` dependencies ([#180]).

### Fixed
- query/cursor: return `NotEnoughData` if a row is unparsed when the stream ends ([#185]).

[#159]: https://github.com/ClickHouse/clickhouse-rs/pull/159
[#170]: https://github.com/ClickHouse/clickhouse-rs/pull/170
[#180]: https://github.com/ClickHouse/clickhouse-rs/pull/180
[#182]: https://github.com/ClickHouse/clickhouse-rs/pull/182
[#185]: https://github.com/ClickHouse/clickhouse-rs/pull/185
[#188]: https://github.com/ClickHouse/clickhouse-rs/pull/188

## [0.13.1] - 2024-10-21
### Added
- query/cursor: add `RowCursor::{decoded_bytes,received_bytes}` methods ([#169]).

### Changed
- query/cursor: improve performance of `RowCursor::next()` ([#169]).

### Fixed
- mock: work with the advanced time via `tokio::time::advance()` ([#165]).

[#165]: https://github.com/ClickHouse/clickhouse-rs/pull/165
[#169]: https://github.com/ClickHouse/clickhouse-rs/pull/169

## [0.13.0] - 2024-09-27
### Added
- query: add `Query::sql_display()` ([#155]).
- client: add `Client::with_product_info()` ([#135]).
- client: add the `User-Agent` header to all requests ([#135]).

### Changed
- MSRV is now 1.70 due to changes in [hyper-rustls v0.27.3].
- tls: revise HTTPS-related features, see README for details ([#140],[#141],[#156]).
- query: support `??` for escaping the `?` symbol in SQL ([#154]).

### Fixed
- insert: don't panic on empty inserts ([#139]).
- uuid: serialization in human-readable formats ([#76]).

[#76]: https://github.com/ClickHouse/clickhouse-rs/pull/76
[#135]: https://github.com/ClickHouse/clickhouse-rs/pull/135
[#139]: https://github.com/ClickHouse/clickhouse-rs/pull/139
[#140]: https://github.com/ClickHouse/clickhouse-rs/pull/140
[#141]: https://github.com/ClickHouse/clickhouse-rs/pull/141
[#154]: https://github.com/ClickHouse/clickhouse-rs/pull/154
[#155]: https://github.com/ClickHouse/clickhouse-rs/pull/155
[#156]: https://github.com/ClickHouse/clickhouse-rs/pull/156
[hyper-rustls v0.27.3]: https://github.com/rustls/hyper-rustls/releases/tag/v%2F0.27.3

## [0.12.2] - 2024-08-20
### Changed
- Now this crate is pure Rust, no more C/C++ dependencies.
- insert: increase max size of frames to improve throughput ([#130]).
- compression: replace `lz4` sys binding with `lz4-flex` (pure Rust).
- compression: replace `clickhouse-rs-cityhash-sys` sys binding with `cityhash-rs` (pure Rust) ([#107]).

### Deprecated
- compression: `Compression::Lz4Hc` is deprecated and becomes an alias to `Compression::Lz4`.

[#130]: https://github.com/ClickHouse/clickhouse-rs/issues/130
[#107]: https://github.com/ClickHouse/clickhouse-rs/issues/107

## [0.12.1] - 2024-08-07
### Added
- query/bind: support `Option` in `query.bind(arg)` ([#119], [#120]).
- client: `Client::with_header()` to provide custom headers ([#98], [#108]).
- query: added `Query::with_option()` similar to `Client::with_option()` ([#123]).
- insert: added `Insert::with_option()` similar to `Client::with_option()` ([#123]).
- inserter: added `Inserter::with_option()` similar to `Client::with_option()` ([#123]).

### Changed
- insert: the outgoing request is now created after the first `Insert::write` call instead of `Insert::new` ([#123]).

[#123]: https://github.com/ClickHouse/clickhouse-rs/pull/123
[#120]: https://github.com/ClickHouse/clickhouse-rs/pull/120
[#119]: https://github.com/ClickHouse/clickhouse-rs/issues/119
[#108]: https://github.com/ClickHouse/clickhouse-rs/pull/108
[#98]: https://github.com/ClickHouse/clickhouse-rs/issues/98

## [0.12.0] - 2024-07-16
### Added
- derive: support `serde::skip_deserializing` ([#83]).
- insert: apply options set on the client ([#90]).
- inserter: can be limited by size, see `Inserter::with_max_bytes()`.
- inserter: `Inserter::pending()` to get stats about still being inserted data.
- inserter: `Inserter::force_commit()` to commit and insert immediately.
- mock: impl `Default` instance for `Mock`.

### Changed
- **BREAKING** bump MSRV to 1.67.
- **BREAKING** replace the `tls` feature with `native-tls` and `rustls-tls` that must be enabled explicitly now.
- **BREAKING** http: `HttpClient` API is changed due to moving to hyper v1.
- **BREAKING** inserter: move under the `inserter` feature.
- **BREAKING** inserter: there is no default limits anymore.
- **BREAKING** inserter: `Inserter::write` is synchronous now.
- **BREAKING** inserter: rename `entries` to `rows`.
- **BREAKING** drop the `wa-37420` feature.
- **BREAKING** remove deprecated items.
- **BREAKING** mock: `provide()`, `watch()` and `watch_only_events()` now accept iterators instead of streams.
- inserter: improve performance of time measurements by using `quanta`.
- inserter: improve performance if the time limit isn't used.
- derive: move to syn v2.
- mock: return a request if no handler is installed ([#89], [#91]).

### Fixed
- watch: support a new syntax.
- uuid: possible unsoundness.
- query: avoid panics during `Query::bind()` calls ([#103]).

[#103]: https://github.com/ClickHouse/clickhouse-rs/issues/103
[#102]: https://github.com/ClickHouse/clickhouse-rs/pull/102
[#91]: https://github.com/ClickHouse/clickhouse-rs/pull/91
[#90]: https://github.com/ClickHouse/clickhouse-rs/pull/90
[#89]: https://github.com/ClickHouse/clickhouse-rs/issues/89
[#83]: https://github.com/ClickHouse/clickhouse-rs/pull/83

## [0.11.6] - 2023-09-27
### Fixed
- client: accept HTTPs urls if `tls` feature is enabled ([#58]).

[#58]: https://github.com/ClickHouse/clickhouse-rs/issues/56

## [0.11.5] - 2023-06-12
### Changed
- inserter: start new insert only when the first row is provided ([#68], [#70]).

[#70]: https://github.com/ClickHouse/clickhouse-rs/pull/70
[#68]: https://github.com/ClickHouse/clickhouse-rs/pull/68

## [0.11.4] - 2023-05-14
### Added
- query: `Query::fetch_optional()`.

### Changed
- query: increase performance up to 40%.

## [0.11.3] - 2023-02-19
### Added
- client: support HTTPS ([#54]).

### Changed
- query: improve throughput (~8%).

### Fixed
- cursor: handle errors sent at the end of a response ([#56]).

[#56]: https://github.com/ClickHouse/clickhouse-rs/issues/56
[#54]: https://github.com/ClickHouse/clickhouse-rs/pull/54

## [0.11.2] - 2023-01-03
### Added
- insert: `with_timeouts` to manage timeouts.
- inserter: `with_timeouts` and `set_timeouts` to manage timeouts.

### Changed
- insert: improve throughput (~30%).
- inserter: set a default value of `max_entries` to 500_000.

## [0.11.1] - 2022-11-25
### Added
- ipv4: `serde::ipv4` for ser/de the `IPv4` type to/from `Ipv4Addr`. Note that `IPv6` requires no annotations.
- time: `serde::time::datetime(64)` for ser/de the [`time::OffsetDateTime`] type to/from `DateTime` and `DateTime64`.
- time: `serde::time::date(32)` for ser/de the [`time::Date`] type to/from `Date` and `Date32`.
- serde: add `::option` variants to support `Option<_>`.

### Changed
- uuid: move to the `serde` submodule.

[`time::OffsetDateTime`]: https://docs.rs/time/latest/time/struct.OffsetDateTime.html
[`time::Date`]: https://docs.rs/time/latest/time/struct.Date.html

## [0.11.0] - 2022-11-10
### Added
- compression: implement Lz4/Lz4Hc compression modes for `INSERT`s ([#39]).
- insert: the `wa-37420` feature to avoid [ClickHouse#37420].
- inserter: new method `Inserter::time_left()`.
- uuid: the `uuid` feature and a corresponding module to ser/de [`uuid::Uuid`] ([#26]).

### Changed
- **BREAKING** decompression: HTTP compression (gzip, zlib and brotli) isn't available anymore, only Lz4.
- inserter: skip timer ticks if `INSERT` is too long ([#20]).

[#39]: https://github.com/ClickHouse/clickhouse-rs/issues/39
[#26]: https://github.com/ClickHouse/clickhouse-rs/issues/26
[#20]: https://github.com/ClickHouse/clickhouse-rs/issues/20
[ClickHouse#37420]: https://github.com/ClickHouse/ClickHouse/issues/37420
[`uuid::Uuid`]: https://docs.rs/uuid/latest/uuid/struct.Uuid.html

## [0.10.0] - 2022-01-18
### Added
- client: `Client::with_http_client` to use custom `hyper::Client`, e.g. for https ([#27]).

### Changed
- watch: run `WATCH` queries with `max_execution_time=0`.
- bind: implement `Bind` for all `Serialize` instances ([#33]).

### Fixed
- Implement `Primitive` for `f64` ([#31]).

[#33]: https://github.com/ClickHouse/clickhouse-rs/issues/33
[#31]: https://github.com/ClickHouse/clickhouse-rs/issues/31
[#27]: https://github.com/ClickHouse/clickhouse-rs/pull/27

## [0.9.3] - 2021-12-21
### Added
- Implement `Primitive` for `f64` and `f32` ([#29]).

### Fixed
- Reset quantities on errors to support reusing `Inserter` after errors ([#30]).

[#30]: https://github.com/ClickHouse/clickhouse-rs/pull/30
[#29]: https://github.com/ClickHouse/clickhouse-rs/issues/29

## [0.9.2] - 2021-11-01
### Changed
- HTTP Keep-alive timeout is restricted to 2s explicitly.

### Fixed
- watch: make a cursor cancellation safe.

## [0.9.1] - 2021-10-25
### Added
- mock: add `record_ddl` handler to test DDL queries.
- mock: add `watch` and `watch_only_events` handlers to test WATCH queries.

## [0.9.0] - 2021-10-25
### Fixed
- query: support borrowed long strings ([#22]).
- query: read the whole response of DDL queries.

### Changed
- **BREAKING**: watch: require the `watch` feature.
- **BREAKING**: watch: only struct rows are allowed because JSON requires names.
- query: queries with invalid URLs fail with `Error::InvalidParams`.
- watch: use `JSONEachRowWithProgress` because of [ClickHouse#22996] ([#23]).

[#23]: https://github.com/ClickHouse/clickhouse-rs/issues/23
[#22]: https://github.com/ClickHouse/clickhouse-rs/issues/22
[ClickHouse#22996]: https://github.com/ClickHouse/ClickHouse/issues/22996

## [0.8.1] - 2021-08-26
### Fixed
- Support `?` inside bound arguments ([#18]).
- Use the `POST` method if a query is bigger than 8KiB ([#19]).

[#19]: https://github.com/ClickHouse/clickhouse-rs/issues/19
[#18]: https://github.com/ClickHouse/clickhouse-rs/issues/18

## [0.8.0] - 2021-07-28
### Fixed
- `RowBinarySerializer::is_human_readable()` returns `false`.

## [0.7.2] - 2021-05-07
### Added
- `Watch::refresh()` to specify `REFRESH` clause.

### Fixed
- `derive(Row)`: handle raw identifiers.

## [0.7.1] - 2021-06-29
### Fixed
- Get rid of "socket is not connected" errors.

### Changed
- Set TCP keepalive to 60 seconds.

## [0.7.0] - 2021-05-31
### Changed
- Replace `reflection::Reflection` with `clickhouse::Row`. It's enough to implement `Row` for top-level `struct`s only.

### Added
- `#[derive(Row)]`

## [0.6.8] - 2021-05-28
### Fixed
- docs: enable the `doc_cfg` feature.

## [0.6.7] - 2021-05-28
### Fixed
- docs: show features on docs.rs.
- Now `test-util` implies `hyper/server`.

## [0.6.6] - 2021-05-28
### Added
- `test` module (available with the `test-util` feature).
- `#[must_use]` for `Query`, `Watch`, `Insert` and `Inserter`.

## [0.6.5] - 2021-05-24
### Added
- `&String` values binding to SQL queries.

## [0.6.4] - 2021-05-14
### Fixed
- Depend explicitly on `tokio/macros`.

## [0.6.3] - 2021-05-11
### Added
- Support for `bool` values storage ([#9]).
- `array`s' binding to SQL queries — useful at `IN` operators, etc ([#9]).
- `String` values binding to SQL queries ([#9]).
- `Query::fetch_all()`
- `sql::Identifier`

### Changed
- Expose `query::Bind` ([#11]).
- Deprecate `Compression::encoding()`.

[#11]: https://github.com/ClickHouse/clickhouse-rs/pull/9
[#9]: https://github.com/ClickHouse/clickhouse-rs/pull/9

## [0.6.2] - 2021-04-12
### Fixed
- watch: bind fileds of the type param.

## [0.6.1] - 2021-04-09
### Fixed
- compression: decompress error messages ([#7]).

[#7]: https://github.com/ClickHouse/clickhouse-rs/pull/7

## [0.6.0] - 2021-03-24
### Changed
- Use tokio v1, hyper v0.14, bytes v1.

## [0.5.1] - 2020-11-22
### Added
- decompression: lz4.

## [0.5.0] - 2020-11-19
### Added
- decompression: gzip, zlib and brotli.

## [0.4.0] - 2020-11-17
### Added
- `Query::fetch_one()`, `Watch::fetch_one()`.
- `Query::fetch()` as a replacement for `Query::rows()`.
- `Watch::fetch()` as a replacement for `Watch::rows()`.
- `Watch::only_events().fetch()` as a replacement for `Watch::events()`.

### Changed
- `Error` is `StdError + Send + Sync + 'static` now.

## [0.3.0] - 2020-10-28
### Added
- Expose cursors (`query::RowCursor`, `watch::{RowCursor, EventCursor}`).

## [0.2.0] - 2020-10-14
### Added
- `Client::inserter()` for infinite inserting into tables.
- `Client::watch()` for `LIVE VIEW` related queries.

### Changed
- Renamed `Query::fetch()` to `Query::rows()`.
- Use `GET` requests for `SELECT` statements.

## [0.1.0] - 2020-10-14
### Added
- Support basic types.
- `Client::insert()` for inserting into tables.
- `Client::query()` for selecting from tables and DDL statements.

<!-- next-url -->
[Unreleased]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.13.2...HEAD
[0.13.2]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.13.1...v0.13.2
[0.13.1]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.13.0...v0.13.1
[0.13.0]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.12.2...v0.13.0
[0.12.2]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.12.1...v0.12.2
[0.12.1]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.11.6...v0.12.0
[0.11.6]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.11.5...v0.11.6
[0.11.5]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.11.4...v0.11.5
[0.11.4]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.11.3...v0.11.4
[0.11.3]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.11.2...v0.11.3
[0.11.2]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.11.1...v0.11.2
[0.11.1]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.9.3...v0.10.0
[0.9.3]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.9.2...v0.9.3
[0.9.2]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.8.1...v0.9.0
[0.8.1]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.7.2...v0.8.0
[0.7.2]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.6.8...v0.7.0
[0.6.8]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.6.7...v0.6.8
[0.6.7]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.6.6...v0.6.7
[0.6.6]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.6.5...v0.6.6
[0.6.5]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.6.4...v0.6.5
[0.6.4]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.6.3...v0.6.4
[0.6.3]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.6.2...v0.6.3
[0.6.2]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/ClickHouse/clickhouse-rs/releases/tag/v0.1.0

```

# derive/Cargo.toml

```toml
[package]
name = "clickhouse-derive"
version = "0.2.0"
description = "A macro for deriving clickhouse::Row"
authors = ["ClickHouse Contributors", "Paul Loyd <pavelko95@gmail.com>"]
repository = "https://github.com/ClickHouse/clickhouse-rs"
homepage = "https://clickhouse.com"
edition = "2021"
license = "MIT OR Apache-2.0"
# update `Cargo.toml` and CI if changed
rust-version = "1.73.0"

[lib]
proc-macro = true

[dependencies]
proc-macro2 = "1.0"
syn = "2.0"
quote = "1.0"
serde_derive_internals = "0.29.1"

```

# derive/src/lib.rs

```rs
use proc_macro2::TokenStream;
use quote::quote;
use serde_derive_internals::{
    attr::{Container, Default as SerdeDefault, Field},
    Ctxt,
};
use syn::{parse_macro_input, Data, DataStruct, DeriveInput, Fields};

fn column_names(data: &DataStruct, cx: &Ctxt, container: &Container) -> TokenStream {
    match &data.fields {
        Fields::Named(fields) => {
            let rename_rule = container.rename_all_rules().deserialize;
            let column_names_iter = fields
                .named
                .iter()
                .enumerate()
                .map(|(index, field)| Field::from_ast(cx, index, field, None, &SerdeDefault::None))
                .filter(|field| !field.skip_serializing() && !field.skip_deserializing())
                .map(|field| {
                    rename_rule
                        .apply_to_field(field.name().serialize_name())
                        .to_string()
                });

            quote! {
                &[#( #column_names_iter,)*]
            }
        }
        Fields::Unnamed(_) => {
            quote! { &[] }
        }
        Fields::Unit => panic!("`Row` cannot be derived for unit structs"),
    }
}

// TODO: support wrappers `Wrapper(Inner)` and `Wrapper<T>(T)`.
// TODO: support the `nested` attribute.
// TODO: support the `crate` attribute.
#[proc_macro_derive(Row)]
pub fn row(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let cx = Ctxt::new();
    let container = Container::from_ast(&cx, &input);
    let name = input.ident;

    let column_names = match &input.data {
        Data::Struct(data) => column_names(data, &cx, &container),
        Data::Enum(_) | Data::Union(_) => panic!("`Row` can be derived only for structs"),
    };

    // TODO: do something more clever?
    let _ = cx.check().expect("derive context error");

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // TODO: replace `clickhouse` with `::clickhouse` here.
    let expanded = quote! {
        #[automatically_derived]
        impl #impl_generics clickhouse::Row for #name #ty_generics #where_clause {
            const COLUMN_NAMES: &'static [&'static str] = #column_names;
        }
    };

    proc_macro::TokenStream::from(expanded)
}

```

# docker-compose.yml

```yml
services:
  clickhouse:
    image: 'clickhouse/clickhouse-server:${CLICKHOUSE_VERSION-24.10-alpine}'
    container_name: 'clickhouse-rs-clickhouse-server'
    ports:
      - '8123:8123'
      - '9000:9000'
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    volumes:
      - './.docker/clickhouse/single_node/config.xml:/etc/clickhouse-server/config.xml'
      - './.docker/clickhouse/users.xml:/etc/clickhouse-server/users.xml'

```

# examples/async_insert.rs

```rs
use std::time::{Duration, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use clickhouse::sql::Identifier;
use clickhouse::{error::Result, Client, Row};

// This example demonstrates how to use asynchronous inserts, avoiding client side batching of the incoming data.
// Suitable for ClickHouse Cloud, too. See https://clickhouse.com/docs/en/optimize/asynchronous-inserts

#[derive(Debug, Serialize, Deserialize, Row)]
struct Event {
    timestamp: u64,
    message: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let table_name = "chrs_async_insert";

    let client = Client::default()
        .with_url("http://localhost:8123")
        // https://clickhouse.com/docs/en/operations/settings/settings#async-insert
        .with_option("async_insert", "1")
        // https://clickhouse.com/docs/en/operations/settings/settings#wait-for-async-insert
        .with_option("wait_for_async_insert", "0");

    client
        .query(
            "
            CREATE OR REPLACE TABLE ? (
                timestamp DateTime64(9),
                message   String
            )
            ENGINE = MergeTree
            ORDER BY timestamp
            ",
        )
        .bind(Identifier(table_name))
        .execute()
        .await?;

    let mut insert = client.insert(table_name)?;
    insert
        .write(&Event {
            timestamp: now(),
            message: "one".into(),
        })
        .await?;
    insert.end().await?;

    loop {
        let events = client
            .query("SELECT ?fields FROM ?")
            .bind(Identifier(table_name))
            .fetch_all::<Event>()
            .await?;
        if !events.is_empty() {
            println!("Async insert was flushed");
            println!("{events:?}");
            break;
        }
        // If you change the `wait_for_async_insert` setting to 1, this line will never be printed;
        // however, without waiting, you will see it in the console output several times,
        // as the data will remain in the server buffer for a bit before the flush happens
        println!("Waiting for async insert flush...");
        tokio::time::sleep(Duration::from_millis(10)).await
    }

    Ok(())
}

fn now() -> u64 {
    UNIX_EPOCH
        .elapsed()
        .expect("invalid system time")
        .as_nanos() as u64
}

```

# examples/clickhouse_cloud.rs

```rs
use clickhouse::sql::Identifier;
use clickhouse::Client;
use clickhouse_derive::Row;
use serde::{Deserialize, Serialize};
use std::env;

// This example requires three environment variables with your instance credentials to be set
//
// - CLICKHOUSE_URL (e.g., https://myservice.clickhouse.cloud:8443)
// - CLICKHOUSE_USER
// - CLICKHOUSE_PASSWORD
//
// Works with either `rustls-tls` or `native-tls` cargo features.

#[tokio::main]
async fn main() -> clickhouse::error::Result<()> {
    let table_name = "chrs_cloud";

    let client = Client::default()
        .with_url(read_env_var("CLICKHOUSE_URL"))
        .with_user(read_env_var("CLICKHOUSE_USER"))
        .with_password(read_env_var("CLICKHOUSE_PASSWORD"));

    // `wait_end_of_query` is required in this case, as we want these DDLs to be executed
    // on the entire Cloud cluster before we receive the response.
    // See https://clickhouse.com/docs/en/interfaces/http/#response-buffering
    client
        .query("DROP TABLE IF EXISTS ?")
        .bind(Identifier(table_name))
        .with_option("wait_end_of_query", "1")
        .execute()
        .await?;

    // Note that you could just use MergeTree with CH Cloud, and omit the `ON CLUSTER` clause.
    // The same applies to other engines as well;
    // e.g., ReplacingMergeTree will become SharedReplacingMergeTree and so on.
    // See https://clickhouse.com/docs/en/cloud/reference/shared-merge-tree#enabling-sharedmergetree
    client
        .query("CREATE TABLE ? (id Int32, name String) ENGINE MergeTree ORDER BY id")
        .bind(Identifier(table_name))
        .with_option("wait_end_of_query", "1")
        .execute()
        .await?;

    let mut insert = client.insert(table_name)?;
    insert
        .write(&Data {
            id: 42,
            name: "foo".into(),
        })
        .await?;
    insert.end().await?;

    let data = client
        .query("SELECT ?fields FROM ?")
        .bind(Identifier(table_name))
        // This setting is optional; use it when you need strong consistency guarantees on the reads
        // See https://clickhouse.com/docs/en/cloud/reference/shared-merge-tree#consistency
        .with_option("select_sequential_consistency", "1")
        .fetch_all::<Data>()
        .await?;

    println!("Stored data: {data:?}");
    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Row)]
struct Data {
    id: u32,
    name: String,
}

fn read_env_var(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("{key} env variable should be set"))
}

```

# examples/clickhouse_settings.rs

```rs
use clickhouse::{error::Result, Client};

/// Besides [`Client::query`], it works similarly with [`Client::insert`] and [`Client::inserter`].
#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default()
        .with_url("http://localhost:8123")
        // This setting is global and will be applied to all queries.
        .with_option("limit", "100");

    let numbers = client
        .query("SELECT number FROM system.numbers")
        // This setting will be applied to this particular query only;
        // it will override the global client setting.
        .with_option("limit", "3")
        .fetch_all::<u64>()
        .await?;

    // note that it prints the first 3 numbers only (because of the setting override)
    println!("{numbers:?}");

    Ok(())
}

```

# examples/custom_http_client.rs

```rs
use std::time::Duration;

use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;

use clickhouse::{error::Result, Client};

#[tokio::main]
async fn main() -> Result<()> {
    let connector = HttpConnector::new(); // or HttpsConnectorBuilder
    let hyper_client = HyperClient::builder(TokioExecutor::new())
        // For how long keep a particular idle socket alive on the client side (in milliseconds).
        // It is supposed to be a fair bit less that the ClickHouse server KeepAlive timeout,
        // which was by default 3 seconds for pre-23.11 versions, and 10 seconds after that.
        .pool_idle_timeout(Duration::from_millis(2_500))
        // Sets the maximum idle Keep-Alive connections allowed in the pool.
        .pool_max_idle_per_host(4)
        .build(connector);

    let client = Client::with_http_client(hyper_client).with_url("http://localhost:8123");

    let numbers = client
        .query("SELECT number FROM system.numbers LIMIT 1")
        .fetch_all::<u64>()
        .await?;
    println!("Numbers: {numbers:?}");

    Ok(())
}

```

# examples/custom_http_headers.rs

```rs
use clickhouse::{error::Result, Client};

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default()
        .with_url("http://localhost:8123")
        // purposefully invalid credentials in the client configuration for the sake of this example
        .with_user("...")
        .with_password("...")
        // these custom headers will override the auth headers generated by the client
        .with_header("X-ClickHouse-User", "default")
        .with_header("X-ClickHouse-Key", "")
        // or, you could just add your custom headers, e.g., for proxy authentication
        .with_header("X-My-Header", "hello");

    let numbers = client
        .query("SELECT number FROM system.numbers LIMIT 1")
        .fetch_all::<u64>()
        .await?;
    println!("Numbers: {numbers:?}");

    Ok(())
}

```

# examples/data_types_derive_containers.rs

```rs
use rand::distributions::Alphanumeric;
use rand::Rng;

use clickhouse::sql::Identifier;
use clickhouse::{error::Result, Client};

// This example covers derivation of container-like ClickHouse data types.
// See also:
// - https://clickhouse.com/docs/en/sql-reference/data-types
// - data_types_derive_simple.rs

#[tokio::main]
async fn main() -> Result<()> {
    let table_name = "chrs_data_types_derive_containers";
    let client = Client::default().with_url("http://localhost:8123");

    client
        .query(
            "
            CREATE OR REPLACE TABLE ?
            (
                arr               Array(String),
                arr2              Array(Array(String)),
                map               Map(String, UInt32),
                tuple             Tuple(String, UInt32),
                nested            Nested(name String, count UInt32),
                point             Point,
                ring              Ring,
                polygon           Polygon,
                multi_polygon     MultiPolygon,
                line_string       LineString,
                multi_line_string MultiLineString
            ) ENGINE MergeTree ORDER BY ();
            ",
        )
        .bind(Identifier(table_name))
        .execute()
        .await?;

    let mut insert = client.insert(table_name)?;
    insert.write(&Row::new()).await?;
    insert.end().await?;

    let rows = client
        .query("SELECT ?fields FROM ?")
        .bind(Identifier(table_name))
        .fetch_all::<Row>()
        .await?;

    println!("{rows:#?}");
    Ok(())
}

// See https://clickhouse.com/docs/en/sql-reference/data-types/geo
type Point = (f64, f64);
type Ring = Vec<Point>;
type Polygon = Vec<Ring>;
type MultiPolygon = Vec<Polygon>;
type LineString = Vec<Point>;
type MultiLineString = Vec<LineString>;

#[derive(Clone, Debug, PartialEq)]
#[derive(clickhouse::Row, serde::Serialize, serde::Deserialize)]
pub struct Row {
    arr: Vec<String>,
    arr2: Vec<Vec<String>>,
    map: Vec<(String, u32)>,
    tuple: (String, u32),
    // Nested columns are internally represented as arrays of the same length
    // https://clickhouse.com/docs/en/sql-reference/data-types/nested-data-structures/nested
    #[serde(rename = "nested.name")]
    nested_name: Vec<String>,
    #[serde(rename = "nested.count")]
    nested_count: Vec<u32>,
    // Geo types
    point: Point,
    ring: Ring,
    polygon: Polygon,
    multi_polygon: MultiPolygon,
    line_string: LineString,
    multi_line_string: MultiLineString,
}

impl Row {
    pub fn new() -> Self {
        let mut rng = rand::thread_rng();
        Row {
            arr: vec![random_str()],
            arr2: vec![vec![random_str()]],
            map: vec![(random_str(), 42)],
            tuple: (random_str(), 144),
            // Nested
            // NB: the length of all vectors/slices representing Nested columns must be the same
            nested_name: vec![random_str(), random_str()],
            nested_count: vec![rng.gen(), rng.gen()],
            // Geo
            point: random_point(),
            ring: random_ring(),
            polygon: random_polygon(),
            multi_polygon: vec![random_polygon()],
            line_string: random_ring(), // on the type level, the same as the Ring
            multi_line_string: random_polygon(), // on the type level, the same as the Polygon
        }
    }
}

impl Default for Row {
    fn default() -> Self {
        Self::new()
    }
}

fn random_str() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(3)
        .map(char::from)
        .collect()
}

fn random_point() -> Point {
    let mut rng = rand::thread_rng();
    (rng.gen(), rng.gen())
}

fn random_ring() -> Ring {
    vec![random_point(), random_point()]
}

fn random_polygon() -> Polygon {
    vec![random_ring(), random_ring()]
}

```

# examples/data_types_derive_simple.rs

```rs
use std::str::FromStr;

use chrono::{DateTime, NaiveDate, Utc};
use fixnum::{
    typenum::{U12, U4, U8},
    FixedPoint,
};
use rand::{distributions::Alphanumeric, seq::SliceRandom, Rng};
use time::{Date, Month, OffsetDateTime, Time};

use clickhouse::{error::Result, sql::Identifier, Client};

// This example covers derivation of _simpler_ ClickHouse data types.
// See also: https://clickhouse.com/docs/en/sql-reference/data-types

#[tokio::main]
async fn main() -> Result<()> {
    let table_name = "chrs_data_types_derive";
    let client = Client::default().with_url("http://localhost:8123");

    client
        .query(
            "
            CREATE OR REPLACE TABLE ?
            (
                int8                 Int8,
                int16                Int16,
                int32                Int32,
                int64                Int64,
                int128               Int128,
                -- int256               Int256,
                uint8                UInt8,
                uint16               UInt16,
                uint32               UInt32,
                uint64               UInt64,
                uint128              UInt128,
                -- uint256              UInt256,
                float32              Float32,
                float64              Float64,
                boolean              Boolean,
                str                  String,
                blob_str             String,
                nullable_str         Nullable(String),
                low_car_str          LowCardinality(String),
                nullable_low_car_str LowCardinality(Nullable(String)),
                fixed_str            FixedString(16),
                uuid                 UUID,
                ipv4                 IPv4,
                ipv6                 IPv6,
                enum8                Enum8('Foo', 'Bar'),
                enum16               Enum16('Qaz' = 42, 'Qux' = 255),
                decimal32_9_4        Decimal(9, 4),
                decimal64_18_8       Decimal(18, 8),
                decimal128_38_12     Decimal(38, 12),
                -- decimal256_76_20           Decimal(76, 20),
                date                 Date,
                date32               Date32,
                datetime             DateTime,
                datetime_tz          DateTime('UTC'),
                datetime64_0         DateTime64(0),
                datetime64_3         DateTime64(3),
                datetime64_6         DateTime64(6),
                datetime64_9         DateTime64(9),
                datetime64_9_tz      DateTime64(9, 'UTC')
            ) ENGINE MergeTree ORDER BY ();
        ",
        )
        .bind(Identifier(table_name))
        .execute()
        .await?;

    let mut insert = client.insert(table_name)?;
    insert.write(&Row::new()).await?;
    insert.end().await?;

    let rows = client
        .query("SELECT ?fields FROM ?")
        .bind(Identifier(table_name))
        .fetch_all::<Row>()
        .await?;

    println!("{rows:#?}");
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
#[derive(clickhouse::Row, serde::Serialize, serde::Deserialize)]
pub struct Row {
    pub int8: i8,
    pub int16: i16,
    pub int32: i32,
    pub int64: i64,
    pub int128: i128,
    pub uint8: u8,
    pub uint16: u16,
    pub uint32: u32,
    pub uint64: u64,
    pub uint128: u128,
    pub float32: f32,
    pub float64: f64,
    pub boolean: bool,
    pub str: String,
    // Avoiding reading/writing strings as UTF-8 for blobs stored in a string column
    #[serde(with = "serde_bytes")]
    pub blob_str: Vec<u8>,
    pub nullable_str: Option<String>,
    // LowCardinality does not affect the struct definition
    pub low_car_str: String,
    // The same applies to a "nested" LowCardinality
    pub nullable_low_car_str: Option<String>,
    // FixedString is represented as raw bytes (similarly to `blob_str`, no UTF-8)
    pub fixed_str: [u8; 16],
    #[serde(with = "clickhouse::serde::uuid")]
    pub uuid: uuid::Uuid,
    #[serde(with = "clickhouse::serde::ipv4")]
    pub ipv4: std::net::Ipv4Addr,
    pub ipv6: std::net::Ipv6Addr,
    pub enum8: Enum8,
    pub enum16: Enum16,
    pub decimal32_9_4: Decimal32,
    pub decimal64_18_8: Decimal64,
    pub decimal128_38_12: Decimal128,
    #[serde(with = "clickhouse::serde::time::date")]
    pub time_date: Date,
    #[serde(with = "clickhouse::serde::time::date32")]
    pub time_date32: Date,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub time_datetime: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub time_datetime_tz: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::secs")]
    pub time_datetime64_0: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub time_datetime64_3: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub time_datetime64_6: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    pub time_datetime64_9: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    pub time_datetime64_9_tz: OffsetDateTime,

    #[serde(with = "clickhouse::serde::chrono::date")]
    pub chrono_date: NaiveDate,
    #[serde(with = "clickhouse::serde::chrono::date32")]
    pub chrono_date32: NaiveDate,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub chrono_datetime: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub chrono_datetime_tz: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::secs")]
    pub chrono_datetime64_0: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub chrono_datetime64_3: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::micros")]
    pub chrono_datetime64_6: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub chrono_datetime64_9: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub chrono_datetime64_9_tz: DateTime<Utc>,
}

// See ClickHouse decimal sizes: https://clickhouse.com/docs/en/sql-reference/data-types/decimal
type Decimal32 = FixedPoint<i32, U4>; // Decimal(9, 4) = Decimal32(4)
type Decimal64 = FixedPoint<i64, U8>; // Decimal(18, 8) = Decimal64(8)
type Decimal128 = FixedPoint<i128, U12>; // Decimal(38, 12) = Decimal128(12)

#[derive(Clone, Debug, PartialEq)]
#[derive(serde_repr::Serialize_repr, serde_repr::Deserialize_repr)]
#[repr(u8)]
pub enum Enum8 {
    Foo = 1,
    Bar = 2,
}

#[derive(Clone, Debug, PartialEq)]
#[derive(serde_repr::Serialize_repr, serde_repr::Deserialize_repr)]
#[repr(u16)]
pub enum Enum16 {
    Qaz = 42,
    Qux = 255,
}

impl Row {
    pub fn new() -> Self {
        let mut rng = rand::thread_rng();
        Row {
            int8: rng.gen(),
            int16: rng.gen(),
            int32: rng.gen(),
            int64: rng.gen(),
            int128: rng.gen(),
            uint8: rng.gen(),
            uint16: rng.gen(),
            uint32: rng.gen(),
            uint64: rng.gen(),
            uint128: rng.gen(),
            float32: rng.gen(),
            float64: rng.gen(),
            boolean: rng.gen(),
            str: random_str(),
            blob_str: rng.gen::<[u8; 3]>().to_vec(),
            nullable_str: Some(random_str()),
            low_car_str: random_str(),
            nullable_low_car_str: Some(random_str()),
            fixed_str: rng.gen(),
            uuid: uuid::Uuid::new_v4(),
            ipv4: std::net::Ipv4Addr::from_str("172.195.0.1").unwrap(),
            ipv6: std::net::Ipv6Addr::from_str("::ffff:acc3:1").unwrap(),
            enum8: [Enum8::Foo, Enum8::Bar]
                .choose(&mut rng)
                .unwrap()
                .to_owned(),
            enum16: [Enum16::Qaz, Enum16::Qux]
                .choose(&mut rng)
                .unwrap()
                .to_owned(),
            // See also: https://clickhouse.com/docs/en/sql-reference/data-types/decimal
            decimal32_9_4: Decimal32::from_str("99999.9999").unwrap(),
            decimal64_18_8: Decimal64::from_str("9999999999.99999999").unwrap(),
            decimal128_38_12: Decimal128::from_str("99999999999999999999999999.999999999999")
                .unwrap(),
            // Allowed values ranges:
            // - Date   = [1970-01-01, 2149-06-06]
            // - Date32 = [1900-01-01, 2299-12-31]
            // See
            // - https://clickhouse.com/docs/en/sql-reference/data-types/date
            // - https://clickhouse.com/docs/en/sql-reference/data-types/date32
            time_date: Date::from_calendar_date(2149, Month::June, 6).unwrap(),
            time_date32: Date::from_calendar_date(2299, Month::December, 31).unwrap(),
            time_datetime: max_datetime(),
            time_datetime_tz: max_datetime(),
            time_datetime64_0: max_datetime64(),
            time_datetime64_3: max_datetime64(),
            time_datetime64_6: max_datetime64(),
            time_datetime64_9: max_datetime64_nanos(),
            time_datetime64_9_tz: max_datetime64_nanos(),

            chrono_date: NaiveDate::from_ymd_opt(2149, 6, 6).unwrap(),
            chrono_date32: NaiveDate::from_ymd_opt(2299, 12, 31).unwrap(),
            chrono_datetime: Utc::now(),
            chrono_datetime_tz: Utc::now(),
            chrono_datetime64_0: Utc::now(),
            chrono_datetime64_3: Utc::now(),
            chrono_datetime64_6: Utc::now(),
            chrono_datetime64_9: Utc::now(),
            chrono_datetime64_9_tz: Utc::now(),
        }
    }
}

impl Default for Row {
    fn default() -> Self {
        Self::new()
    }
}

fn random_str() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(3)
        .map(char::from)
        .collect()
}

fn max_datetime() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(u32::MAX.into()).unwrap()
}

// The allowed range for DateTime64(8) and lower is
// [1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999] UTC
// See https://clickhouse.com/docs/en/sql-reference/data-types/datetime64
fn max_datetime64() -> OffsetDateTime {
    // 2262-04-11 23:47:16
    OffsetDateTime::new_utc(
        Date::from_calendar_date(2299, Month::December, 31).unwrap(),
        Time::from_hms_micro(23, 59, 59, 999_999).unwrap(),
    )
}

// DateTime64(8)/DateTime(9) allowed range is
// [1900-01-01 00:00:00, 2262-04-11 23:47:16] UTC
// See https://clickhouse.com/docs/en/sql-reference/data-types/datetime64
fn max_datetime64_nanos() -> OffsetDateTime {
    OffsetDateTime::new_utc(
        Date::from_calendar_date(2262, Month::April, 11).unwrap(),
        Time::from_hms_nano(23, 47, 15, 999_999_999).unwrap(),
    )
}

```

# examples/data_types_new_json.rs

```rs
use clickhouse_derive::Row;
use serde::{Deserialize, Serialize};

use clickhouse::sql::Identifier;
use clickhouse::{error::Result, Client};

// Requires ClickHouse 24.10+, as the `input_format_binary_read_json_as_string` and `output_format_binary_write_json_as_string` settings were added in that version.
// Inserting and selecting a row with a JSON column as a string.
// See also: https://clickhouse.com/docs/en/sql-reference/data-types/newjson

#[tokio::main]
async fn main() -> Result<()> {
    let table_name = "chrs_data_types_new_json";
    let client = Client::default()
        .with_url("http://localhost:8123")
        // All these settings can instead be applied on the query or insert level with the same `with_option` method.
        // Enable new JSON type usage
        .with_option("allow_experimental_json_type", "1")
        // Enable inserting JSON columns as a string
        .with_option("input_format_binary_read_json_as_string", "1")
        // Enable selecting JSON columns as a string
        .with_option("output_format_binary_write_json_as_string", "1");

    client
        .query(
            "
            CREATE OR REPLACE TABLE ?
            (
                id   UInt64,
                data JSON
            ) ENGINE MergeTree ORDER BY id;
        ",
        )
        .bind(Identifier(table_name))
        .execute()
        .await?;

    let row = Row {
        id: 1,
        data: r#"
        {
            "name": "John Doe",
            "age": 42,
            "phones": [
                "+123 456 789",
                "+987 654 321"
            ]
        }"#
        .to_string(),
    };

    let mut insert = client.insert(table_name)?;
    insert.write(&row).await?;
    insert.end().await?;

    let db_row = client
        .query("SELECT ?fields FROM ? LIMIT 1")
        .bind(Identifier(table_name))
        .fetch_one::<Row>()
        .await?;

    println!("{db_row:#?}");

    // You can then use any JSON library to parse the JSON string, e.g., serde_json.
    let json_value: serde_json::Value = serde_json::from_str(&db_row.data).expect("Invalid JSON");
    println!("Extracted name from JSON: {}", json_value["name"]);

    Ok(())
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct Row {
    id: u64,
    data: String,
}

```

# examples/data_types_variant.rs

```rs
use clickhouse_derive::Row;
use serde::{Deserialize, Serialize};

use clickhouse::sql::Identifier;
use clickhouse::{error::Result, Client};

// See also: https://clickhouse.com/docs/en/sql-reference/data-types/variant

#[tokio::main]
async fn main() -> Result<()> {
    let table_name = "chrs_data_types_variant";
    let client = Client::default().with_url("http://localhost:8123");

    // No matter the order of the definition on the Variant types in the DDL, this particular Variant will always be sorted as follows:
    // Variant(Array(UInt16), Bool, FixedString(6), Float32, Float64, Int128, Int16, Int32, Int64, Int8, String, UInt128, UInt16, UInt32, UInt64, UInt8)
    client
        .query(
            "
            CREATE OR REPLACE TABLE ?
            (
                `id`  UInt64,
                `var` Variant(
                          Array(UInt16),
                          Bool,
                          Date,
                          FixedString(6),
                          Float32, Float64,
                          Int128, Int16, Int32, Int64, Int8,
                          String,
                          UInt128, UInt16, UInt32, UInt64, UInt8
                      )
            )
            ENGINE = MergeTree
            ORDER BY id",
        )
        .bind(Identifier(table_name))
        .with_option("allow_experimental_variant_type", "1")
        // This is required only if we are mixing similar types in the Variant definition
        // In this case, this is various Int/UInt types, Float32/Float64, and String/FixedString
        // Omit this option if there are no similar types in the definition
        .with_option("allow_suspicious_variant_types", "1")
        .execute()
        .await?;

    let mut insert = client.insert(table_name)?;
    let rows_to_insert = get_rows();
    for row in rows_to_insert {
        insert.write(&row).await?;
    }
    insert.end().await?;

    let rows = client
        .query("SELECT ?fields FROM ?")
        .bind(Identifier(table_name))
        .fetch_all::<MyRow>()
        .await?;

    println!("{rows:#?}");
    Ok(())
}

fn get_rows() -> Vec<MyRow> {
    vec![
        MyRow {
            id: 1,
            var: MyRowVariant::Array(vec![1, 2]),
        },
        MyRow {
            id: 2,
            var: MyRowVariant::Boolean(true),
        },
        MyRow {
            id: 3,
            var: MyRowVariant::Date(
                time::Date::from_calendar_date(2021, time::Month::January, 1).unwrap(),
            ),
        },
        MyRow {
            id: 4,
            var: MyRowVariant::FixedString(*b"foobar"),
        },
        MyRow {
            id: 5,
            var: MyRowVariant::Float32(100.5),
        },
        MyRow {
            id: 6,
            var: MyRowVariant::Float64(200.1),
        },
        MyRow {
            id: 7,
            var: MyRowVariant::Int8(2),
        },
        MyRow {
            id: 8,
            var: MyRowVariant::Int16(3),
        },
        MyRow {
            id: 9,
            var: MyRowVariant::Int32(4),
        },
        MyRow {
            id: 10,
            var: MyRowVariant::Int64(5),
        },
        MyRow {
            id: 11,
            var: MyRowVariant::Int128(6),
        },
        MyRow {
            id: 12,
            var: MyRowVariant::String("my_string".to_string()),
        },
        MyRow {
            id: 13,
            var: MyRowVariant::UInt8(7),
        },
        MyRow {
            id: 14,
            var: MyRowVariant::UInt16(8),
        },
        MyRow {
            id: 15,
            var: MyRowVariant::UInt32(9),
        },
        MyRow {
            id: 16,
            var: MyRowVariant::UInt64(10),
        },
        MyRow {
            id: 17,
            var: MyRowVariant::UInt128(11),
        },
    ]
}

// As the inner Variant types are _always_ sorted alphabetically,
// Rust enum variants should be defined in the _exactly_ same order as it is in the data type;
// their names are irrelevant, only the order of the types matters.
// This enum represents Variant(Array(UInt16), Bool, Date, FixedString(6), Float32, Float64, Int128, Int16, Int32, Int64, Int8, String, UInt128, UInt16, UInt32, UInt64, UInt8)
#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum MyRowVariant {
    Array(Vec<i16>),
    Boolean(bool),
    // attributes should work in this case, too
    #[serde(with = "clickhouse::serde::time::date")]
    Date(time::Date),
    // NB: by default, fetched as raw bytes
    FixedString([u8; 6]),
    Float32(f32),
    Float64(f64),
    Int128(i128),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int8(i8),
    String(String),
    UInt128(u128),
    UInt16(i16),
    UInt32(u32),
    UInt64(u64),
    UInt8(i8),
}

#[derive(Debug, PartialEq, Row, Serialize, Deserialize)]
struct MyRow {
    id: u64,
    var: MyRowVariant,
}

```

# examples/enums.rs

```rs
use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use clickhouse::{error::Result, Client, Row};

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default().with_url("http://localhost:8123");

    client
        .query("DROP TABLE IF EXISTS event_log")
        .execute()
        .await?;

    client
        .query(
            "
            CREATE TABLE event_log (
                timestamp       DateTime64(9),
                message         String,
                level           Enum8(
                                    'Debug' = 1,
                                    'Info' = 2,
                                    'Warn' = 3,
                                    'Error' = 4
                                )
            )
            ENGINE = MergeTree
            ORDER BY timestamp",
        )
        .execute()
        .await?;

    #[derive(Debug, Serialize, Deserialize, Row)]
    struct Event {
        timestamp: u64,
        message: String,
        level: Level,
    }

    // How to define enums that map to `Enum8`/`Enum16`.
    #[derive(Debug, Serialize_repr, Deserialize_repr)]
    #[repr(u8)]
    enum Level {
        Debug = 1,
        Info = 2,
        Warn = 3,
        Error = 4,
    }

    let mut insert = client.insert("event_log")?;
    insert
        .write(&Event {
            timestamp: now(),
            message: "one".into(),
            level: Level::Info,
        })
        .await?;
    insert.end().await?;

    let events = client
        .query("SELECT ?fields FROM event_log")
        .fetch_all::<Event>()
        .await?;
    println!("{events:?}");

    Ok(())
}

fn now() -> u64 {
    UNIX_EPOCH
        .elapsed()
        .expect("invalid system time")
        .as_nanos() as u64
}

```

# examples/inserter.rs

```rs
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc::{self, error::TryRecvError, Receiver},
    time::timeout,
};

use clickhouse::{error::Result, sql::Identifier, Client, Row};

const TABLE_NAME: &str = "chrs_inserter";

#[derive(Debug, Row, Serialize, Deserialize)]
struct MyRow {
    no: u32,
}

// Pattern 1: dense streams
// ------------------------
// This pattern is useful when the stream is dense, i.e. with no/small pauses
// between rows. For instance, when reading from a file or another database.
// In other words, this pattern is applicable for ETL-like tasks.
async fn dense(client: &Client, mut rx: Receiver<u32>) -> Result<()> {
    let mut inserter = client
        .inserter(TABLE_NAME)?
        // We limit the number of rows to be inserted in a single `INSERT` statement.
        // We use small value (100) for the example only.
        // See documentation of `with_max_rows` for details.
        .with_max_rows(100)
        // You can also use other limits. For instance, limit by the size.
        // First reached condition will end the current `INSERT`.
        .with_max_bytes(1_048_576);

    while let Some(no) = rx.recv().await {
        inserter.write(&MyRow { no })?;
        inserter.commit().await?;
    }

    inserter.end().await?;
    Ok(())
}

// Pattern 2: sparse streams
// -------------------------
// This pattern is useful when the stream is sparse, i.e. with pauses between
// rows. For instance, when streaming a real-time stream of events into CH.
// Some rows are arriving one by one with delay, some batched.
async fn sparse(client: &Client, mut rx: Receiver<u32>) -> Result<()> {
    let mut inserter = client
        .inserter(TABLE_NAME)?
        // Slice the stream into chunks (one `INSERT` per chunk) by time.
        // See documentation of `with_period` for details.
        .with_period(Some(Duration::from_millis(100)))
        // If you have a lot of parallel inserters (e.g. on multiple nodes),
        // it's reasonable to add some bias to the period to spread the load.
        .with_period_bias(0.1)
        // We also can use other limits. This is useful when the stream is
        // recovered after a long time of inactivity (e.g. restart of service or CH).
        .with_max_rows(500_000);

    loop {
        let no = match rx.try_recv() {
            Ok(event) => event,
            Err(TryRecvError::Empty) => {
                // If there is no available events, we should wait for the next one.
                // However, we don't know when the next event will arrive.
                // So, we should wait no longer than the left time of the current period.
                let time_left = inserter.time_left().expect("with_period is set");

                // Note: `rx.recv()` must be cancel safe for your channel.
                // This is true for popular `tokio`, `futures-channel`, `flume` channels.
                match timeout(time_left, rx.recv()).await {
                    Ok(Some(event)) => event,
                    // The stream is closed.
                    Ok(None) => break,
                    // Timeout
                    Err(_) => {
                        // If the period is over, we allow the inserter to end the current `INSERT`
                        // statement. If no `INSERT` is in progress, this call is no-op.
                        inserter.commit().await?;
                        continue;
                    }
                }
            }
            Err(TryRecvError::Disconnected) => break,
        };

        inserter.write(&MyRow { no })?;
        inserter.commit().await?;

        // You can use result of `commit()` to get the number of rows inserted.
        // It's useful not only for statistics but also to implement
        // at-least-once delivery by sending this info back to the sender,
        // where all unacknowledged events should be stored in this case.
    }

    inserter.end().await?;
    Ok(())
}

fn spawn_data_generator(n: u32, sparse: bool) -> Receiver<u32> {
    let (tx, rx) = mpsc::channel(1000);

    tokio::spawn(async move {
        for no in 0..n {
            if sparse {
                let delay_ms = if no % 100 == 0 { 20 } else { 2 };
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }

            tx.send(no).await.unwrap();
        }
    });

    rx
}

async fn fetch_batches(client: &Client) -> Result<Vec<(String, u64)>> {
    client
        .query(
            "SELECT toString(insertion_time), count()
             FROM ?
             GROUP BY insertion_time
             ORDER BY insertion_time",
        )
        .bind(Identifier(TABLE_NAME))
        .fetch_all::<(String, u64)>()
        .await
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default().with_url("http://localhost:8123");

    client
        .query(
            "CREATE OR REPLACE TABLE ? (
                 no UInt32,
                 insertion_time DateTime64(6) DEFAULT now64(6)
             )
             ENGINE = MergeTree
             ORDER BY no",
        )
        .bind(Identifier(TABLE_NAME))
        .execute()
        .await?;

    println!("Pattern 1: dense streams");
    let rx = spawn_data_generator(1000, false);
    dense(&client, rx).await?;

    // Prints 10 batches with 100 rows in each.
    for (insertion_time, count) in fetch_batches(&client).await? {
        println!("{}: {} rows", insertion_time, count);
    }

    client
        .query("TRUNCATE TABLE ?")
        .bind(Identifier(TABLE_NAME))
        .execute()
        .await?;

    println!("\nPattern 2: sparse streams");
    let rx = spawn_data_generator(1000, true);
    sparse(&client, rx).await?;

    // Prints batches every 100±10ms.
    for (insertion_time, count) in fetch_batches(&client).await? {
        println!("{}: {} rows", insertion_time, count);
    }

    Ok(())
}

```

# examples/mock.rs

```rs
use clickhouse::{error::Result, test, Client, Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
#[derive(Serialize, Deserialize, Row)]
struct SomeRow {
    no: u32,
}

async fn make_create(client: &Client) -> Result<()> {
    client.query("CREATE TABLE test").execute().await
}

async fn make_select(client: &Client) -> Result<Vec<SomeRow>> {
    client
        .query("SELECT ?fields FROM `who cares`")
        .fetch_all::<SomeRow>()
        .await
}

async fn make_insert(client: &Client, data: &[SomeRow]) -> Result<()> {
    let mut insert = client.insert("who cares")?;
    for row in data {
        insert.write(row).await?;
    }
    insert.end().await
}

#[cfg(feature = "watch")]
async fn make_watch(client: &Client) -> Result<(u64, SomeRow)> {
    client
        .watch("SELECT max(no) no FROM test")
        .fetch_one::<SomeRow>()
        .await
}

#[cfg(feature = "watch")]
async fn make_watch_only_events(client: &Client) -> Result<u64> {
    client
        .watch("SELECT max(no) no FROM test")
        .only_events()
        .fetch_one()
        .await
}

#[tokio::main]
async fn main() {
    let mock = test::Mock::new();
    let client = Client::default().with_url(mock.url());
    let list = vec![SomeRow { no: 1 }, SomeRow { no: 2 }];

    // How to test DDL.
    let recording = mock.add(test::handlers::record_ddl());
    make_create(&client).await.unwrap();
    assert!(recording.query().await.contains("CREATE TABLE"));

    // How to test SELECT.
    mock.add(test::handlers::provide(list.clone()));
    let rows = make_select(&client).await.unwrap();
    assert_eq!(rows, list);

    // How to test failures.
    mock.add(test::handlers::failure(test::status::FORBIDDEN));
    let reason = make_select(&client).await;
    assert_eq!(format!("{reason:?}"), r#"Err(BadResponse("Forbidden"))"#);

    // How to test INSERT.
    let recording = mock.add(test::handlers::record());
    make_insert(&client, &list).await.unwrap();
    let rows: Vec<SomeRow> = recording.collect().await;
    assert_eq!(rows, list);

    // How to test WATCH.
    #[cfg(feature = "watch")]
    {
        // Check `CREATE LIVE VIEW` (for `watch(query)` case only).
        let recording = mock.add(test::handlers::record_ddl());
        mock.add(test::handlers::watch(list.into_iter().map(|row| (42, row))));
        let (version, row) = make_watch(&client).await.unwrap();
        assert!(recording.query().await.contains("CREATE LIVE VIEW"));
        assert_eq!(version, 42);
        assert_eq!(row, SomeRow { no: 1 });

        // `EVENTS`.
        let recording = mock.add(test::handlers::record_ddl());
        mock.add(test::handlers::watch_only_events(3..5));
        let version = make_watch_only_events(&client).await.unwrap();
        assert!(recording.query().await.contains("CREATE LIVE VIEW"));
        assert_eq!(version, 3);
    }
}

```

# examples/query_id.rs

```rs
use clickhouse::{error::Result, Client};
use uuid::Uuid;

/// Besides [`Client::query`], it works similarly with [`Client::insert`] and [`Client::inserter`].
#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default().with_url("http://localhost:8123");

    let query_id = Uuid::new_v4().to_string();

    let numbers = client
        .query("SELECT number FROM system.numbers LIMIT 1")
        .with_option("query_id", &query_id)
        .fetch_all::<u64>()
        .await?;
    println!("Numbers: {numbers:?}");

    // For the sake of this example, force flush the records into the system.query_log table,
    // so we can immediately fetch the query information using the query_id
    client.query("SYSTEM FLUSH LOGS").execute().await?;

    let logged_query = client
        .query("SELECT query FROM system.query_log WHERE query_id = ?")
        .bind(&query_id)
        .fetch_one::<String>()
        .await?;
    println!("Query from system.query_log: {logged_query}");

    Ok(())
}

```

# examples/README.md

```md
# ClickHouse Rust client examples

## Overview

We aim to cover various scenarios of client usage with these examples. You should be able to run any of these examples, see [How to run](#how-to-run) section below.

If something is missing, or you found a mistake in one of these examples, please open an issue or a pull request.

### General usage

- [usage.rs](usage.rs) - creating tables, executing other DDLs, inserting the data, and selecting it back. Additionally, it covers `WATCH` queries. Optional cargo features: `inserter`, `watch`.
- [mock.rs](mock.rs) - writing tests with `mock` feature. Cargo features: requires `test-util`.
- [inserter.rs](inserter.rs) - using the client-side batching via the `inserter` feature. Cargo features: requires `inserter`.
- [async_insert.rs](async_insert.rs) - using the server-side batching via the [asynchronous inserts](https://clickhouse.com/docs/en/optimize/asynchronous-inserts) ClickHouse feature
- [clickhouse_cloud.rs](clickhouse_cloud.rs) - using the client with ClickHouse Cloud, highlighting a few relevant settings (`wait_end_of_query`, `select_sequential_consistency`). Cargo features: requires `rustls-tls`; the code also works with `native-tls`.
- [clickhouse_settings.rs](clickhouse_settings.rs) - applying various ClickHouse settings on the query level

### Data types

- [data_types_derive_simple.rs](data_types_derive_simple.rs) - deriving simpler ClickHouse data types in a struct. Required cargo features: `time`, `uuid`.
- [data_types_derive_containers.rs](data_types_derive_containers.rs) - deriving container-like (Array, Tuple, Map, Nested, Geo) ClickHouse data types in a struct.
- [data_types_variant.rs](data_types_variant.rs) - working with the [Variant data type](https://clickhouse.com/docs/en/sql-reference/data-types/variant).
- [data_types_new_json.rs](data_types_new_json.rs) - working with the [new JSON data type](https://clickhouse.com/docs/en/sql-reference/data-types/newjson) as a String.

### Special cases

- [custom_http_client.rs](custom_http_client.rs) - using a custom Hyper client, tweaking its connection pool settings
- [custom_http_headers.rs](custom_http_headers.rs) - setting additional HTTP headers to the client, or overriding the generated ones
- [query_id.rs](query_id.rs) - setting a specific `query_id` on the query level
- [session_id.rs](session_id.rs) - using the client in the session context with temporary tables
- [stream_into_file.rs](stream_into_file.rs) - streaming the query result as raw bytes into a file in an arbitrary format. Required cargo features: `futures03`.
- [stream_arbitrary_format_rows.rs](stream_arbitrary_format_rows.rs) - streaming the query result in an arbitrary format, row by row. Required cargo features: `futures03`.

## How to run

### Prerequisites

* An [up-to-date Rust installation](https://www.rust-lang.org/tools/install)
* ClickHouse server (see below)

### Running the examples

The examples will require a running ClickHouse server on your machine. 

You could [install it directly](https://clickhouse.com/docs/en/install), or run it via Docker:

\`\`\`sh
docker run -d -p 8123:8123 -p 9000:9000 --name chrs-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
\`\`\`

Then, you should be able to run a particular example via the command-line with:

\`\`\`sh
cargo run --package clickhouse --example async_insert
\`\`\`

If a particular example requires a cargo feature, you could run it as follows:

\`\`\`sh
cargo run --package clickhouse --example usage --features inserter watch
\`\`\`

Additionally, the individual examples should be runnable via the IDE such as CLion or RustRover.

```

# examples/session_id.rs

```rs
use clickhouse_derive::Row;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use clickhouse::sql::Identifier;
use clickhouse::{error::Result, Client};

/// Besides [`Client::with_option`], which will be applied for all requests,
/// `session_id` (and other settings) can be set separately for a particular `query`, `insert`,
/// or when using the `inserter` feature.
///
/// This example uses temporary tables feature to demonstrate the `session_id` usage.
///
/// # Important
/// With clustered deployments, due to lack of "sticky sessions", you need to be connected
/// to a _particular cluster node_ in order to properly utilize this feature, cause, for example,
/// a round-robin load-balancer will not guarantee that the consequent requests will be processed
/// by the same ClickHouse node.
///
/// See also:
/// - https://clickhouse.com/docs/en/sql-reference/statements/create/table#temporary-tables
/// - https://github.com/ClickHouse/ClickHouse/issues/21748
/// - `examples/clickhouse_settings.rs`.
#[tokio::main]
async fn main() -> Result<()> {
    let table_name = "chrs_session_id";
    let session_id = Uuid::new_v4().to_string();

    let client = Client::default()
        .with_url("http://localhost:8123")
        .with_option("session_id", &session_id);

    client
        .query("CREATE TEMPORARY TABLE ? (i Int32)")
        .bind(Identifier(table_name))
        .execute()
        .await?;

    #[derive(Row, Serialize, Deserialize, Debug)]
    struct MyRow {
        i: i32,
    }

    let mut insert = client.insert(table_name)?;
    insert.write(&MyRow { i: 42 }).await?;
    insert.end().await?;

    let data = client
        .query("SELECT ?fields FROM ?")
        .bind(Identifier(table_name))
        .fetch_all::<MyRow>()
        .await?;

    println!("Temporary table data: {data:?}");
    Ok(())
}

```

# examples/stream_arbitrary_format_rows.rs

```rs
use tokio::io::AsyncBufReadExt;

use clickhouse::Client;

/// An example of streaming raw data in an arbitrary format leveraging the
/// [`AsyncBufReadExt`] helpers. In this case, the format is `JSONEachRow`.
/// Incoming data is then split into lines, and each line is deserialized into
/// `serde_json::Value`, a dynamic representation of JSON values.
///
/// Similarly, it can be used with other formats such as CSV, TSV, and others
/// that produce each row on a new line; the only difference will be in how the
/// data is parsed. See also: https://clickhouse.com/docs/en/interfaces/formats
///
/// Note: `lines()` produces a new `String` for each line, so it's not the
/// most performant way to interate over lines.
#[tokio::main]
async fn main() {
    let client = Client::default().with_url("http://localhost:8123");
    let mut lines = client
        .query(
            "SELECT number, hex(randomPrintableASCII(20)) AS hex_str
             FROM system.numbers
             LIMIT 100",
        )
        .fetch_bytes("JSONEachRow")
        .unwrap()
        .lines();

    while let Some(line) = lines.next_line().await.unwrap() {
        let value: serde_json::Value = serde_json::de::from_str(&line).unwrap();
        println!("JSONEachRow value: {}", value);
    }
}

```

# examples/stream_into_file.rs

```rs
use clickhouse::{query::BytesCursor, Client};
use std::time::Instant;
use tokio::{fs::File, io::AsyncWriteExt};

// Examples of streaming the result of a query in an arbitrary format into a
// file. In this case, `CSVWithNamesAndTypes` format is used.
// Check also other formats in https://clickhouse.com/docs/en/interfaces/formats.
//
// Note: there is no need to wrap `File` into `BufWriter` because `BytesCursor`
// is buffered internally already and produces chunks of data.

const NUMBERS: u32 = 100_000;

fn query(numbers: u32) -> BytesCursor {
    let client = Client::default().with_url("http://localhost:8123");

    client
        .query(
            "SELECT number, hex(randomPrintableASCII(20)) AS hex_str
             FROM system.numbers
             LIMIT {limit: Int32}",
        )
        .param("limit", numbers)
        .fetch_bytes("CSVWithNamesAndTypes")
        .unwrap()
}

// Pattern 1: use the `tokio::io::copy_buf` helper.
//
// It shows integration with `tokio::io::AsyncBufWriteExt` trait.
async fn tokio_copy_buf(filename: &str) {
    let mut cursor = query(NUMBERS);
    let mut file = File::create(filename).await.unwrap();
    tokio::io::copy_buf(&mut cursor, &mut file).await.unwrap();
}

// Pattern 2: use `BytesCursor::next()`.
async fn cursor_next(filename: &str) {
    let mut cursor = query(NUMBERS);
    let mut file = File::create(filename).await.unwrap();

    while let Some(bytes) = cursor.next().await.unwrap() {
        file.write_all(&bytes).await.unwrap();
        println!("chunk of {}B written to {filename}", bytes.len());
    }
}

// Pattern 3: use the `futures::(Try)StreamExt` traits.
#[cfg(feature = "futures03")]
async fn futures03_stream(filename: &str) {
    use futures::TryStreamExt;

    let mut cursor = query(NUMBERS);
    let mut file = File::create(filename).await.unwrap();

    while let Some(bytes) = cursor.try_next().await.unwrap() {
        file.write_all(&bytes).await.unwrap();
        println!("chunk of {}B written to {filename}", bytes.len());
    }
}

#[tokio::main]
async fn main() {
    let start = Instant::now();
    tokio_copy_buf("output-1.csv").await;
    println!("written to output-1.csv in {:?}", start.elapsed());

    let start = Instant::now();
    cursor_next("output-2.csv").await;
    println!("written to output-2.csv in {:?}", start.elapsed());

    #[cfg(feature = "futures03")]
    {
        let start = Instant::now();
        futures03_stream("output-3.csv").await;
        println!("written to output-3.csv in {:?}", start.elapsed());
    }
}

```

# examples/usage.rs

```rs
use serde::{Deserialize, Serialize};

use clickhouse::{error::Result, sql, Client, Row};

#[derive(Debug, Row, Serialize, Deserialize)]
struct MyRow<'a> {
    no: u32,
    name: &'a str,
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct MyRowOwned {
    no: u32,
    name: String,
}

async fn ddl(client: &Client) -> Result<()> {
    client.query("DROP TABLE IF EXISTS some").execute().await?;
    client
        .query(
            "
            CREATE TABLE some(no UInt32, name LowCardinality(String))
            ENGINE = MergeTree
            ORDER BY no
        ",
        )
        .execute()
        .await
}

async fn insert(client: &Client) -> Result<()> {
    let mut insert = client.insert("some")?;
    for i in 0..1000 {
        insert.write(&MyRow { no: i, name: "foo" }).await?;
    }

    insert.end().await
}

// This is a very basic example of using the `inserter` feature.
// See `inserter.rs` for real-world patterns.
#[cfg(feature = "inserter")]
async fn inserter(client: &Client) -> Result<()> {
    let mut inserter = client
        .inserter("some")?
        .with_max_rows(100_000)
        .with_period(Some(std::time::Duration::from_secs(15)));

    for i in 0..1000 {
        inserter.write(&MyRow { no: i, name: "foo" })?;
        inserter.commit().await?;
    }

    inserter.end().await?;
    Ok(())
}

async fn fetch(client: &Client) -> Result<()> {
    let mut cursor = client
        .query("SELECT ?fields FROM some WHERE name = ? AND no BETWEEN ? AND ?")
        .bind("foo")
        .bind(500)
        .bind(504)
        .fetch::<MyRow<'_>>()?;

    while let Some(row) = cursor.next().await? {
        println!("{row:?}");
    }

    Ok(())
}

async fn fetch_all(client: &Client) -> Result<()> {
    let vec = client
        .query("SELECT ?fields FROM ? WHERE no BETWEEN ? AND ?")
        .bind(sql::Identifier("some"))
        .bind(500)
        .bind(504)
        .fetch_all::<MyRowOwned>()
        .await?;

    println!("{vec:?}");

    Ok(())
}

async fn delete(client: &Client) -> Result<()> {
    client
        .clone()
        .with_option("mutations_sync", "1")
        .query("ALTER TABLE some DELETE WHERE no >= ?")
        .bind(500)
        .execute()
        .await?;

    Ok(())
}

async fn select_count(client: &Client) -> Result<()> {
    let count = client
        .query("SELECT count() FROM some")
        .fetch_one::<u64>()
        .await?;

    println!("count() = {count}");

    Ok(())
}

#[cfg(feature = "watch")]
async fn watch(client: &Client) -> Result<()> {
    let mut cursor = client
        .watch("SELECT max(no) no, argMax(name, some.no) name FROM some")
        .fetch::<MyRow<'_>>()?;

    let (version, row) = cursor.next().await?.unwrap();
    println!("version={version}, row={row:?}");

    let mut insert = client.insert("some")?;
    let row = MyRow {
        no: row.no + 1,
        name: "bar",
    };
    insert.write(&row).await?;
    insert.end().await?;

    let (version, row) = cursor.next().await?.unwrap();
    println!("version={version}, row={row:?}");

    // Or you can request only events without data.
    let mut cursor = client
        // It's possible to specify a view name.
        .watch("lv_f2ac5347c013c5b9a6c1aab7192dd97c2748daa0")
        .limit(10)
        .only_events()
        .fetch()?;

    println!("{:?}", cursor.next().await);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default().with_url("http://localhost:8123");

    ddl(&client).await?;
    insert(&client).await?;
    #[cfg(feature = "inserter")]
    inserter(&client).await?;
    select_count(&client).await?;
    fetch(&client).await?;
    fetch_all(&client).await?;
    delete(&client).await?;
    select_count(&client).await?;
    #[cfg(feature = "watch")]
    watch(&client).await?;

    Ok(())
}

```

# LICENSE-APACHE

```
                              Apache License
                        Version 2.0, January 2004
                     http://www.apache.org/licenses/

TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

1. Definitions.

   "License" shall mean the terms and conditions for use, reproduction,
   and distribution as defined by Sections 1 through 9 of this document.

   "Licensor" shall mean the copyright owner or entity authorized by
   the copyright owner that is granting the License.

   "Legal Entity" shall mean the union of the acting entity and all
   other entities that control, are controlled by, or are under common
   control with that entity. For the purposes of this definition,
   "control" means (i) the power, direct or indirect, to cause the
   direction or management of such entity, whether by contract or
   otherwise, or (ii) ownership of fifty percent (50%) or more of the
   outstanding shares, or (iii) beneficial ownership of such entity.

   "You" (or "Your") shall mean an individual or Legal Entity
   exercising permissions granted by this License.

   "Source" form shall mean the preferred form for making modifications,
   including but not limited to software source code, documentation
   source, and configuration files.

   "Object" form shall mean any form resulting from mechanical
   transformation or translation of a Source form, including but
   not limited to compiled object code, generated documentation,
   and conversions to other media types.

   "Work" shall mean the work of authorship, whether in Source or
   Object form, made available under the License, as indicated by a
   copyright notice that is included in or attached to the work
   (an example is provided in the Appendix below).

   "Derivative Works" shall mean any work, whether in Source or Object
   form, that is based on (or derived from) the Work and for which the
   editorial revisions, annotations, elaborations, or other modifications
   represent, as a whole, an original work of authorship. For the purposes
   of this License, Derivative Works shall not include works that remain
   separable from, or merely link (or bind by name) to the interfaces of,
   the Work and Derivative Works thereof.

   "Contribution" shall mean any work of authorship, including
   the original version of the Work and any modifications or additions
   to that Work or Derivative Works thereof, that is intentionally
   submitted to Licensor for inclusion in the Work by the copyright owner
   or by an individual or Legal Entity authorized to submit on behalf of
   the copyright owner. For the purposes of this definition, "submitted"
   means any form of electronic, verbal, or written communication sent
   to the Licensor or its representatives, including but not limited to
   communication on electronic mailing lists, source code control systems,
   and issue tracking systems that are managed by, or on behalf of, the
   Licensor for the purpose of discussing and improving the Work, but
   excluding communication that is conspicuously marked or otherwise
   designated in writing by the copyright owner as "Not a Contribution."

   "Contributor" shall mean Licensor and any individual or Legal Entity
   on behalf of whom a Contribution has been received by Licensor and
   subsequently incorporated within the Work.

2. Grant of Copyright License. Subject to the terms and conditions of
   this License, each Contributor hereby grants to You a perpetual,
   worldwide, non-exclusive, no-charge, royalty-free, irrevocable
   copyright license to reproduce, prepare Derivative Works of,
   publicly display, publicly perform, sublicense, and distribute the
   Work and such Derivative Works in Source or Object form.

3. Grant of Patent License. Subject to the terms and conditions of
   this License, each Contributor hereby grants to You a perpetual,
   worldwide, non-exclusive, no-charge, royalty-free, irrevocable
   (except as stated in this section) patent license to make, have made,
   use, offer to sell, sell, import, and otherwise transfer the Work,
   where such license applies only to those patent claims licensable
   by such Contributor that are necessarily infringed by their
   Contribution(s) alone or by combination of their Contribution(s)
   with the Work to which such Contribution(s) was submitted. If You
   institute patent litigation against any entity (including a
   cross-claim or counterclaim in a lawsuit) alleging that the Work
   or a Contribution incorporated within the Work constitutes direct
   or contributory patent infringement, then any patent licenses
   granted to You under this License for that Work shall terminate
   as of the date such litigation is filed.

4. Redistribution. You may reproduce and distribute copies of the
   Work or Derivative Works thereof in any medium, with or without
   modifications, and in Source or Object form, provided that You
   meet the following conditions:

   (a) You must give any other recipients of the Work or
       Derivative Works a copy of this License; and

   (b) You must cause any modified files to carry prominent notices
       stating that You changed the files; and

   (c) You must retain, in the Source form of any Derivative Works
       that You distribute, all copyright, patent, trademark, and
       attribution notices from the Source form of the Work,
       excluding those notices that do not pertain to any part of
       the Derivative Works; and

   (d) If the Work includes a "NOTICE" text file as part of its
       distribution, then any Derivative Works that You distribute must
       include a readable copy of the attribution notices contained
       within such NOTICE file, excluding those notices that do not
       pertain to any part of the Derivative Works, in at least one
       of the following places: within a NOTICE text file distributed
       as part of the Derivative Works; within the Source form or
       documentation, if provided along with the Derivative Works; or,
       within a display generated by the Derivative Works, if and
       wherever such third-party notices normally appear. The contents
       of the NOTICE file are for informational purposes only and
       do not modify the License. You may add Your own attribution
       notices within Derivative Works that You distribute, alongside
       or as an addendum to the NOTICE text from the Work, provided
       that such additional attribution notices cannot be construed
       as modifying the License.

   You may add Your own copyright statement to Your modifications and
   may provide additional or different license terms and conditions
   for use, reproduction, or distribution of Your modifications, or
   for any such Derivative Works as a whole, provided Your use,
   reproduction, and distribution of the Work otherwise complies with
   the conditions stated in this License.

5. Submission of Contributions. Unless You explicitly state otherwise,
   any Contribution intentionally submitted for inclusion in the Work
   by You to the Licensor shall be under the terms and conditions of
   this License, without any additional terms or conditions.
   Notwithstanding the above, nothing herein shall supersede or modify
   the terms of any separate license agreement you may have executed
   with Licensor regarding such Contributions.

6. Trademarks. This License does not grant permission to use the trade
   names, trademarks, service marks, or product names of the Licensor,
   except as required for reasonable and customary use in describing the
   origin of the Work and reproducing the content of the NOTICE file.

7. Disclaimer of Warranty. Unless required by applicable law or
   agreed to in writing, Licensor provides the Work (and each
   Contributor provides its Contributions) on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
   implied, including, without limitation, any warranties or conditions
   of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
   PARTICULAR PURPOSE. You are solely responsible for determining the
   appropriateness of using or redistributing the Work and assume any
   risks associated with Your exercise of permissions under this License.

8. Limitation of Liability. In no event and under no legal theory,
   whether in tort (including negligence), contract, or otherwise,
   unless required by applicable law (such as deliberate and grossly
   negligent acts) or agreed to in writing, shall any Contributor be
   liable to You for damages, including any direct, indirect, special,
   incidental, or consequential damages of any character arising as a
   result of this License or out of the use or inability to use the
   Work (including but not limited to damages for loss of goodwill,
   work stoppage, computer failure or malfunction, or any and all
   other commercial damages or losses), even if such Contributor
   has been advised of the possibility of such damages.

9. Accepting Warranty or Additional Liability. While redistributing
   the Work or Derivative Works thereof, You may choose to offer,
   and charge a fee for, acceptance of support, warranty, indemnity,
   or other liability obligations and/or rights consistent with this
   License. However, in accepting such obligations, You may act only
   on Your own behalf and on Your sole responsibility, not on behalf
   of any other Contributor, and only if You agree to indemnify,
   defend, and hold each Contributor harmless for any liability
   incurred by, or claims asserted against, such Contributor by reason
   of your accepting any such warranty or additional liability.

END OF TERMS AND CONDITIONS

```

# LICENSE-MIT

```
Permission is hereby granted, free of charge, to any
person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the
Software without restriction, including without
limitation the rights to use, copy, modify, merge,
publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software
is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice
shall be included in all copies or substantial portions
of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.

```

# README.md

```md
# clickhouse-rs

Official pure Rust typed client for ClickHouse DB.

[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![License][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]

[crates-badge]: https://img.shields.io/crates/v/clickhouse.svg
[crates-url]: https://crates.io/crates/clickhouse
[docs-badge]: https://docs.rs/clickhouse/badge.svg
[docs-url]: https://docs.rs/clickhouse
[license-badge]: https://img.shields.io/badge/license-MIT_OR_Apache--2.0-blue.svg
[license-url]: https://github.com/ClickHouse/clickhouse-rs/blob/main/LICENSE-MIT
[actions-badge]: https://github.com/ClickHouse/clickhouse-rs/actions/workflows/ci.yml/badge.svg
[actions-url]: https://github.com/ClickHouse/clickhouse-rs/actions/workflows/ci.yml

* Uses `serde` for encoding/decoding rows.
* Supports `serde` attributes: `skip_serializing`, `skip_deserializing`, `rename`.
* Uses `RowBinary` encoding over HTTP transport.
    * There are plans to switch to `Native` over TCP.
* Supports TLS (see `native-tls` and `rustls-tls` features below).
* Supports compression and decompression (LZ4 and LZ4HC).
* Provides API for selecting.
* Provides API for inserting.
* Provides API for infinite transactional (see below) inserting.
* Provides API for watching live views.
* Provides mocks for unit testing.

Note: [ch2rs](https://github.com/ClickHouse/ch2rs) is useful to generate a row type from ClickHouse.

## Usage

To use the crate, add this to your `Cargo.toml`:
\`\`\`toml
[dependencies]
clickhouse = "0.13.2"

[dev-dependencies]
clickhouse = { version = "0.13.2", features = ["test-util"] }
\`\`\`

<details>
<summary>

### Note about ClickHouse prior to v22.6

</summary>

CH server older than v22.6 (2022-06-16) handles `RowBinary` [incorrectly](https://github.com/ClickHouse/ClickHouse/issues/37420) in some rare cases. Use 0.11 and enable `wa-37420` feature to solve this problem. Don't use it for newer versions.

</details>
<details>
<summary>

### Create a client

</summary>

\`\`\`rust,ignore
use clickhouse::Client;

let client = Client::default()
    .with_url("http://localhost:8123")
    .with_user("name")
    .with_password("123")
    .with_database("test");
\`\`\`

* Reuse created clients or clone them in order to reuse a connection pool.

</details>
<details>
<summary>

### Select rows

</summary>

\`\`\`rust,ignore
use serde::Deserialize;
use clickhouse::Row;

#[derive(Row, Deserialize)]
struct MyRow<'a> {
    no: u32,
    name: &'a str,
}

let mut cursor = client
    .query("SELECT ?fields FROM some WHERE no BETWEEN ? AND ?")
    .bind(500)
    .bind(504)
    .fetch::<MyRow<'_>>()?;

while let Some(row) = cursor.next().await? { .. }
\`\`\`

* Placeholder `?fields` is replaced with `no, name` (fields of `Row`).
* Placeholder `?` is replaced with values in following `bind()` calls.
* Convenient `fetch_one::<Row>()` and `fetch_all::<Row>()` can be used to get a first row or all rows correspondingly.
* `sql::Identifier` can be used to bind table names.

Note that cursors can return an error even after producing some rows. To avoid this, use `client.with_option("wait_end_of_query", "1")` in order to enable buffering on the server-side. [More details](https://clickhouse.com/docs/en/interfaces/http/#response-buffering). The `buffer_size` option can be useful too.

</details>
<details>
<summary>

### Insert a batch

</summary>

\`\`\`rust,ignore
use serde::Serialize;
use clickhouse::Row;

#[derive(Row, Serialize)]
struct MyRow {
    no: u32,
    name: String,
}

let mut insert = client.insert("some")?;
insert.write(&MyRow { no: 0, name: "foo".into() }).await?;
insert.write(&MyRow { no: 1, name: "bar".into() }).await?;
insert.end().await?;
\`\`\`

* If `end()` isn't called, the `INSERT` is aborted.
* Rows are being sent progressively to spread network load.
* ClickHouse inserts batches atomically only if all rows fit in the same partition and their number is less [`max_insert_block_size`](https://clickhouse.com/docs/en/operations/settings/settings#max_insert_block_size).

</details>
<details>
<summary>

### Infinite inserting

</summary>

Requires the `inserter` feature.

\`\`\`rust,ignore
let mut inserter = client.inserter("some")?
    .with_timeouts(Some(Duration::from_secs(5)), Some(Duration::from_secs(20)))
    .with_max_bytes(50_000_000)
    .with_max_rows(750_000)
    .with_period(Some(Duration::from_secs(15)));

inserter.write(&MyRow { no: 0, name: "foo".into() })?;
inserter.write(&MyRow { no: 1, name: "bar".into() })?;
let stats = inserter.commit().await?;
if stats.rows > 0 {
    println!(
        "{} bytes, {} rows, {} transactions have been inserted",
        stats.bytes, stats.rows, stats.transactions,
    );
}
\`\`\`

Please, read [examples](https://github.com/ClickHouse/clickhouse-rs/tree/main/examples/inserter.rs) to understand how to use it properly in different real-world cases.

* `Inserter` ends an active insert in `commit()` if thresholds (`max_bytes`, `max_rows`, `period`) are reached.
* The interval between ending active `INSERT`s can be biased by using `with_period_bias` to avoid load spikes by parallel inserters.
* `Inserter::time_left()` can be used to detect when the current period ends. Call `Inserter::commit()` again to check limits if your stream emits items rarely.
* Time thresholds implemented by using [quanta](https://docs.rs/quanta) crate to speed the inserter up. Not used if `test-util` is enabled (thus, time can be managed by `tokio::time::advance()` in custom tests).
* All rows between `commit()` calls are inserted in the same `INSERT` statement.
* Do not forget to flush if you want to terminate inserting:
\`\`\`rust,ignore
inserter.end().await?;
\`\`\`

</details>
<details>
<summary>

### Perform DDL

</summary>

\`\`\`rust,ignore
client.query("DROP TABLE IF EXISTS some").execute().await?;
\`\`\`

</details>
<details>
<summary>

### Live views

</summary>

Requires the `watch` feature.

\`\`\`rust,ignore
let mut cursor = client
    .watch("SELECT max(no), argMax(name, no) FROM some")
    .fetch::<Row<'_>>()?;

let (version, row) = cursor.next().await?.unwrap();
println!("live view updated: version={}, row={:?}", version, row);

// Use `only_events()` to iterate over versions only.
let mut cursor = client.watch("some_live_view").limit(20).only_events().fetch()?;
println!("live view updated: version={:?}", cursor.next().await?);
\`\`\`

* Use [carefully](https://github.com/ClickHouse/ClickHouse/issues/28309#issuecomment-908666042).
* This code uses or creates if not exists a temporary live view named `lv_{sha1(query)}` to reuse the same live view by parallel watchers.
* You can specify a name instead of a query.
* This API uses `JSONEachRowWithProgress` under the hood because of [the issue](https://github.com/ClickHouse/ClickHouse/issues/22996).
* Only struct rows can be used. Avoid `fetch::<u64>()` and other without specified names.

</details>

See [examples](https://github.com/ClickHouse/clickhouse-rs/tree/main/examples).

## Feature Flags
* `lz4` (enabled by default) — enables `Compression::Lz4`. If enabled, `Compression::Lz4` is used by default for all queries except for `WATCH`.
* `inserter` — enables `client.inserter()`.
* `test-util` — adds mocks. See [the example](https://github.com/ClickHouse/clickhouse-rs/tree/main/examples/mock.rs). Use it only in `dev-dependencies`.
* `watch` — enables `client.watch` functionality. See the corresponding section for details.
* `uuid` — adds `serde::uuid` to work with [uuid](https://docs.rs/uuid) crate.
* `time` — adds `serde::time` to work with [time](https://docs.rs/time) crate.
* `chrono` — adds `serde::chrono` to work with [chrono](https://docs.rs/chrono) crate.

### TLS
By default, TLS is disabled and one or more following features must be enabled to use HTTPS urls:
* `native-tls` — uses [native-tls], utilizing dynamic linking (e.g. against OpenSSL).
* `rustls-tls` — enables `rustls-tls-aws-lc` and `rustls-tls-webpki-roots` features.
* `rustls-tls-aws-lc` — uses [rustls] with the `aws-lc` cryptography implementation.
* `rustls-tls-ring` — uses [rustls] with the `ring` cryptography implementation.
* `rustls-tls-webpki-roots` — uses [rustls] with certificates provided by the [webpki-roots] crate.
* `rustls-tls-native-roots` — uses [rustls] with certificates provided by the [rustls-native-certs] crate.

If multiple features are enabled, the following priority is applied:
* `native-tls` > `rustls-tls-aws-lc` > `rustls-tls-ring`
* `rustls-tls-native-roots` > `rustls-tls-webpki-roots`

How to choose between all these features? Here are some considerations:
* A good starting point is `rustls-tls`, e.g. if you use ClickHouse Cloud.
* To be more environment-agnostic, prefer `rustls-tls` over `native-tls`.
* Enable `rustls-tls-native-roots` or `native-tls` if you want to use self-signed certificates.

[native-tls]: https://docs.rs/native-tls
[rustls]: https://docs.rs/rustls
[webpki-roots]: https://docs.rs/webpki-roots
[rustls-native-certs]: https://docs.rs/rustls-native-certs

## Data Types
* `(U)Int(8|16|32|64|128)` maps to/from corresponding `(u|i)(8|16|32|64|128)` types or newtypes around them.
* `(U)Int256` aren't supported directly, but there is [a workaround for it](https://github.com/ClickHouse/clickhouse-rs/issues/48).
* `Float(32|64)` maps to/from corresponding `f(32|64)` or newtypes around them.
* `Decimal(32|64|128)` maps to/from corresponding `i(32|64|128)` or newtypes around them. It's more convenient to use [fixnum](https://github.com/loyd/fixnum) or another implementation of signed fixed-point numbers.
* `Boolean` maps to/from `bool` or newtypes around it.
* `String` maps to/from any string or bytes types, e.g. `&str`, `&[u8]`, `String`, `Vec<u8>` or [`SmartString`](https://docs.rs/smartstring/latest/smartstring/struct.SmartString.html). Newtypes are also supported. To store bytes, consider using [serde_bytes](https://docs.rs/serde_bytes/latest/serde_bytes/), because it's more efficient.
    <details>
    <summary>Example</summary>

    \`\`\`rust,ignore
    #[derive(Row, Debug, Serialize, Deserialize)]
    struct MyRow<'a> {
        str: &'a str,
        string: String,
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
        #[serde(with = "serde_bytes")]
        byte_slice: &'a [u8],
    }
    \`\`\`
    </details>
* `FixedString(N)` is supported as an array of bytes, e.g. `[u8; N]`.
    <details>
    <summary>Example</summary>
  
    \`\`\`rust,ignore
    #[derive(Row, Debug, Serialize, Deserialize)]
    struct MyRow {
        fixed_str: [u8; 16], // FixedString(16)
    }
    \`\`\`
    </details>
* `Enum(8|16)` are supported using [serde_repr](https://docs.rs/serde_repr/latest/serde_repr/).
    <details>
    <summary>Example</summary>

    \`\`\`rust,ignore
    use serde_repr::{Deserialize_repr, Serialize_repr};

    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        level: Level,
    }

    #[derive(Debug, Serialize_repr, Deserialize_repr)]
    #[repr(u8)]
    enum Level {
        Debug = 1,
        Info = 2,
        Warn = 3,
        Error = 4,
    }
    \`\`\`
    </details>
* `UUID` maps to/from [`uuid::Uuid`](https://docs.rs/uuid/latest/uuid/struct.Uuid.html) by using `serde::uuid`. Requires the `uuid` feature.
    <details>
    <summary>Example</summary>

    \`\`\`rust,ignore
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::uuid")]
        uuid: uuid::Uuid,
    }
    \`\`\`
    </details>
* `IPv6` maps to/from [`std::net::Ipv6Addr`](https://doc.rust-lang.org/stable/std/net/struct.Ipv6Addr.html).
* `IPv4` maps to/from [`std::net::Ipv4Addr`](https://doc.rust-lang.org/stable/std/net/struct.Ipv4Addr.html) by using `serde::ipv4`.
    <details>
    <summary>Example</summary>

    \`\`\`rust,ignore
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::ipv4")]
        ipv4: std::net::Ipv4Addr,
    }
    \`\`\`
    </details>
* `Date` maps to/from `u16` or a newtype around it and represents a number of days elapsed since `1970-01-01`. The following external types are supported: 
    * [`time::Date`](https://docs.rs/time/latest/time/struct.Date.html) is supported by using `serde::time::date`, requiring the `time` feature. 
    * [`chrono::NaiveDate`](https://docs.rs/chrono/latest/chrono/struct.NaiveDate.html) is supported by using `serde::chrono::date`, requiring the `chrono` feature. 
    <details>
    <summary>Example</summary>

    \`\`\`rust,ignore
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        days: u16,
        #[serde(with = "clickhouse::serde::time::date")]
        date: Date,
        // if you prefer using chrono:
        #[serde(with = "clickhouse::serde::chrono::date")]
        date_chrono: NaiveDate,
    }

    \`\`\`
    </details>
* `Date32` maps to/from `i32` or a newtype around it and represents a number of days elapsed since `1970-01-01`. The following external types are supported: 
    * [`time::Date`](https://docs.rs/time/latest/time/struct.Date.html) is supported by using `serde::time::date32`, requiring the `time` feature. 
    * [`chrono::NaiveDate`](https://docs.rs/chrono/latest/chrono/struct.NaiveDate.html) is supported by using `serde::chrono::date32`, requiring the `chrono` feature. 
    <details>
    <summary>Example</summary>

    \`\`\`rust,ignore
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        days: i32,
        #[serde(with = "clickhouse::serde::time::date32")]
        date: Date,
        // if you prefer using chrono:
        #[serde(with = "clickhouse::serde::chrono::date32")]
        date_chrono: NaiveDate,

    }

    \`\`\`
    </details>
* `DateTime` maps to/from `u32` or a newtype around it and represents a number of seconds elapsed since UNIX epoch. The following external types are supported:
    * [`time::OffsetDateTime`](https://docs.rs/time/latest/time/struct.OffsetDateTime.html) is supported by using `serde::time::datetime`, requiring the `time` feature. 
    * [`chrono::DateTime<Utc>`](https://docs.rs/chrono/latest/chrono/struct.DateTime.html) is supported by using `serde::chrono::datetime`, requiring the `chrono` feature. 
    <details>
    <summary>Example</summary>

    \`\`\`rust,ignore
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        ts: u32,
        #[serde(with = "clickhouse::serde::time::datetime")]
        dt: OffsetDateTime,
        // if you prefer using chrono:
        #[serde(with = "clickhouse::serde::chrono::datetime")]
        dt_chrono: DateTime<Utc>,        
    }
    \`\`\`
    </details>
* `DateTime64(_)` maps to/from `i64` or a newtype around it and represents a time elapsed since UNIX epoch. The following external types are supported:
    * [`time::OffsetDateTime`](https://docs.rs/time/latest/time/struct.OffsetDateTime.html) is supported by using `serde::time::datetime64::*`, requiring the `time` feature. 
    * [`chrono::DateTime<Utc>`](https://docs.rs/chrono/latest/chrono/struct.DateTime.html) is supported by using `serde::chrono::datetime64::*`, requiring the `chrono` feature. 
    <details>
    <summary>Example</summary>

    \`\`\`rust,ignore
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        ts: i64, // elapsed s/us/ms/ns depending on `DateTime64(X)`
        #[serde(with = "clickhouse::serde::time::datetime64::secs")]
        dt64s: OffsetDateTime,  // `DateTime64(0)`
        #[serde(with = "clickhouse::serde::time::datetime64::millis")]
        dt64ms: OffsetDateTime, // `DateTime64(3)`
        #[serde(with = "clickhouse::serde::time::datetime64::micros")]
        dt64us: OffsetDateTime, // `DateTime64(6)`
        #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
        dt64ns: OffsetDateTime, // `DateTime64(9)`
        // if you prefer using chrono:
        #[serde(with = "clickhouse::serde::chrono::datetime64::secs")]
        dt64s_chrono: DateTime<Utc>,  // `DateTime64(0)`
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        dt64ms_chrono: DateTime<Utc>, // `DateTime64(3)`
        #[serde(with = "clickhouse::serde::chrono::datetime64::micros")]
        dt64us_chrono: DateTime<Utc>, // `DateTime64(6)`
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        dt64ns_chrono: DateTime<Utc>, // `DateTime64(9)`
    }


    \`\`\`
    </details>
* `Tuple(A, B, ...)` maps to/from `(A, B, ...)` or a newtype around it.
* `Array(_)` maps to/from any slice, e.g. `Vec<_>`, `&[_]`. Newtypes are also supported.
* `Map(K, V)` behaves like `Array((K, V))`.
* `LowCardinality(_)` is supported seamlessly.
* `Nullable(_)` maps to/from `Option<_>`. For `clickhouse::serde::*` helpers add `::option`.
    <details>
    <summary>Example</summary>

    \`\`\`rust,ignore
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::ipv4::option")]
        ipv4_opt: Option<Ipv4Addr>,
    }
    \`\`\`
    </details>
* `Nested` is supported by providing multiple arrays with renaming.
    <details>
    <summary>Example</summary>

    \`\`\`rust,ignore
    // CREATE TABLE test(items Nested(name String, count UInt32))
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        #[serde(rename = "items.name")]
        items_name: Vec<String>,
        #[serde(rename = "items.count")]
        items_count: Vec<u32>,
    }
    \`\`\`
    </details>
* `Geo` types are supported. `Point` behaves like a tuple `(f64, f64)`, and the rest of the types are just slices of points. 
    <details>
    <summary>Example</summary>

    \`\`\`rust,ignore
    type Point = (f64, f64);
    type Ring = Vec<Point>;
    type Polygon = Vec<Ring>;
    type MultiPolygon = Vec<Polygon>;
    type LineString = Vec<Point>;
    type MultiLineString = Vec<LineString>;
  
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        point: Point,
        ring: Ring,
        polygon: Polygon,
        multi_polygon: MultiPolygon,
        line_string: LineString,
        multi_line_string: MultiLineString,
    }
    \`\`\`
    </details>
* `Variant` data type is supported as a Rust enum. As the inner Variant types are _always_ sorted alphabetically, Rust enum variants should be defined in the _exactly_ same order as it is in the data type; their names are irrelevant, only the order of the types matters. This following example has a column defined as `Variant(Array(UInt16), Bool, Date, String, UInt32)`:
    <details>
    <summary>Example</summary>
    
    \`\`\`rust,ignore
    #[derive(Serialize, Deserialize)]
    enum MyRowVariant {
        Array(Vec<i16>),
        Boolean(bool),
        #[serde(with = "clickhouse::serde::time::date")]
        Date(time::Date),
        String(String),
        UInt32(u32),
    }
    
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        id: u64,
        var: MyRowVariant,
    }
    \`\`\`
    </details>
* [New `JSON` data type](https://clickhouse.com/docs/en/sql-reference/data-types/newjson) is currently supported as a string when using ClickHouse 24.10+. See [this example](examples/data_types_new_json.rs) for more details.
* `Dynamic` data type is not supported for now.

See also the additional examples:

* [Simpler ClickHouse data types](examples/data_types_derive_simple.rs)
* [Container-like ClickHouse data types](examples/data_types_derive_containers.rs)
* [Variant data type](examples/data_types_variant.rs)

## Mocking
The crate provides utils for mocking CH server and testing DDL, `SELECT`, `INSERT` and `WATCH` queries.

The functionality can be enabled with the `test-util` feature. Use it **only** in dev-dependencies.

See [the example](https://github.com/ClickHouse/clickhouse-rs/tree/main/examples/mock.rs).

```

# release.toml

```toml
pre-release-commit-message = "chore: release {{version}}"
pre-release-replacements = [
    {file="README.md", search="^clickhouse =(.*)\"[0-9.]+\"", replace="{{crate_name}} =${1}\"{{version}}\""},
    {file="CHANGELOG.md", search="Unreleased", replace="{{version}}"},
    {file="CHANGELOG.md", search="\\.\\.\\.HEAD", replace="...{{tag_name}}", exactly=1},
    {file="CHANGELOG.md", search="ReleaseDate", replace="{{date}}"},
    {file="CHANGELOG.md", search="<!-- next-header -->", replace="<!-- next-header -->\n\n## [Unreleased] - ReleaseDate", exactly=1},
    {file="CHANGELOG.md", search="<!-- next-url -->", replace="<!-- next-url -->\n[Unreleased]: https://github.com/ClickHouse/clickhouse-rs/compare/{{tag_name}}...HEAD", exactly=1},
]
allow-branch = ["main"]

```

# rustfmt.toml

```toml
edition = "2021"
merge_derives = false
imports_granularity = "Crate"
normalize_comments = true
reorder_impl_items = true
wrap_comments = true

```

# src/bytes_ext.rs

```rs
use bytes::{Bytes, BytesMut};

#[derive(Default)]
pub(crate) struct BytesExt {
    bytes: Bytes,
    cursor: usize,
}

impl BytesExt {
    #[inline(always)]
    pub(crate) fn slice(&self) -> &[u8] {
        &self.bytes[self.cursor..]
    }

    #[inline(always)]
    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.cursor
    }

    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        debug_assert!(self.cursor <= self.bytes.len());
        self.cursor >= self.bytes.len()
    }

    #[inline(always)]
    pub(crate) fn set_remaining(&mut self, n: usize) {
        // We can use `bytes.advance()` here, but it's slower.
        self.cursor = self.bytes.len() - n;
    }

    #[cfg(any(test, feature = "lz4", feature = "watch"))]
    #[inline(always)]
    pub(crate) fn advance(&mut self, n: usize) {
        debug_assert!(n <= self.remaining());

        // We can use `bytes.advance()` here, but it's slower.
        self.cursor += n;
    }

    #[inline(always)]
    pub(crate) fn extend(&mut self, chunk: Bytes) {
        if self.is_empty() {
            // Most of the time, we read the next chunk after consuming the previous one.
            self.bytes = chunk;
            self.cursor = 0;
        } else {
            // Some bytes are left in the buffer, we need to merge them with the next chunk.
            self.extend_slow(chunk);
        }
    }

    #[cold]
    #[inline(never)]
    fn extend_slow(&mut self, chunk: Bytes) {
        let total = self.remaining() + chunk.len();
        let mut new_bytes = BytesMut::with_capacity(total);
        let capacity = new_bytes.capacity();
        new_bytes.extend_from_slice(self.slice());
        new_bytes.extend_from_slice(&chunk);
        debug_assert_eq!(new_bytes.capacity(), capacity);
        self.bytes = new_bytes.freeze();
        self.cursor = 0;
    }
}

#[test]
fn it_works() {
    let mut bytes = BytesExt::default();
    assert!(bytes.slice().is_empty());
    assert_eq!(bytes.remaining(), 0);

    bytes.extend(Bytes::from_static(b"hello"));
    assert_eq!(bytes.slice(), b"hello");
    assert_eq!(bytes.remaining(), 5);

    bytes.advance(3);
    assert_eq!(bytes.slice(), b"lo");
    assert_eq!(bytes.remaining(), 2);

    bytes.extend(Bytes::from_static(b"l"));
    assert_eq!(bytes.slice(), b"lol");
    assert_eq!(bytes.remaining(), 3);

    bytes.set_remaining(1);
    assert_eq!(bytes.slice(), b"l");
    assert_eq!(bytes.remaining(), 1);
}

```

# src/compression/lz4.rs

```rs
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use cityhash_rs::cityhash_102_128;
use futures::{ready, stream::Stream};
use lz4_flex::block;

use crate::{
    bytes_ext::BytesExt,
    error::{Error, Result},
    response::Chunk,
};

const MAX_COMPRESSED_SIZE: u32 = 1024 * 1024 * 1024;

pub(crate) struct Lz4Decoder<S> {
    stream: S,
    bytes: BytesExt,
    meta: Option<Lz4Meta>,
}

impl<S> Stream for Lz4Decoder<S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let meta = loop {
            let size = self.bytes.remaining();
            let required_size = self
                .meta
                .as_ref()
                .map_or(LZ4_META_SIZE, Lz4Meta::total_size);

            if size < required_size {
                let stream = Pin::new(&mut self.stream);
                match ready!(stream.poll_next(cx)) {
                    Some(Ok(chunk)) => {
                        self.bytes.extend(chunk);
                        continue;
                    }
                    Some(Err(err)) => return Some(Err(err)).into(),
                    None if size > 0 => {
                        let err = Error::Decompression("malformed data".into());
                        return Poll::Ready(Some(Err(err)));
                    }
                    None => return Poll::Ready(None),
                }
            }

            debug_assert!(size >= required_size);

            match self.meta.take() {
                Some(meta) => break meta,
                None => self.meta = Some(self.read_meta()?),
            };
        };

        let data = self.read_data(&meta)?;
        let net_size = meta.total_size();
        self.bytes.advance(net_size);

        Poll::Ready(Some(Ok(Chunk { data, net_size })))
    }
}

// Meta = checksum + header
// - [16b] checksum
// - [ 1b] magic number (0x82)
// - [ 4b] compressed size (data + header)
// - [ 4b] uncompressed size
const LZ4_CHECKSUM_SIZE: usize = 16;
const LZ4_HEADER_SIZE: usize = 9;
const LZ4_META_SIZE: usize = LZ4_CHECKSUM_SIZE + LZ4_HEADER_SIZE;
const LZ4_MAGIC: u8 = 0x82;

struct Lz4Meta {
    checksum: u128,
    compressed_size: u32,
    uncompressed_size: u32,
}

impl Lz4Meta {
    fn total_size(&self) -> usize {
        LZ4_CHECKSUM_SIZE + self.compressed_size as usize
    }

    fn read(mut bytes: &[u8]) -> Result<Lz4Meta> {
        let checksum = bytes.get_u128_le();
        let magic = bytes.get_u8();
        let compressed_size = bytes.get_u32_le();
        let uncompressed_size = bytes.get_u32_le();

        if magic != LZ4_MAGIC {
            return Err(Error::Decompression("incorrect magic number".into()));
        }

        if compressed_size > MAX_COMPRESSED_SIZE {
            return Err(Error::Decompression("too big compressed data".into()));
        }

        Ok(Lz4Meta {
            checksum,
            compressed_size,
            uncompressed_size,
        })
    }

    fn write_checksum(&self, mut buffer: &mut [u8]) {
        buffer.put_u128_le(self.checksum);
    }

    fn write_header(&self, mut buffer: &mut [u8]) {
        buffer.put_u8(LZ4_MAGIC);
        buffer.put_u32_le(self.compressed_size);
        buffer.put_u32_le(self.uncompressed_size);
    }
}

impl<S> Lz4Decoder<S> {
    pub(crate) fn new(stream: S) -> Self {
        Self {
            stream,
            bytes: BytesExt::default(),
            meta: None,
        }
    }

    fn read_meta(&mut self) -> Result<Lz4Meta> {
        Lz4Meta::read(self.bytes.slice())
    }

    fn read_data(&mut self, meta: &Lz4Meta) -> Result<Bytes> {
        let total_size = meta.total_size();
        let bytes = &self.bytes.slice()[..total_size];

        let actual_checksum = calc_checksum(&bytes[LZ4_CHECKSUM_SIZE..]);
        if actual_checksum != meta.checksum {
            return Err(Error::Decompression("checksum mismatch".into()));
        }

        let uncompressed = block::decompress_size_prepended(&bytes[(LZ4_META_SIZE - 4)..])
            .map_err(|err| Error::Decompression(err.into()))?;

        debug_assert_eq!(uncompressed.len() as u32, meta.uncompressed_size);
        Ok(uncompressed.into())
    }
}

fn calc_checksum(buffer: &[u8]) -> u128 {
    let hash = cityhash_102_128(buffer);
    hash.rotate_right(64)
}

pub(crate) fn compress(uncompressed: &[u8]) -> Result<Bytes> {
    let max_compressed_size = block::get_maximum_output_size(uncompressed.len());

    let mut buffer = BytesMut::new();
    buffer.resize(LZ4_META_SIZE + max_compressed_size, 0);

    let compressed_data_size = block::compress_into(uncompressed, &mut buffer[LZ4_META_SIZE..])
        .map_err(|err| Error::Compression(err.into()))?;

    buffer.truncate(LZ4_META_SIZE + compressed_data_size);

    let mut meta = Lz4Meta {
        checksum: 0, // will be calculated below.
        compressed_size: (LZ4_HEADER_SIZE + compressed_data_size) as u32,
        uncompressed_size: uncompressed.len() as u32,
    };

    meta.write_header(&mut buffer[LZ4_CHECKSUM_SIZE..]);
    meta.checksum = calc_checksum(&buffer[LZ4_CHECKSUM_SIZE..]);
    meta.write_checksum(&mut buffer[..]);

    Ok(buffer.freeze())
}

#[tokio::test]
async fn it_decompresses() {
    use futures::stream::{self, TryStreamExt};

    let expected = vec![
        1u8, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105, 110, 103, 3, 97, 98,
        99,
    ];

    let source = vec![
        245_u8, 5, 222, 235, 225, 158, 59, 108, 225, 31, 65, 215, 66, 66, 36, 92,   // checksum
        0x82, // magic number
        34, 0, 0, 0, // compressed size (data + header)
        23, 0, 0, 0, // uncompressed size
        240, 8, 1, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105, 110, 103, 3,
        97, 98, 99,
    ];

    async fn test(chunks: &[&[u8]], expected: &[u8]) {
        let stream = stream::iter(
            chunks
                .iter()
                .map(|s| Bytes::copy_from_slice(s))
                .map(Ok::<_, Error>)
                .collect::<Vec<_>>(),
        );
        let mut decoder = Lz4Decoder::new(stream);
        let actual = decoder.try_next().await.unwrap().unwrap();
        assert_eq!(actual.data, expected);
        assert_eq!(
            actual.net_size,
            chunks.iter().map(|s| s.len()).sum::<usize>()
        );
    }

    // 1 chunk.
    test(&[&source], &expected).await;

    // 2 chunks.
    for i in 0..source.len() {
        let (left, right) = source.split_at(i);
        test(&[left, right], &expected).await;

        // 3 chunks.
        for j in i..source.len() {
            let (right_a, right_b) = right.split_at(j - i);
            test(&[left, right_a, right_b], &expected).await;
        }
    }
}

#[test]
fn it_compresses() {
    let source = vec![
        1u8, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105, 110, 103, 3, 97, 98,
        99,
    ];

    let expected = vec![
        245_u8, 5, 222, 235, 225, 158, 59, 108, 225, 31, 65, 215, 66, 66, 36, 92, 130, 34, 0, 0, 0,
        23, 0, 0, 0, 240, 8, 1, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105,
        110, 103, 3, 97, 98, 99,
    ];

    let actual = compress(&source).unwrap();
    assert_eq!(actual, expected);
}

```

# src/compression/mod.rs

```rs
#[cfg(feature = "lz4")]
pub(crate) mod lz4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Compression {
    /// Disables any compression.
    /// Used by default if the `lz4` feature is disabled.
    None,
    /// Uses `LZ4` codec to (de)compress.
    /// Used by default if the `lz4` feature is enabled.
    #[cfg(feature = "lz4")]
    Lz4,
    /// Uses `LZ4HC` codec to compress and `LZ4` to decompress.
    /// High compression levels are useful in networks with low bandwidth.
    /// Affects only `INSERT`s, because others are compressed by the server.
    /// Possible levels: `[1, 12]`. Recommended level range: `[4, 9]`.
    ///
    /// Deprecated: `lz4_flex` doesn't support HC mode yet: [lz4_flex#165].
    ///
    /// [lz4_flex#165]: https://github.com/PSeitz/lz4_flex/issues/165
    #[cfg(feature = "lz4")]
    #[deprecated(note = "use `Compression::Lz4` instead")]
    Lz4Hc(i32),
}

impl Default for Compression {
    #[cfg(feature = "lz4")]
    #[inline]
    fn default() -> Self {
        if cfg!(feature = "test-util") {
            Compression::None
        } else {
            Compression::Lz4
        }
    }

    #[cfg(not(feature = "lz4"))]
    #[inline]
    fn default() -> Self {
        Compression::None
    }
}

impl Compression {
    pub(crate) fn is_lz4(&self) -> bool {
        *self != Compression::None
    }
}

```

# src/cursors/bytes.rs

```rs
use crate::{cursors::RawCursor, error::Result, response::Response};
use bytes::{Buf, Bytes, BytesMut};
use std::{
    io::Result as IoResult,
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio::io::{AsyncBufRead, AsyncRead, ReadBuf};

/// A cursor over raw bytes of the response returned by [`Query::fetch_bytes`].
///
/// Unlike [`RowCursor`] which emits rows deserialized as structures from
/// RowBinary, this cursor emits raw bytes without deserialization.
///
/// # Integration
///
/// Additionally to [`BytesCursor::next`] and [`BytesCursor::collect`],
/// this cursor implements:
/// * [`AsyncRead`] and [`AsyncBufRead`] for `tokio`-based ecosystem.
/// * [`futures::Stream`], [`futures::AsyncRead`] and [`futures::AsyncBufRead`]
///   for `futures`-based ecosystem. (requires the `futures03` feature)
///
/// For instance, if the requested format emits each row on a newline
/// (e.g. `JSONEachRow`, `CSV`, `TSV`, etc.), the cursor can be read line by
/// line using `AsyncBufReadExt::lines`. Note that this method
/// produces a new `String` for each line, so it's not the most performant way
/// to iterate.
///
/// Note: methods of these traits use [`std::io::Error`] for errors.
/// To get an original error from this crate, use `From` conversion.
///
/// [`RowCursor`]: crate::query::RowCursor
/// [`Query::fetch_bytes`]: crate::query::Query::fetch_bytes
pub struct BytesCursor {
    raw: RawCursor,
    bytes: Bytes,
}

// TODO: what if any next/poll_* called AFTER error returned?

impl BytesCursor {
    pub(crate) fn new(response: Response) -> Self {
        Self {
            raw: RawCursor::new(response),
            bytes: Bytes::default(),
        }
    }

    /// Emits the next bytes chunk.
    ///
    /// # Cancel safety
    ///
    /// This method is cancellation safe.
    pub async fn next(&mut self) -> Result<Option<Bytes>> {
        assert!(
            self.bytes.is_empty(),
            "mixing `BytesCursor::next()` and `AsyncRead` API methods is not allowed"
        );

        self.raw.next().await
    }

    /// Collects the whole response into a single [`Bytes`].
    ///
    /// # Cancel safety
    ///
    /// This method is NOT cancellation safe.
    /// If cancelled, already collected bytes are lost.
    pub async fn collect(&mut self) -> Result<Bytes> {
        let mut chunks = Vec::new();
        let mut total_len = 0;

        while let Some(chunk) = self.next().await? {
            total_len += chunk.len();
            chunks.push(chunk);
        }

        // The whole response is in a single chunk.
        if chunks.len() == 1 {
            return Ok(chunks.pop().unwrap());
        }

        let mut collected = BytesMut::with_capacity(total_len);
        for chunk in chunks {
            collected.extend_from_slice(&chunk);
        }
        debug_assert_eq!(collected.capacity(), total_len);

        Ok(collected.freeze())
    }

    #[cold]
    fn poll_refill(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<bool>> {
        debug_assert_eq!(self.bytes.len(), 0);

        // Theoretically, `self.raw.poll_next(cx)` can return empty chunks.
        // In this case, we should continue polling until we get a non-empty chunk or
        // end of stream in order to avoid false positive `Ok(0)` in I/O traits.
        while self.bytes.is_empty() {
            match ready!(self.raw.poll_next(cx)?) {
                Some(chunk) => self.bytes = chunk,
                None => return Poll::Ready(Ok(false)),
            }
        }

        Poll::Ready(Ok(true))
    }

    /// Returns the total size in bytes received from the CH server since
    /// the cursor was created.
    ///
    /// This method counts only size without HTTP headers for now.
    /// It can be changed in the future without notice.
    #[inline]
    pub fn received_bytes(&self) -> u64 {
        self.raw.received_bytes()
    }

    /// Returns the total size in bytes decompressed since the cursor was
    /// created.
    #[inline]
    pub fn decoded_bytes(&self) -> u64 {
        self.raw.decoded_bytes()
    }
}

impl AsyncRead for BytesCursor {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        while buf.remaining() > 0 {
            if self.bytes.is_empty() && !ready!(self.poll_refill(cx)?) {
                break;
            }

            let len = self.bytes.len().min(buf.remaining());
            let bytes = self.bytes.slice(..len);
            buf.put_slice(&bytes[0..len]);
            self.bytes.advance(len);
        }

        Poll::Ready(Ok(()))
    }
}

impl AsyncBufRead for BytesCursor {
    #[inline]
    fn poll_fill_buf(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<&[u8]>> {
        if self.bytes.is_empty() {
            ready!(self.poll_refill(cx)?);
        }

        Poll::Ready(Ok(&self.get_mut().bytes))
    }

    #[inline]
    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        assert!(
            amt <= self.bytes.len(),
            "invalid `AsyncBufRead::consume` usage"
        );
        self.bytes.advance(amt);
    }
}

#[cfg(feature = "futures03")]
impl futures::AsyncRead for BytesCursor {
    #[inline]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        let mut buf = ReadBuf::new(buf);
        ready!(AsyncRead::poll_read(self, cx, &mut buf)?);
        Poll::Ready(Ok(buf.filled().len()))
    }
}

#[cfg(feature = "futures03")]
impl futures::AsyncBufRead for BytesCursor {
    #[inline]
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<&[u8]>> {
        AsyncBufRead::poll_fill_buf(self, cx)
    }

    #[inline]
    fn consume(self: Pin<&mut Self>, amt: usize) {
        AsyncBufRead::consume(self, amt);
    }
}

#[cfg(feature = "futures03")]
impl futures::stream::Stream for BytesCursor {
    type Item = crate::error::Result<bytes::Bytes>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        assert!(
            self.bytes.is_empty(),
            "mixing `Stream` and `AsyncRead` API methods is not allowed"
        );

        self.raw.poll_next(cx).map(Result::transpose)
    }
}

#[cfg(feature = "futures03")]
impl futures::stream::FusedStream for BytesCursor {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.bytes.is_empty() && self.raw.is_terminated()
    }
}

```

# src/cursors/json.rs

```rs
use crate::{
    bytes_ext::BytesExt,
    cursors::RawCursor,
    error::{Error, Result},
    response::Response,
};
use serde::Deserialize;
use std::marker::PhantomData;

pub(crate) struct JsonCursor<T> {
    raw: RawCursor,
    bytes: BytesExt,
    line: String,
    _marker: PhantomData<T>,
}

// We use `JSONEachRowWithProgress` to avoid infinite HTTP connections.
// See https://github.com/ClickHouse/ClickHouse/issues/22996 for details.
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum JsonRow<T> {
    Row(T),
    Progress {},
}

impl<T> JsonCursor<T> {
    const INITIAL_BUFFER_SIZE: usize = 1024;

    pub(crate) fn new(response: Response) -> Self {
        Self {
            raw: RawCursor::new(response),
            bytes: BytesExt::default(),
            line: String::with_capacity(Self::INITIAL_BUFFER_SIZE),
            _marker: PhantomData,
        }
    }

    pub(crate) async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<T>>
    where
        T: Deserialize<'b>,
    {
        use bytes::Buf;
        use std::io::BufRead;

        loop {
            self.line.clear();

            let read = match self.bytes.slice().reader().read_line(&mut self.line) {
                Ok(read) => read,
                Err(err) => return Err(Error::Custom(err.to_string())),
            };

            if let Some(line) = self.line.strip_suffix('\n') {
                self.bytes.advance(read);

                match serde_json::from_str(super::workaround_51132(line)) {
                    Ok(JsonRow::Row(value)) => return Ok(Some(value)),
                    Ok(JsonRow::Progress { .. }) => continue,
                    Err(err) => return Err(Error::BadResponse(err.to_string())),
                }
            }

            match self.raw.next().await? {
                Some(chunk) => self.bytes.extend(chunk),
                None => return Ok(None),
            }
        }
    }
}

```

# src/cursors/mod.rs

```rs
#[cfg(feature = "watch")]
pub(crate) use self::json::JsonCursor;
pub(crate) use self::raw::RawCursor;
pub use self::{bytes::BytesCursor, row::RowCursor};

mod bytes;
#[cfg(feature = "watch")]
mod json;
mod raw;
mod row;

// XXX: it was a workaround for https://github.com/rust-lang/rust/issues/51132,
//      but introduced #24 and must be fixed.
fn workaround_51132<'a, T: ?Sized>(ptr: &T) -> &'a T {
    // SAFETY: actually, it leads to unsoundness, see #24
    unsafe { &*(ptr as *const T) }
}

```

# src/cursors/raw.rs

```rs
use crate::{
    error::Result,
    response::{Chunks, Response, ResponseFuture},
};
use bytes::Bytes;
use futures::Stream;
use std::{
    pin::pin,
    task::{ready, Context, Poll},
};

/// A cursor over raw bytes of a query response.
/// All other cursors are built on top of this one.
pub(crate) struct RawCursor(RawCursorState);

enum RawCursorState {
    Waiting(ResponseFuture),
    Loading(RawCursorLoading),
}

struct RawCursorLoading {
    chunks: Chunks,
    net_size: u64,
    data_size: u64,
}

impl RawCursor {
    pub(crate) fn new(response: Response) -> Self {
        Self(RawCursorState::Waiting(response.into_future()))
    }

    pub(crate) async fn next(&mut self) -> Result<Option<Bytes>> {
        std::future::poll_fn(|cx| self.poll_next(cx)).await
    }

    pub(crate) fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Bytes>>> {
        if let RawCursorState::Loading(state) = &mut self.0 {
            let chunks = pin!(&mut state.chunks);

            Poll::Ready(match ready!(chunks.poll_next(cx)?) {
                Some(chunk) => {
                    state.net_size += chunk.net_size as u64;
                    state.data_size += chunk.data.len() as u64;
                    Ok(Some(chunk.data))
                }
                None => Ok(None),
            })
        } else {
            ready!(self.poll_resolve(cx)?);
            self.poll_next(cx)
        }
    }

    #[cold]
    #[inline(never)]
    fn poll_resolve(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let RawCursorState::Waiting(future) = &mut self.0 else {
            panic!("poll_resolve called in invalid state");
        };

        // Poll the future, but don't return the result yet.
        // In case of an error, we should replace the current state anyway
        // in order to provide proper fused behavior of the cursor.
        let res = ready!(future.as_mut().poll(cx));
        let mut chunks = Chunks::empty();
        let res = res.map(|c| chunks = c);

        self.0 = RawCursorState::Loading(RawCursorLoading {
            chunks,
            net_size: 0,
            data_size: 0,
        });

        Poll::Ready(res)
    }

    pub(crate) fn received_bytes(&self) -> u64 {
        match &self.0 {
            RawCursorState::Loading(state) => state.net_size,
            RawCursorState::Waiting(_) => 0,
        }
    }

    pub(crate) fn decoded_bytes(&self) -> u64 {
        match &self.0 {
            RawCursorState::Loading(state) => state.data_size,
            RawCursorState::Waiting(_) => 0,
        }
    }

    #[cfg(feature = "futures03")]
    pub(crate) fn is_terminated(&self) -> bool {
        match &self.0 {
            RawCursorState::Loading(state) => state.chunks.is_terminated(),
            RawCursorState::Waiting(_) => false,
        }
    }
}

```

# src/cursors/row.rs

```rs
use crate::{
    bytes_ext::BytesExt,
    cursors::RawCursor,
    error::{Error, Result},
    response::Response,
    rowbinary,
};
use serde::Deserialize;
use std::marker::PhantomData;

/// A cursor that emits rows deserialized as structures from RowBinary.
#[must_use]
pub struct RowCursor<T> {
    raw: RawCursor,
    bytes: BytesExt,
    _marker: PhantomData<T>,
}

impl<T> RowCursor<T> {
    pub(crate) fn new(response: Response) -> Self {
        Self {
            raw: RawCursor::new(response),
            bytes: BytesExt::default(),
            _marker: PhantomData,
        }
    }

    /// Emits the next row.
    ///
    /// The result is unspecified if it's called after `Err` is returned.
    ///
    /// # Cancel safety
    ///
    /// This method is cancellation safe.
    pub async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<T>>
    where
        T: Deserialize<'b>,
    {
        loop {
            let mut slice = super::workaround_51132(self.bytes.slice());

            match rowbinary::deserialize_from(&mut slice) {
                Ok(value) => {
                    self.bytes.set_remaining(slice.len());
                    return Ok(Some(value));
                }
                Err(Error::NotEnoughData) => {}
                Err(err) => return Err(err),
            }

            match self.raw.next().await? {
                Some(chunk) => self.bytes.extend(chunk),
                None if self.bytes.remaining() > 0 => {
                    // If some data is left, we have an incomplete row in the buffer.
                    // This is usually a schema mismatch on the client side.
                    return Err(Error::NotEnoughData);
                }
                None => return Ok(None),
            }
        }
    }

    /// Returns the total size in bytes received from the CH server since
    /// the cursor was created.
    ///
    /// This method counts only size without HTTP headers for now.
    /// It can be changed in the future without notice.
    #[inline]
    pub fn received_bytes(&self) -> u64 {
        self.raw.received_bytes()
    }

    /// Returns the total size in bytes decompressed since the cursor was
    /// created.
    #[inline]
    pub fn decoded_bytes(&self) -> u64 {
        self.raw.decoded_bytes()
    }
}

```

# src/error.rs

```rs
//! Contains [`Error`] and corresponding [`Result`].

use std::{error::Error as StdError, fmt, io, result, str::Utf8Error};

use serde::{de, ser};

/// A result with a specified [`Error`] type.
pub type Result<T, E = Error> = result::Result<T, E>;

type BoxedError = Box<dyn StdError + Send + Sync>;

/// Represents all possible errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum Error {
    #[error("invalid params: {0}")]
    InvalidParams(#[source] BoxedError),
    #[error("network error: {0}")]
    Network(#[source] BoxedError),
    #[error("compression error: {0}")]
    Compression(#[source] BoxedError),
    #[error("decompression error: {0}")]
    Decompression(#[source] BoxedError),
    #[error("no rows returned by a query that expected to return at least one row")]
    RowNotFound,
    #[error("sequences must have a known size ahead of time")]
    SequenceMustHaveLength,
    #[error("`deserialize_any` is not supported")]
    DeserializeAnyNotSupported,
    #[error("not enough data, probably a row type mismatches a database schema")]
    NotEnoughData,
    #[error("string is not valid utf8")]
    InvalidUtf8Encoding(#[from] Utf8Error),
    #[error("tag for enum is not valid")]
    InvalidTagEncoding(usize),
    #[error("max number of types in the Variant data type is 255, got {0}")]
    VariantDiscriminatorIsOutOfBound(usize),
    #[error("a custom error message from serde: {0}")]
    Custom(String),
    #[error("bad response: {0}")]
    BadResponse(String),
    #[error("timeout expired")]
    TimedOut,
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("{0}")]
    Other(BoxedError),
}

assert_impl_all!(Error: StdError, Send, Sync);

impl From<hyper::Error> for Error {
    fn from(error: hyper::Error) -> Self {
        Self::Network(Box::new(error))
    }
}

impl From<hyper_util::client::legacy::Error> for Error {
    fn from(error: hyper_util::client::legacy::Error) -> Self {
        Self::Network(Box::new(error))
    }
}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl From<Error> for io::Error {
    fn from(error: Error) -> Self {
        io::Error::new(io::ErrorKind::Other, error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        // TODO: after MSRV 1.79 replace with `io::Error::downcast`.
        if error.get_ref().is_some_and(|r| r.is::<Error>()) {
            *error.into_inner().unwrap().downcast::<Error>().unwrap()
        } else {
            Self::Other(error.into())
        }
    }
}

#[test]
fn roundtrip_io_error() {
    let orig = Error::NotEnoughData;

    // Error -> io::Error
    let orig_str = orig.to_string();
    let io = io::Error::from(orig);
    assert_eq!(io.kind(), io::ErrorKind::Other);
    assert_eq!(io.to_string(), orig_str);

    // io::Error -> Error
    let orig = Error::from(io);
    assert!(matches!(orig, Error::NotEnoughData));
}

```

# src/headers.rs

```rs
use crate::ProductInfo;
use hyper::header::USER_AGENT;
use hyper::http::request::Builder;
use std::collections::HashMap;
use std::env::consts::OS;

fn get_user_agent(products_info: &[ProductInfo]) -> String {
    // See https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-crates
    let pkg_ver = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
    let rust_ver = option_env!("CARGO_PKG_RUST_VERSION").unwrap_or("unknown");
    let default_agent = format!("clickhouse-rs/{pkg_ver} (lv:rust/{rust_ver}, os:{OS})");
    if products_info.is_empty() {
        default_agent
    } else {
        let products = products_info
            .iter()
            .rev()
            .map(|product_info| product_info.to_string())
            .collect::<Vec<String>>()
            .join(" ");
        format!("{products} {default_agent}")
    }
}

pub(crate) fn with_request_headers(
    mut builder: Builder,
    headers: &HashMap<String, String>,
    products_info: &[ProductInfo],
) -> Builder {
    for (name, value) in headers {
        builder = builder.header(name, value);
    }
    builder = builder.header(USER_AGENT.to_string(), get_user_agent(products_info));
    builder
}

```

# src/http_client.rs

```rs
use std::time::Duration;

use hyper::Request;
use hyper_util::{
    client::legacy::{
        connect::{Connect, HttpConnector},
        Client, Client as HyperClient, ResponseFuture,
    },
    rt::TokioExecutor,
};
use sealed::sealed;

use crate::request_body::RequestBody;

/// A trait for underlying HTTP client.
///
/// Firstly, now it is implemented only for
/// `hyper_util::client::legacy::Client`, it's impossible to use another HTTP
/// client.
///
/// Secondly, although it's stable in terms of semver, it will be changed in the
/// future (e.g. to support more runtimes, not only tokio). Thus, prefer to open
/// a feature request instead of implementing this trait manually.
#[sealed]
pub trait HttpClient: Send + Sync + 'static {
    fn request(&self, req: Request<RequestBody>) -> ResponseFuture;
}

#[sealed]
impl<C> HttpClient for Client<C, RequestBody>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    fn request(&self, req: Request<RequestBody>) -> ResponseFuture {
        self.request(req)
    }
}

// === Default ===

const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

// ClickHouse uses 3s by default.
// See https://github.com/ClickHouse/ClickHouse/blob/368cb74b4d222dc5472a7f2177f6bb154ebae07a/programs/server/config.xml#L201
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) fn default() -> impl HttpClient {
    let mut connector = HttpConnector::new();

    // TODO: make configurable in `Client::builder()`.
    connector.set_keepalive(Some(TCP_KEEPALIVE));

    connector.enforce_http(!cfg!(any(
        feature = "native-tls",
        feature = "rustls-tls-aws-lc",
        feature = "rustls-tls-ring",
    )));

    #[cfg(feature = "native-tls")]
    let connector = hyper_tls::HttpsConnector::new_with_connector(connector);

    #[cfg(all(feature = "rustls-tls-aws-lc", not(feature = "native-tls")))]
    let connector =
        prepare_hyper_rustls_connector(connector, rustls::crypto::aws_lc_rs::default_provider());

    #[cfg(all(
        feature = "rustls-tls-ring",
        not(feature = "rustls-tls-aws-lc"),
        not(feature = "native-tls"),
    ))]
    let connector =
        prepare_hyper_rustls_connector(connector, rustls::crypto::ring::default_provider());

    HyperClient::builder(TokioExecutor::new())
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .build(connector)
}

#[cfg(not(feature = "native-tls"))]
#[cfg(any(feature = "rustls-tls-aws-lc", feature = "rustls-tls-ring"))]
fn prepare_hyper_rustls_connector(
    connector: HttpConnector,
    provider: rustls::crypto::CryptoProvider,
) -> hyper_rustls::HttpsConnector<HttpConnector> {
    #[cfg(not(feature = "rustls-tls-webpki-roots"))]
    #[cfg(not(feature = "rustls-tls-native-roots"))]
    compile_error!(
        "`rustls-tls-aws-lc` and `rustls-tls-ring` features require either \
         `rustls-tls-webpki-roots` or `rustls-tls-native-roots` feature to be enabled"
    );

    #[cfg(feature = "rustls-tls-native-roots")]
    let builder = hyper_rustls::HttpsConnectorBuilder::new()
        .with_provider_and_native_roots(provider)
        .unwrap();

    #[cfg(all(
        feature = "rustls-tls-webpki-roots",
        not(feature = "rustls-tls-native-roots")
    ))]
    let builder = hyper_rustls::HttpsConnectorBuilder::new()
        .with_provider_and_webpki_roots(provider)
        .unwrap();

    builder
        .https_or_http()
        .enable_http1()
        .wrap_connector(connector)
}

```

# src/insert.rs

```rs
use std::{future::Future, marker::PhantomData, mem, panic, pin::Pin, time::Duration};

use bytes::{Bytes, BytesMut};
use hyper::{self, Request};
use replace_with::replace_with_or_abort;
use serde::Serialize;
use tokio::{
    task::JoinHandle,
    time::{Instant, Sleep},
};
use url::Url;

use crate::headers::with_request_headers;
use crate::{
    error::{Error, Result},
    request_body::{ChunkSender, RequestBody},
    response::Response,
    row::{self, Row},
    rowbinary, Client, Compression,
};

// The desired max frame size.
const BUFFER_SIZE: usize = 256 * 1024;
// Threshold to send a chunk. Should be slightly less than `BUFFER_SIZE`
// to avoid extra reallocations in case of a big last row.
const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 2048;

const_assert!(BUFFER_SIZE.is_power_of_two()); // to use the whole buffer's capacity

/// Performs one `INSERT`.
///
/// The [`Insert::end`] must be called to finalize the `INSERT`.
/// Otherwise, the whole `INSERT` will be aborted.
///
/// Rows are being sent progressively to spread network load.
#[must_use]
pub struct Insert<T> {
    state: InsertState,
    buffer: BytesMut,
    #[cfg(feature = "lz4")]
    compression: Compression,
    send_timeout: Option<Duration>,
    end_timeout: Option<Duration>,
    // Use boxed `Sleep` to reuse a timer entry, it improves performance.
    // Also, `tokio::time::timeout()` significantly increases a future's size.
    sleep: Pin<Box<Sleep>>,
    _marker: PhantomData<fn() -> T>, // TODO: test contravariance.
}

enum InsertState {
    NotStarted {
        client: Box<Client>,
        sql: String,
    },
    Active {
        sender: ChunkSender,
        handle: JoinHandle<Result<()>>,
    },
    Terminated {
        handle: JoinHandle<Result<()>>,
    },
    Completed,
}

impl InsertState {
    fn sender(&mut self) -> Option<&mut ChunkSender> {
        match self {
            InsertState::Active { sender, .. } => Some(sender),
            _ => None,
        }
    }

    fn handle(&mut self) -> Option<&mut JoinHandle<Result<()>>> {
        match self {
            InsertState::Active { handle, .. } | InsertState::Terminated { handle } => Some(handle),
            _ => None,
        }
    }

    fn client_with_sql(&self) -> Option<(&Client, &str)> {
        match self {
            InsertState::NotStarted { client, sql } => Some((client, sql)),
            _ => None,
        }
    }

    fn terminated(&mut self) {
        replace_with_or_abort(self, |_self| match _self {
            InsertState::NotStarted { .. } => InsertState::Completed, // empty insert
            InsertState::Active { handle, .. } => InsertState::Terminated { handle },
            _ => unreachable!(),
        });
    }

    fn with_option(&mut self, name: impl Into<String>, value: impl Into<String>) {
        assert!(matches!(self, InsertState::NotStarted { .. }));
        replace_with_or_abort(self, |_self| match _self {
            InsertState::NotStarted { mut client, sql } => {
                client.add_option(name, value);
                InsertState::NotStarted { client, sql }
            }
            _ => unreachable!(),
        });
    }
}

// It should be a regular function, but it decreases performance.
macro_rules! timeout {
    ($self:expr, $timeout:ident, $fut:expr) => {{
        if let Some(timeout) = $self.$timeout {
            $self.sleep.as_mut().reset(Instant::now() + timeout);
        }

        tokio::select! {
            res = $fut => Some(res),
            _ = &mut $self.sleep, if $self.$timeout.is_some() => None,
        }
    }};
}

impl<T> Insert<T> {
    // TODO: remove Result
    pub(crate) fn new(client: &Client, table: &str) -> Result<Self>
    where
        T: Row,
    {
        let fields = row::join_column_names::<T>()
            .expect("the row type must be a struct or a wrapper around it");

        // TODO: what about escaping a table name?
        // https://clickhouse.com/docs/en/sql-reference/syntax#identifiers
        let sql = format!("INSERT INTO {}({}) FORMAT RowBinary", table, fields);

        Ok(Self {
            state: InsertState::NotStarted {
                client: Box::new(client.clone()),
                sql,
            },
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
            #[cfg(feature = "lz4")]
            compression: client.compression,
            send_timeout: None,
            end_timeout: None,
            sleep: Box::pin(tokio::time::sleep(Duration::new(0, 0))),
            _marker: PhantomData,
        })
    }

    /// Sets timeouts for different operations.
    ///
    /// `send_timeout` restricts time on sending a data chunk to a socket.
    /// `None` disables the timeout, it's a default.
    /// It's roughly equivalent to `tokio::time::timeout(insert.write(...))`.
    ///
    /// `end_timeout` restricts time on waiting for a response from the CH
    /// server. Thus, it includes all work needed to handle `INSERT` by the
    /// CH server, e.g. handling all materialized views and so on.
    /// `None` disables the timeout, it's a default.
    /// It's roughly equivalent to `tokio::time::timeout(insert.end(...))`.
    ///
    /// These timeouts are much more performant (~x10) than wrapping `write()`
    /// and `end()` calls into `tokio::time::timeout()`.
    pub fn with_timeouts(
        mut self,
        send_timeout: Option<Duration>,
        end_timeout: Option<Duration>,
    ) -> Self {
        self.set_timeouts(send_timeout, end_timeout);
        self
    }

    /// Similar to [`Client::with_option`], but for this particular INSERT
    /// statement only.
    ///
    /// # Panics
    /// If called after the request is started, e.g., after [`Insert::write`].
    #[track_caller]
    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.state.with_option(name, value);
        self
    }

    pub(crate) fn set_timeouts(
        &mut self,
        send_timeout: Option<Duration>,
        end_timeout: Option<Duration>,
    ) {
        self.send_timeout = send_timeout;
        self.end_timeout = end_timeout;
    }

    /// Serializes the provided row into an internal buffer.
    /// Once the buffer is full, it's sent to a background task writing to the
    /// socket.
    ///
    /// Close to:
    /// \`\`\`ignore
    /// async fn write<T>(&self, row: &T) -> Result<usize>;
    /// \`\`\`
    ///
    /// A returned future doesn't depend on the row's lifetime.
    ///
    /// Returns an error if the row cannot be serialized or the background task
    /// failed. Once failed, the whole `INSERT` is aborted and cannot be
    /// used anymore.
    ///
    /// # Panics
    /// If called after the previous call that returned an error.
    pub fn write<'a>(&'a mut self, row: &T) -> impl Future<Output = Result<()>> + 'a + Send
    where
        T: Serialize,
    {
        let result = self.do_write(row);

        async move {
            result?;
            if self.buffer.len() >= MIN_CHUNK_SIZE {
                self.send_chunk().await?;
            }
            Ok(())
        }
    }

    #[inline(always)]
    pub(crate) fn do_write(&mut self, row: &T) -> Result<usize>
    where
        T: Serialize,
    {
        match self.state {
            InsertState::NotStarted { .. } => self.init_request(),
            InsertState::Active { .. } => Ok(()),
            _ => panic!("write() after error"),
        }?;

        let old_buf_size = self.buffer.len();
        let result = rowbinary::serialize_into(&mut self.buffer, row);
        let written = self.buffer.len() - old_buf_size;

        if result.is_err() {
            self.abort();
        }

        result.and(Ok(written))
    }

    /// Ends `INSERT`, the server starts processing the data.
    ///
    /// Succeeds if the server returns 200, that means the `INSERT` was handled
    /// successfully, including all materialized views and quorum writes.
    ///
    /// NOTE: If it isn't called, the whole `INSERT` is aborted.
    pub async fn end(mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            self.send_chunk().await?;
        }
        self.state.terminated();
        self.wait_handle().await
    }

    async fn send_chunk(&mut self) -> Result<()> {
        debug_assert!(matches!(self.state, InsertState::Active { .. }));

        // Hyper uses non-trivial and inefficient schema of buffering chunks.
        // It's difficult to determine when allocations occur.
        // So, instead we control it manually here and rely on the system allocator.
        let chunk = self.take_and_prepare_chunk()?;

        let sender = self.state.sender().unwrap(); // checked above

        let is_timed_out = match timeout!(self, send_timeout, sender.send(chunk)) {
            Some(true) => return Ok(()),
            Some(false) => false, // an actual error will be returned from `wait_handle`
            None => true,
        };

        // Error handling.

        self.abort();

        // TODO: is it required to wait the handle in the case of timeout?
        let res = self.wait_handle().await;

        if is_timed_out {
            Err(Error::TimedOut)
        } else {
            res?; // a real error should be here.
            Err(Error::Network("channel closed".into()))
        }
    }

    async fn wait_handle(&mut self) -> Result<()> {
        match self.state.handle() {
            Some(handle) => {
                let result = match timeout!(self, end_timeout, &mut *handle) {
                    Some(Ok(res)) => res,
                    Some(Err(err)) if err.is_panic() => panic::resume_unwind(err.into_panic()),
                    Some(Err(err)) => Err(Error::Custom(format!("unexpected error: {err}"))),
                    None => {
                        // We can do nothing useful here, so just shut down the background task.
                        handle.abort();
                        Err(Error::TimedOut)
                    }
                };
                self.state = InsertState::Completed;
                result
            }
            _ => Ok(()),
        }
    }

    #[cfg(feature = "lz4")]
    fn take_and_prepare_chunk(&mut self) -> Result<Bytes> {
        Ok(if self.compression.is_lz4() {
            let compressed = crate::compression::lz4::compress(&self.buffer)?;
            self.buffer.clear();
            compressed
        } else {
            mem::replace(&mut self.buffer, BytesMut::with_capacity(BUFFER_SIZE)).freeze()
        })
    }

    #[cfg(not(feature = "lz4"))]
    fn take_and_prepare_chunk(&mut self) -> Result<Bytes> {
        Ok(mem::replace(&mut self.buffer, BytesMut::with_capacity(BUFFER_SIZE)).freeze())
    }

    #[cold]
    #[track_caller]
    #[inline(never)]
    fn init_request(&mut self) -> Result<()> {
        debug_assert!(matches!(self.state, InsertState::NotStarted { .. }));
        let (client, sql) = self.state.client_with_sql().unwrap(); // checked above

        let mut url = Url::parse(&client.url).map_err(|err| Error::InvalidParams(err.into()))?;
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &client.database {
            pairs.append_pair("database", database);
        }

        pairs.append_pair("query", sql);

        if client.compression.is_lz4() {
            pairs.append_pair("decompress", "1");
        }

        for (name, value) in &client.options {
            pairs.append_pair(name, value);
        }

        drop(pairs);

        let mut builder = Request::post(url.as_str());
        builder = with_request_headers(builder, &client.headers, &client.products_info);

        if let Some(user) = &client.user {
            builder = builder.header("X-ClickHouse-User", user);
        }

        if let Some(password) = &client.password {
            builder = builder.header("X-ClickHouse-Key", password);
        }

        let (sender, body) = RequestBody::chunked();

        let request = builder
            .body(body)
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;

        let future = client.http.request(request);
        // TODO: introduce `Executor` to allow bookkeeping of spawned tasks.
        let handle =
            tokio::spawn(async move { Response::new(future, Compression::None).finish().await });

        self.state = InsertState::Active { handle, sender };
        Ok(())
    }

    fn abort(&mut self) {
        if let Some(sender) = self.state.sender() {
            sender.abort();
        }
    }
}

impl<T> Drop for Insert<T> {
    fn drop(&mut self) {
        self.abort();
    }
}

```

# src/inserter.rs

```rs
use std::mem;

use serde::Serialize;
use tokio::time::Duration;

use crate::{error::Result, insert::Insert, row::Row, ticks::Ticks, Client};

/// Performs multiple consecutive `INSERT`s.
///
/// By default, it **doesn't** end the current active `INSERT` automatically.
/// Use `with_max_bytes`, `with_max_rows` and `with_period` to set limits.
/// Alternatively, call `force_commit` to forcibly end an active `INSERT`.
///
/// Rows are being sent progressively to spread network load.
///
/// All rows written by [`Inserter::write()`] between [`Inserter::commit()`]
/// calls are sent in one `INSERT` statement.
#[must_use]
pub struct Inserter<T> {
    client: Client,
    table: String,
    max_bytes: u64,
    max_rows: u64,
    send_timeout: Option<Duration>,
    end_timeout: Option<Duration>,
    insert: Option<Insert<T>>,
    ticks: Ticks,
    pending: Quantities,
    in_transaction: bool,
}

/// Statistics about pending or inserted data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quantities {
    /// The number of uncompressed bytes.
    pub bytes: u64,
    /// The number for rows (calls of [`Inserter::write`]).
    pub rows: u64,
    /// The number of nonempty transactions (calls of [`Inserter::commit`]).
    pub transactions: u64,
}

impl Quantities {
    /// Just zero quantities, nothing special.
    pub const ZERO: Quantities = Quantities {
        bytes: 0,
        rows: 0,
        transactions: 0,
    };
}

impl<T> Inserter<T>
where
    T: Row,
{
    // TODO: (breaking change) remove `Result`.
    pub(crate) fn new(client: &Client, table: &str) -> Result<Self> {
        Ok(Self {
            client: client.clone(),
            table: table.into(),
            max_bytes: u64::MAX,
            max_rows: u64::MAX,
            send_timeout: None,
            end_timeout: None,
            insert: None,
            ticks: Ticks::default(),
            pending: Quantities::ZERO,
            in_transaction: false,
        })
    }

    /// See [`Insert::with_timeouts()`].
    ///
    /// Note that [`Inserter::commit()`] can call [`Insert::end()`] inside,
    /// so `end_timeout` is also applied to `commit()` method.
    pub fn with_timeouts(
        mut self,
        send_timeout: Option<Duration>,
        end_timeout: Option<Duration>,
    ) -> Self {
        self.set_timeouts(send_timeout, end_timeout);
        self
    }

    /// The maximum number of uncompressed bytes in one `INSERT` statement.
    ///
    /// This is the soft limit, which can be exceeded if rows between
    /// [`Inserter::commit()`] calls are larger than set value.
    ///
    /// Note: ClickHouse inserts batches atomically only if all rows fit in the
    /// same partition and their number is less [`max_insert_block_size`].
    ///
    /// Unlimited (`u64::MAX`) by default.
    ///
    /// [`max_insert_block_size`]: https://clickhouse.tech/docs/en/operations/settings/settings/#settings-max_insert_block_size
    pub fn with_max_bytes(mut self, threshold: u64) -> Self {
        self.set_max_bytes(threshold);
        self
    }

    /// The maximum number of rows in one `INSERT` statement.
    ///
    /// In order to reduce overhead of merging small parts by ClickHouse, use
    /// larger values (e.g. 100_000 or even larger). Consider also/instead
    /// [`Inserter::with_max_bytes()`] if rows can be large.
    ///
    /// This is the soft limit, which can be exceeded if multiple rows are
    /// written between [`Inserter::commit()`] calls.
    ///
    /// Note: ClickHouse inserts batches atomically only if all rows fit in the
    /// same partition and their number is less [`max_insert_block_size`].
    ///
    /// Unlimited (`u64::MAX`) by default.
    ///
    /// [`max_insert_block_size`]: https://clickhouse.tech/docs/en/operations/settings/settings/#settings-max_insert_block_size
    pub fn with_max_rows(mut self, threshold: u64) -> Self {
        self.set_max_rows(threshold);
        self
    }

    /// The time between `INSERT`s.
    ///
    /// Note that [`Inserter`] doesn't spawn tasks or threads to check the
    /// elapsed time, all checks are performend only on [`Inserter::commit()`].
    /// However, it's possible to use [`Inserter::time_left()`] and set a
    /// timer up to call [`Inserter::commit()`] to check passed time again.
    ///
    /// Usually, it's reasonable to use 1-10s period, but it depends on
    /// desired delay for reading the data from the table.
    /// Larger values = less overhead for merging parts by CH.
    /// Smaller values = less delay for readers.
    ///
    /// Extra ticks are skipped if the previous `INSERT` is still in progress:
    /// \`\`\`text
    /// Expected ticks: |     1     |     2     |     3     |     4     |     5     |     6     |
    /// Actual ticks:   | work -----|          delay          | work ---| work -----| work -----|
    /// \`\`\`
    ///
    /// Unlimited (`None`) by default.
    pub fn with_period(mut self, period: Option<Duration>) -> Self {
        self.set_period(period);
        self
    }

    /// Adds a bias to the period, so actual period is in the following range:
    ///
    /// \`\`\`text
    ///   [period * (1 - bias), period * (1 + bias)]
    /// \`\`\`
    ///
    /// The `bias` parameter is clamped to the range `[0, 1]`.
    ///
    /// It helps to avoid producing a lot of `INSERT`s at the same time by
    /// multiple inserters.
    pub fn with_period_bias(mut self, bias: f64) -> Self {
        self.set_period_bias(bias);
        self
    }

    /// Similar to [`Client::with_option`], but for the INSERT statements
    /// generated by this [`Inserter`] only.
    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.client.add_option(name, value);
        self
    }

    /// See [`Inserter::with_timeouts()`].
    pub fn set_timeouts(&mut self, send_timeout: Option<Duration>, end_timeout: Option<Duration>) {
        self.send_timeout = send_timeout;
        self.end_timeout = end_timeout;
        if let Some(insert) = &mut self.insert {
            insert.set_timeouts(self.send_timeout, self.end_timeout);
        }
    }

    /// See [`Inserter::with_max_bytes()`].
    pub fn set_max_bytes(&mut self, threshold: u64) {
        self.max_bytes = threshold;
    }

    /// See [`Inserter::with_max_rows()`].
    pub fn set_max_rows(&mut self, threshold: u64) {
        self.max_rows = threshold;
    }

    /// See [`Inserter::with_period()`].
    pub fn set_period(&mut self, period: Option<Duration>) {
        self.ticks.set_period(period);
        self.ticks.reschedule();
    }

    /// See [`Inserter::with_period_bias()`].
    pub fn set_period_bias(&mut self, bias: f64) {
        self.ticks.set_period_bias(bias);
        self.ticks.reschedule();
    }

    /// How much time we have until the next tick.
    ///
    /// `None` if the period isn't configured.
    pub fn time_left(&mut self) -> Option<Duration> {
        self.ticks.time_left()
    }

    /// Returns statistics about data not yet inserted into ClickHouse.
    pub fn pending(&self) -> &Quantities {
        &self.pending
    }

    /// Serializes the provided row into an internal buffer.
    ///
    /// To check the limits and send the data to ClickHouse, call
    /// [`Inserter::commit()`].
    ///
    /// # Panics
    /// If called after the previous call that returned an error.
    #[inline]
    pub fn write(&mut self, row: &T) -> Result<()>
    where
        T: Serialize,
    {
        if self.insert.is_none() {
            self.init_insert()?;
        }

        match self.insert.as_mut().unwrap().do_write(row) {
            Ok(bytes) => {
                self.pending.bytes += bytes as u64;
                self.pending.rows += 1;

                if !self.in_transaction {
                    self.pending.transactions += 1;
                    self.in_transaction = true;
                }

                Ok(())
            }
            Err(err) => {
                self.pending = Quantities::ZERO;
                Err(err)
            }
        }
    }

    /// Checks limits and ends the current `INSERT` if they are reached.
    pub async fn commit(&mut self) -> Result<Quantities> {
        if !self.limits_reached() {
            self.in_transaction = false;
            return Ok(Quantities::ZERO);
        }

        self.force_commit().await
    }

    /// Ends the current `INSERT` unconditionally.
    pub async fn force_commit(&mut self) -> Result<Quantities> {
        self.in_transaction = false;

        let quantities = mem::replace(&mut self.pending, Quantities::ZERO);
        let result = self.insert().await;
        self.ticks.reschedule();
        result?;
        Ok(quantities)
    }

    /// Ends the current `INSERT` and whole `Inserter` unconditionally.
    ///
    /// If it isn't called, the current `INSERT` is aborted.
    pub async fn end(mut self) -> Result<Quantities> {
        self.insert().await?;
        Ok(self.pending)
    }

    fn limits_reached(&self) -> bool {
        self.pending.rows >= self.max_rows
            || self.pending.bytes >= self.max_bytes
            || self.ticks.reached()
    }

    async fn insert(&mut self) -> Result<()> {
        if let Some(insert) = self.insert.take() {
            insert.end().await?;
        }
        Ok(())
    }

    #[cold]
    #[inline(never)]
    fn init_insert(&mut self) -> Result<()> {
        debug_assert!(self.insert.is_none());
        debug_assert_eq!(self.pending, Quantities::ZERO);

        let mut new_insert: Insert<T> = self.client.insert(&self.table)?;
        new_insert.set_timeouts(self.send_timeout, self.end_timeout);
        self.insert = Some(new_insert);
        Ok(())
    }
}

```

# src/lib.rs

```rs
#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

#[macro_use]
extern crate static_assertions;

use self::{error::Result, http_client::HttpClient};
use std::{collections::HashMap, fmt::Display, sync::Arc};

pub use self::{compression::Compression, row::Row};
pub use clickhouse_derive::Row;

pub mod error;
pub mod insert;
#[cfg(feature = "inserter")]
pub mod inserter;
pub mod query;
pub mod serde;
pub mod sql;
#[cfg(feature = "test-util")]
pub mod test;
#[cfg(feature = "watch")]
pub mod watch;

mod bytes_ext;
mod compression;
mod cursors;
mod headers;
mod http_client;
mod request_body;
mod response;
mod row;
mod rowbinary;
#[cfg(feature = "inserter")]
mod ticks;

/// A client containing HTTP pool.
#[derive(Clone)]
pub struct Client {
    http: Arc<dyn HttpClient>,

    url: String,
    database: Option<String>,
    user: Option<String>,
    password: Option<String>,
    compression: Compression,
    options: HashMap<String, String>,
    headers: HashMap<String, String>,
    products_info: Vec<ProductInfo>,
}

#[derive(Clone)]
struct ProductInfo {
    name: String,
    version: String,
}

impl Display for ProductInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.name, self.version)
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::with_http_client(http_client::default())
    }
}

impl Client {
    /// Creates a new client with a specified underlying HTTP client.
    ///
    /// See `HttpClient` for details.
    pub fn with_http_client(client: impl HttpClient) -> Self {
        Self {
            http: Arc::new(client),
            url: String::new(),
            database: None,
            user: None,
            password: None,
            compression: Compression::default(),
            options: HashMap::new(),
            headers: HashMap::new(),
            products_info: Vec::default(),
        }
    }

    /// Specifies ClickHouse's url. Should point to HTTP endpoint.
    ///
    /// # Examples
    /// \`\`\`
    /// # use clickhouse::Client;
    /// let client = Client::default().with_url("http://localhost:8123");
    /// \`\`\`
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    /// Specifies a database name.
    ///
    /// # Examples
    /// \`\`\`
    /// # use clickhouse::Client;
    /// let client = Client::default().with_database("test");
    /// \`\`\`
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Specifies a user.
    ///
    /// # Examples
    /// \`\`\`
    /// # use clickhouse::Client;
    /// let client = Client::default().with_user("test");
    /// \`\`\`
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Specifies a password.
    ///
    /// # Examples
    /// \`\`\`
    /// # use clickhouse::Client;
    /// let client = Client::default().with_password("secret");
    /// \`\`\`
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Specifies a compression mode. See [`Compression`] for details.
    /// By default, `Lz4` is used.
    ///
    /// # Examples
    /// \`\`\`
    /// # use clickhouse::{Client, Compression};
    /// # #[cfg(feature = "lz4")]
    /// let client = Client::default().with_compression(Compression::Lz4Hc(4));
    /// \`\`\`
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Used to specify options that will be passed to all queries.
    ///
    /// # Example
    /// \`\`\`
    /// # use clickhouse::Client;
    /// Client::default().with_option("allow_nondeterministic_mutations", "1");
    /// \`\`\`
    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(name.into(), value.into());
        self
    }

    /// Used to specify a header that will be passed to all queries.
    ///
    /// # Example
    /// \`\`\`
    /// # use clickhouse::Client;
    /// Client::default().with_header("Cookie", "A=1");
    /// \`\`\`
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Specifies the product name and version that will be included
    /// in the default User-Agent header. Multiple products are supported.
    /// This could be useful for the applications built on top of this client.
    ///
    /// # Examples
    ///
    /// Sample default User-Agent header:
    ///
    /// \`\`\`plaintext
    /// clickhouse-rs/0.12.2 (lv:rust/1.67.0, os:macos)
    /// \`\`\`
    ///
    /// Sample User-Agent with a single product information:
    ///
    /// \`\`\`
    /// # use clickhouse::Client;
    /// let client = Client::default().with_product_info("MyDataSource", "v1.0.0");
    /// \`\`\`
    ///
    /// \`\`\`plaintext
    /// MyDataSource/v1.0.0 clickhouse-rs/0.12.2 (lv:rust/1.67.0, os:macos)
    /// \`\`\`
    ///
    /// Sample User-Agent with multiple products information
    /// (NB: the products are added in the reverse order of
    /// [`Client::with_product_info`] calls, which could be useful to add
    /// higher abstraction layers first):
    ///
    /// \`\`\`
    /// # use clickhouse::Client;
    /// let client = Client::default()
    ///     .with_product_info("MyDataSource", "v1.0.0")
    ///     .with_product_info("MyApp", "0.0.1");
    /// \`\`\`
    ///
    /// \`\`\`plaintext
    /// MyApp/0.0.1 MyDataSource/v1.0.0 clickhouse-rs/0.12.2 (lv:rust/1.67.0, os:macos)
    /// \`\`\`
    pub fn with_product_info(
        mut self,
        product_name: impl Into<String>,
        product_version: impl Into<String>,
    ) -> Self {
        self.products_info.push(ProductInfo {
            name: product_name.into(),
            version: product_version.into(),
        });
        self
    }

    /// Starts a new INSERT statement.
    ///
    /// # Panics
    /// If `T` has unnamed fields, e.g. tuples.
    pub fn insert<T: Row>(&self, table: &str) -> Result<insert::Insert<T>> {
        insert::Insert::new(self, table)
    }

    /// Creates an inserter to perform multiple INSERTs.
    #[cfg(feature = "inserter")]
    pub fn inserter<T: Row>(&self, table: &str) -> Result<inserter::Inserter<T>> {
        inserter::Inserter::new(self, table)
    }

    /// Starts a new SELECT/DDL query.
    pub fn query(&self, query: &str) -> query::Query {
        query::Query::new(self, query)
    }

    /// Starts a new WATCH query.
    ///
    /// The `query` can be either the table name or a SELECT query.
    /// In the second case, a new LV table is created.
    #[cfg(feature = "watch")]
    pub fn watch(&self, query: &str) -> watch::Watch {
        watch::Watch::new(self, query)
    }

    /// Used internally to modify the options map of an _already cloned_
    /// [`Client`] instance.
    pub(crate) fn add_option(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.options.insert(name.into(), value.into());
    }
}

/// This is a private API exported only for internal purposes.
/// Do not use it in your code directly, it doesn't follow semver.
#[doc(hidden)]
pub mod _priv {
    #[cfg(feature = "lz4")]
    pub fn lz4_compress(uncompressed: &[u8]) -> super::Result<bytes::Bytes> {
        crate::compression::lz4::compress(uncompressed)
    }
}

```

# src/query.rs

```rs
use hyper::{header::CONTENT_LENGTH, Method, Request};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use url::Url;

use crate::{
    error::{Error, Result},
    headers::with_request_headers,
    request_body::RequestBody,
    response::Response,
    row::Row,
    sql::{ser, Bind, SqlBuilder},
    Client,
};

const MAX_QUERY_LEN_TO_USE_GET: usize = 8192;

pub use crate::cursors::{BytesCursor, RowCursor};

#[must_use]
#[derive(Clone)]
pub struct Query {
    client: Client,
    sql: SqlBuilder,
}

impl Query {
    pub(crate) fn new(client: &Client, template: &str) -> Self {
        Self {
            client: client.clone(),
            sql: SqlBuilder::new(template),
        }
    }

    /// Display SQL query as string.
    pub fn sql_display(&self) -> &impl Display {
        &self.sql
    }

    /// Binds `value` to the next `?` in the query.
    ///
    /// The `value`, which must either implement [`Serialize`] or be an
    /// [`Identifier`], will be appropriately escaped.
    ///
    /// All possible errors will be returned as [`Error::InvalidParams`]
    /// during query execution (`execute()`, `fetch()` etc).
    ///
    /// WARNING: This means that the query must not have any extra `?`, even if
    /// they are in a string literal! Use `??` to have plain `?` in query.
    ///
    /// [`Serialize`]: serde::Serialize
    /// [`Identifier`]: crate::sql::Identifier
    #[track_caller]
    pub fn bind(mut self, value: impl Bind) -> Self {
        self.sql.bind_arg(value);
        self
    }

    /// Executes the query.
    pub async fn execute(self) -> Result<()> {
        self.do_execute(false)?.finish().await
    }

    /// Executes the query, returning a [`RowCursor`] to obtain results.
    ///
    /// # Example
    ///
    /// \`\`\`
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// #[derive(clickhouse::Row, serde::Deserialize)]
    /// struct MyRow<'a> {
    ///     no: u32,
    ///     name: &'a str,
    /// }
    ///
    /// let mut cursor = clickhouse::Client::default()
    ///     .query("SELECT ?fields FROM some WHERE no BETWEEN 0 AND 1")
    ///     .fetch::<MyRow<'_>>()?;
    ///
    /// while let Some(MyRow { name, no }) = cursor.next().await? {
    ///     println!("{name}: {no}");
    /// }
    /// # Ok(()) }
    /// \`\`\`
    pub fn fetch<T: Row>(mut self) -> Result<RowCursor<T>> {
        self.sql.bind_fields::<T>();
        self.sql.set_output_format("RowBinary");

        let response = self.do_execute(true)?;
        Ok(RowCursor::new(response))
    }

    /// Executes the query and returns just a single row.
    ///
    /// Note that `T` must be owned.
    pub async fn fetch_one<T>(self) -> Result<T>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        match self.fetch()?.next().await {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(Error::RowNotFound),
            Err(err) => Err(err),
        }
    }

    /// Executes the query and returns at most one row.
    ///
    /// Note that `T` must be owned.
    pub async fn fetch_optional<T>(self) -> Result<Option<T>>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        self.fetch()?.next().await
    }

    /// Executes the query and returns all the generated results,
    /// collected into a Vec.
    ///
    /// Note that `T` must be owned.
    pub async fn fetch_all<T>(self) -> Result<Vec<T>>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        let mut result = Vec::new();
        let mut cursor = self.fetch::<T>()?;

        while let Some(row) = cursor.next().await? {
            result.push(row);
        }

        Ok(result)
    }

    /// Executes the query, returning a [`BytesCursor`] to obtain results as raw
    /// bytes containing data in the [provided format].
    ///
    /// [provided format]: https://clickhouse.com/docs/en/interfaces/formats
    pub fn fetch_bytes(mut self, format: impl Into<String>) -> Result<BytesCursor> {
        self.sql.set_output_format(format);
        let response = self.do_execute(true)?;
        Ok(BytesCursor::new(response))
    }

    pub(crate) fn do_execute(self, read_only: bool) -> Result<Response> {
        let query = self.sql.finish()?;

        let mut url =
            Url::parse(&self.client.url).map_err(|err| Error::InvalidParams(Box::new(err)))?;
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &self.client.database {
            pairs.append_pair("database", database);
        }

        let use_post = !read_only || query.len() > MAX_QUERY_LEN_TO_USE_GET;

        let (method, body, content_length) = if use_post {
            if read_only {
                pairs.append_pair("readonly", "1");
            }
            let len = query.len();
            (Method::POST, RequestBody::full(query), len)
        } else {
            pairs.append_pair("query", &query);
            (Method::GET, RequestBody::empty(), 0)
        };

        if self.client.compression.is_lz4() {
            pairs.append_pair("compress", "1");
        }

        for (name, value) in &self.client.options {
            pairs.append_pair(name, value);
        }
        drop(pairs);

        let mut builder = Request::builder().method(method).uri(url.as_str());
        builder = with_request_headers(builder, &self.client.headers, &self.client.products_info);

        if content_length == 0 {
            builder = builder.header(CONTENT_LENGTH, "0");
        } else {
            builder = builder.header(CONTENT_LENGTH, content_length.to_string());
        }

        if let Some(user) = &self.client.user {
            builder = builder.header("X-ClickHouse-User", user);
        }

        if let Some(password) = &self.client.password {
            builder = builder.header("X-ClickHouse-Key", password);
        }

        let request = builder
            .body(body)
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;

        let future = self.client.http.request(request);
        Ok(Response::new(future, self.client.compression))
    }

    /// Similar to [`Client::with_option`], but for this particular query only.
    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.client.add_option(name, value);
        self
    }

    /// Specify server side parameter for query.
    ///
    /// In queries you can reference params as {name: type} e.g. {val: Int32}.
    pub fn param(mut self, name: &str, value: impl Serialize) -> Self {
        let mut param = String::from("");
        if let Err(err) = ser::write_param(&mut param, &value) {
            self.sql = SqlBuilder::Failed(format!("invalid param: {err}"));
            self
        } else {
            self.with_option(format!("param_{name}"), param)
        }
    }
}

```

# src/request_body.rs

```rs
use std::{
    error::Error as StdError,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{SinkExt, Stream};
use futures_channel::mpsc;
use hyper::body::{Body, Frame, SizeHint};

// === RequestBody ===

pub struct RequestBody(Inner);

enum Inner {
    Full(Bytes),
    Chunked(mpsc::Receiver<Message>),
}

enum Message {
    Chunk(Bytes),
    Abort,
}

impl RequestBody {
    pub(crate) fn empty() -> Self {
        Self(Inner::Full(Bytes::new()))
    }

    pub(crate) fn full(content: String) -> Self {
        Self(Inner::Full(Bytes::from(content)))
    }

    pub(crate) fn chunked() -> (ChunkSender, Self) {
        let (tx, rx) = mpsc::channel(0); // each sender gets a guaranteed slot
        let sender = ChunkSender(tx);
        (sender, Self(Inner::Chunked(rx)))
    }
}

impl Body for RequestBody {
    type Data = Bytes;
    type Error = Box<dyn StdError + Send + Sync>;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match &mut self.get_mut().0 {
            Inner::Full(bytes) if bytes.is_empty() => Poll::Ready(None),
            Inner::Full(bytes) => Poll::Ready(Some(Ok(Frame::data(mem::take(bytes))))),
            Inner::Chunked(rx) => match Pin::new(rx).poll_next(cx) {
                Poll::Ready(Some(Message::Chunk(bytes))) => {
                    Poll::Ready(Some(Ok(Frame::data(bytes))))
                }
                Poll::Ready(Some(Message::Abort)) => Poll::Ready(Some(Err("aborted".into()))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.0 {
            Inner::Full(bytes) => bytes.is_empty(),
            Inner::Chunked(_) => false, // default `Body::is_end_stream()`
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.0 {
            Inner::Full(bytes) => SizeHint::with_exact(bytes.len() as u64),
            Inner::Chunked(_) => SizeHint::default(), // default `Body::size_hint()`
        }
    }
}

// === ChunkSender ===

pub(crate) struct ChunkSender(mpsc::Sender<Message>);

impl ChunkSender {
    pub(crate) async fn send(&mut self, chunk: Bytes) -> bool {
        self.0.send(Message::Chunk(chunk)).await.is_ok()
    }

    pub(crate) fn abort(&self) {
        // `clone()` allows to send even if the channel is full.
        let _ = self.0.clone().try_send(Message::Abort);
    }
}

```

# src/response.rs

```rs
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bstr::ByteSlice;
use bytes::{BufMut, Bytes};
use futures::{
    future,
    stream::{self, Stream, TryStreamExt},
};
use http_body_util::BodyExt as _;
use hyper::{
    body::{Body as _, Incoming},
    StatusCode,
};
use hyper_util::client::legacy::ResponseFuture as HyperResponseFuture;

#[cfg(feature = "lz4")]
use crate::compression::lz4::Lz4Decoder;
use crate::{
    compression::Compression,
    error::{Error, Result},
};

// === Response ===

pub(crate) enum Response {
    // Headers haven't been received yet.
    // `Box<_>` improves performance by reducing the size of the whole future.
    Waiting(ResponseFuture),
    // Headers have been received, streaming the body.
    Loading(Chunks),
}

pub(crate) type ResponseFuture = Pin<Box<dyn Future<Output = Result<Chunks>> + Send>>;

impl Response {
    pub(crate) fn new(response: HyperResponseFuture, compression: Compression) -> Self {
        Self::Waiting(Box::pin(async move {
            let response = response.await?;
            let status = response.status();
            let body = response.into_body();

            if status == StatusCode::OK {
                // More likely to be successful, start streaming.
                // It still can fail, but we'll handle it in `DetectDbException`.
                Ok(Chunks::new(body, compression))
            } else {
                // An instantly failed request.
                Err(collect_bad_response(status, body, compression).await)
            }
        }))
    }

    pub(crate) fn into_future(self) -> ResponseFuture {
        match self {
            Self::Waiting(future) => future,
            Self::Loading(_) => panic!("response is already streaming"),
        }
    }

    pub(crate) async fn finish(&mut self) -> Result<()> {
        let chunks = loop {
            match self {
                Self::Waiting(future) => *self = Self::Loading(future.await?),
                Self::Loading(chunks) => break chunks,
            }
        };

        while chunks.try_next().await?.is_some() {}
        Ok(())
    }
}

#[cold]
#[inline(never)]
async fn collect_bad_response(
    status: StatusCode,
    body: Incoming,
    compression: Compression,
) -> Error {
    // Collect the whole body into one contiguous buffer to simplify handling.
    // Only network errors can occur here and we return them instead of status code
    // because it means the request can be repeated to get a more detailed error.
    //
    // TODO: we don't implement any length checks and a malicious peer (e.g. MITM)
    //       might make us consume arbitrary amounts of memory.
    let raw_bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        // If we can't collect the body, return standardised reason for the status code.
        Err(_) => return Error::BadResponse(stringify_status(status)),
    };

    // Try to decompress the body, because CH uses compression even for errors.
    let stream = stream::once(future::ready(Result::<_>::Ok(raw_bytes.slice(..))));
    let stream = Decompress::new(stream, compression).map_ok(|chunk| chunk.data);

    // We're collecting already fetched chunks, thus only decompression errors can
    // be here. If decompression is failed, we should try the raw body because
    // it can be sent without any compression if some proxy is used, which
    // typically know nothing about CH params.
    let bytes = collect_bytes(stream).await.unwrap_or(raw_bytes);

    let reason = String::from_utf8(bytes.into())
        .map(|reason| reason.trim().into())
        // If we have a unreadable response, return standardised reason for the status code.
        .unwrap_or_else(|_| stringify_status(status));

    Error::BadResponse(reason)
}

async fn collect_bytes(stream: impl Stream<Item = Result<Bytes>>) -> Result<Bytes> {
    futures::pin_mut!(stream);

    let mut bytes = Vec::new();

    // TODO: avoid extra copying if there is only one chunk in the stream.
    while let Some(chunk) = stream.try_next().await? {
        bytes.put(chunk);
    }

    Ok(bytes.into())
}

fn stringify_status(status: StatusCode) -> String {
    format!(
        "{} {}",
        status.as_str(),
        status.canonical_reason().unwrap_or("<unknown>"),
    )
}

// === Chunks ===

pub(crate) struct Chunk {
    pub(crate) data: Bytes,
    pub(crate) net_size: usize,
}

// * Uses `Option<_>` to make this stream fused.
// * Uses `Box<_>` in order to reduce the size of cursors.
pub(crate) struct Chunks(Option<Box<DetectDbException<Decompress<IncomingStream>>>>);

impl Chunks {
    fn new(stream: Incoming, compression: Compression) -> Self {
        let stream = IncomingStream(stream);
        let stream = Decompress::new(stream, compression);
        let stream = DetectDbException(stream);
        Self(Some(Box::new(stream)))
    }

    pub(crate) fn empty() -> Self {
        Self(None)
    }

    #[cfg(feature = "futures03")]
    pub(crate) fn is_terminated(&self) -> bool {
        self.0.is_none()
    }
}

impl Stream for Chunks {
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // We use `take()` to make the stream fused, including the case of panics.
        if let Some(mut stream) = self.0.take() {
            let res = Pin::new(&mut stream).poll_next(cx);

            if matches!(res, Poll::Pending | Poll::Ready(Some(Ok(_)))) {
                self.0 = Some(stream);
            }

            res
        } else {
            Poll::Ready(None)
        }
    }

    // `size_hint()` is unimplemented because unused.
}

// === IncomingStream ===

// * Produces bytes from incoming data frames.
// * Skips trailer frames (CH doesn't use them for now).
// * Converts hyper errors to our own.
struct IncomingStream(Incoming);

impl Stream for IncomingStream {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut incoming = Pin::new(&mut self.get_mut().0);

        loop {
            break match incoming.as_mut().poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(bytes) => Poll::Ready(Some(Ok(bytes))),
                    Err(_frame) => continue,
                },
                Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            };
        }
    }
}

// === Decompress ===

enum Decompress<S> {
    Plain(S),
    #[cfg(feature = "lz4")]
    Lz4(Lz4Decoder<S>),
}

impl<S> Decompress<S> {
    fn new(stream: S, compression: Compression) -> Self {
        match compression {
            Compression::None => Self::Plain(stream),
            #[cfg(feature = "lz4")]
            #[allow(deprecated)]
            Compression::Lz4 | Compression::Lz4Hc(_) => Self::Lz4(Lz4Decoder::new(stream)),
        }
    }
}

impl<S> Stream for Decompress<S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream)
                .poll_next(cx)
                .map_ok(|bytes| Chunk {
                    net_size: bytes.len(),
                    data: bytes,
                })
                .map_err(Into::into),
            #[cfg(feature = "lz4")]
            Self::Lz4(stream) => Pin::new(stream).poll_next(cx),
        }
    }
}

// === DetectDbException ===

struct DetectDbException<S>(S);

impl<S> Stream for DetectDbException<S>
where
    S: Stream<Item = Result<Chunk>> + Unpin,
{
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = Pin::new(&mut self.0).poll_next(cx);

        if let Poll::Ready(Some(Ok(chunk))) = &res {
            if let Some(err) = extract_exception(&chunk.data) {
                return Poll::Ready(Some(Err(err)));
            }
        }

        res
    }
}

// Format:
// \`\`\`
//   <data>Code: <code>. DB::Exception: <desc> (version <version> (official build))\n
// \`\`\`
fn extract_exception(chunk: &[u8]) -> Option<Error> {
    // `))\n` is very rare in real data, so it's fast dirty check.
    // In random data, it occurs with a probability of ~6*10^-8 only.
    if chunk.ends_with(b"))\n") {
        extract_exception_slow(chunk)
    } else {
        None
    }
}

#[cold]
#[inline(never)]
fn extract_exception_slow(chunk: &[u8]) -> Option<Error> {
    let index = chunk.rfind(b"Code:")?;

    if !chunk[index..].contains_str(b"DB::Exception:") {
        return None;
    }

    let exception = String::from_utf8_lossy(&chunk[index..chunk.len() - 1]);
    Some(Error::BadResponse(exception.into()))
}

```

# src/row.rs

```rs
use crate::sql;

pub trait Row {
    const COLUMN_NAMES: &'static [&'static str];

    // TODO: count
    // TODO: different list for SELECT/INSERT (de/ser)
}

// Actually, it's not public now.
pub trait Primitive {}

macro_rules! impl_primitive_for {
    ($t:ty, $($other:tt)*) => {
        impl Primitive for $t {}
        impl_primitive_for!($($other)*);
    };
    () => {};
}

// TODO: char? &str? SocketAddr? Path? Duration? NonZero*?
impl_primitive_for![
    bool, String, u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64,
];

macro_rules! impl_row_for_tuple {
    ($i:ident $($other:ident)+) => {
        /// Two forms are supported:
        /// * (P1, P2, ...)
        /// * (SomeRow, P1, P2, ...)
        ///
        /// The second one is useful for queries like
        /// `SELECT ?fields, count() FROM .. GROUP BY ?fields`.
        impl<$i: Row, $($other: Primitive),+> Row for ($i, $($other),+) {
            const COLUMN_NAMES: &'static [&'static str] = $i::COLUMN_NAMES;
        }

        impl_row_for_tuple!($($other)+);
    };
    ($i:ident) => {};
}

// TODO: revise this?
impl Primitive for () {}

impl<P: Primitive> Row for P {
    const COLUMN_NAMES: &'static [&'static str] = &[];
}

impl_row_for_tuple!(T0 T1 T2 T3 T4 T5 T6 T7 T8);

impl<T> Row for Vec<T> {
    const COLUMN_NAMES: &'static [&'static str] = &[];
}

/// Collects all field names in depth and joins them with comma.
pub(crate) fn join_column_names<R: Row>() -> Option<String> {
    if R::COLUMN_NAMES.is_empty() {
        return None;
    }

    let out = R::COLUMN_NAMES
        .iter()
        .enumerate()
        .fold(String::new(), |mut res, (idx, name)| {
            if idx > 0 {
                res.push(',');
            }
            sql::escape::identifier(name, &mut res).expect("impossible");
            res
        });

    Some(out)
}

#[cfg(test)]
mod tests {
    // XXX: need for `derive(Row)`. Provide `row(crate = ..)` instead.
    use crate as clickhouse;
    use clickhouse::Row;

    use super::*;

    #[test]
    fn it_grabs_simple_struct() {
        #[derive(Row)]
        #[allow(dead_code)]
        struct Simple1 {
            one: u32,
        }

        #[derive(Row)]
        #[allow(dead_code)]
        struct Simple2 {
            one: u32,
            two: u32,
        }

        assert_eq!(join_column_names::<Simple1>().unwrap(), "`one`");
        assert_eq!(join_column_names::<Simple2>().unwrap(), "`one`,`two`");
    }

    #[test]
    fn it_grabs_mix() {
        #[derive(Row)]
        struct SomeRow {
            _a: u32,
        }

        assert_eq!(join_column_names::<(SomeRow, u32)>().unwrap(), "`_a`");
    }

    #[test]
    fn it_supports_renaming() {
        use serde::Serialize;

        #[derive(Row, Serialize)]
        #[allow(dead_code)]
        struct TopLevel {
            #[serde(rename = "two")]
            one: u32,
        }

        assert_eq!(join_column_names::<TopLevel>().unwrap(), "`two`");
    }

    #[test]
    fn it_skips_serializing() {
        use serde::Serialize;

        #[derive(Row, Serialize)]
        #[allow(dead_code)]
        struct TopLevel {
            one: u32,
            #[serde(skip_serializing)]
            two: u32,
        }

        assert_eq!(join_column_names::<TopLevel>().unwrap(), "`one`");
    }

    #[test]
    fn it_skips_deserializing() {
        use serde::Deserialize;

        #[derive(Row, Deserialize)]
        #[allow(dead_code)]
        struct TopLevel {
            one: u32,
            #[serde(skip_deserializing)]
            two: u32,
        }

        assert_eq!(join_column_names::<TopLevel>().unwrap(), "`one`");
    }

    #[test]
    fn it_rejects_other() {
        #[allow(dead_code)]
        #[derive(Row)]
        struct NamedTuple(u32, u32);

        assert_eq!(join_column_names::<u32>(), None);
        assert_eq!(join_column_names::<(u32, u64)>(), None);
        assert_eq!(join_column_names::<NamedTuple>(), None);
    }

    #[test]
    fn it_handles_raw_identifiers() {
        use serde::Serialize;

        #[derive(Row, Serialize)]
        #[allow(dead_code)]
        struct MyRow {
            r#type: u32,
            #[serde(rename = "if")]
            r#match: u32,
        }

        assert_eq!(join_column_names::<MyRow>().unwrap(), "`type`,`if`");
    }
}

```

# src/rowbinary/de.rs

```rs
use std::{convert::TryFrom, mem, str};

use crate::error::{Error, Result};
use bytes::Buf;
use serde::{
    de::{DeserializeSeed, Deserializer, EnumAccess, SeqAccess, VariantAccess, Visitor},
    Deserialize,
};

/// Deserializes a value from `input` with a row encoded in `RowBinary`.
///
/// It accepts _a reference to_ a byte slice because it somehow leads to a more
/// performant generated code than `(&[u8]) -> Result<(T, usize)>` and even
/// `(&[u8], &mut Option<T>) -> Result<usize>`.
pub(crate) fn deserialize_from<'data, T: Deserialize<'data>>(input: &mut &'data [u8]) -> Result<T> {
    let mut deserializer = RowBinaryDeserializer { input };
    T::deserialize(&mut deserializer)
}

/// A deserializer for the RowBinary format.
///
/// See https://clickhouse.com/docs/en/interfaces/formats#rowbinary for details.
struct RowBinaryDeserializer<'cursor, 'data> {
    input: &'cursor mut &'data [u8],
}

impl<'data> RowBinaryDeserializer<'_, 'data> {
    fn read_vec(&mut self, size: usize) -> Result<Vec<u8>> {
        Ok(self.read_slice(size)?.to_vec())
    }

    fn read_slice(&mut self, size: usize) -> Result<&'data [u8]> {
        ensure_size(&mut self.input, size)?;
        let slice = &self.input[..size];
        self.input.advance(size);
        Ok(slice)
    }

    fn read_size(&mut self) -> Result<usize> {
        let size = get_unsigned_leb128(&mut self.input)?;
        // TODO: what about another error?
        usize::try_from(size).map_err(|_| Error::NotEnoughData)
    }
}

#[inline]
fn ensure_size(buffer: impl Buf, size: usize) -> Result<()> {
    if buffer.remaining() < size {
        Err(Error::NotEnoughData)
    } else {
        Ok(())
    }
}

macro_rules! impl_num {
    ($ty:ty, $deser_method:ident, $visitor_method:ident, $reader_method:ident) => {
        #[inline]
        fn $deser_method<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
            ensure_size(&mut self.input, mem::size_of::<$ty>())?;
            let value = self.input.$reader_method();
            visitor.$visitor_method(value)
        }
    };
}

impl<'data> Deserializer<'data> for &mut RowBinaryDeserializer<'_, 'data> {
    type Error = Error;

    impl_num!(i8, deserialize_i8, visit_i8, get_i8);

    impl_num!(i16, deserialize_i16, visit_i16, get_i16_le);

    impl_num!(i32, deserialize_i32, visit_i32, get_i32_le);

    impl_num!(i64, deserialize_i64, visit_i64, get_i64_le);

    impl_num!(i128, deserialize_i128, visit_i128, get_i128_le);

    impl_num!(u8, deserialize_u8, visit_u8, get_u8);

    impl_num!(u16, deserialize_u16, visit_u16, get_u16_le);

    impl_num!(u32, deserialize_u32, visit_u32, get_u32_le);

    impl_num!(u64, deserialize_u64, visit_u64, get_u64_le);

    impl_num!(u128, deserialize_u128, visit_u128, get_u128_le);

    impl_num!(f32, deserialize_f32, visit_f32, get_f32_le);

    impl_num!(f64, deserialize_f64, visit_f64, get_f64_le);

    #[inline]
    fn deserialize_any<V: Visitor<'data>>(self, _: V) -> Result<V::Value> {
        Err(Error::DeserializeAnyNotSupported)
    }

    #[inline]
    fn deserialize_unit<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        // TODO: revise this.
        visitor.visit_unit()
    }

    #[inline]
    fn deserialize_char<V: Visitor<'data>>(self, _: V) -> Result<V::Value> {
        panic!("character types are unsupported: `char`");
    }

    #[inline]
    fn deserialize_bool<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        ensure_size(&mut self.input, 1)?;
        match self.input.get_u8() {
            0 => visitor.visit_bool(false),
            1 => visitor.visit_bool(true),
            v => Err(Error::InvalidTagEncoding(v as usize)),
        }
    }

    #[inline]
    fn deserialize_str<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        let size = self.read_size()?;
        let slice = self.read_slice(size)?;
        let str = str::from_utf8(slice).map_err(Error::from)?;
        visitor.visit_borrowed_str(str)
    }

    #[inline]
    fn deserialize_string<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        let size = self.read_size()?;
        let vec = self.read_vec(size)?;
        let string = String::from_utf8(vec).map_err(|err| Error::from(err.utf8_error()))?;
        visitor.visit_string(string)
    }

    #[inline]
    fn deserialize_bytes<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        let size = self.read_size()?;
        let slice = self.read_slice(size)?;
        visitor.visit_borrowed_bytes(slice)
    }

    #[inline]
    fn deserialize_byte_buf<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        let size = self.read_size()?;
        visitor.visit_byte_buf(self.read_vec(size)?)
    }

    #[inline]
    fn deserialize_identifier<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_u8(visitor)
    }

    #[inline]
    fn deserialize_enum<V: Visitor<'data>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        struct Access<'de, 'cursor, 'data> {
            deserializer: &'de mut RowBinaryDeserializer<'cursor, 'data>,
        }
        struct VariantDeserializer<'de, 'cursor, 'data> {
            deserializer: &'de mut RowBinaryDeserializer<'cursor, 'data>,
        }
        impl<'data> VariantAccess<'data> for VariantDeserializer<'_, '_, 'data> {
            type Error = Error;

            fn unit_variant(self) -> Result<()> {
                Err(Error::Unsupported("unit variants".to_string()))
            }

            fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
            where
                T: DeserializeSeed<'data>,
            {
                DeserializeSeed::deserialize(seed, &mut *self.deserializer)
            }

            fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value>
            where
                V: Visitor<'data>,
            {
                self.deserializer.deserialize_tuple(len, visitor)
            }

            fn struct_variant<V>(
                self,
                fields: &'static [&'static str],
                visitor: V,
            ) -> Result<V::Value>
            where
                V: Visitor<'data>,
            {
                self.deserializer.deserialize_tuple(fields.len(), visitor)
            }
        }

        impl<'de, 'cursor, 'data> EnumAccess<'data> for Access<'de, 'cursor, 'data> {
            type Error = Error;
            type Variant = VariantDeserializer<'de, 'cursor, 'data>;

            fn variant_seed<T>(self, seed: T) -> Result<(T::Value, Self::Variant), Self::Error>
            where
                T: DeserializeSeed<'data>,
            {
                let value = seed.deserialize(&mut *self.deserializer)?;
                let deserializer = VariantDeserializer {
                    deserializer: self.deserializer,
                };
                Ok((value, deserializer))
            }
        }
        visitor.visit_enum(Access { deserializer: self })
    }

    #[inline]
    fn deserialize_tuple<V: Visitor<'data>>(self, len: usize, visitor: V) -> Result<V::Value> {
        struct Access<'de, 'cursor, 'data> {
            deserializer: &'de mut RowBinaryDeserializer<'cursor, 'data>,
            len: usize,
        }

        impl<'data> SeqAccess<'data> for Access<'_, '_, 'data> {
            type Error = Error;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
            where
                T: DeserializeSeed<'data>,
            {
                if self.len > 0 {
                    self.len -= 1;
                    let value = DeserializeSeed::deserialize(seed, &mut *self.deserializer)?;
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }

            fn size_hint(&self) -> Option<usize> {
                Some(self.len)
            }
        }

        visitor.visit_seq(Access {
            deserializer: self,
            len,
        })
    }

    #[inline]
    fn deserialize_option<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        ensure_size(&mut self.input, 1)?;

        match self.input.get_u8() {
            0 => visitor.visit_some(&mut *self),
            1 => visitor.visit_none(),
            v => Err(Error::InvalidTagEncoding(v as usize)),
        }
    }

    #[inline]
    fn deserialize_seq<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        let len = self.read_size()?;
        self.deserialize_tuple(len, visitor)
    }

    #[inline]
    fn deserialize_map<V: Visitor<'data>>(self, _visitor: V) -> Result<V::Value> {
        panic!("maps are unsupported, use `Vec<(A, B)>` instead");
    }

    #[inline]
    fn deserialize_struct<V: Visitor<'data>>(
        self,
        _name: &str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_tuple(fields.len(), visitor)
    }

    #[inline]
    fn deserialize_newtype_struct<V: Visitor<'data>>(
        self,
        _name: &str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    #[inline]
    fn deserialize_unit_struct<V: Visitor<'data>>(
        self,
        name: &'static str,
        _visitor: V,
    ) -> Result<V::Value> {
        panic!("unit types are unsupported: `{name}`");
    }

    #[inline]
    fn deserialize_tuple_struct<V: Visitor<'data>>(
        self,
        name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value> {
        panic!("tuple struct types are unsupported: `{name}`");
    }

    #[inline]
    fn deserialize_ignored_any<V: Visitor<'data>>(self, _visitor: V) -> Result<V::Value> {
        panic!("ignored types are unsupported");
    }

    #[inline]
    fn is_human_readable(&self) -> bool {
        false
    }
}

fn get_unsigned_leb128(mut buffer: impl Buf) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0;

    loop {
        ensure_size(&mut buffer, 1)?;

        let byte = buffer.get_u8();
        value |= (byte as u64 & 0x7f) << shift;

        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
        if shift > 57 {
            // TODO: what about another error?
            return Err(Error::NotEnoughData);
        }
    }

    Ok(value)
}

#[test]
fn it_deserializes_unsigned_leb128() {
    let buf = &[0xe5, 0x8e, 0x26][..];
    assert_eq!(get_unsigned_leb128(buf).unwrap(), 624_485);
}

```

# src/rowbinary/mod.rs

```rs
pub(crate) use de::deserialize_from;
pub(crate) use ser::serialize_into;

mod de;
mod ser;
#[cfg(test)]
mod tests;

```

# src/rowbinary/ser.rs

```rs
use bytes::BufMut;
use serde::{
    ser::{Impossible, SerializeSeq, SerializeStruct, SerializeTuple, Serializer},
    Serialize,
};

use crate::error::{Error, Result};

/// Serializes `value` using the RowBinary format and writes to `buffer`.
pub(crate) fn serialize_into(buffer: impl BufMut, value: &impl Serialize) -> Result<()> {
    let mut serializer = RowBinarySerializer { buffer };
    value.serialize(&mut serializer)?;
    Ok(())
}

/// A serializer for the RowBinary format.
///
/// See https://clickhouse.com/docs/en/interfaces/formats#rowbinary for details.
struct RowBinarySerializer<B> {
    buffer: B,
}

macro_rules! impl_num {
    ($ty:ty, $ser_method:ident, $writer_method:ident) => {
        #[inline]
        fn $ser_method(self, v: $ty) -> Result<()> {
            self.buffer.$writer_method(v);
            Ok(())
        }
    };
}

impl<B: BufMut> Serializer for &'_ mut RowBinarySerializer<B> {
    type Error = Error;
    type Ok = ();
    type SerializeMap = Impossible<(), Error>;
    type SerializeSeq = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Impossible<(), Error>;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Impossible<(), Error>;
    type SerializeTupleVariant = Impossible<(), Error>;

    impl_num!(i8, serialize_i8, put_i8);

    impl_num!(i16, serialize_i16, put_i16_le);

    impl_num!(i32, serialize_i32, put_i32_le);

    impl_num!(i64, serialize_i64, put_i64_le);

    impl_num!(i128, serialize_i128, put_i128_le);

    impl_num!(u8, serialize_u8, put_u8);

    impl_num!(u16, serialize_u16, put_u16_le);

    impl_num!(u32, serialize_u32, put_u32_le);

    impl_num!(u64, serialize_u64, put_u64_le);

    impl_num!(u128, serialize_u128, put_u128_le);

    impl_num!(f32, serialize_f32, put_f32_le);

    impl_num!(f64, serialize_f64, put_f64_le);

    #[inline]
    fn serialize_bool(self, v: bool) -> Result<()> {
        self.buffer.put_u8(v as _);
        Ok(())
    }

    #[inline]
    fn serialize_char(self, _v: char) -> Result<()> {
        panic!("character types are unsupported: `char`");
    }

    #[inline]
    fn serialize_str(self, v: &str) -> Result<()> {
        put_unsigned_leb128(&mut self.buffer, v.len() as u64);
        self.buffer.put_slice(v.as_bytes());
        Ok(())
    }

    #[inline]
    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        put_unsigned_leb128(&mut self.buffer, v.len() as u64);
        self.buffer.put_slice(v);
        Ok(())
    }

    #[inline]
    fn serialize_none(self) -> Result<()> {
        self.buffer.put_u8(1);
        Ok(())
    }

    #[inline]
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<()> {
        self.buffer.put_u8(0);
        value.serialize(self)
    }

    #[inline]
    fn serialize_unit(self) -> Result<()> {
        panic!("unit types are unsupported: `()`");
    }

    #[inline]
    fn serialize_unit_struct(self, name: &'static str) -> Result<()> {
        panic!("unit types are unsupported: `{name}`");
    }

    #[inline]
    fn serialize_unit_variant(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        panic!("unit variant types are unsupported: `{name}::{variant}`");
    }

    #[inline]
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<()> {
        value.serialize(self)
    }

    #[inline]
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<()> {
        // TODO:
        //  - Now this code implicitly allows using enums at the top level.
        //    However, instead of a more descriptive panic, it ends with a "not enough data." error.
        //  - Also, it produces an unclear message for a forgotten `serde_repr` (Enum8 and Enum16).
        //  See https://github.com/ClickHouse/clickhouse-rs/pull/170#discussion_r1848549636

        // Max number of types in the Variant data type is 255
        // See also: https://github.com/ClickHouse/ClickHouse/issues/54864
        if variant_index > 255 {
            return Err(Error::VariantDiscriminatorIsOutOfBound(
                variant_index as usize,
            ));
        }
        self.buffer.put_u8(variant_index as u8);
        value.serialize(self)
    }

    #[inline]
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        let len = len.ok_or(Error::SequenceMustHaveLength)?;
        put_unsigned_leb128(&mut self.buffer, len as u64);
        Ok(self)
    }

    #[inline]
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    #[inline]
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        panic!("tuple struct types are unsupported: `{name}`");
    }

    #[inline]
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        panic!("tuple variant types are unsupported: `{name}::{variant}`");
    }

    #[inline]
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        panic!("maps are unsupported, use `Vec<(A, B)>` instead");
    }

    #[inline]
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(self)
    }

    #[inline]
    fn serialize_struct_variant(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        panic!("struct variant types are unsupported: `{name}::{variant}`");
    }

    #[inline]
    fn is_human_readable(&self) -> bool {
        false
    }
}

impl<B: BufMut> SerializeStruct for &mut RowBinarySerializer<B> {
    type Error = Error;
    type Ok = ();

    #[inline]
    fn serialize_field<T: Serialize + ?Sized>(&mut self, _: &'static str, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<B: BufMut> SerializeSeq for &'_ mut RowBinarySerializer<B> {
    type Error = Error;
    type Ok = ();

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<B: BufMut> SerializeTuple for &'_ mut RowBinarySerializer<B> {
    type Error = Error;
    type Ok = ();

    #[inline]
    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

fn put_unsigned_leb128(mut buffer: impl BufMut, mut value: u64) {
    while {
        let mut byte = value as u8 & 0x7f;
        value >>= 7;

        if value != 0 {
            byte |= 0x80;
        }

        buffer.put_u8(byte);

        value != 0
    } {}
}

#[test]
fn it_serializes_unsigned_leb128() {
    let mut vec = Vec::new();

    put_unsigned_leb128(&mut vec, 624_485);

    assert_eq!(vec, [0xe5, 0x8e, 0x26]);
}

```

# src/rowbinary/tests.rs

```rs
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Timestamp32(u32);

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Timestamp64(u64);

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct FixedPoint64(i64);

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct FixedPoint128(i128);

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Sample<'a> {
    int8: i8,
    int32: i32,
    int64: i64,
    uint8: u8,
    uint32: u32,
    uint64: u64,
    float32: f32,
    float64: f64,
    datetime: Timestamp32,
    datetime64: Timestamp64,
    decimal64: FixedPoint64,
    decimal128: FixedPoint128,
    string: &'a str,
    #[serde(with = "serde_bytes")]
    blob: &'a [u8],
    optional_decimal64: Option<FixedPoint64>,
    optional_datetime: Option<Timestamp32>,
    fixed_string: [u8; 4],
    array: Vec<i8>,
    boolean: bool,
}

fn sample() -> Sample<'static> {
    Sample {
        int8: -42,
        int32: -3242,
        int64: -6442,
        uint8: 42,
        uint32: 3242,
        uint64: 6442,
        float32: 42.42,
        float64: 42.42,
        datetime: Timestamp32(2_301_990_162),
        datetime64: Timestamp64(2_301_990_162_123),
        decimal64: FixedPoint64(4242 * 10_000_000),
        decimal128: FixedPoint128(4242 * 10_000_000),
        string: "01234",
        blob: &[0, 1, 2, 3, 4],
        optional_decimal64: None,
        optional_datetime: Some(Timestamp32(2_301_990_162)),
        fixed_string: [b'B', b'T', b'C', 0],
        array: vec![-42, 42, -42, 42],
        boolean: true,
    }
}

fn sample_serialized() -> Vec<u8> {
    vec![
        // [Int8] -42
        0xd6, //
        // [Int32] -3242
        0x56, 0xf3, 0xff, 0xff, //
        // [Int64] -6442
        0xd6, 0xe6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, //
        // [UInt8] 42
        0x2a, //
        // [UInt32] 3242
        0xaa, 0x0c, 0x00, 0x00, //
        // [UInt64] 6442
        0x2a, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
        // [Float32] 42.42
        0x14, 0xae, 0x29, 0x42, //
        // [Float64] 42.42
        0xf6, 0x28, 0x5c, 0x8f, 0xc2, 0x35, 0x45, 0x40, //
        // [DateTime] 2042-12-12 12:42:42
        //       (ts: 2301990162)
        0x12, 0x95, 0x35, 0x89, //
        // [DateTime64(3)] 2042-12-12 12:42:42'123
        //       (ts: 2301990162123)
        0xcb, 0x4e, 0x4e, 0xf9, 0x17, 0x02, 0x00, 0x00, //
        // [Decimal64(9)] 42.420000000
        0x00, 0xd5, 0x6d, 0xe0, 0x09, 0x00, 0x00, 0x00, //
        // [Decimal128(9)] 42.420000000
        0x00, 0xd5, 0x6d, 0xe0, 0x09, 0x00, 0x00, 0x00, //
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
        // [String] 5 "01234"
        0x05, 0x30, 0x31, 0x32, 0x33, 0x34, //
        // [String] 5 [0, 1, 2, 3, 4]
        0x05, 0x00, 0x01, 0x02, 0x03, 0x04, //
        // [Nullable(Decimal64(9))] NULL
        0x01, //
        // [Nullable(DateTime)] 2042-12-12 12:42:42
        //       (ts: 2301990162)
        0x00, 0x12, 0x95, 0x35, 0x89, //
        // [FixedString(4)] [b'B', b'T', b'C', 0]
        0x42, 0x54, 0x43, 0x00, //
        // [Array(Int32)] [-42, 42, -42, 42]
        0x04, 0xd6, 0x2a, 0xd6, 0x2a, //
        // [Boolean] true
        0x01, //
    ]
}

#[test]
fn it_serializes() {
    let mut actual = Vec::new();
    super::serialize_into(&mut actual, &sample()).unwrap();
    assert_eq!(actual, sample_serialized());
}

#[test]
fn it_deserializes() {
    let input = sample_serialized();

    for i in 0..input.len() {
        let (mut left, mut right) = input.split_at(i);

        // It shouldn't panic.
        let _: Result<Sample<'_>, _> = super::deserialize_from(&mut left);
        let _: Result<Sample<'_>, _> = super::deserialize_from(&mut right);

        let actual: Sample<'_> = super::deserialize_from(&mut input.as_slice()).unwrap();
        assert_eq!(actual, sample());
    }
}

```

# src/serde.rs

```rs
//! Contains ser/de modules for different external types.

use serde::{
    de::{Deserialize, Deserializer},
    ser::{Serialize, Serializer},
};

macro_rules! option {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        pub mod option {
            use super::*;

            struct $name(super::$name);

            impl Serialize for $name {
                fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                    super::serialize(&self.0, serializer)
                }
            }

            impl<'de> Deserialize<'de> for $name {
                fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                    super::deserialize(deserializer).map($name)
                }
            }

            pub fn serialize<S>(v: &Option<super::$name>, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                v.clone().map($name).serialize(serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<super::$name>, D::Error>
            where
                D: Deserializer<'de>,
            {
                let opt: Option<$name> = Deserialize::deserialize(deserializer)?;
                Ok(opt.map(|v| v.0))
            }
        }
    };
}

/// Ser/de [`std::net::Ipv4Addr`] to/from `IPv4`.
pub mod ipv4 {
    use std::net::Ipv4Addr;

    use super::*;

    option!(
        Ipv4Addr,
        "Ser/de `Option<Ipv4Addr>` to/from `Nullable(IPv4)`."
    );

    pub fn serialize<S>(ipv4: &Ipv4Addr, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        u32::from(*ipv4).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Ipv4Addr, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ip: u32 = Deserialize::deserialize(deserializer)?;
        Ok(Ipv4Addr::from(ip))
    }
}

/// Ser/de [`::uuid::Uuid`] to/from `UUID`.
#[cfg(feature = "uuid")]
pub mod uuid {
    use ::uuid::Uuid;
    use serde::de::Error;

    use super::*;

    option!(Uuid, "Ser/de `Option<Uuid>` to/from `Nullable(UUID)`.");

    pub fn serialize<S>(uuid: &Uuid, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            uuid.to_string().serialize(serializer)
        } else {
            let bytes = uuid.as_u64_pair();
            bytes.serialize(serializer)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Uuid, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let uuid_str: &str = Deserialize::deserialize(deserializer)?;
            Uuid::parse_str(uuid_str).map_err(D::Error::custom)
        } else {
            let bytes: (u64, u64) = Deserialize::deserialize(deserializer)?;
            Ok(Uuid::from_u64_pair(bytes.0, bytes.1))
        }
    }
}

#[cfg(feature = "chrono")]
pub mod chrono {
    use super::*;
    use ::chrono::{DateTime, Utc};
    use serde::{de::Error as _, ser::Error as _};

    pub mod datetime {
        use super::*;

        type DateTimeUtc = DateTime<Utc>;

        option!(
            DateTimeUtc,
            "Ser/de `Option<DateTime<Utc>>` to/from `Nullable(DateTime)`."
        );

        pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let ts = dt.timestamp();

            u32::try_from(ts)
                .map_err(|_| S::Error::custom(format!("{dt} cannot be represented as DateTime")))?
                .serialize(serializer)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
        where
            D: Deserializer<'de>,
        {
            let ts: u32 = Deserialize::deserialize(deserializer)?;
            DateTime::<Utc>::from_timestamp(i64::from(ts), 0).ok_or_else(|| {
                D::Error::custom(format!("{ts} cannot be converted to DateTime<Utc>"))
            })
        }
    }

    /// Contains modules to ser/de `DateTime<Utc>` to/from `DateTime64(_)`.
    pub mod datetime64 {
        use super::*;
        type DateTimeUtc = DateTime<Utc>;

        /// Ser/de `DateTime<Utc>` to/from `DateTime64(0)` (seconds).
        pub mod secs {
            use super::*;

            option!(
                DateTimeUtc,
                "Ser/de `Option<OffsetDateTime>` to/from `Nullable(DateTime64(0))`."
            );

            pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let ts = dt.timestamp();
                ts.serialize(serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
            where
                D: Deserializer<'de>,
            {
                let ts: i64 = Deserialize::deserialize(deserializer)?;
                DateTime::<Utc>::from_timestamp(ts, 0).ok_or_else(|| {
                    D::Error::custom(format!("Can't create DateTime<Utc> from {ts}"))
                })
            }
        }

        /// Ser/de `DateTime<Utc>` to/from `DateTime64(3)` (milliseconds).
        pub mod millis {
            use super::*;

            option!(
                DateTimeUtc,
                "Ser/de `Option<DateTime<Utc>>` to/from `Nullable(DateTime64(3))`."
            );

            pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let ts = dt.timestamp_millis();
                ts.serialize(serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
            where
                D: Deserializer<'de>,
            {
                let ts: i64 = Deserialize::deserialize(deserializer)?;
                DateTime::<Utc>::from_timestamp_millis(ts).ok_or_else(|| {
                    D::Error::custom(format!("Can't create DateTime<Utc> from {ts}"))
                })
            }
        }

        /// Ser/de `DateTime<Utc>` to/from `DateTime64(6)` (microseconds).
        pub mod micros {
            use super::*;

            option!(
                DateTimeUtc,
                "Ser/de `Option<DateTime<Utc>>` to/from `Nullable(DateTime64(6))`."
            );

            pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let ts = dt.timestamp_micros();
                ts.serialize(serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
            where
                D: Deserializer<'de>,
            {
                let ts: i64 = Deserialize::deserialize(deserializer)?;
                DateTime::<Utc>::from_timestamp_micros(ts).ok_or_else(|| {
                    D::Error::custom(format!("Can't create DateTime<Utc> from {ts}"))
                })
            }
        }

        /// Ser/de `DateTime<Utc>` to/from `DateTime64(9)` (nanoseconds).
        pub mod nanos {
            use super::*;

            option!(
                DateTimeUtc,
                "Ser/de `Option<DateTime<Utc>>` to/from `Nullable(DateTime64(9))`."
            );

            pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let ts = dt.timestamp_nanos_opt().ok_or_else(|| {
                    S::Error::custom(format!("{dt} cannot be represented as DateTime64"))
                })?;
                ts.serialize(serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
            where
                D: Deserializer<'de>,
            {
                let ts: i64 = Deserialize::deserialize(deserializer)?;
                Ok(DateTime::<Utc>::from_timestamp_nanos(ts))
            }
        }
    }

    /// Ser/de `time::Date` to/from `Date`.
    pub mod date {
        use super::*;
        use ::chrono::{Duration, NaiveDate};

        option!(
            NaiveDate,
            "Ser/de `Option<NaiveDate>` to/from `Nullable(Date)`."
        );

        const ORIGIN: Option<NaiveDate> = NaiveDate::from_yo_opt(1970, 1);

        pub fn serialize<S>(date: &NaiveDate, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let origin = ORIGIN.unwrap();
            if *date < origin {
                let msg = format!("{date} cannot be represented as Date");
                return Err(S::Error::custom(msg));
            }

            let elapsed = *date - origin; // cannot underflow: checked above
            let days = elapsed.num_days();

            u16::try_from(days)
                .map_err(|_| S::Error::custom(format!("{date} cannot be represented as Date")))?
                .serialize(serializer)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
        where
            D: Deserializer<'de>,
        {
            let days: u16 = Deserialize::deserialize(deserializer)?;
            Ok(ORIGIN.unwrap() + Duration::days(i64::from(days))) // cannot overflow: always < `Date::MAX`
        }
    }

    /// Ser/de `time::Date` to/from `Date32`.
    pub mod date32 {
        use ::chrono::{Duration, NaiveDate};

        use super::*;

        option!(
            NaiveDate,
            "Ser/de `Option<NaiveDate>` to/from `Nullable(Date32)`."
        );

        const ORIGIN: Option<NaiveDate> = NaiveDate::from_yo_opt(1970, 1);

        // NOTE: actually, it's 1925 and 2283 with a tail for versions before 22.8-lts.
        const MIN: Option<NaiveDate> = NaiveDate::from_yo_opt(1900, 1);
        const MAX: Option<NaiveDate> = NaiveDate::from_yo_opt(2299, 365);

        pub fn serialize<S>(date: &NaiveDate, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            if *date < MIN.unwrap() || *date > MAX.unwrap() {
                let msg = format!("{date} cannot be represented as Date");
                return Err(S::Error::custom(msg));
            }

            let elapsed = *date - ORIGIN.unwrap(); // cannot underflow: checked above
            let days = elapsed.num_days();

            i32::try_from(days)
                .map_err(|_| S::Error::custom(format!("{date} cannot be represented as Date32")))?
                .serialize(serializer)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
        where
            D: Deserializer<'de>,
        {
            let days: i32 = Deserialize::deserialize(deserializer)?;

            // It shouldn't overflow, because clamped by CH and < `Date::MAX`.
            // TODO: ensure CH clamps when an invalid value is inserted in binary format.
            Ok(ORIGIN.unwrap() + Duration::days(i64::from(days)))
        }
    }
}

/// Ser/de [`::time::OffsetDateTime`] and [`::time::Date`].
#[cfg(feature = "time")]
pub mod time {
    use std::convert::TryFrom;

    use ::time::{error::ComponentRange, Date, Duration, OffsetDateTime};
    use serde::{de::Error as _, ser::Error as _};

    use super::*;

    /// Ser/de `OffsetDateTime` to/from `DateTime`.
    pub mod datetime {
        use super::*;

        option!(
            OffsetDateTime,
            "Ser/de `Option<OffsetDateTime>` to/from `Nullable(DateTime)`."
        );

        pub fn serialize<S>(dt: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let ts = dt.unix_timestamp();

            u32::try_from(ts)
                .map_err(|_| S::Error::custom(format!("{dt} cannot be represented as DateTime")))?
                .serialize(serializer)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
        where
            D: Deserializer<'de>,
        {
            let ts: u32 = Deserialize::deserialize(deserializer)?;
            OffsetDateTime::from_unix_timestamp(i64::from(ts)).map_err(D::Error::custom)
        }
    }

    /// Contains modules to ser/de `OffsetDateTime` to/from `DateTime64(_)`.
    pub mod datetime64 {
        use super::*;

        /// Ser/de `OffsetDateTime` to/from `DateTime64(0)`.
        pub mod secs {
            use super::*;

            option!(
                OffsetDateTime,
                "Ser/de `Option<OffsetDateTime>` to/from `Nullable(DateTime64(0))`."
            );

            pub fn serialize<S>(dt: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                do_serialize(dt, 1_000_000_000, serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
            where
                D: Deserializer<'de>,
            {
                do_deserialize(deserializer, 1_000_000_000)
            }
        }

        /// Ser/de `OffsetDateTime` to/from `DateTime64(3)`.
        pub mod millis {
            use super::*;

            option!(
                OffsetDateTime,
                "Ser/de `Option<OffsetDateTime>` to/from `Nullable(DateTime64(3))`."
            );

            pub fn serialize<S>(dt: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                do_serialize(dt, 1_000_000, serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
            where
                D: Deserializer<'de>,
            {
                do_deserialize(deserializer, 1_000_000)
            }
        }

        /// Ser/de `OffsetDateTime` to/from `DateTime64(6)`.
        pub mod micros {
            use super::*;

            option!(
                OffsetDateTime,
                "Ser/de `Option<OffsetDateTime>` to/from `Nullable(DateTime64(6))`."
            );

            pub fn serialize<S>(dt: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                do_serialize(dt, 1_000, serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
            where
                D: Deserializer<'de>,
            {
                do_deserialize(deserializer, 1_000)
            }
        }

        /// Ser/de `OffsetDateTime` to/from `DateTime64(9)`.
        pub mod nanos {
            use super::*;

            option!(
                OffsetDateTime,
                "Ser/de `Option<OffsetDateTime>` to/from `Nullable(DateTime64(9))`."
            );

            pub fn serialize<S>(dt: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                do_serialize(dt, 1, serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
            where
                D: Deserializer<'de>,
            {
                do_deserialize(deserializer, 1)
            }
        }

        fn do_serialize<S>(dt: &OffsetDateTime, div: i128, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let ts = dt.unix_timestamp_nanos() / div;

            i64::try_from(ts)
                .map_err(|_| S::Error::custom(format!("{dt} cannot be represented as DateTime64")))?
                .serialize(serializer)
        }

        fn do_deserialize<'de, D>(deserializer: D, mul: i128) -> Result<OffsetDateTime, D::Error>
        where
            D: Deserializer<'de>,
        {
            let ts: i64 = Deserialize::deserialize(deserializer)?;
            let ts = i128::from(ts) * mul; // cannot overflow: `mul` fits in `i64`
            OffsetDateTime::from_unix_timestamp_nanos(ts).map_err(D::Error::custom)
        }
    }

    /// Ser/de `time::Date` to/from `Date`.
    pub mod date {
        use super::*;

        option!(
            Date,
            "Ser/de `Option<time::Date>` to/from `Nullable(Date)`."
        );

        const ORIGIN: Result<Date, ComponentRange> = Date::from_ordinal_date(1970, 1);

        pub fn serialize<S>(date: &Date, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let origin = ORIGIN.unwrap();
            if *date < origin {
                let msg = format!("{date} cannot be represented as Date");
                return Err(S::Error::custom(msg));
            }

            let elapsed = *date - origin; // cannot underflow: checked above
            let days = elapsed.whole_days();

            u16::try_from(days)
                .map_err(|_| S::Error::custom(format!("{date} cannot be represented as Date")))?
                .serialize(serializer)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Date, D::Error>
        where
            D: Deserializer<'de>,
        {
            let days: u16 = Deserialize::deserialize(deserializer)?;
            Ok(ORIGIN.unwrap() + Duration::days(i64::from(days))) // cannot overflow: always < `Date::MAX`
        }
    }

    /// Ser/de `time::Date` to/from `Date32`.
    pub mod date32 {
        use super::*;

        option!(
            Date,
            "Ser/de `Option<time::Date>` to/from `Nullable(Date32)`."
        );

        const ORIGIN: Result<Date, ComponentRange> = Date::from_ordinal_date(1970, 1);

        // NOTE: actually, it's 1925 and 2283 with a tail for versions before 22.8-lts.
        const MIN: Result<Date, ComponentRange> = Date::from_ordinal_date(1900, 1);
        const MAX: Result<Date, ComponentRange> = Date::from_ordinal_date(2299, 365);

        pub fn serialize<S>(date: &Date, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            if *date < MIN.unwrap() || *date > MAX.unwrap() {
                let msg = format!("{date} cannot be represented as Date");
                return Err(S::Error::custom(msg));
            }

            let elapsed = *date - ORIGIN.unwrap(); // cannot underflow: checked above
            let days = elapsed.whole_days();

            i32::try_from(days)
                .map_err(|_| S::Error::custom(format!("{date} cannot be represented as Date32")))?
                .serialize(serializer)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Date, D::Error>
        where
            D: Deserializer<'de>,
        {
            let days: i32 = Deserialize::deserialize(deserializer)?;

            // It shouldn't overflow, because clamped by CH and < `Date::MAX`.
            // TODO: ensure CH clamps when an invalid value is inserted in binary format.
            Ok(ORIGIN.unwrap() + Duration::days(i64::from(days)))
        }
    }
}

```

# src/sql/bind.rs

```rs
use std::fmt;

use sealed::sealed;
use serde::Serialize;

use super::{escape, ser};

#[sealed]
pub trait Bind {
    #[doc(hidden)]
    fn write(&self, dst: &mut impl fmt::Write) -> Result<(), String>;
}

#[sealed]
impl<S: Serialize> Bind for S {
    #[inline]
    fn write(&self, mut dst: &mut impl fmt::Write) -> Result<(), String> {
        ser::write_arg(&mut dst, self)
    }
}

/// Bound the provided string as an identifier.
/// It can be used for table names, for instance.
pub struct Identifier<'a>(pub &'a str);

#[sealed]
impl Bind for Identifier<'_> {
    #[inline]
    fn write(&self, dst: &mut impl fmt::Write) -> Result<(), String> {
        escape::identifier(self.0, dst).map_err(|err| err.to_string())
    }
}

```

# src/sql/escape.rs

```rs
use std::fmt;

// Trust clickhouse-connect https://github.com/ClickHouse/clickhouse-connect/blob/5d85563410f3ec378cb199ec51d75e033211392c/clickhouse_connect/driver/binding.py#L15

// See https://clickhouse.tech/docs/en/sql-reference/syntax/#syntax-string-literal
pub(crate) fn string(src: &str, dst: &mut impl fmt::Write) -> fmt::Result {
    dst.write_char('\'')?;
    escape(src, dst)?;
    dst.write_char('\'')
}

// See https://clickhouse.tech/docs/en/sql-reference/syntax/#syntax-identifiers
pub(crate) fn identifier(src: &str, dst: &mut impl fmt::Write) -> fmt::Result {
    dst.write_char('`')?;
    escape(src, dst)?;
    dst.write_char('`')
}

pub(crate) fn escape(src: &str, dst: &mut impl fmt::Write) -> fmt::Result {
    const REPLACE: &[char] = &['\\', '\'', '`', '\t', '\n'];
    let mut rest = src;
    while let Some(nextidx) = rest.find(REPLACE) {
        let (before, after) = rest.split_at(nextidx);
        rest = &after[1..];
        dst.write_str(before)?;
        dst.write_char('\\')?;
        dst.write_str(&after[..1])?;
    }
    dst.write_str(rest)
}

#[test]
fn it_escapes_string() {
    let mut actual = String::new();
    string(r"f\o'o '' b\'ar'", &mut actual).unwrap();
    assert_eq!(actual, r"'f\\o\'o \'\' b\\\'ar\''");
}

#[test]
fn it_escapes_identifier() {
    let mut actual = String::new();
    identifier(r"f\o`o `` b\`ar`", &mut actual).unwrap();
    assert_eq!(actual, r"`f\\o\`o \`\` b\\\`ar\``");
}

```

# src/sql/mod.rs

```rs
use std::fmt::{self, Display, Write};

use crate::{
    error::{Error, Result},
    row::{self, Row},
};

pub use bind::{Bind, Identifier};

mod bind;
pub(crate) mod escape;
pub(crate) mod ser;

#[derive(Debug, Clone)]
pub(crate) enum SqlBuilder {
    InProgress(Vec<Part>, Option<String>),
    Failed(String),
}

#[derive(Debug, Clone)]
pub(crate) enum Part {
    Arg,
    Fields,
    Text(String),
}

/// Display SQL query as string.
impl fmt::Display for SqlBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlBuilder::InProgress(parts, output_format_opt) => {
                for part in parts {
                    match part {
                        Part::Arg => f.write_char('?')?,
                        Part::Fields => f.write_str("?fields")?,
                        Part::Text(text) => f.write_str(text)?,
                    }
                }
                if let Some(output_format) = output_format_opt {
                    f.write_str(&format!(" FORMAT {output_format}"))?
                }
            }
            SqlBuilder::Failed(err) => f.write_str(err)?,
        }
        Ok(())
    }
}

impl SqlBuilder {
    pub(crate) fn new(template: &str) -> Self {
        let mut parts = Vec::new();
        let mut rest = template;
        while let Some(idx) = rest.find('?') {
            if rest[idx + 1..].starts_with('?') {
                parts.push(Part::Text(rest[..idx + 1].to_string()));
                rest = &rest[idx + 2..];
                continue;
            } else if idx != 0 {
                parts.push(Part::Text(rest[..idx].to_string()));
            }

            rest = &rest[idx + 1..];
            if let Some(restfields) = rest.strip_prefix("fields") {
                parts.push(Part::Fields);
                rest = restfields;
            } else {
                parts.push(Part::Arg);
            }
        }

        if !rest.is_empty() {
            parts.push(Part::Text(rest.to_string()));
        }

        SqlBuilder::InProgress(parts, None)
    }

    pub(crate) fn set_output_format(&mut self, format: impl Into<String>) {
        if let Self::InProgress(_, format_opt) = self {
            *format_opt = Some(format.into());
        }
    }

    pub(crate) fn bind_arg(&mut self, value: impl Bind) {
        let Self::InProgress(parts, _) = self else {
            return;
        };

        if let Some(part) = parts.iter_mut().find(|p| matches!(p, Part::Arg)) {
            let mut s = String::new();

            if let Err(err) = value.write(&mut s) {
                return self.error(format_args!("invalid argument: {err}"));
            }

            *part = Part::Text(s);
        } else {
            self.error("unexpected bind(), all arguments are already bound");
        }
    }

    pub(crate) fn bind_fields<T: Row>(&mut self) {
        let Self::InProgress(parts, _) = self else {
            return;
        };

        if let Some(fields) = row::join_column_names::<T>() {
            for part in parts.iter_mut().filter(|p| matches!(p, Part::Fields)) {
                *part = Part::Text(fields.clone());
            }
        } else if parts.iter().any(|p| matches!(p, Part::Fields)) {
            self.error("argument ?fields cannot be used with non-struct row types");
        }
    }

    pub(crate) fn finish(mut self) -> Result<String> {
        let mut sql = String::new();

        if let Self::InProgress(parts, _) = &self {
            for part in parts {
                match part {
                    Part::Text(text) => sql.push_str(text),
                    Part::Arg => {
                        self.error("unbound query argument");
                        break;
                    }
                    Part::Fields => {
                        self.error("unbound query argument ?fields");
                        break;
                    }
                }
            }
        }

        match self {
            Self::InProgress(_, output_format_opt) => {
                if let Some(output_format) = output_format_opt {
                    sql.push_str(&format!(" FORMAT {output_format}"))
                }
                Ok(sql)
            }
            Self::Failed(err) => Err(Error::InvalidParams(err.into())),
        }
    }

    fn error(&mut self, err: impl Display) {
        *self = Self::Failed(format!("invalid SQL: {err}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // XXX: need for `derive(Row)`. Provide `row(crate = ..)` instead.
    use crate as clickhouse;
    use clickhouse_derive::Row;

    #[allow(unused)]
    #[derive(Row)]
    struct Row {
        a: u32,
        b: u32,
    }

    #[allow(unused)]
    #[derive(Row)]
    struct Unnamed(u32, u32);

    #[test]
    fn bound_args() {
        let mut sql = SqlBuilder::new("SELECT ?fields FROM test WHERE a = ? AND b < ?");
        assert_eq!(
            sql.to_string(),
            "SELECT ?fields FROM test WHERE a = ? AND b < ?"
        );

        sql.bind_arg("foo");
        assert_eq!(
            sql.to_string(),
            "SELECT ?fields FROM test WHERE a = 'foo' AND b < ?"
        );

        sql.bind_arg(42);
        assert_eq!(
            sql.to_string(),
            "SELECT ?fields FROM test WHERE a = 'foo' AND b < 42"
        );

        sql.bind_fields::<Row>();
        assert_eq!(
            sql.to_string(),
            "SELECT `a`,`b` FROM test WHERE a = 'foo' AND b < 42"
        );

        assert_eq!(
            sql.finish().unwrap(),
            r"SELECT `a`,`b` FROM test WHERE a = 'foo' AND b < 42"
        );
    }

    #[test]
    fn in_clause() {
        fn t(arg: &[&str], expected: &str) {
            let mut sql = SqlBuilder::new("SELECT ?fields FROM test WHERE a IN ?");
            sql.bind_arg(arg);
            sql.bind_fields::<Row>();
            assert_eq!(sql.finish().unwrap(), expected);
        }

        const ARGS: &[&str] = &["bar", "baz", "foobar"];
        t(&ARGS[..0], r"SELECT `a`,`b` FROM test WHERE a IN []");
        t(&ARGS[..1], r"SELECT `a`,`b` FROM test WHERE a IN ['bar']");
        t(
            &ARGS[..2],
            r"SELECT `a`,`b` FROM test WHERE a IN ['bar','baz']",
        );
        t(
            ARGS,
            r"SELECT `a`,`b` FROM test WHERE a IN ['bar','baz','foobar']",
        );
    }

    // See #18.
    #[test]
    fn question_marks_inside() {
        let mut sql = SqlBuilder::new("SELECT 1 FROM test WHERE a IN ? AND b = ?");
        sql.bind_arg(&["a?b", "c?"][..]);
        sql.bind_arg("a?");
        assert_eq!(
            sql.finish().unwrap(),
            r"SELECT 1 FROM test WHERE a IN ['a?b','c?'] AND b = 'a?'"
        );
    }

    #[test]
    fn question_escape() {
        let sql = SqlBuilder::new("SELECT 1 FROM test WHERE a IN 'a??b'");
        assert_eq!(
            sql.finish().unwrap(),
            r"SELECT 1 FROM test WHERE a IN 'a?b'"
        );
    }

    #[test]
    fn option_as_null() {
        let mut sql = SqlBuilder::new("SELECT 1 FROM test WHERE a = ?");
        sql.bind_arg(None::<u32>);
        assert_eq!(sql.finish().unwrap(), r"SELECT 1 FROM test WHERE a = NULL");
    }

    #[test]
    fn option_as_value() {
        let mut sql = SqlBuilder::new("SELECT 1 FROM test WHERE a = ?");
        sql.bind_arg(Some(1u32));
        assert_eq!(sql.finish().unwrap(), r"SELECT 1 FROM test WHERE a = 1");
    }

    #[test]
    fn failures() {
        let mut sql = SqlBuilder::new("SELECT 1");
        sql.bind_arg(42);
        let err = sql.finish().unwrap_err();
        assert!(err.to_string().contains("all arguments are already bound"));

        let mut sql = SqlBuilder::new("SELECT ?fields");
        sql.bind_fields::<Unnamed>();
        let err = sql.finish().unwrap_err();
        assert!(err
            .to_string()
            .contains("argument ?fields cannot be used with non-struct row types"));

        let mut sql = SqlBuilder::new("SELECT a FROM test WHERE b = ? AND c = ?");
        sql.bind_arg(42);
        let err = sql.finish().unwrap_err();
        assert!(err.to_string().contains("unbound query argument"));

        let mut sql = SqlBuilder::new("SELECT ?fields FROM test WHERE b = ?");
        sql.bind_arg(42);
        let err = sql.finish().unwrap_err();
        assert!(err.to_string().contains("unbound query argument ?fields"));
    }
}

```

# src/sql/ser.rs

```rs
use std::fmt::{self, Write};

use serde::{
    ser::{self, SerializeSeq, SerializeTuple, Serializer},
    Serialize,
};
use thiserror::Error;

use super::escape;

// === SerializerError ===

#[derive(Debug, Error)]
enum SerializerError {
    #[error("{0} is unsupported")]
    Unsupported(&'static str),
    #[error("{0}")]
    Custom(String),
}

impl ser::Error for SerializerError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl From<fmt::Error> for SerializerError {
    fn from(err: fmt::Error) -> Self {
        Self::Custom(err.to_string())
    }
}

// === SqlSerializer ===

type Result<T = (), E = SerializerError> = std::result::Result<T, E>;
type Impossible = ser::Impossible<(), SerializerError>;

struct SqlSerializer<'a, W> {
    writer: &'a mut W,
}

macro_rules! unsupported {
    ($ser_method:ident($ty:ty) -> $ret:ty, $($other:tt)*) => {
        #[inline]
        fn $ser_method(self, _v: $ty) -> $ret {
            Err(SerializerError::Unsupported(stringify!($ser_method)))
        }
        unsupported!($($other)*);
    };
    ($ser_method:ident($ty:ty), $($other:tt)*) => {
        unsupported!($ser_method($ty) -> Result, $($other)*);
    };
    ($ser_method:ident, $($other:tt)*) => {
        #[inline]
        fn $ser_method(self) -> Result {
            Err(SerializerError::Unsupported(stringify!($ser_method)))
        }
        unsupported!($($other)*);
    };
    () => {};
}

macro_rules! forward_to_display {
    ($ser_method:ident($ty:ty), $($other:tt)*) => {
        #[inline]
        fn $ser_method(self, v: $ty) -> Result {
            write!(self.writer, "{}", &v)?;
            Ok(())
        }
        forward_to_display!($($other)*);
    };
    () => {};
}

impl<'a, W: Write> Serializer for SqlSerializer<'a, W> {
    type Error = SerializerError;
    type Ok = ();
    type SerializeMap = Impossible;
    type SerializeSeq = SqlListSerializer<'a, W>;
    type SerializeStruct = Impossible;
    type SerializeStructVariant = Impossible;
    type SerializeTuple = SqlListSerializer<'a, W>;
    type SerializeTupleStruct = Impossible;
    type SerializeTupleVariant = Impossible;

    unsupported!(
        serialize_map(Option<usize>) -> Result<Impossible>,
        serialize_bytes(&[u8]),
        serialize_unit,
        serialize_unit_struct(&'static str),
    );

    forward_to_display!(
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_i128(i128),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_u128(u128),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_bool(bool),
    );

    #[inline]
    fn serialize_char(self, value: char) -> Result {
        let mut tmp = [0u8; 4];
        self.serialize_str(value.encode_utf8(&mut tmp))
    }

    #[inline]
    fn serialize_str(self, value: &str) -> Result {
        escape::string(value, self.writer)?;
        Ok(())
    }

    #[inline]
    fn serialize_seq(self, _len: Option<usize>) -> Result<SqlListSerializer<'a, W>> {
        self.writer.write_char('[')?;
        Ok(SqlListSerializer {
            writer: self.writer,
            has_items: false,
            closing_char: ']',
        })
    }

    #[inline]
    fn serialize_tuple(self, _len: usize) -> Result<SqlListSerializer<'a, W>> {
        self.writer.write_char('(')?;
        Ok(SqlListSerializer {
            writer: self.writer,
            has_items: false,
            closing_char: ')',
        })
    }

    #[inline]
    fn serialize_some<T: Serialize + ?Sized>(self, _value: &T) -> Result {
        _value.serialize(self)
    }

    #[inline]
    fn serialize_none(self) -> std::result::Result<Self::Ok, Self::Error> {
        self.writer.write_str("NULL")?;
        Ok(())
    }

    #[inline]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result {
        escape::string(variant, self.writer)?;
        Ok(())
    }

    #[inline]
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result {
        value.serialize(self)
    }

    #[inline]
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result {
        Err(SerializerError::Unsupported("serialize_newtype_variant"))
    }

    #[inline]
    fn serialize_tuple_struct(self, _name: &'static str, _len: usize) -> Result<Impossible> {
        Err(SerializerError::Unsupported("serialize_tuple_struct"))
    }

    #[inline]
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Impossible> {
        Err(SerializerError::Unsupported("serialize_tuple_variant"))
    }

    #[inline]
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Err(SerializerError::Unsupported("serialize_struct"))
    }

    #[inline]
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(SerializerError::Unsupported("serialize_struct_variant"))
    }

    #[inline]
    fn is_human_readable(&self) -> bool {
        true
    }
}

// === SqlListSerializer ===

struct SqlListSerializer<'a, W> {
    writer: &'a mut W,
    has_items: bool,
    closing_char: char,
}

impl<W: Write> SerializeSeq for SqlListSerializer<'_, W> {
    type Error = SerializerError;
    type Ok = ();

    #[inline]
    fn serialize_element<T>(&mut self, value: &T) -> Result
    where
        T: Serialize + ?Sized,
    {
        if self.has_items {
            self.writer.write_char(',')?;
        }

        self.has_items = true;

        value.serialize(SqlSerializer {
            writer: self.writer,
        })
    }

    #[inline]
    fn end(self) -> Result {
        self.writer.write_char(self.closing_char)?;
        Ok(())
    }
}

impl<W: Write> SerializeTuple for SqlListSerializer<'_, W> {
    type Error = SerializerError;
    type Ok = ();

    #[inline]
    fn serialize_element<T>(&mut self, value: &T) -> Result
    where
        T: Serialize + ?Sized,
    {
        SerializeSeq::serialize_element(self, value)
    }

    #[inline]
    fn end(self) -> Result {
        SerializeSeq::end(self)
    }
}

// === ParamSerializer ===

struct ParamSerializer<'a, W> {
    writer: &'a mut W,
}

impl<'a, W: Write> Serializer for ParamSerializer<'a, W> {
    type Error = SerializerError;
    type Ok = ();
    type SerializeMap = Impossible;
    type SerializeSeq = SqlListSerializer<'a, W>;
    type SerializeStruct = Impossible;
    type SerializeStructVariant = Impossible;
    type SerializeTuple = SqlListSerializer<'a, W>;
    type SerializeTupleStruct = Impossible;
    type SerializeTupleVariant = Impossible;

    unsupported!(
        serialize_map(Option<usize>) -> Result<Impossible>,
        serialize_bytes(&[u8]),
        serialize_unit,
        serialize_unit_struct(&'static str),
    );

    forward_to_display!(
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_i128(i128),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_u128(u128),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_bool(bool),
    );

    #[inline]
    fn serialize_char(self, value: char) -> Result {
        let mut tmp = [0u8; 4];
        self.serialize_str(value.encode_utf8(&mut tmp))
    }

    #[inline]
    fn serialize_str(self, value: &str) -> Result {
        // ClickHouse expects strings in params to be unquoted until inside a nested type
        // nested types go through serialize_seq which'll quote strings
        Ok(escape::escape(value, self.writer)?)
    }

    #[inline]
    fn serialize_seq(self, _len: Option<usize>) -> Result<SqlListSerializer<'a, W>> {
        self.writer.write_char('[')?;
        Ok(SqlListSerializer {
            writer: self.writer,
            has_items: false,
            closing_char: ']',
        })
    }

    #[inline]
    fn serialize_tuple(self, _len: usize) -> Result<SqlListSerializer<'a, W>> {
        self.writer.write_char('(')?;
        Ok(SqlListSerializer {
            writer: self.writer,
            has_items: false,
            closing_char: ')',
        })
    }

    #[inline]
    fn serialize_some<T: Serialize + ?Sized>(self, _value: &T) -> Result {
        _value.serialize(self)
    }

    #[inline]
    fn serialize_none(self) -> std::result::Result<Self::Ok, Self::Error> {
        self.writer.write_str("NULL")?;
        Ok(())
    }

    #[inline]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result {
        escape::string(variant, self.writer)?;
        Ok(())
    }

    #[inline]
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result {
        value.serialize(self)
    }

    #[inline]
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result {
        Err(SerializerError::Unsupported("serialize_newtype_variant"))
    }

    #[inline]
    fn serialize_tuple_struct(self, _name: &'static str, _len: usize) -> Result<Impossible> {
        Err(SerializerError::Unsupported("serialize_tuple_struct"))
    }

    #[inline]
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Impossible> {
        Err(SerializerError::Unsupported("serialize_tuple_variant"))
    }

    #[inline]
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Err(SerializerError::Unsupported("serialize_struct"))
    }

    #[inline]
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(SerializerError::Unsupported("serialize_struct_variant"))
    }

    #[inline]
    fn is_human_readable(&self) -> bool {
        true
    }
}

// === Public API ===

pub(crate) fn write_arg(writer: &mut impl Write, value: &impl Serialize) -> Result<(), String> {
    value
        .serialize(SqlSerializer { writer })
        .map_err(|err| err.to_string())
}

pub(crate) fn write_param(writer: &mut impl Write, value: &impl Serialize) -> Result<(), String> {
    value
        .serialize(ParamSerializer { writer })
        .map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(v: impl Serialize) -> String {
        let mut out = String::new();
        write_arg(&mut out, &v).unwrap();
        out
    }

    #[test]
    fn it_writes_numeric_primitives() {
        assert_eq!(check(42), "42");
        assert_eq!(check(42.5), "42.5");
        assert_eq!(check(42u128), "42");
    }

    #[test]
    fn it_writes_chars() {
        assert_eq!(check('8'), "'8'");
        assert_eq!(check('\''), "'\\''");
        // TODO: assert_eq!(check('\n'), "'\\n'");
    }

    #[test]
    fn it_writes_strings() {
        assert_eq!(check("ab"), "'ab'");
        assert_eq!(check("a'b"), "'a\\'b'");
        // TODO: assert_eq!(check("a\nb"), "'a\\nb'");
    }

    #[test]
    fn it_writes_unit_variants() {
        #[derive(Serialize)]
        enum Enum {
            A,
        }
        assert_eq!(check(Enum::A), "'A'");
    }

    #[test]
    fn it_writes_newtypes() {
        #[derive(Serialize)]
        struct N(u32);
        #[derive(Serialize)]
        struct F(f64);

        assert_eq!(check(N(42)), "42");
        assert_eq!(check(F(42.5)), "42.5");
    }

    #[test]
    fn it_writes_arrays() {
        assert_eq!(check(&[42, 43][..]), "[42,43]");
        assert_eq!(check(vec![42, 43]), "[42,43]");
    }

    #[test]
    fn it_writes_tuples() {
        assert_eq!(check((42, 43)), "(42,43)");
    }

    #[test]
    fn it_writes_options() {
        assert_eq!(check(None::<i32>), "NULL");
        assert_eq!(check(Some(32)), "32");
        assert_eq!(check(Some(vec![42, 43])), "[42,43]");
    }

    #[test]
    fn it_fails_on_unsupported() {
        let mut out = String::new();
        assert!(write_arg(&mut out, &std::collections::HashMap::<u32, u32>::new()).is_err());
        assert!(write_arg(&mut out, &()).is_err());

        #[derive(Serialize)]
        struct Unit;
        assert!(write_arg(&mut out, &Unit).is_err());

        #[derive(Serialize)]
        struct Struct {
            a: u32,
        }
        assert!(write_arg(&mut out, &Struct { a: 42 }).is_err());

        #[derive(Serialize)]
        struct TupleStruct(u32, u32);
        assert!(write_arg(&mut out, &TupleStruct(42, 42)).is_err());

        #[derive(Serialize)]
        enum Enum {
            Newtype(u32),
            Tuple(u32, u32),
            Struct { a: u32 },
        }
        assert!(write_arg(&mut out, &Enum::Newtype(42)).is_err());
        assert!(write_arg(&mut out, &Enum::Tuple(42, 42)).is_err());
        assert!(write_arg(&mut out, &Enum::Struct { a: 42 }).is_err());
    }
}

```

# src/test/handlers.rs

```rs
use std::marker::PhantomData;

use bytes::Bytes;
use futures::channel::oneshot;
use hyper::{Request, Response, StatusCode};
use sealed::sealed;
use serde::{Deserialize, Serialize};

use super::{Handler, HandlerFn};
use crate::rowbinary;

const BUFFER_INITIAL_CAPACITY: usize = 1024;

// === Thunk ===

struct Thunk(Response<Bytes>);

#[sealed]
impl super::Handler for Thunk {
    type Control = ();

    fn make(self) -> (HandlerFn, Self::Control) {
        (Box::new(|_| self.0), ())
    }
}

// === failure ===

#[track_caller]
pub fn failure(status: StatusCode) -> impl Handler {
    let reason = status.canonical_reason().unwrap_or("<unknown status code>");

    Response::builder()
        .status(status)
        .body(Bytes::from(reason))
        .map(Thunk)
        .expect("invalid builder")
}

// === provide ===

#[track_caller]
pub fn provide<T>(rows: impl IntoIterator<Item = T>) -> impl Handler
where
    T: Serialize,
{
    let mut buffer = Vec::with_capacity(BUFFER_INITIAL_CAPACITY);
    for row in rows {
        rowbinary::serialize_into(&mut buffer, &row).expect("failed to serialize");
    }
    Thunk(Response::new(buffer.into()))
}

// === record ===

struct RecordHandler<T>(PhantomData<T>);

#[sealed]
impl<T> super::Handler for RecordHandler<T> {
    type Control = RecordControl<T>;

    #[doc(hidden)]
    fn make(self) -> (HandlerFn, Self::Control) {
        let (tx, rx) = oneshot::channel();
        let marker = PhantomData;
        let control = RecordControl { rx, marker };

        let h = Box::new(move |request: Request<Bytes>| -> Response<Bytes> {
            let body = request.into_body();
            let _ = tx.send(body);
            Response::new(<_>::default())
        });

        (h, control)
    }
}

pub struct RecordControl<T> {
    rx: oneshot::Receiver<Bytes>,
    marker: PhantomData<T>,
}

impl<T> RecordControl<T>
where
    T: for<'a> Deserialize<'a>,
{
    pub async fn collect<C>(self) -> C
    where
        C: Default + Extend<T>,
    {
        let bytes = self.rx.await.expect("query canceled");
        let slice = &mut (&bytes[..]);
        let mut result = C::default();

        while !slice.is_empty() {
            let row: T = rowbinary::deserialize_from(slice).expect("failed to deserialize");
            result.extend(std::iter::once(row));
        }

        result
    }
}

#[track_caller]
pub fn record<T>() -> impl Handler<Control = RecordControl<T>> {
    RecordHandler(PhantomData)
}

// === record_ddl ===

struct RecordDdlHandler;

#[sealed]
impl super::Handler for RecordDdlHandler {
    type Control = RecordDdlControl;

    #[doc(hidden)]
    fn make(self) -> (HandlerFn, Self::Control) {
        let (tx, rx) = oneshot::channel();
        let control = RecordDdlControl(rx);

        let h = Box::new(move |request: Request<Bytes>| -> Response<Bytes> {
            let body = request.into_body();
            let _ = tx.send(body);
            Response::new(<_>::default())
        });

        (h, control)
    }
}

pub struct RecordDdlControl(oneshot::Receiver<Bytes>);

impl RecordDdlControl {
    pub async fn query(self) -> String {
        let buffer = self.0.await.expect("query canceled");
        String::from_utf8(buffer.to_vec()).expect("query is not DDL")
    }
}

pub fn record_ddl() -> impl Handler<Control = RecordDdlControl> {
    RecordDdlHandler
}

// === watch ===

#[cfg(feature = "watch")]
#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
enum JsonRow<T> {
    Row(T),
}

#[cfg(feature = "watch")]
#[track_caller]
pub fn watch<T>(rows: impl IntoIterator<Item = (u64, T)>) -> impl Handler
where
    T: Serialize,
{
    #[derive(Serialize)]
    struct RowPayload<T> {
        _version: u64,
        #[serde(flatten)]
        data: T,
    }

    let mut buffer = Vec::with_capacity(BUFFER_INITIAL_CAPACITY);
    for (_version, data) in rows {
        let payload = RowPayload { _version, data };
        let row = JsonRow::Row(payload);
        serde_json::to_writer(&mut buffer, &row).expect("failed to serialize");
        buffer.push(b'\n');
    }

    Thunk(Response::new(Bytes::from(buffer)))
}

#[cfg(feature = "watch")]
#[track_caller]
pub fn watch_only_events(rows: impl IntoIterator<Item = u64>) -> impl Handler {
    #[derive(Serialize)]
    struct EventPayload {
        version: u64,
    }

    let mut buffer = Vec::with_capacity(BUFFER_INITIAL_CAPACITY);
    for version in rows {
        let payload = EventPayload { version };
        let row = JsonRow::Row(payload);
        serde_json::to_writer(&mut buffer, &row).expect("failed to serialize");
        buffer.push(b'\n');
    }

    Thunk(Response::new(Bytes::from(buffer)))
}

```

# src/test/mock.rs

```rs
use std::{
    collections::VecDeque,
    convert::Infallible,
    error::Error,
    net::SocketAddr,
    sync::{Arc, Mutex},
    thread,
};

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::{body::Incoming, server::conn, service, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::{net::TcpListener, task::AbortHandle};

use super::{Handler, HandlerFn};

/// A mock server for testing.
pub struct Mock {
    url: String,
    shared: Arc<Mutex<Shared>>,
    non_exhaustive: bool,
    server_handle: AbortHandle,
}

/// Shared between the server and the test.
#[derive(Default)]
struct Shared {
    handlers: VecDeque<HandlerFn>,
    /// An error from the background server task.
    /// Propagated as a panic in test cases.
    error: Option<Box<dyn Error + Send + Sync>>,
}

impl Mock {
    /// Starts a new test server and returns a handle to it.
    #[track_caller]
    pub fn new() -> Self {
        let (addr, listener) = {
            let addr = SocketAddr::from(([127, 0, 0, 1], 0));
            let listener = std::net::TcpListener::bind(addr).expect("cannot bind a listener");
            listener
                .set_nonblocking(true)
                .expect("cannot set non-blocking mode");
            let addr = listener.local_addr().expect("cannot get a local address");
            let listener = TcpListener::from_std(listener).expect("cannot convert to tokio");
            (addr, listener)
        };

        let shared = Arc::new(Mutex::new(Shared::default()));
        let server_handle = tokio::spawn(server(listener, shared.clone()));

        Self {
            url: format!("http://{addr}"),
            shared,
            non_exhaustive: false,
            server_handle: server_handle.abort_handle(),
        }
    }

    /// Returns a test server's URL to provide into [`Client`].
    ///
    /// [`Client`]: crate::Client::with_url
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Adds a handler to the test server for the next request.
    ///
    /// Can be called multiple times to enqueue multiple handlers.
    ///
    /// If [`Mock::non_exhaustive()`] is not called, the destructor will panic
    /// if not all handlers are called by the end of the test.
    #[track_caller]
    pub fn add<H: Handler>(&self, handler: H) -> H::Control {
        self.propagate_server_error();

        if self.server_handle.is_finished() {
            panic!("impossible to add a handler: the test server is terminated");
        }

        let (handler, control) = handler.make();
        self.shared.lock().unwrap().handlers.push_back(handler);
        control
    }

    /// Allows unused handlers to be left after the test ends.
    pub fn non_exhaustive(&mut self) {
        self.non_exhaustive = true;
    }

    #[track_caller]
    fn propagate_server_error(&self) {
        if let Some(error) = &self.shared.lock().unwrap().error {
            panic!("server error: {error}");
        }
    }
}

impl Default for Mock {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.server_handle.abort();

        if thread::panicking() {
            return;
        }

        self.propagate_server_error();

        if !self.non_exhaustive && !self.shared.lock().unwrap().handlers.is_empty() {
            panic!("test ended, but not all responses have been consumed");
        }
    }
}

async fn server(listener: TcpListener, shared: Arc<Mutex<Shared>>) {
    let error = loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(err) => break err.into(),
        };

        let serving = conn::http1::Builder::new()
            // N.B.: We set no timeouts here because it works incorrectly with
            // advanced time via `tokio::time::advance(duration)`.
            .keep_alive(false)
            .serve_connection(
                TokioIo::new(stream),
                service::service_fn(|request| handle(request, &shared)),
            );

        if let Err(err) = serving.await {
            break err.into();
        }
    };

    shared.lock().unwrap().error.get_or_insert(error);
}

async fn handle(
    request: Request<Incoming>,
    shared: &Mutex<Shared>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let response = do_handle(request, shared).await.unwrap_or_else(|err| {
        let bytes = Bytes::from(err.to_string());

        // Prevents further usage of the mock.
        shared.lock().unwrap().error.get_or_insert(err);

        Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(Full::new(bytes))
            .unwrap()
    });

    Ok(response)
}

async fn do_handle(
    request: Request<Incoming>,
    shared: &Mutex<Shared>,
) -> Result<Response<Full<Bytes>>, Box<dyn Error + Send + Sync>> {
    let Some(handler) = shared.lock().unwrap().handlers.pop_front() else {
        // TODO: provide better error, e.g. some part of parsed body.
        return Err(format!("no installed handler for an incoming request: {request:?}").into());
    };

    let (parts, body) = request.into_parts();
    let body = body.collect().await?.to_bytes();

    let request = Request::from_parts(parts, body);
    let response = handler(request).map(Full::new);

    Ok(response)
}

```

# src/test/mod.rs

```rs
use bytes::Bytes;
use hyper::{Request, Response, StatusCode};
use sealed::sealed;

pub use self::mock::Mock;

pub mod handlers;
mod mock;

#[sealed]
pub trait Handler {
    type Control;

    #[doc(hidden)]
    fn make(self) -> (HandlerFn, Self::Control);
}

type HandlerFn = Box<dyn FnOnce(Request<Bytes>) -> Response<Bytes> + Send>;

// List: https://github.com/ClickHouse/ClickHouse/blob/495c6e03aa9437dac3cd7a44ab3923390bef9982/src/Server/HTTPHandler.cpp#L132
pub mod status {
    use super::*;

    pub const UNAUTHORIZED: StatusCode = StatusCode::UNAUTHORIZED;
    pub const FORBIDDEN: StatusCode = StatusCode::FORBIDDEN;
    pub const BAD_REQUEST: StatusCode = StatusCode::BAD_REQUEST;
    pub const NOT_FOUND: StatusCode = StatusCode::NOT_FOUND;
    pub const PAYLOAD_TOO_LARGE: StatusCode = StatusCode::PAYLOAD_TOO_LARGE;
    pub const NOT_IMPLEMENTED: StatusCode = StatusCode::NOT_IMPLEMENTED;
    pub const SERVICE_UNAVAILABLE: StatusCode = StatusCode::SERVICE_UNAVAILABLE;
    pub const LENGTH_REQUIRED: StatusCode = StatusCode::LENGTH_REQUIRED;
    pub const INTERNAL_SERVER_ERROR: StatusCode = StatusCode::INTERNAL_SERVER_ERROR;
}

```

# src/ticks.rs

```rs
use tokio::time::Duration;

const PERIOD_THRESHOLD: Duration = Duration::from_secs(365 * 24 * 3600);

// === Instant ===

// More efficient `Instant` based on TSC.
#[cfg(not(feature = "test-util"))]
type Instant = quanta::Instant;

#[cfg(feature = "test-util")]
type Instant = tokio::time::Instant;

// === Ticks ===

pub(crate) struct Ticks {
    period: Duration,
    max_bias: f64,
    origin: Instant,
    next_at: Option<Instant>,
}

impl Default for Ticks {
    fn default() -> Self {
        Self {
            period: Duration::MAX,
            max_bias: 0.,
            origin: Instant::now(),
            next_at: None,
        }
    }
}

impl Ticks {
    pub(crate) fn set_period(&mut self, period: Option<Duration>) {
        self.period = period.unwrap_or(Duration::MAX);
    }

    pub(crate) fn set_period_bias(&mut self, max_bias: f64) {
        self.max_bias = max_bias.clamp(0., 1.);
    }

    pub(crate) fn time_left(&self) -> Option<Duration> {
        self.next_at
            .map(|n| n.saturating_duration_since(Instant::now()))
    }

    pub(crate) fn reached(&self) -> bool {
        self.next_at.is_some_and(|n| Instant::now() >= n)
    }

    pub(crate) fn reschedule(&mut self) {
        self.next_at = self.calc_next_at();
    }

    fn calc_next_at(&mut self) -> Option<Instant> {
        // Disabled ticks, do nothing.
        if self.period >= PERIOD_THRESHOLD {
            return None;
        }

        let now = Instant::now();
        let elapsed = now - self.origin;

        let coef = (elapsed.subsec_nanos() & 0xffff) as f64 / 65535.;
        let max_bias = self.period.mul_f64(self.max_bias);
        let bias = max_bias.mul_f64(coef);
        let n = elapsed.as_nanos().checked_div(self.period.as_nanos())?;

        let next_at = self.origin + self.period * (n + 1) as u32 + 2 * bias - max_bias;

        // Special case if after skipping we hit biased zone.
        if next_at <= now {
            next_at.checked_add(self.period)
        } else {
            Some(next_at)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "test-util")] // only with `tokio::time::Instant`
    #[tokio::test(start_paused = true)]
    async fn smoke() {
        // No bias.
        let mut ticks = Ticks::default();
        ticks.set_period(Some(Duration::from_secs(10)));
        ticks.reschedule();

        assert_eq!(ticks.time_left(), Some(Duration::from_secs(10)));
        assert!(!ticks.reached());
        tokio::time::advance(Duration::from_secs(3)).await;
        ticks.reschedule();
        assert_eq!(ticks.time_left(), Some(Duration::from_secs(7)));
        assert!(!ticks.reached());
        tokio::time::advance(Duration::from_secs(7)).await;
        assert!(ticks.reached());
        ticks.reschedule();
        assert_eq!(ticks.time_left(), Some(Duration::from_secs(10)));
        assert!(!ticks.reached());

        // Up to 10% bias.
        ticks.set_period_bias(0.1);
        ticks.reschedule();
        assert_eq!(ticks.time_left(), Some(Duration::from_secs(9)));
        assert!(!ticks.reached());
        tokio::time::advance(Duration::from_secs(12)).await;
        assert!(ticks.reached());
        ticks.reschedule();
        assert_eq!(ticks.time_left(), Some(Duration::from_secs(7)));
        assert!(!ticks.reached());

        // Try other seeds.
        tokio::time::advance(Duration::from_nanos(32768)).await;
        ticks.reschedule();
        assert_eq!(
            ticks.time_left(),
            Some(Duration::from_secs_f64(7.999982492))
        );

        tokio::time::advance(Duration::from_nanos(32767)).await;
        ticks.reschedule();
        assert_eq!(
            ticks.time_left(),
            Some(Duration::from_secs_f64(8.999934465))
        );
    }

    #[cfg(feature = "test-util")] // only with `tokio::time::Instant`
    #[tokio::test(start_paused = true)]
    async fn skip_extra_ticks() {
        let mut ticks = Ticks::default();
        ticks.set_period(Some(Duration::from_secs(10)));
        ticks.set_period_bias(0.1);
        ticks.reschedule();

        // Trivial case, just skip several ticks.
        assert_eq!(ticks.time_left(), Some(Duration::from_secs(9)));
        assert!(!ticks.reached());
        tokio::time::advance(Duration::from_secs(30)).await;
        assert!(ticks.reached());
        ticks.reschedule();
        assert_eq!(ticks.time_left(), Some(Duration::from_secs(9)));
        assert!(!ticks.reached());

        // Hit biased zone.
        tokio::time::advance(Duration::from_secs(19)).await;
        assert!(ticks.reached());
        ticks.reschedule();
        assert_eq!(ticks.time_left(), Some(Duration::from_secs(10)));
        assert!(!ticks.reached());
    }

    #[tokio::test]
    async fn disabled() {
        let mut ticks = Ticks::default();
        assert_eq!(ticks.time_left(), None);
        assert!(!ticks.reached());
        ticks.reschedule();
        assert_eq!(ticks.time_left(), None);
        assert!(!ticks.reached());

        // Not disabled.
        ticks.set_period(Some(Duration::from_secs(10)));
        ticks.reschedule();
        assert!(ticks.time_left().unwrap() < Duration::from_secs(10));
        assert!(!ticks.reached());

        // Explicitly.
        ticks.set_period(None);
        ticks.reschedule();
        assert_eq!(ticks.time_left(), None);
        assert!(!ticks.reached());

        // Zero duration.
        ticks.set_period(Some(Duration::from_secs(0)));
        ticks.reschedule();
        assert_eq!(ticks.time_left(), None);
        assert!(!ticks.reached());

        // Too big duration.
        ticks.set_period(Some(PERIOD_THRESHOLD));
        ticks.reschedule();
        assert_eq!(ticks.time_left(), None);
        assert!(!ticks.reached());
    }
}

```

# src/watch.rs

```rs
use std::{fmt::Write, time::Duration};

use serde::Deserialize;
use sha1::{Digest, Sha1};

use crate::{
    cursors::JsonCursor,
    error::{Error, Result},
    row::Row,
    sql::{Bind, SqlBuilder},
    Client, Compression,
};

#[must_use]
pub struct Watch<V = Rows> {
    client: Client,
    sql: SqlBuilder,
    refresh: Option<Duration>,
    limit: Option<usize>,
    _kind: V,
}

pub struct Rows;
pub struct Events;

impl<V> Watch<V> {
    /// See [`Query::bind()`] for details.
    ///
    /// [`Query::bind()`]: crate::query::Query::bind
    #[track_caller]
    pub fn bind(mut self, value: impl Bind) -> Self {
        self.sql.bind_arg(value);
        self
    }

    /// Limits the number of updates after initial one.
    pub fn limit(mut self, limit: impl Into<Option<usize>>) -> Self {
        self.limit = limit.into();
        self
    }

    /// See [docs](https://clickhouse.com/docs/en/sql-reference/statements/create/view#with-refresh-clause).
    ///
    /// Makes sense only for SQL queries (`client.watch("SELECT X")`).
    pub fn refresh(mut self, interval: impl Into<Option<Duration>>) -> Self {
        self.refresh = interval.into();
        self
    }

    // TODO: `groups()` for `(Version, &[T])`.

    fn cursor<T: Row>(mut self, only_events: bool) -> Result<CursorWithInit<T>> {
        self.sql.bind_fields::<T>();
        let sql = self.sql.finish()?;
        let (sql, view) = if is_table_name(&sql) {
            (None, sql)
        } else {
            let view = make_live_view_name(&sql);
            (Some(sql), view)
        };

        let params = WatchParams {
            sql,
            view,
            refresh: self.refresh,
            limit: self.limit,
            only_events,
        };

        Ok(CursorWithInit::Preparing(self.client, params))
    }
}

impl Watch<Rows> {
    pub(crate) fn new(client: &Client, template: &str) -> Self {
        let client = client
            .clone()
            // TODO: check again.
            // It seems `WATCH` and compression are incompatible.
            .with_compression(Compression::None)
            .with_option("max_execution_time", "0")
            .with_option("allow_experimental_live_view", "1")
            .with_option("output_format_json_quote_64bit_integers", "0");

        Self {
            client,
            sql: SqlBuilder::new(template),
            refresh: None,
            limit: None,
            _kind: Rows,
        }
    }

    pub fn only_events(self) -> Watch<Events> {
        Watch {
            client: self.client,
            sql: self.sql,
            refresh: self.refresh,
            limit: self.limit,
            _kind: Events,
        }
    }

    /// # Panics
    /// Panics if `T` are rows without specified names.
    /// Only structs are supported in this API.
    #[track_caller]
    pub fn fetch<T: Row>(self) -> Result<RowCursor<T>> {
        assert!(
            !T::COLUMN_NAMES.is_empty(),
            "only structs are supported in the watch API"
        );

        Ok(RowCursor(self.cursor(false)?))
    }

    pub async fn fetch_one<T>(self) -> Result<(Version, T)>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        match self.limit(1).fetch()?.next().await {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(Error::RowNotFound),
            Err(err) => Err(err),
        }
    }
}

impl Watch<Events> {
    pub fn fetch(self) -> Result<EventCursor> {
        Ok(EventCursor(self.cursor(true)?))
    }

    pub async fn fetch_one(self) -> Result<Version> {
        match self.limit(1).fetch()?.next().await {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(Error::RowNotFound),
            Err(err) => Err(err),
        }
    }
}

pub type Version = u64; // TODO: NonZeroU64

// === EventCursor ===

/// A cursor that emits only versions.
pub struct EventCursor(CursorWithInit<EventPayload>);

#[derive(Deserialize)]
struct EventPayload {
    version: Version,
}

impl Row for EventPayload {
    const COLUMN_NAMES: &'static [&'static str] = &[];
}

impl EventCursor {
    /// Emits the next version.
    ///
    /// An result is unspecified if it's called after `Err` is returned.
    pub async fn next(&mut self) -> Result<Option<Version>> {
        Ok(self.0.next().await?.map(|payload| payload.version))
    }
}

// === RowCursor ===

/// A cursor that emits `(Version, T)`.
pub struct RowCursor<T>(CursorWithInit<RowPayload<T>>);

#[derive(Deserialize)]
struct RowPayload<T> {
    _version: Version,
    #[serde(flatten)]
    data: T,
}

impl<T: Row> Row for RowPayload<T> {
    const COLUMN_NAMES: &'static [&'static str] = T::COLUMN_NAMES;
}

impl<T> RowCursor<T> {
    /// Emits the next row.
    ///
    /// An result is unspecified if it's called after `Err` is returned.
    pub async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<(Version, T)>>
    where
        T: Deserialize<'b> + Row,
    {
        Ok(self
            .0
            .next()
            .await?
            .map(|payload| (payload._version, payload.data)))
    }
}

// === CursorWithInit ===

#[allow(clippy::large_enum_variant)]
enum CursorWithInit<T> {
    Preparing(Client, WatchParams),
    Fetching(JsonCursor<T>),
}

struct WatchParams {
    sql: Option<String>,
    view: String,
    refresh: Option<Duration>,
    limit: Option<usize>,
    only_events: bool,
}

impl<T> CursorWithInit<T> {
    async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<T>>
    where
        T: Deserialize<'b>,
    {
        if let Self::Preparing(client, params) = self {
            let cursor = init_cursor(client, params).await?;
            *self = Self::Fetching(cursor);
        }

        match self {
            Self::Fetching(cursor) => cursor.next().await,
            Self::Preparing(..) => unreachable!(),
        }
    }
}

#[cold]
async fn init_cursor<T>(client: &Client, params: &WatchParams) -> Result<JsonCursor<T>> {
    if let Some(sql) = &params.sql {
        let refresh_sql = params
            .refresh
            .map_or_else(String::new, |d| format!(" REFRESH {}", d.as_secs()));

        let create_sql = format!(
            "CREATE LIVE VIEW IF NOT EXISTS {}{} AS {}",
            params.view, refresh_sql, sql
        );

        client.query(&create_sql).execute().await?;
    }

    let events = if params.only_events { " EVENTS" } else { "" };
    let mut watch_sql = format!("WATCH {}{}", params.view, events);

    if let Some(limit) = params.limit {
        let _ = write!(&mut watch_sql, " LIMIT {limit}");
    }

    watch_sql.push_str(" FORMAT JSONEachRowWithProgress");

    let response = client.query(&watch_sql).do_execute(true)?;
    Ok(JsonCursor::new(response))
}

fn is_table_name(sql: &str) -> bool {
    // TODO: support quoted identifiers.
    sql.split_ascii_whitespace().take(2).count() == 1
}

fn make_live_view_name(sql: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(sql.as_bytes());
    let result = hasher.finalize();

    let mut name = String::with_capacity(40);
    for word in &result[..] {
        let _ = write!(&mut name, "{word:02x}");
    }

    format!("lv_{name}")
}

#[test]
fn it_makes_live_view_name() {
    let a = make_live_view_name("SELECT 1");
    let b = make_live_view_name("SELECT 2");

    assert_ne!(a, b);
    assert_eq!(a.len(), 3 + 40);
    assert_eq!(b.len(), 3 + 40);
}

```

# tests/it/chrono.rs

```rs
#![cfg(feature = "chrono")]

use std::ops::RangeBounds;

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use rand::{
    distributions::{Distribution, Standard},
    Rng,
};
use serde::{Deserialize, Serialize};

use clickhouse::Row;

#[tokio::test]
async fn datetime() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::chrono::datetime")]
        dt: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime::option")]
        dt_opt: Option<DateTime<Utc>>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::secs")]
        dt64s: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::secs::option")]
        dt64s_opt: Option<DateTime<Utc>>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        dt64ms: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis::option")]
        dt64ms_opt: Option<DateTime<Utc>>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::micros")]
        dt64us: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::micros::option")]
        dt64us_opt: Option<DateTime<Utc>>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        dt64ns: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos::option")]
        dt64ns_opt: Option<DateTime<Utc>>,
    }

    #[derive(Debug, Deserialize, Row)]
    struct MyRowStr {
        dt: String,
        dt64s: String,
        dt64ms: String,
        dt64us: String,
        dt64ns: String,
    }

    client
        .query(
            "
            CREATE TABLE test(
                dt          DateTime,
                dt_opt      Nullable(DateTime),
                dt64s       DateTime64(0),
                dt64s_opt   Nullable(DateTime64(0)),
                dt64ms      DateTime64(3),
                dt64ms_opt  Nullable(DateTime64(3)),
                dt64us      DateTime64(6),
                dt64us_opt  Nullable(DateTime64(6)),
                dt64ns      DateTime64(9),
                dt64ns_opt  Nullable(DateTime64(9))
            )
            ENGINE = MergeTree ORDER BY dt
        ",
        )
        .execute()
        .await
        .unwrap();
    let d = NaiveDate::from_ymd_opt(2022, 11, 13).unwrap();
    let dt_s = d.and_hms_opt(15, 27, 42).unwrap().and_utc();
    let dt_ms = d.and_hms_milli_opt(15, 27, 42, 123).unwrap().and_utc();
    let dt_us = d.and_hms_micro_opt(15, 27, 42, 123456).unwrap().and_utc();
    let dt_ns = d.and_hms_nano_opt(15, 27, 42, 123456789).unwrap().and_utc();

    let original_row = MyRow {
        dt: dt_s,
        dt_opt: Some(dt_s),
        dt64s: dt_s,
        dt64s_opt: Some(dt_s),
        dt64ms: dt_ms,
        dt64ms_opt: Some(dt_ms),
        dt64us: dt_us,
        dt64us_opt: Some(dt_us),
        dt64ns: dt_ns,
        dt64ns_opt: Some(dt_ns),
    };

    let mut insert = client.insert("test").unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let row = client
        .query("SELECT ?fields FROM test")
        .fetch_one::<MyRow>()
        .await
        .unwrap();

    let row_str = client
        .query(
            "
            SELECT toString(dt),
                   toString(dt64s),
                   toString(dt64ms),
                   toString(dt64us),
                   toString(dt64ns)
              FROM test
        ",
        )
        .fetch_one::<MyRowStr>()
        .await
        .unwrap();

    assert_eq!(row, original_row);
    assert_eq!(row_str.dt, &original_row.dt.to_string()[..19]);
    assert_eq!(row_str.dt64s, &original_row.dt64s.to_string()[..19]);
    assert_eq!(row_str.dt64ms, &original_row.dt64ms.to_string()[..23]);
    assert_eq!(row_str.dt64us, &original_row.dt64us.to_string()[..26]);
    assert_eq!(row_str.dt64ns, &original_row.dt64ns.to_string()[..29]);
}

#[tokio::test]
async fn date() {
    let client = prepare_database!();

    #[derive(Debug, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::chrono::date")]
        date: NaiveDate,
        #[serde(with = "clickhouse::serde::chrono::date::option")]
        date_opt: Option<NaiveDate>,
    }

    client
        .query(
            "
            CREATE TABLE test(
                date        Date,
                date_opt    Nullable(Date)
            ) ENGINE = MergeTree ORDER BY date
        ",
        )
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert("test").unwrap();

    let dates = generate_dates(1970..2149, 100);
    for &date in &dates {
        let original_row = MyRow {
            date,
            date_opt: Some(date),
        };

        insert.write(&original_row).await.unwrap();
    }
    insert.end().await.unwrap();

    let actual = client
        .query("SELECT ?fields, toString(date) FROM test ORDER BY date")
        .fetch_all::<(MyRow, String)>()
        .await
        .unwrap();

    assert_eq!(actual.len(), dates.len());

    for ((row, date_str), expected) in actual.iter().zip(dates) {
        assert_eq!(row.date, expected);
        assert_eq!(row.date_opt, Some(expected));
        assert_eq!(date_str, &expected.to_string());
    }
}

#[tokio::test]
async fn date32() {
    let client = prepare_database!();

    #[derive(Debug, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::chrono::date32")]
        date: NaiveDate,
        #[serde(with = "clickhouse::serde::chrono::date32::option")]
        date_opt: Option<NaiveDate>,
    }

    client
        .query(
            "
            CREATE TABLE test(
                date        Date32,
                date_opt    Nullable(Date32)
            ) ENGINE = MergeTree ORDER BY date
        ",
        )
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert("test").unwrap();

    let dates = generate_dates(1925..2283, 100); // TODO: 1900..=2299 for newer versions.
    for &date in &dates {
        let original_row = MyRow {
            date,
            date_opt: Some(date),
        };

        insert.write(&original_row).await.unwrap();
    }
    insert.end().await.unwrap();

    let actual = client
        .query("SELECT ?fields, toString(date) FROM test ORDER BY date")
        .fetch_all::<(MyRow, String)>()
        .await
        .unwrap();

    assert_eq!(actual.len(), dates.len());

    for ((row, date_str), expected) in actual.iter().zip(dates) {
        assert_eq!(row.date, expected);
        assert_eq!(row.date_opt, Some(expected));
        assert_eq!(date_str, &expected.to_string());
    }
}

// Distribution isn't implemented for `chrono` types, but we can lift the implementation from the `time` crate: https://docs.rs/time/latest/src/time/rand.rs.html#14-20
struct NaiveDateWrapper(NaiveDate);

impl Distribution<NaiveDateWrapper> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> NaiveDateWrapper {
        NaiveDateWrapper(
            NaiveDate::from_num_days_from_ce_opt(
                rng.gen_range(
                    NaiveDate::MIN.num_days_from_ce()..=NaiveDate::MAX.num_days_from_ce(),
                ),
            )
            .unwrap(),
        )
    }
}

fn generate_dates(years: impl RangeBounds<i32>, count: usize) -> Vec<NaiveDate> {
    let mut rng = rand::thread_rng();
    let mut dates: Vec<_> = (&mut rng)
        .sample_iter(Standard)
        .filter_map(|date: NaiveDateWrapper| {
            if years.contains(&date.0.year()) {
                Some(date.0)
            } else {
                None
            }
        })
        .take(count)
        .collect();

    dates.sort_unstable();
    dates
}

```

# tests/it/compression.rs

```rs
use clickhouse::{Client, Compression};

use crate::{create_simple_table, SimpleRow};

async fn check(client: Client) {
    create_simple_table(&client, "test").await;

    let mut insert = client.insert("test").unwrap();
    for i in 0..200_000 {
        insert.write(&SimpleRow::new(i, "foo")).await.unwrap();
    }
    insert.end().await.unwrap();

    // Check data.

    let (sum_no, sum_len) = client
        .query("SELECT sum(id), sum(length(data)) FROM test")
        .fetch_one::<(u64, u64)>()
        .await
        .unwrap();

    assert_eq!(sum_no, 19_999_900_000);
    assert_eq!(sum_len, 600_000);
}

#[tokio::test]
async fn none() {
    let client = prepare_database!().with_compression(Compression::None);
    check(client).await;
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn lz4() {
    let client = prepare_database!().with_compression(Compression::Lz4);
    check(client).await;
}

```

# tests/it/cursor_error.rs

```rs
use serde::Deserialize;

use clickhouse::{error::Error, Client, Compression, Row};

#[tokio::test]
async fn deferred() {
    let client = prepare_database!();
    max_execution_time(client, false).await;
}

#[tokio::test]
async fn wait_end_of_query() {
    let client = prepare_database!();
    max_execution_time(client, true).await;
}

async fn max_execution_time(mut client: Client, wait_end_of_query: bool) {
    if wait_end_of_query {
        client = client.with_option("wait_end_of_query", "1")
    }

    // TODO: check different `timeout_overflow_mode`
    let mut cursor = client
        .with_compression(Compression::None)
        .with_option("max_execution_time", "0.1")
        .query("SELECT toUInt8(65 + number % 5) FROM system.numbers LIMIT 100000000")
        .fetch::<u8>()
        .unwrap();

    let mut i = 0u64;

    let err = loop {
        match cursor.next().await {
            Ok(Some(no)) => {
                // Check that we haven't parsed something extra.
                assert_eq!(no, (65 + i % 5) as u8);
                i += 1;
            }
            Ok(None) => panic!("DB exception hasn't been found"),
            Err(err) => break err,
        }
    };

    assert!(wait_end_of_query ^ (i != 0));
    assert!(err.to_string().contains("TIMEOUT_EXCEEDED"));
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn deferred_lz4() {
    let client = prepare_database!().with_compression(Compression::Lz4);

    client
        .query("CREATE TABLE test(no UInt32) ENGINE = MergeTree ORDER BY no")
        .execute()
        .await
        .unwrap();

    #[derive(serde::Serialize, clickhouse::Row)]
    struct Row {
        no: u32,
    }

    let part_count = 100;
    let part_size = 100_000;

    // Due to compression we need more complex test here: write a lot of big parts.
    for i in 0..part_count {
        let mut insert = client.insert("test").unwrap();

        for j in 0..part_size {
            let row = Row {
                no: i * part_size + j,
            };

            insert.write(&row).await.unwrap();
        }

        insert.end().await.unwrap();
    }

    let mut cursor = client
        .with_option("max_execution_time", "0.1")
        .query("SELECT no FROM test")
        .fetch::<u32>()
        .unwrap();

    let mut i = 0;

    let err = loop {
        match cursor.next().await {
            Ok(Some(_)) => i += 1,
            Ok(None) => panic!("DB exception hasn't been found"),
            Err(err) => break err,
        }
    };

    assert_ne!(i, 0); // we're interested only in errors during processing
    assert!(err.to_string().contains("TIMEOUT_EXCEEDED"));
}

// See #185.
#[tokio::test]
async fn invalid_schema() {
    #[derive(Debug, Row, Deserialize)]
    #[allow(dead_code)]
    struct MyRow {
        no: u32,
        dec: Option<String>, // valid schema: u64-based types
    }

    let client = prepare_database!();

    client
        .query(
            "CREATE TABLE test(no UInt32, dec Nullable(Decimal64(4)))
             ENGINE = MergeTree
             ORDER BY no",
        )
        .execute()
        .await
        .unwrap();

    client
        .query("INSERT INTO test VALUES (1, 1.1), (2, 2.2), (3, 3.3)")
        .execute()
        .await
        .unwrap();

    let err = client
        .query("SELECT ?fields FROM test")
        .fetch_all::<MyRow>()
        .await
        .unwrap_err();

    assert!(matches!(err, Error::NotEnoughData));
}

```

# tests/it/cursor_stats.rs

```rs
use clickhouse::{Client, Compression};

use crate::{create_simple_table, SimpleRow};

async fn check(client: Client, expected_ratio: f64) {
    create_simple_table(&client, "test").await;

    let mut insert = client.insert("test").unwrap();
    for i in 0..1_000 {
        insert.write(&SimpleRow::new(i, "foobar")).await.unwrap();
    }
    insert.end().await.unwrap();

    let mut cursor = client
        .query("SELECT * FROM test")
        .fetch::<SimpleRow>()
        .unwrap();

    let mut received = cursor.received_bytes();
    let mut decoded = cursor.decoded_bytes();
    assert_eq!(received, 0);
    assert_eq!(decoded, 0);

    while cursor.next().await.unwrap().is_some() {
        assert!(cursor.received_bytes() >= received);
        assert!(cursor.decoded_bytes() >= decoded);
        received = cursor.received_bytes();
        decoded = cursor.decoded_bytes();
    }

    assert_eq!(decoded, 15000);
    assert_eq!(cursor.received_bytes(), dbg!(received));
    assert_eq!(cursor.decoded_bytes(), dbg!(decoded));
    assert_eq!(
        (decoded as f64 / received as f64 * 10.).round() / 10.,
        expected_ratio
    );
}

#[tokio::test]
async fn none() {
    let client = prepare_database!().with_compression(Compression::None);
    check(client, 1.0).await;
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn lz4() {
    let client = prepare_database!().with_compression(Compression::Lz4);
    check(client, 3.7).await;
}

```

# tests/it/fetch_bytes.rs

```rs
use clickhouse::error::Error;
use std::str::from_utf8;
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

#[tokio::test]
async fn single_chunk() {
    let client = prepare_database!();

    let mut cursor = client
        .query("SELECT number FROM system.numbers LIMIT 3")
        .fetch_bytes("CSV")
        .unwrap();

    let mut total_chunks = 0;
    let mut buffer = Vec::<u8>::new();
    while let Some(chunk) = cursor.next().await.unwrap() {
        buffer.extend(chunk);
        total_chunks += 1;
    }

    assert_eq!(from_utf8(&buffer).unwrap(), "0\n1\n2\n");
    assert_eq!(total_chunks, 1);
    assert_eq!(cursor.decoded_bytes(), 6);
}

#[tokio::test]
async fn multiple_chunks() {
    let client = prepare_database!();

    let mut cursor = client
        .query("SELECT number FROM system.numbers LIMIT 3")
        // each number will go into a separate chunk
        .with_option("max_block_size", "1")
        .fetch_bytes("CSV")
        .unwrap();

    let mut total_chunks = 0;
    let mut buffer = Vec::<u8>::new();
    while let Some(data) = cursor.next().await.unwrap() {
        buffer.extend(data);
        total_chunks += 1;
    }

    assert_eq!(from_utf8(&buffer).unwrap(), "0\n1\n2\n");
    assert_eq!(total_chunks, 3);
    assert_eq!(cursor.decoded_bytes(), 6);
}

#[tokio::test]
async fn error() {
    let client = prepare_database!();

    let mut bytes_cursor = client
        .query("SELECT sleepEachRow(0.05) AS s FROM system.numbers LIMIT 30")
        .with_option("max_block_size", "1")
        .with_option("max_execution_time", "0.01")
        .fetch_bytes("JSONEachRow")
        .unwrap();

    let err = bytes_cursor.next().await;
    println!("{:?}", err);
    assert!(matches!(err, Err(Error::BadResponse(_))));
}

#[tokio::test]
async fn lines() {
    let client = prepare_database!();
    let expected = ["0", "1", "2"];

    for n in 0..4 {
        let mut lines = client
            .query("SELECT number FROM system.numbers LIMIT {limit: Int32}")
            .param("limit", n)
            // each number will go into a separate chunk
            .with_option("max_block_size", "1")
            .fetch_bytes("CSV")
            .unwrap()
            .lines();

        let mut actual = Vec::<String>::new();
        while let Some(data) = lines.next_line().await.unwrap() {
            actual.push(data);
        }

        assert_eq!(actual, &expected[..n]);
    }
}

#[tokio::test]
async fn collect() {
    let client = prepare_database!();
    let expected = b"0\n1\n2\n3\n";

    for n in 0..4 {
        let mut cursor = client
            .query("SELECT number FROM system.numbers LIMIT {limit: Int32}")
            .param("limit", n)
            // each number will go into a separate chunk
            .with_option("max_block_size", "1")
            .fetch_bytes("CSV")
            .unwrap();

        let data = cursor.collect().await.unwrap();
        assert_eq!(&data[..], &expected[..n * 2]);

        // The cursor is fused.
        assert_eq!(&cursor.collect().await.unwrap()[..], b"");
    }
}

#[tokio::test]
async fn async_read() {
    let client = prepare_database!();
    let limit = 1000;

    let mut cursor = client
        .query("SELECT number, number FROM system.numbers LIMIT {limit: Int32}")
        .param("limit", limit)
        .with_option("max_block_size", "3")
        .fetch_bytes("CSV")
        .unwrap();

    #[allow(clippy::format_collect)]
    let expected = (0..limit)
        .map(|n| format!("{n},{n}\n"))
        .collect::<String>()
        .into_bytes();

    let mut actual = vec![0; expected.len()];
    let mut index = 0;
    while index < actual.len() {
        let step = (1 + index % 10).min(actual.len() - index);
        let buf = &mut actual[index..(index + step)];
        assert_eq!(cursor.read_exact(buf).await.unwrap(), step);
        index += step;
    }

    assert_eq!(cursor.read(&mut [0]).await.unwrap(), 0); // EOF
    assert_eq!(cursor.decoded_bytes(), expected.len() as u64);
    assert_eq!(actual, expected);
}

```

# tests/it/insert.rs

```rs
use crate::{create_simple_table, fetch_rows, flush_query_log, SimpleRow};
use clickhouse::{sql::Identifier, Client, Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct RenameRow {
    #[serde(rename = "fix_id")]
    pub(crate) fix_id: i64,
    #[serde(rename = "extComplexId")]
    pub(crate) complex_id: String,
    pub(crate) ext_float: f64,
}

async fn create_rename_table(client: &Client, table_name: &str) {
    client
        .query("CREATE TABLE ?(fixId UInt64, extComplexId String, extFloat Float64) ENGINE = MergeTree ORDER BY fixId")
        .bind(Identifier(table_name))
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn keeps_client_options() {
    let table_name = "insert_keeps_client_options";
    let query_id = uuid::Uuid::new_v4().to_string();
    let (client_setting_name, client_setting_value) = ("max_block_size", "1000");
    let (insert_setting_name, insert_setting_value) = ("async_insert", "1");

    let client = prepare_database!().with_option(client_setting_name, client_setting_value);
    create_simple_table(&client, table_name).await;

    let row = SimpleRow::new(42, "foo");

    let mut insert = client
        .insert(table_name)
        .unwrap()
        .with_option(insert_setting_name, insert_setting_value)
        .with_option("query_id", &query_id);

    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    flush_query_log(&client).await;

    let (has_insert_setting, has_client_setting) = client
        .query(&format!(
            "
            SELECT
              Settings['{insert_setting_name}'] = '{insert_setting_value}',
              Settings['{client_setting_name}'] = '{client_setting_value}'
            FROM system.query_log
            WHERE query_id = ?
            AND type = 'QueryFinish'
            AND query_kind = 'Insert'
            "
        ))
        .bind(&query_id)
        .fetch_one::<(bool, bool)>()
        .await
        .unwrap();

    assert!(
        has_insert_setting, "{}",
        format!("should contain {insert_setting_name} = {insert_setting_value} (from the insert options)")
    );
    assert!(
        has_client_setting, "{}",
        format!("should contain {client_setting_name} = {client_setting_value} (from the client options)")
    );

    let rows = fetch_rows::<SimpleRow>(&client, table_name).await;
    assert_eq!(rows, vec!(row))
}

#[tokio::test]
async fn overrides_client_options() {
    let table_name = "insert_overrides_client_options";
    let query_id = uuid::Uuid::new_v4().to_string();
    let (setting_name, setting_value, override_value) = ("async_insert", "0", "1");

    let client = prepare_database!().with_option(setting_name, setting_value);
    create_simple_table(&client, table_name).await;

    let row = SimpleRow::new(42, "foo");

    let mut insert = client
        .insert(table_name)
        .unwrap()
        .with_option(setting_name, override_value)
        .with_option("query_id", &query_id);

    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    flush_query_log(&client).await;

    let has_setting_override = client
        .query(&format!(
            "
            SELECT Settings['{setting_name}'] = '{override_value}'
            FROM system.query_log
            WHERE query_id = ?
            AND type = 'QueryFinish'
            AND query_kind = 'Insert'
            "
        ))
        .bind(&query_id)
        .fetch_one::<bool>()
        .await
        .unwrap();

    assert!(
        has_setting_override,
        "{}",
        format!("should contain {setting_name} = {override_value} (from the insert options)")
    );

    let rows = fetch_rows::<SimpleRow>(&client, table_name).await;
    assert_eq!(rows, vec!(row))
}

#[tokio::test]
async fn empty_insert() {
    // https://github.com/ClickHouse/clickhouse-rs/issues/137

    let table_name = "insert_empty";
    let query_id = uuid::Uuid::new_v4().to_string();

    let client = prepare_database!();
    create_simple_table(&client, table_name).await;

    let insert = client
        .insert::<SimpleRow>(table_name)
        .unwrap()
        .with_option("query_id", &query_id);

    insert.end().await.unwrap();

    let rows = fetch_rows::<SimpleRow>(&client, table_name).await;
    assert!(rows.is_empty())
}

#[tokio::test]
async fn rename_insert() {
    let table_name = "insert_rename";
    let query_id = uuid::Uuid::new_v4().to_string();

    let client = prepare_database!();
    create_rename_table(&client, table_name).await;

    let row = RenameRow {
        fix_id: 42,
        complex_id: String::from("foo"),
        ext_float: 0.5,
    };

    let mut insert = client
        .insert(table_name)
        .unwrap()
        .with_option("query_id", &query_id);

    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    flush_query_log(&client).await;

    let rows = fetch_rows::<RenameRow>(&client, table_name).await;
    assert_eq!(rows, vec!(row))
}

```

# tests/it/inserter.rs

```rs
#![cfg(feature = "inserter")]

use std::string::ToString;

use serde::Serialize;

use clickhouse::{inserter::Quantities, Client, Row};

use crate::{create_simple_table, fetch_rows, flush_query_log, SimpleRow};

#[derive(Debug, Row, Serialize)]
struct MyRow {
    data: String,
}

impl MyRow {
    fn new(data: impl ToString) -> Self {
        Self {
            data: data.to_string(),
        }
    }
}

async fn create_table(client: &Client) {
    client
        .query("CREATE TABLE test(data String) ENGINE = MergeTree ORDER BY data")
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn force_commit() {
    let client = prepare_database!();
    create_table(&client).await;

    let mut inserter = client.inserter("test").unwrap();
    let rows = 100;

    for i in 1..=rows {
        inserter.write(&MyRow::new(i)).unwrap();
        assert_eq!(inserter.commit().await.unwrap(), Quantities::ZERO);

        if i % 10 == 0 {
            assert_eq!(inserter.force_commit().await.unwrap().rows, 10);
        }
    }

    assert_eq!(inserter.end().await.unwrap(), Quantities::ZERO);

    let (count, sum) = client
        .query("SELECT count(), sum(toUInt64(data)) FROM test")
        .fetch_one::<(u64, u64)>()
        .await
        .unwrap();

    assert_eq!(count, rows);
    assert_eq!(sum, (1..=rows).sum::<u64>());
}

#[tokio::test]
async fn limited_by_rows() {
    let client = prepare_database!();
    create_table(&client).await;

    let mut inserter = client.inserter("test").unwrap().with_max_rows(10);
    let rows = 100;

    for i in (2..=rows).step_by(2) {
        let row = MyRow::new(i - 1);
        inserter.write(&row).unwrap();
        let row = MyRow::new(i);
        inserter.write(&row).unwrap();

        let inserted = inserter.commit().await.unwrap();
        let pending = inserter.pending();

        if i % 10 == 0 {
            assert_ne!(inserted.bytes, 0);
            assert_eq!(inserted.rows, 10);
            assert_eq!(inserted.transactions, 5);
            assert_eq!(pending, &Quantities::ZERO);
        } else {
            assert_eq!(inserted, Quantities::ZERO);
            assert_ne!(pending.bytes, 0);
            assert_eq!(pending.rows, i % 10);
            assert_eq!(pending.transactions, (i % 10) / 2);
        }
    }

    assert_eq!(inserter.end().await.unwrap(), Quantities::ZERO);

    let (count, sum) = client
        .query("SELECT count(), sum(toUInt64(data)) FROM test")
        .fetch_one::<(u64, u64)>()
        .await
        .unwrap();

    assert_eq!(count, rows);
    assert_eq!(sum, (1..=rows).sum::<u64>());
}

#[tokio::test]
async fn limited_by_bytes() {
    let client = prepare_database!();
    create_table(&client).await;

    let mut inserter = client.inserter("test").unwrap().with_max_bytes(100);
    let rows = 100;

    let row = MyRow::new("x".repeat(9));

    for i in 1..=rows {
        inserter.write(&row).unwrap();

        let inserted = inserter.commit().await.unwrap();
        let pending = inserter.pending();

        if i % 10 == 0 {
            assert_eq!(inserted.bytes, 100);
            assert_eq!(inserted.rows, 10);
            assert_eq!(inserted.transactions, 10);
            assert_eq!(pending, &Quantities::ZERO);
        } else {
            assert_eq!(inserted, Quantities::ZERO);
            assert_eq!(pending.bytes, (i % 10) * 10);
            assert_eq!(pending.rows, i % 10);
            assert_eq!(pending.transactions, i % 10);
        }
    }

    assert_eq!(inserter.end().await.unwrap(), Quantities::ZERO);

    let count = client
        .query("SELECT count() FROM test")
        .fetch_one::<u64>()
        .await
        .unwrap();

    assert_eq!(count, rows);
}

#[cfg(feature = "test-util")] // only with `tokio::time::Instant`
#[tokio::test(start_paused = true)]
async fn limited_by_time() {
    use std::time::Duration;

    let client = prepare_database!();
    create_table(&client).await;

    let period = Duration::from_secs(1);
    let mut inserter = client.inserter("test").unwrap().with_period(Some(period));
    let rows = 100;

    for i in 1..=rows {
        let row = MyRow::new(i);
        inserter.write(&row).unwrap();

        tokio::time::sleep(period / 10).await;

        let inserted = inserter.commit().await.unwrap();
        let pending = inserter.pending();

        if i % 10 == 0 {
            assert_ne!(inserted.bytes, 0);
            assert_eq!(inserted.rows, 10);
            assert_eq!(inserted.transactions, 10);
            assert_eq!(pending, &Quantities::ZERO);
        } else {
            assert_eq!(inserted, Quantities::ZERO);
            assert_ne!(pending.bytes, 0);
            assert_eq!(pending.rows, i % 10);
            assert_eq!(pending.transactions, i % 10);
        }
    }

    assert_eq!(inserter.end().await.unwrap(), Quantities::ZERO);

    let (count, sum) = client
        .query("SELECT count(), sum(toUInt64(data)) FROM test")
        .fetch_one::<(u64, u64)>()
        .await
        .unwrap();

    assert_eq!(count, rows);
    assert_eq!(sum, (1..=rows).sum::<u64>());
}

/// Similar to [`crate::insert::keeps_client_options`] with minor differences.
#[tokio::test]
async fn keeps_client_options() {
    let table_name = "inserter_keeps_client_options";
    let query_id = uuid::Uuid::new_v4().to_string();
    let (client_setting_name, client_setting_value) = ("max_block_size", "1000");
    let (insert_setting_name, insert_setting_value) = ("async_insert", "1");

    let client = prepare_database!().with_option(client_setting_name, client_setting_value);
    create_simple_table(&client, table_name).await;

    let row = SimpleRow::new(42, "foo");

    let mut inserter = client
        .inserter(table_name)
        .unwrap()
        .with_option("async_insert", "1")
        .with_option("query_id", &query_id);

    inserter.write(&row).unwrap();
    inserter.end().await.unwrap();

    flush_query_log(&client).await;

    let (has_insert_setting, has_client_setting) = client
        .query(&format!(
            "
            SELECT
              Settings['{insert_setting_name}'] = '{insert_setting_value}',
              Settings['{client_setting_name}'] = '{client_setting_value}'
            FROM system.query_log
            WHERE query_id = ?
            AND type = 'QueryFinish'
            AND query_kind = 'Insert'
            "
        ))
        .bind(&query_id)
        .fetch_one::<(bool, bool)>()
        .await
        .unwrap();

    assert!(
        has_insert_setting, "{}",
        format!("should contain {insert_setting_name} = {insert_setting_value} (from the insert options)")
    );
    assert!(
        has_client_setting, "{}",
        format!("should contain {client_setting_name} = {client_setting_value} (from the client options)")
    );

    let rows = fetch_rows::<SimpleRow>(&client, table_name).await;
    assert_eq!(rows, vec!(row))
}

/// Similar to [`crate::insert::overrides_client_options`] with minor differences.
#[tokio::test]
async fn overrides_client_options() {
    let table_name = "inserter_overrides_client_options";
    let query_id = uuid::Uuid::new_v4().to_string();
    let (setting_name, setting_value, override_value) = ("async_insert", "0", "1");

    let client = prepare_database!().with_option(setting_name, setting_value);
    create_simple_table(&client, table_name).await;

    let row = SimpleRow::new(42, "foo");

    let mut inserter = client
        .inserter(table_name)
        .unwrap()
        .with_option("async_insert", override_value)
        .with_option("query_id", &query_id);

    inserter.write(&row).unwrap();
    inserter.end().await.unwrap();

    flush_query_log(&client).await;

    let has_setting_override = client
        .query(&format!(
            "
            SELECT Settings['{setting_name}'] = '{override_value}'
            FROM system.query_log
            WHERE query_id = ?
            AND type = 'QueryFinish'
            AND query_kind = 'Insert'
            "
        ))
        .bind(&query_id)
        .fetch_one::<bool>()
        .await
        .unwrap();

    assert!(
        has_setting_override,
        "{}",
        format!("should contain {setting_name} = {override_value} (from the inserter options)")
    );

    let rows = fetch_rows::<SimpleRow>(&client, table_name).await;
    assert_eq!(rows, vec!(row))
}

```

# tests/it/ip.rs

```rs
use std::net::{Ipv4Addr, Ipv6Addr};

use serde::{Deserialize, Serialize};

use clickhouse::Row;

#[tokio::test]
async fn smoke() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::ipv4")]
        ipv4: Ipv4Addr,
        ipv6: Ipv6Addr, // requires no annotations.
        #[serde(with = "clickhouse::serde::ipv4::option")]
        ipv4_opt: Option<Ipv4Addr>,
        ipv6_opt: Option<Ipv6Addr>, // requires no annotations.
    }

    client
        .query(
            "
            CREATE TABLE test(
                ipv4 IPv4,
                ipv6 IPv6,
                ipv4_opt Nullable(IPv4),
                ipv6_opt Nullable(IPv6),
            ) ENGINE = MergeTree ORDER BY ipv4
        ",
        )
        .execute()
        .await
        .unwrap();

    let original_row = MyRow {
        ipv4: Ipv4Addr::new(192, 168, 0, 1),
        ipv6: Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0xafc8, 0x10, 0x1),
        ipv4_opt: Some(Ipv4Addr::new(192, 168, 0, 1)),
        ipv6_opt: Some(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0xafc8, 0x10, 0x1)),
    };

    let mut insert = client.insert("test").unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let (row, row_ipv4_str, row_ipv6_str) = client
        .query("SELECT ?fields, toString(ipv4), toString(ipv6) FROM test")
        .fetch_one::<(MyRow, String, String)>()
        .await
        .unwrap();

    assert_eq!(row, original_row);
    assert_eq!(row_ipv4_str, original_row.ipv4.to_string());
    assert_eq!(row_ipv6_str, original_row.ipv6.to_string());
}

```

# tests/it/main.rs

```rs
use clickhouse::{sql, sql::Identifier, Client, Row};
use serde::{Deserialize, Serialize};

macro_rules! prepare_database {
    () => {
        crate::_priv::prepare_database({
            fn f() {}
            fn type_name_of_val<T>(_: T) -> &'static str {
                std::any::type_name::<T>()
            }
            type_name_of_val(f)
        })
        .await
    };
}

#[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
struct SimpleRow {
    id: u64,
    data: String,
}

impl SimpleRow {
    fn new(id: u64, data: impl ToString) -> Self {
        Self {
            id,
            data: data.to_string(),
        }
    }
}

async fn create_simple_table(client: &Client, table_name: &str) {
    client
        .query("CREATE TABLE ?(id UInt64, data String) ENGINE = MergeTree ORDER BY id")
        .bind(Identifier(table_name))
        .execute()
        .await
        .unwrap();
}

async fn fetch_rows<T>(client: &Client, table_name: &str) -> Vec<T>
where
    T: Row + for<'b> Deserialize<'b>,
{
    client
        .query("SELECT ?fields FROM ?")
        .bind(Identifier(table_name))
        .fetch_all::<T>()
        .await
        .unwrap()
}

async fn flush_query_log(client: &Client) {
    client.query("SYSTEM FLUSH LOGS").execute().await.unwrap();
}

mod chrono;
mod compression;
mod cursor_error;
mod cursor_stats;
mod fetch_bytes;
mod insert;
mod inserter;
mod ip;
mod mock;
mod nested;
mod query;
mod time;
mod user_agent;
mod uuid;
mod variant;
mod watch;

const HOST: &str = "localhost:8123";

mod _priv {
    use super::*;

    pub(crate) async fn prepare_database(fn_path: &str) -> Client {
        let name = make_db_name(fn_path);
        let client = Client::default().with_url(format!("http://{HOST}"));

        client
            .query("DROP DATABASE IF EXISTS ?")
            .bind(sql::Identifier(&name))
            .execute()
            .await
            .expect("cannot drop db");

        client
            .query("CREATE DATABASE ?")
            .bind(sql::Identifier(&name))
            .execute()
            .await
            .expect("cannot create db");

        client.with_database(name)
    }

    // `it::compression::lz4::{{closure}}::f` -> `chrs__compression__lz4`
    fn make_db_name(fn_path: &str) -> String {
        assert!(fn_path.starts_with("it::"));
        let mut iter = fn_path.split("::").skip(1);
        let module = iter.next().unwrap();
        let test = iter.next().unwrap();
        format!("chrs__{module}__{test}")
    }
}

```

# tests/it/mock.rs

```rs
#![cfg(feature = "test-util")]

use std::time::Duration;

use clickhouse::{test, Client};

use crate::SimpleRow;

async fn test_provide() {
    let mock = test::Mock::new();
    let client = Client::default().with_url(mock.url());
    let expected = vec![SimpleRow::new(1, "one"), SimpleRow::new(2, "two")];
    mock.add(test::handlers::provide(&expected));

    let actual = crate::fetch_rows::<SimpleRow>(&client, "doesn't matter").await;
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn provide() {
    test_provide().await;

    // Same but with the advanced time.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(100_000)).await;
    test_provide().await;
}

```

# tests/it/nested.rs

```rs
use serde::{Deserialize, Serialize};

use clickhouse::Row;

#[tokio::test]
async fn smoke() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        no: i32,
        #[serde(rename = "items.name")]
        items_name: Vec<String>,
        #[serde(rename = "items.count")]
        items_count: Vec<u32>,
    }

    client
        .query(
            "
        CREATE TABLE test(
            no      Int32,
            items   Nested(
                name    String,
                count   UInt32
            )
        )
        ENGINE = MergeTree ORDER BY no
    ",
        )
        .execute()
        .await
        .unwrap();

    let original_row = MyRow {
        no: 42,
        items_name: vec!["foo".into(), "bar".into()],
        items_count: vec![1, 5],
    };

    let mut insert = client.insert("test").unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let row = client
        .query("SELECT ?fields FROM test")
        .fetch_one::<MyRow>()
        .await
        .unwrap();

    assert_eq!(row, original_row);
}

```

# tests/it/query.rs

```rs
use serde::{Deserialize, Serialize};

use clickhouse::{error::Error, Row};

#[tokio::test]
async fn smoke() {
    let client = prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow<'a> {
        no: u32,
        name: &'a str,
    }

    // Create a table.
    client
        .query(
            "
            CREATE TABLE test(no UInt32, name LowCardinality(String))
            ENGINE = MergeTree
            ORDER BY no
        ",
        )
        .execute()
        .await
        .unwrap();

    // Write to the table.
    let mut insert = client.insert("test").unwrap();
    for i in 0..1000 {
        insert.write(&MyRow { no: i, name: "foo" }).await.unwrap();
    }

    insert.end().await.unwrap();

    // Read from the table.
    let mut cursor = client
        .query("SELECT ?fields FROM test WHERE name = ? AND no BETWEEN ? AND ?.2")
        .bind("foo")
        .bind(500)
        .bind((42, 504))
        .fetch::<MyRow<'_>>()
        .unwrap();

    let mut i = 500;

    while let Some(row) = cursor.next().await.unwrap() {
        assert_eq!(row.no, i);
        assert_eq!(row.name, "foo");
        i += 1;
    }
}

#[tokio::test]
async fn fetch_one_and_optional() {
    let client = prepare_database!();

    client
        .query("CREATE TABLE test(n String) ENGINE = MergeTree ORDER BY n")
        .execute()
        .await
        .unwrap();

    let q = "SELECT * FROM test";
    let got_string = client.query(q).fetch_optional::<String>().await.unwrap();
    assert_eq!(got_string, None);

    let got_string = client.query(q).fetch_one::<String>().await;
    assert!(matches!(got_string, Err(Error::RowNotFound)));

    #[derive(Serialize, Row)]
    struct Row {
        n: String,
    }

    let mut insert = client.insert("test").unwrap();
    insert.write(&Row { n: "foo".into() }).await.unwrap();
    insert.write(&Row { n: "bar".into() }).await.unwrap();
    insert.end().await.unwrap();

    let got_string = client.query(q).fetch_optional::<String>().await.unwrap();
    assert_eq!(got_string, Some("bar".into()));

    let got_string = client.query(q).fetch_one::<String>().await.unwrap();
    assert_eq!(got_string, "bar");
}

#[tokio::test]
async fn server_side_param() {
    let client = prepare_database!();

    let result = client
        .query("SELECT plus({val1: Int32}, {val2: Int32}) AS result")
        .param("val1", 42)
        .param("val2", 144)
        .fetch_one::<u64>()
        .await
        .expect("failed to fetch u64");
    assert_eq!(result, 186);

    let result = client
        .query("SELECT {val1: String} AS result")
        .param("val1", "string")
        .fetch_one::<String>()
        .await
        .expect("failed to fetch string");
    assert_eq!(result, "string");

    let result = client
        .query("SELECT {val1: String} AS result")
        .param("val1", "\x01\x02\x03\\ \"\'")
        .fetch_one::<String>()
        .await
        .expect("failed to fetch string");
    assert_eq!(result, "\x01\x02\x03\\ \"\'");

    let result = client
        .query("SELECT {val1: Array(String)} AS result")
        .param("val1", vec!["a", "bc"])
        .fetch_one::<Vec<String>>()
        .await
        .expect("failed to fetch string");
    assert_eq!(result, &["a", "bc"]);
}

// See #19.
#[tokio::test]
async fn long_query() {
    let client = prepare_database!();

    client
        .query("CREATE TABLE test(n String) ENGINE = MergeTree ORDER BY n")
        .execute()
        .await
        .unwrap();

    let long_string = "A".repeat(100_000);

    let got_string = client
        .query("select ?")
        .bind(&long_string)
        .fetch_one::<String>()
        .await
        .unwrap();

    assert_eq!(got_string, long_string);
}

// See #22.
#[tokio::test]
async fn big_borrowed_str() {
    let client = prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow<'a> {
        no: u32,
        body: &'a str,
    }

    client
        .query("CREATE TABLE test(no UInt32, body String) ENGINE = MergeTree ORDER BY no")
        .execute()
        .await
        .unwrap();

    let long_string = "A".repeat(10000);

    let mut insert = client.insert("test").unwrap();
    insert
        .write(&MyRow {
            no: 0,
            body: &long_string,
        })
        .await
        .unwrap();
    insert.end().await.unwrap();

    let mut cursor = client
        .query("SELECT ?fields FROM test")
        .fetch::<MyRow<'_>>()
        .unwrap();

    let row = cursor.next().await.unwrap().unwrap();
    assert_eq!(row.body, long_string);
}

// See #31.
#[tokio::test]
async fn all_floats() {
    let client = prepare_database!();

    client
        .query("CREATE TABLE test(no UInt32, f Float64) ENGINE = MergeTree ORDER BY no")
        .execute()
        .await
        .unwrap();

    #[derive(Row, Serialize)]
    struct Row {
        no: u32,
        f: f64,
    }

    let mut insert = client.insert("test").unwrap();
    insert.write(&Row { no: 0, f: 42.5 }).await.unwrap();
    insert.write(&Row { no: 1, f: 43.5 }).await.unwrap();
    insert.end().await.unwrap();

    let vec = client
        .query("SELECT f FROM test")
        .fetch_all::<f64>()
        .await
        .unwrap();

    assert_eq!(vec, &[42.5, 43.5]);
}

#[tokio::test]
async fn keeps_client_options() {
    let (client_setting_name, client_setting_value) = ("max_block_size", "1000");
    let (query_setting_name, query_setting_value) = ("date_time_input_format", "basic");

    let client = prepare_database!().with_option(client_setting_name, client_setting_value);

    let value = client
        .query("SELECT value FROM system.settings WHERE name = ? OR name = ? ORDER BY name")
        .bind(query_setting_name)
        .bind(client_setting_name)
        .with_option(query_setting_name, query_setting_value)
        .fetch_all::<String>()
        .await
        .unwrap();

    // should keep the client options
    assert_eq!(value, vec!(query_setting_value, client_setting_value));
}

#[tokio::test]
async fn overrides_client_options() {
    let (setting_name, setting_value, override_value) = ("max_block_size", "1000", "2000");

    let client = prepare_database!().with_option(setting_name, setting_value);

    let value = client
        .query("SELECT value FROM system.settings WHERE name = ?")
        .bind(setting_name)
        .with_option(setting_name, override_value)
        .fetch_one::<String>()
        .await
        .unwrap();

    // should override the client options
    assert_eq!(value, override_value);
}

#[tokio::test]
async fn prints_query() {
    let client = prepare_database!();

    let q = client.query("SELECT ?fields FROM test WHERE a = ? AND b < ?");
    assert_eq!(
        format!("{}", q.sql_display()),
        "SELECT ?fields FROM test WHERE a = ? AND b < ?"
    );
}

```

# tests/it/time.rs

```rs
#![cfg(feature = "time")]

use std::ops::RangeBounds;

use rand::{distributions::Standard, Rng};
use serde::{Deserialize, Serialize};
use time::{macros::datetime, Date, OffsetDateTime};

use clickhouse::Row;

#[tokio::test]
async fn datetime() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::time::datetime")]
        dt: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime::option")]
        dt_opt: Option<OffsetDateTime>,
        #[serde(with = "clickhouse::serde::time::datetime64::secs")]
        dt64s: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::secs::option")]
        dt64s_opt: Option<OffsetDateTime>,
        #[serde(with = "clickhouse::serde::time::datetime64::millis")]
        dt64ms: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::millis::option")]
        dt64ms_opt: Option<OffsetDateTime>,
        #[serde(with = "clickhouse::serde::time::datetime64::micros")]
        dt64us: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::micros::option")]
        dt64us_opt: Option<OffsetDateTime>,
        #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
        dt64ns: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::nanos::option")]
        dt64ns_opt: Option<OffsetDateTime>,
    }

    #[derive(Debug, Deserialize, Row)]
    struct MyRowStr {
        dt: String,
        dt64s: String,
        dt64ms: String,
        dt64us: String,
        dt64ns: String,
    }

    client
        .query(
            "
            CREATE TABLE test(
                dt          DateTime,
                dt_opt      Nullable(DateTime),
                dt64s       DateTime64(0),
                dt64s_opt   Nullable(DateTime64(0)),
                dt64ms      DateTime64(3),
                dt64ms_opt  Nullable(DateTime64(3)),
                dt64us      DateTime64(6),
                dt64us_opt  Nullable(DateTime64(6)),
                dt64ns      DateTime64(9),
                dt64ns_opt  Nullable(DateTime64(9))
            )
            ENGINE = MergeTree ORDER BY dt
        ",
        )
        .execute()
        .await
        .unwrap();

    let original_row = MyRow {
        dt: datetime!(2022-11-13 15:27:42 UTC),
        dt_opt: Some(datetime!(2022-11-13 15:27:42 UTC)),
        dt64s: datetime!(2022-11-13 15:27:42 UTC),
        dt64s_opt: Some(datetime!(2022-11-13 15:27:42 UTC)),
        dt64ms: datetime!(2022-11-13 15:27:42.123 UTC),
        dt64ms_opt: Some(datetime!(2022-11-13 15:27:42.123 UTC)),
        dt64us: datetime!(2022-11-13 15:27:42.123456 UTC),
        dt64us_opt: Some(datetime!(2022-11-13 15:27:42.123456 UTC)),
        dt64ns: datetime!(2022-11-13 15:27:42.123456789 UTC),
        dt64ns_opt: Some(datetime!(2022-11-13 15:27:42.123456789 UTC)),
    };

    let mut insert = client.insert("test").unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let row = client
        .query("SELECT ?fields FROM test")
        .fetch_one::<MyRow>()
        .await
        .unwrap();

    let row_str = client
        .query(
            "
            SELECT toString(dt),
                   toString(dt64s),
                   toString(dt64ms),
                   toString(dt64us),
                   toString(dt64ns)
              FROM test
        ",
        )
        .fetch_one::<MyRowStr>()
        .await
        .unwrap();

    assert_eq!(row, original_row);
    assert_eq!(row_str.dt, &original_row.dt.to_string()[..19]);
    assert_eq!(row_str.dt64s, &original_row.dt64s.to_string()[..19]);
    assert_eq!(row_str.dt64ms, &original_row.dt64ms.to_string()[..23]);
    assert_eq!(row_str.dt64us, &original_row.dt64us.to_string()[..26]);
    assert_eq!(row_str.dt64ns, &original_row.dt64ns.to_string()[..29]);
}

#[tokio::test]
async fn date() {
    let client = prepare_database!();

    #[derive(Debug, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::time::date")]
        date: Date,
        #[serde(with = "clickhouse::serde::time::date::option")]
        date_opt: Option<Date>,
    }

    client
        .query(
            "
            CREATE TABLE test(
                date        Date,
                date_opt    Nullable(Date)
            ) ENGINE = MergeTree ORDER BY date
        ",
        )
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert("test").unwrap();

    let dates = generate_dates(1970..2149, 100);
    for &date in &dates {
        let original_row = MyRow {
            date,
            date_opt: Some(date),
        };

        insert.write(&original_row).await.unwrap();
    }
    insert.end().await.unwrap();

    let actual = client
        .query("SELECT ?fields, toString(date) FROM test ORDER BY date")
        .fetch_all::<(MyRow, String)>()
        .await
        .unwrap();

    assert_eq!(actual.len(), dates.len());

    for ((row, date_str), expected) in actual.iter().zip(dates) {
        assert_eq!(row.date, expected);
        assert_eq!(row.date_opt, Some(expected));
        assert_eq!(date_str, &expected.to_string());
    }
}

#[tokio::test]
async fn date32() {
    let client = prepare_database!();

    #[derive(Debug, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::time::date32")]
        date: Date,
        #[serde(with = "clickhouse::serde::time::date32::option")]
        date_opt: Option<Date>,
    }

    client
        .query(
            "
            CREATE TABLE test(
                date        Date32,
                date_opt    Nullable(Date32)
            ) ENGINE = MergeTree ORDER BY date
        ",
        )
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert("test").unwrap();

    let dates = generate_dates(1925..2283, 100); // TODO: 1900..=2299 for newer versions.
    for &date in &dates {
        let original_row = MyRow {
            date,
            date_opt: Some(date),
        };

        insert.write(&original_row).await.unwrap();
    }
    insert.end().await.unwrap();

    let actual = client
        .query("SELECT ?fields, toString(date) FROM test ORDER BY date")
        .fetch_all::<(MyRow, String)>()
        .await
        .unwrap();

    assert_eq!(actual.len(), dates.len());

    for ((row, date_str), expected) in actual.iter().zip(dates) {
        assert_eq!(row.date, expected);
        assert_eq!(row.date_opt, Some(expected));
        assert_eq!(date_str, &expected.to_string());
    }
}

fn generate_dates(years: impl RangeBounds<i32>, count: usize) -> Vec<Date> {
    let mut rng = rand::thread_rng();
    let mut dates: Vec<_> = (&mut rng)
        .sample_iter(Standard)
        .filter(|date: &Date| years.contains(&date.year()))
        .take(count)
        .collect();

    dates.sort_unstable();
    dates
}

```

# tests/it/user_agent.rs

```rs
use crate::{create_simple_table, flush_query_log, SimpleRow};
use clickhouse::sql::Identifier;
use clickhouse::Client;

const PKG_VER: &str = env!("CARGO_PKG_VERSION");
const RUST_VER: &str = env!("CARGO_PKG_RUST_VERSION");
const OS: &str = std::env::consts::OS;

#[tokio::test]
async fn default_user_agent() {
    let table_name = "chrs_default_user_agent";
    let client = prepare_database!();
    let expected_user_agent = format!("clickhouse-rs/{PKG_VER} (lv:rust/{RUST_VER}, os:{OS})");
    assert_queries_user_agents(&client, table_name, &expected_user_agent).await;
}

#[tokio::test]
async fn user_agent_with_single_product_info() {
    let table_name = "chrs_user_agent_with_single_product_info";
    let client = prepare_database!().with_product_info("my-app", "0.1.0");
    let expected_user_agent =
        format!("my-app/0.1.0 clickhouse-rs/{PKG_VER} (lv:rust/{RUST_VER}, os:{OS})");
    assert_queries_user_agents(&client, table_name, &expected_user_agent).await;
}

#[tokio::test]
async fn user_agent_with_multiple_product_info() {
    let table_name = "chrs_user_agent_with_multiple_product_info";
    let client = prepare_database!()
        .with_product_info("my-datasource", "2.5.0")
        .with_product_info("my-app", "0.1.0");
    let expected_user_agent = format!(
        "my-app/0.1.0 my-datasource/2.5.0 clickhouse-rs/{PKG_VER} (lv:rust/{RUST_VER}, os:{OS})"
    );
    assert_queries_user_agents(&client, table_name, &expected_user_agent).await;
}

async fn assert_queries_user_agents(client: &Client, table_name: &str, expected_user_agent: &str) {
    let row = SimpleRow::new(42, "foo");

    create_simple_table(client, table_name).await;

    let mut insert = client.insert(table_name).unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    let rows = client
        .query("SELECT ?fields FROM ?")
        .bind(Identifier(table_name))
        .fetch_all::<SimpleRow>()
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], row);

    flush_query_log(client).await;

    let recorded_user_agents = client
        .query(&format!(
            "
            SELECT http_user_agent
            FROM system.query_log
            WHERE type = 'QueryFinish'
            AND (
              query LIKE 'SELECT%FROM%{table_name}%'
              OR
              query LIKE 'INSERT%INTO%{table_name}%'
            )
            ORDER BY event_time_microseconds DESC
            LIMIT 2
            "
        ))
        .fetch_all::<String>()
        .await
        .unwrap();

    assert_eq!(recorded_user_agents.len(), 2);
    assert_eq!(recorded_user_agents[0], expected_user_agent);
    assert_eq!(recorded_user_agents[1], expected_user_agent);
}

```

# tests/it/uuid.rs

```rs
#![cfg(feature = "uuid")]

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use clickhouse::Row;

#[tokio::test]
async fn smoke() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::uuid")]
        uuid: Uuid,
        #[serde(with = "clickhouse::serde::uuid::option")]
        uuid_opt: Option<Uuid>,
    }

    client
        .query(
            "
            CREATE TABLE test(
                uuid UUID,
                uuid_opt Nullable(UUID)
            ) ENGINE = MergeTree ORDER BY uuid
        ",
        )
        .execute()
        .await
        .unwrap();

    let uuid = Uuid::new_v4();
    println!("uuid: {uuid}");

    let original_row = MyRow {
        uuid,
        uuid_opt: Some(uuid),
    };

    let mut insert = client.insert("test").unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let (row, row_uuid_str) = client
        .query("SELECT ?fields, toString(uuid) FROM test")
        .fetch_one::<(MyRow, String)>()
        .await
        .unwrap();

    assert_eq!(row, original_row);
    assert_eq!(row_uuid_str, original_row.uuid.to_string());
}

#[tokio::test]
async fn human_readable_smoke() {
    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct OursRow {
        #[serde(with = "clickhouse::serde::uuid")]
        uuid: Uuid,
    }

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct TheirsRow {
        uuid: Uuid,
    }

    let uuid = Uuid::new_v4();

    let row = OursRow { uuid };

    let row2 = TheirsRow { uuid };

    let s1 = serde_json::to_string(&row).unwrap();
    let s2 = serde_json::to_string(&row2).unwrap();

    assert_eq!(s1, s2);

    let new_row2: TheirsRow = serde_json::from_str(&s2).unwrap();

    assert_eq!(new_row2, row2);
}

```

# tests/it/variant.rs

```rs
#![cfg(feature = "time")]

use serde::{Deserialize, Serialize};
use time::Month::January;

use clickhouse::Row;

// See also: https://clickhouse.com/docs/en/sql-reference/data-types/variant

#[tokio::test]
async fn variant_data_type() {
    let client = prepare_database!();

    // NB: Inner Variant types are _always_ sorted alphabetically,
    // and should be defined in _exactly_ the same order in the enum.
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum MyRowVariant {
        Array(Vec<i16>),
        Boolean(bool),
        // attributes should work in this case, too
        #[serde(with = "clickhouse::serde::time::date")]
        Date(time::Date),
        FixedString([u8; 6]),
        Float32(f32),
        Float64(f64),
        Int128(i128),
        Int16(i16),
        Int32(i32),
        Int64(i64),
        Int8(i8),
        String(String),
        UInt128(u128),
        UInt16(i16),
        UInt32(u32),
        UInt64(u64),
        UInt8(i8),
    }

    #[derive(Debug, PartialEq, Row, Serialize, Deserialize)]
    struct MyRow {
        var: MyRowVariant,
    }

    // No matter the order of the definition on the Variant types, it will always be sorted as follows:
    // Variant(Array(UInt16), Bool, FixedString(6), Float32, Float64, Int128, Int16, Int32, Int64, Int8, String, UInt128, UInt16, UInt32, UInt64, UInt8)
    client
        .query(
            "
            CREATE OR REPLACE TABLE test_var
            (
                `var` Variant(
                    Array(UInt16),
                    Bool,
                    Date,
                    FixedString(6),
                    Float32, Float64,
                    Int128, Int16, Int32, Int64, Int8,
                    String,
                    UInt128, UInt16, UInt32, UInt64, UInt8
                )
            )
            ENGINE = MergeTree
            ORDER BY ()",
        )
        .with_option("allow_experimental_variant_type", "1")
        .with_option("allow_suspicious_variant_types", "1")
        .execute()
        .await
        .unwrap();

    let vars = [
        MyRowVariant::Array(vec![1, 2]),
        MyRowVariant::Boolean(true),
        MyRowVariant::Date(time::Date::from_calendar_date(2021, January, 1).unwrap()),
        MyRowVariant::FixedString(*b"foobar"),
        MyRowVariant::Float32(100.5),
        MyRowVariant::Float64(200.1),
        MyRowVariant::Int8(2),
        MyRowVariant::Int16(3),
        MyRowVariant::Int32(4),
        MyRowVariant::Int64(5),
        MyRowVariant::Int128(6),
        MyRowVariant::String("my_string".to_string()),
        MyRowVariant::UInt8(7),
        MyRowVariant::UInt16(8),
        MyRowVariant::UInt32(9),
        MyRowVariant::UInt64(10),
        MyRowVariant::UInt128(11),
    ];

    let rows = vars.map(|var| MyRow { var });

    // Write to the table.
    let mut insert = client.insert("test_var").unwrap();
    for row in &rows {
        insert.write(row).await.unwrap();
    }
    insert.end().await.unwrap();

    // Read from the table.
    let result_rows = client
        .query("SELECT ?fields FROM test_var")
        .fetch_all::<MyRow>()
        .await
        .unwrap();

    assert_eq!(result_rows, rows)
}

```

# tests/it/watch.rs

```rs
#![cfg(feature = "watch")]

use serde::{Deserialize, Serialize};

use clickhouse::{Client, Row};

#[derive(Debug, PartialEq, Row, Serialize, Deserialize)]
struct MyRow {
    num: u32,
}

async fn create_table(client: &Client) {
    client
        .query(
            "
            CREATE TABLE test(num UInt32)
            ENGINE = MergeTree
            ORDER BY num
        ",
        )
        .execute()
        .await
        .unwrap();
}

async fn insert_into_table(client: &Client, rows: &[MyRow]) {
    let mut insert = client.insert("test").unwrap();
    for row in rows {
        insert.write(row).await.unwrap();
    }
    insert.end().await.unwrap();
}

#[tokio::test]
async fn changes() {
    let client = prepare_database!();

    create_table(&client).await;

    let mut cursor1 = client
        .watch("SELECT ?fields FROM test ORDER BY num")
        .limit(1)
        .fetch::<MyRow>()
        .unwrap();

    let mut cursor2 = client
        .watch("SELECT sum(num) as num FROM test")
        .fetch::<MyRow>()
        .unwrap();

    // Insert first batch.
    insert_into_table(&client, &[MyRow { num: 1 }, MyRow { num: 2 }]).await;
    assert_eq!(cursor1.next().await.unwrap(), Some((1, MyRow { num: 1 })));
    assert_eq!(cursor1.next().await.unwrap(), Some((1, MyRow { num: 2 })));
    assert_eq!(cursor2.next().await.unwrap(), Some((1, MyRow { num: 3 })));

    // Insert second batch.
    insert_into_table(&client, &[MyRow { num: 3 }, MyRow { num: 4 }]).await;
    assert_eq!(cursor1.next().await.unwrap(), Some((2, MyRow { num: 1 })));
    assert_eq!(cursor1.next().await.unwrap(), Some((2, MyRow { num: 2 })));
    assert_eq!(cursor1.next().await.unwrap(), Some((2, MyRow { num: 3 })));
    assert_eq!(cursor1.next().await.unwrap(), Some((2, MyRow { num: 4 })));
    assert_eq!(cursor2.next().await.unwrap(), Some((2, MyRow { num: 10 })));

    // Insert third batch.
    insert_into_table(&client, &[MyRow { num: 5 }, MyRow { num: 6 }]).await;
    assert_eq!(cursor1.next().await.unwrap(), None);
    assert_eq!(cursor2.next().await.unwrap(), Some((3, MyRow { num: 21 })));
}

#[tokio::test]
async fn events() {
    let client = prepare_database!();

    create_table(&client).await;

    let mut cursor1 = client
        .watch("SELECT num FROM test ORDER BY num")
        .limit(1)
        .only_events()
        .fetch()
        .unwrap();

    let mut cursor2 = client
        .watch("SELECT sum(num) as num FROM test")
        .only_events()
        .fetch()
        .unwrap();

    // Insert first batch.
    insert_into_table(&client, &[MyRow { num: 1 }, MyRow { num: 2 }]).await;
    assert_eq!(cursor1.next().await.unwrap(), Some(1));
    assert_eq!(cursor2.next().await.unwrap(), Some(1));

    // Insert second batch.
    insert_into_table(&client, &[MyRow { num: 3 }, MyRow { num: 4 }]).await;
    assert_eq!(cursor1.next().await.unwrap(), Some(2));
    assert_eq!(cursor2.next().await.unwrap(), Some(2));

    // Insert third batch.
    insert_into_table(&client, &[MyRow { num: 5 }, MyRow { num: 6 }]).await;
    assert_eq!(cursor1.next().await.unwrap(), None);
    assert_eq!(cursor2.next().await.unwrap(), Some(3));
}

```

