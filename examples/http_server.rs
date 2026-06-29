/// Starts the lsmdb HTTP REST server on 0.0.0.0:8080.
///
/// Endpoints:
///   GET    /key/<key>          → {"key":"…","value":"…"} or 404
///   PUT    /key/<key>          body = plain-text value
///   DELETE /key/<key>
///   GET    /scan?from=&to=     → [{key,value},…]
///   GET    /prefix/<prefix>    → [{key,value},…]
///   GET    /snapshot           → {seq, entries:[{key,value}]}
///   GET    /stats              → engine stats JSON
///   GET    /metrics            → Prometheus text format
///
/// Quick test (requires curl):
///   curl -X PUT  http://localhost:8080/key/hello -d "world"
///   curl         http://localhost:8080/key/hello
///   curl         http://localhost:8080/stats
///   curl         http://localhost:8080/metrics
use lsmdb::{SharedLsmEngine, http_api};
use std::path::PathBuf;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Data directory: $DATA_DIR env var, or ./lsmdb-data
    let data_dir: PathBuf = std::env::var("DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("lsmdb-data"));

    std::fs::create_dir_all(&data_dir)?;
    let db = SharedLsmEngine::open(&data_dir)?;

    // Seed a few keys so the server has something to show on first run
    db.put("greeting", "hello from lsmdb")?;
    db.put("version",  env!("CARGO_PKG_VERSION"))?;
    db.put("user:alice", "Alice Smith")?;
    db.put("user:bob",   "Bob Jones")?;

    let addr = "0.0.0.0:8080";
    let listener = TcpListener::bind(addr).await?;
    println!("lsmdb HTTP server listening on http://{addr}");
    println!("data directory: {}", data_dir.display());
    println!();
    println!("try:");
    println!("  curl http://localhost:8080/key/greeting");
    println!("  curl -X PUT http://localhost:8080/key/mykey -d myvalue");
    println!("  curl 'http://localhost:8080/scan?from=user:&to=user:~'");
    println!("  curl http://localhost:8080/prefix/user:");
    println!("  curl http://localhost:8080/stats");
    println!("  curl http://localhost:8080/metrics");

    let app = http_api::make_router(db);
    axum::serve(listener, app).await
}
