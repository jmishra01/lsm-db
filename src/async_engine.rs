// =============================================================
// Async API (#new)
//
// Problem: `LsmEngine` and `SharedLsmEngine` are synchronous.
// Every write calls `WAL.flush()` which is a blocking syscall.
// Calling blocking code from within a Tokio task stalls the
// async runtime thread, reducing throughput under concurrent I/O.
//
// Solution: `AsyncEngine` wraps `SharedLsmEngine` and offloads
// every blocking operation to `tokio::task::spawn_blocking`.
// The caller gets a fully async API compatible with `.await`.
//
// Usage:
// ```rust
// let db = AsyncEngine::open("/data/mydb").await?;
// db.put("key", "value").await?;
// let v = db.get("key").await?;
// ```
//
// When to use which API
// ---------------------
//   SharedLsmEngine  → single-threaded apps, CLI tools, tests.
//   AsyncEngine      → Tokio-based servers, the HTTP API handler,
//                      and any code that processes many requests
//                      concurrently on a shared thread pool.
// =============================================================

use std::path::PathBuf;
use std::io;

use crate::engine::SharedLsmEngine;
use crate::snapshot::{Snapshot, WriteBatch};

/// An async wrapper around `SharedLsmEngine`.
///
/// All methods return `Future`s and are safe to `.await` from Tokio tasks.
#[derive(Clone)]
pub struct AsyncEngine {
    inner: SharedLsmEngine,
}

impl AsyncEngine {
    /// Open or create a database at `path`.
    pub async fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let inner = tokio::task::spawn_blocking(move || SharedLsmEngine::open(&path))
            .await
            .map_err(|e| io::Error::other(e))??;
        Ok(Self { inner })
    }

    /// Open with explicit column families.
    pub async fn open_with_cfs(path: impl Into<PathBuf>, cfs: Vec<String>) -> io::Result<Self> {
        let path = path.into();
        let inner = tokio::task::spawn_blocking(move || {
            let cf_refs: Vec<&str> = cfs.iter().map(|s| s.as_str()).collect();
            SharedLsmEngine::open_with_cfs(&path, &cf_refs)
        })
        .await
        .map_err(|e| io::Error::other(e))??;
        Ok(Self { inner })
    }

    pub async fn put(
        &self,
        key: impl Into<Vec<u8>> + Send + 'static,
        value: impl Into<Vec<u8>> + Send + 'static,
    ) -> io::Result<()> {
        let db = self.inner.clone();
        tokio::task::spawn_blocking(move || db.put(key, value))
            .await
            .map_err(|e| io::Error::other(e))?
    }

    pub async fn put_cf(
        &self,
        cf: String,
        key: impl Into<Vec<u8>> + Send + 'static,
        value: impl Into<Vec<u8>> + Send + 'static,
    ) -> io::Result<()> {
        let db = self.inner.clone();
        tokio::task::spawn_blocking(move || db.put_cf(&cf, key, value))
            .await
            .map_err(|e| io::Error::other(e))?
    }

    pub async fn put_with_ttl(
        &self,
        key: impl Into<Vec<u8>> + Send + 'static,
        value: impl Into<Vec<u8>> + Send + 'static,
        ttl_ms: u64,
    ) -> io::Result<()> {
        let db = self.inner.clone();
        tokio::task::spawn_blocking(move || db.put_with_ttl(key, value, ttl_ms))
            .await
            .map_err(|e| io::Error::other(e))?
    }

    pub async fn get(
        &self,
        key: impl Into<Vec<u8>> + Send + 'static,
    ) -> io::Result<Option<Vec<u8>>> {
        let key = key.into();
        let db = self.inner.clone();
        tokio::task::spawn_blocking(move || db.get(key))
            .await
            .map_err(|e| io::Error::other(e))?
    }

    pub async fn get_cf(
        &self,
        cf: String,
        key: impl Into<Vec<u8>> + Send + 'static,
    ) -> io::Result<Option<Vec<u8>>> {
        let key = key.into();
        let db = self.inner.clone();
        tokio::task::spawn_blocking(move || db.get_cf(&cf, key))
            .await
            .map_err(|e| io::Error::other(e))?
    }

    pub async fn delete(
        &self,
        key: impl Into<Vec<u8>> + Send + 'static,
    ) -> io::Result<()> {
        let db = self.inner.clone();
        tokio::task::spawn_blocking(move || db.delete(key))
            .await
            .map_err(|e| io::Error::other(e))?
    }

    pub async fn delete_range(
        &self,
        from: impl Into<Vec<u8>> + Send + 'static,
        to:   impl Into<Vec<u8>> + Send + 'static,
    ) -> io::Result<()> {
        let db = self.inner.clone();
        tokio::task::spawn_blocking(move || db.delete_range(from, to))
            .await
            .map_err(|e| io::Error::other(e))?
    }

    pub async fn scan(
        &self,
        from: Vec<u8>,
        to:   Vec<u8>,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let db = self.inner.clone();
        tokio::task::spawn_blocking(move || db.scan(from, to))
            .await
            .map_err(|e| io::Error::other(e))?
    }

    pub async fn scan_prefix(
        &self,
        prefix: Vec<u8>,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let db = self.inner.clone();
        tokio::task::spawn_blocking(move || db.scan_prefix(prefix))
            .await
            .map_err(|e| io::Error::other(e))?
    }

    pub async fn write_batch(&self, batch: WriteBatch) -> io::Result<()> {
        let db = self.inner.clone();
        tokio::task::spawn_blocking(move || db.write_batch(batch))
            .await
            .map_err(|e| io::Error::other(e))?
    }

    pub async fn snapshot(&self) -> io::Result<Snapshot> {
        let db = self.inner.clone();
        tokio::task::spawn_blocking(move || db.snapshot())
            .await
            .map_err(|e| io::Error::other(e))?
    }

    /// Access the underlying `SharedLsmEngine` for operations not yet
    /// wrapped in the async API.
    pub fn inner(&self) -> &SharedLsmEngine { &self.inner }
}
