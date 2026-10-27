//! A tiny HTTP/1.1 range server: serves the files under one root and
//! records every byte range it sends (`GET`, with or without `Range`), so
//! an engine in another process is measured at the storage boundary. A
//! `HEAD` sends no body and is not recorded. Paths outside the root are
//! refused with 404.

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bellwether_adapters::measure::Fetch;

/// A running server.
pub struct RangeServer {
    /// `http://127.0.0.1:<port>`.
    pub base: String,
    log: Arc<Mutex<Vec<Fetch>>>,
    stop: Arc<AtomicBool>,
}

impl RangeServer {
    /// Serves `root` on an ephemeral local port.
    pub fn start(root: &Path) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let log = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let root = root.canonicalize().unwrap();
        {
            let log = Arc::clone(&log);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                for conn in listener.incoming() {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let Ok(conn) = conn else { continue };
                    let log = Arc::clone(&log);
                    let root = root.clone();
                    std::thread::spawn(move || {
                        let _ = serve(conn, &root, &log);
                    });
                }
            });
        }
        Self { base, log, stop }
    }

    /// Every range sent so far.
    pub fn fetches(&self) -> Vec<Fetch> {
        self.log.lock().unwrap().clone()
    }

    /// Forgets the ranges sent so far.
    pub fn reset(&self) {
        self.log.lock().unwrap().clear();
    }
}

impl Drop for RangeServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wake the accept loop.
        let _ = TcpStream::connect(self.base.trim_start_matches("http://"));
    }
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parses `bytes=a-b`, `bytes=a-`, or `bytes=-n` against `size`.
pub fn parse_range(h: &str, size: u64) -> Option<(u64, u64)> {
    let spec = h.trim().strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let (a, b) = spec.split_once('-')?;
    let (start, end) = if a.is_empty() {
        let n: u64 = b.parse().ok()?;
        (size.saturating_sub(n), size)
    } else {
        let start: u64 = a.parse().ok()?;
        let end = if b.is_empty() {
            size
        } else {
            b.parse::<u64>().ok()?.saturating_add(1).min(size)
        };
        (start, end)
    };
    (start < end).then_some((start, end))
}

fn serve(conn: TcpStream, root: &Path, log: &Mutex<Vec<Fetch>>) -> std::io::Result<()> {
    let mut reader = BufReader::new(conn.try_clone()?);
    let mut conn = conn;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Ok(());
        }
        let mut parts = line.split_whitespace();
        let method = parts.next().unwrap_or("").to_string();
        let target = parts.next().unwrap_or("").to_string();
        let mut range = None;
        loop {
            let mut h = String::new();
            if reader.read_line(&mut h)? == 0 {
                return Ok(());
            }
            let h = h.trim_end();
            if h.is_empty() {
                break;
            }
            if let Some((k, v)) = h.split_once(':') {
                if k.eq_ignore_ascii_case("range") {
                    range = Some(v.trim().to_string());
                }
            }
        }
        let path = PathBuf::from(percent_decode(target.split('?').next().unwrap_or("")));
        let file = path
            .canonicalize()
            .ok()
            .filter(|p| p.starts_with(root) && p.is_file());
        let Some(file) = file else {
            conn.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")?;
            continue;
        };
        let size = std::fs::metadata(&file)?.len();
        let (start, end, status) = match range.as_deref().map(|r| parse_range(r, size)) {
            Some(Some((a, b))) => (a, b, "206 Partial Content"),
            Some(None) => {
                conn.write_all(
                    format!(
                        "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{size}\r\nContent-Length: 0\r\n\r\n"
                    )
                    .as_bytes(),
                )?;
                continue;
            }
            None => (0, size, "200 OK"),
        };
        let mut head = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nContent-Type: application/octet-stream\r\nLast-Modified: Mon, 01 Jan 2024 00:00:00 GMT\r\nETag: \"{size}\"\r\n",
            if method == "HEAD" { size } else { end - start }
        );
        if status.starts_with("206") {
            head.push_str(&format!("Content-Range: bytes {start}-{}/{size}\r\n", end - 1));
        }
        head.push_str("\r\n");
        conn.write_all(head.as_bytes())?;
        if method == "GET" {
            let mut f = std::fs::File::open(&file)?;
            f.seek(SeekFrom::Start(start))?;
            let mut buf = vec![0u8; usize::try_from(end - start).unwrap()];
            f.read_exact(&mut buf)?;
            conn.write_all(&buf)?;
            log.lock().unwrap().push(Fetch {
                path: file.display().to_string(),
                range: start..end,
            });
        }
        conn.flush()?;
    }
}
