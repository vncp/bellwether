//! Durable writes and compare-and-swap for the local filesystem backend.
//!
//! `object_store`'s `LocalFileSystem` implements `PutMode::Create` but not
//! `PutMode::Update`, and never fsyncs. The local backend therefore writes
//! itself:
//!
//! - Every write goes to a staging file `<target>#<20 random digits>` in the
//!   same directory and is moved into place (`rename` to overwrite,
//!   `hard_link` to create without clobbering), so a crash or concurrent
//!   reader never sees a partial file.
//! - Durability follows the commit boundary (ADR 034): conditional writes
//!   (`manifest.json`, `manifests/`, detached signatures) and ledger records
//!   (`ledger/`) fsync their staging file and parent directory on write;
//!   on Unix, segment writes (`segments/`) defer their sync to
//!   [`crate::StoreExt::sync_segments`], called once by `commit` before
//!   updating `manifest.json`; rebuildable cache entries (`cache/`) skip
//!   `sync_all`. Where no directory sync exists ([`DIR_SYNC_BARRIER`] is
//!   false: Windows, whose standard library cannot flush a directory), there
//!   is no barrier to defer to, so each segment's staging file is fsynced
//!   before its rename, as a ledger record's is.
//! - Conditional writes (`put_if_absent`, `put_if_version`) run under an
//!   exclusive kernel `flock` on `<target>.lock#0`, taken with `try_lock`
//!   until a deadline ([`LOCK_TIMEOUT`]), then [`Error::Store`]. The kernel
//!   releases the lock when the handle drops or the process dies, so there is
//!   never a stale lock to heal (ADR 003, 015). Correct on local disks, not
//!   on NFS.
//! - After a successful conditional write, leftover `<target>#<digits>`
//!   staging files (from crashed writers) are removed. This is safe because
//!   every conditional write to the target holds the lock. Lock files have a
//!   single-digit suffix and are never matched.
//!
//! `object_store`'s local listing hides file names of the form
//! `name#<digits>` (`is_valid_file_path` in its `local.rs`), so neither
//! staging files nor lock files ever appear in listings. The flip side: a
//! staging file left by a crashed *unconditional* write (a segment or cache
//! entry) is invisible to GC and is not cleaned up automatically.

use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use bellwether_types::{Error, Hash, Result, StoreVersion};

/// How long a conditional write waits for the per-path lock.
pub const LOCK_TIMEOUT: Duration = Duration::from_secs(30);

fn io_err(what: &str, path: &Path, e: &std::io::Error) -> Error {
    Error::Store(format!("{what} {}: {e}", path.display()))
}

/// Content-hash version token used by the local backend.
pub(crate) fn content_token(bytes: &[u8]) -> StoreVersion {
    StoreVersion(Hash::of(bytes).to_hex())
}

/// Whether [`sync_dir`] flushes a directory on this platform, so segment
/// writes may defer their durability to one [`crate::StoreExt::sync_segments`]
/// barrier (ADR 034). On Windows the standard library has no directory
/// handle to flush, `sync_dir` is a no-op, and segments are fsynced one by
/// one instead.
const DIR_SYNC_BARRIER: bool = cfg!(unix);

/// Fsyncs directory `dir` (its entries) where the platform allows it; a
/// no-op where it does not (see [`DIR_SYNC_BARRIER`]).
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)
            .and_then(|d| d.sync_all())
            .map_err(|e| io_err("fsync directory", dir, &e))?;
    }
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Syncs `dir` when it exists; a no-op when it has not been created yet.
pub(crate) fn sync_dir_if_exists(dir: &Path) -> Result<()> {
    if dir.is_dir() {
        sync_dir(dir)?;
    }
    Ok(())
}

/// Creates `dir` and any missing ancestors durably: after creation, the
/// nearest pre-existing ancestor and every newly created directory are
/// fsynced, so the new entries survive a crash. This covers a brand-new
/// index root and its `segments/`, `cache/` and other directories on first
/// write.
pub(crate) fn create_dirs_durably(dir: &Path) -> Result<()> {
    if dir.is_dir() {
        return Ok(());
    }
    let mut missing = Vec::new();
    let mut existing = dir;
    while !existing.exists() {
        missing.push(existing.to_path_buf());
        match existing.parent() {
            Some(p) => existing = p,
            None => break,
        }
    }
    std::fs::create_dir_all(dir).map_err(|e| io_err("create directory", dir, &e))?;
    if existing.exists() {
        sync_dir(existing)?;
    }
    for d in missing.iter().rev() {
        sync_dir(d)?;
    }
    Ok(())
}

fn staging_name(target: &Path) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = Hash::of(
        format!(
            "{:?}{:?}{}{}",
            std::time::SystemTime::now(),
            std::thread::current().id(),
            std::process::id(),
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )
        .as_bytes(),
    );
    // Decimal suffix, always 20 digits, so `object_store`'s local listing
    // (which hides `name#<digits>`) never shows staging files, and it can
    // never collide with a lock file's single-digit `#0`.
    let mut word = [0u8; 8];
    word.copy_from_slice(&n.0[..8]);
    let digits = u64::from_le_bytes(word) % 8_000_000_000_000_000_000 + 10_000_000_000_000_000_000;
    let mut name = target.as_os_str().to_owned();
    name.push(format!("#{digits}"));
    PathBuf::from(name)
}

/// Whether `file_name` is a staging file of `target_name`: `target#<digits>`
/// with more than one digit.
fn is_staging_of(file_name: &str, target_name: &str) -> bool {
    file_name
        .strip_prefix(target_name)
        .and_then(|rest| rest.strip_prefix('#'))
        .is_some_and(|d| d.len() > 1 && d.bytes().all(|b| b.is_ascii_digit()))
}

/// Writes `body` to a staging file next to `target`, fsyncing the file when
/// `durable` is true.
fn write_staging_with(target: &Path, body: &[u8], durable: bool) -> Result<PathBuf> {
    let parent = target
        .parent()
        .ok_or_else(|| Error::Store(format!("no parent for {}", target.display())))?;
    if durable {
        create_dirs_durably(parent)?;
    } else if !parent.is_dir() {
        std::fs::create_dir_all(parent).map_err(|e| io_err("create directory", parent, &e))?;
    }
    let staging = staging_name(target);
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)
        .map_err(|e| io_err("create staging file", &staging, &e))?;
    let res = if durable {
        f.write_all(body).and_then(|()| f.sync_all())
    } else {
        f.write_all(body)
    };
    if let Err(e) = res {
        let _ = std::fs::remove_file(&staging);
        return Err(io_err("write staging file", &staging, &e));
    }
    Ok(staging)
}

/// Writes `body` to a fsynced staging file next to `target`.
fn write_staging(target: &Path, body: &[u8]) -> Result<PathBuf> {
    write_staging_with(target, body, true)
}

fn parent_of(target: &Path) -> &Path {
    target.parent().unwrap_or_else(|| Path::new("."))
}

/// Durable unconditional write: staging + fsync + rename + fsync directory.
pub(crate) fn put_overwrite(target: &Path, body: &[u8]) -> Result<()> {
    let staging = write_staging(target, body)?;
    if let Err(e) = std::fs::rename(&staging, target) {
        let _ = std::fs::remove_file(&staging);
        return Err(io_err("rename into", target, &e));
    }
    sync_dir(parent_of(target))
}

/// Atomic unconditional write without per-file fsync (ADR 034): staging +
/// rename. Readers never see a partial file; durability is either not
/// required (`cache/`) or deferred to the commit barrier (`segments/`, on
/// platforms with [`DIR_SYNC_BARRIER`]).
pub(crate) fn put_atomic(target: &Path, body: &[u8]) -> Result<()> {
    let staging = write_staging_with(target, body, false)?;
    if let Err(e) = std::fs::rename(&staging, target) {
        let _ = std::fs::remove_file(&staging);
        return Err(io_err("rename into", target, &e));
    }
    Ok(())
}

/// Whether a content-addressed write of `path` may skip its per-file
/// `sync_all` (ADR 034): rebuildable cache entries always (`cache/` is
/// regenerable scratch); segments only where the commit barrier flushes them
/// ([`DIR_SYNC_BARRIER`]: `segments/` is synced before `manifest.json` is
/// updated); ledger records and any other content-addressed objects never.
fn skips_file_sync(path: &str) -> bool {
    let dir = path
        .trim_matches('/')
        .rsplit_once('/')
        .and_then(|(parent, _)| parent.rsplit('/').next());
    match dir {
        Some(crate::manifest::CACHE_DIR) => true,
        Some(crate::manifest::SEGMENTS_DIR) => DIR_SYNC_BARRIER,
        _ => false,
    }
}

/// Content-addressed write of `path` (`target` on disk): atomic staging +
/// rename, with the per-file sync [`skips_file_sync`] decides.
pub(crate) fn put_content_addressed(path: &str, target: &Path, body: &[u8]) -> Result<()> {
    if skips_file_sync(path) {
        put_atomic(target, body)
    } else {
        put_overwrite(target, body)
    }
}

/// Holds the per-path lock until dropped.
struct PathLock(#[allow(dead_code)] File);

fn lock(target: &Path) -> Result<PathLock> {
    use fs4::fs_std::FileExt as _;
    create_dirs_durably(parent_of(target))?;
    // `<target>.lock#0`: hidden from `object_store` listings by the digit
    // suffix, and never matched by `is_staging_of` (one digit only).
    let mut name = target.as_os_str().to_owned();
    name.push(".lock#0");
    let lock_path = PathBuf::from(name);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .map_err(|e| io_err("open lock", &lock_path, &e))?;
    let deadline = Instant::now() + LOCK_TIMEOUT;
    loop {
        match file.try_lock_exclusive() {
            Ok(true) => return Ok(PathLock(file)),
            Ok(false) => {}
            Err(e) => return Err(io_err("lock", &lock_path, &e)),
        }
        if Instant::now() >= deadline {
            return Err(Error::Store(format!(
                "timed out after {LOCK_TIMEOUT:?} waiting for lock {}",
                lock_path.display()
            )));
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Removes leftover `<target>#*` staging files. Called with the lock held.
fn clean_staging(target: &Path) {
    let (Some(parent), Some(name)) = (target.parent(), target.file_name()) else {
        return;
    };
    let name = name.to_string_lossy().into_owned();
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        if is_staging_of(&entry.file_name().to_string_lossy(), &name) {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

fn current_token(target: &Path) -> Result<Option<StoreVersion>> {
    match std::fs::read(target) {
        Ok(b) => Ok(Some(content_token(&b))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io_err("read", target, &e)),
    }
}

/// Reads `target` whole on the calling thread: `None` when it is absent (or
/// a directory, as `object_store`'s local `get` answers). The local
/// backend's reads skip `object_store`'s blocking-pool round trip, which
/// cost more than the read itself on the plan path.
pub(crate) fn read_whole(target: &Path) -> Result<Option<bytes::Bytes>> {
    match std::fs::read(target) {
        Ok(b) => Ok(Some(bytes::Bytes::from(b))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) if target.is_dir() => Ok(None),
        Err(e) => Err(io_err("read", target, &e)),
    }
}

/// Reads `range` of `target` on the calling thread, `None` when it is
/// absent. A range past the end of the file is an error naming both
/// lengths, as `object_store`'s local `get_range` refuses it.
pub(crate) fn read_range(
    target: &Path,
    range: std::ops::Range<u64>,
) -> Result<Option<bytes::Bytes>> {
    #[cfg(unix)]
    use std::os::unix::fs::FileExt as _;
    let file = match File::open(target) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_err("open", target, &e)),
    };
    let len = usize::try_from(range.end.saturating_sub(range.start))
        .map_err(|_| Error::Store(format!("range {range:?} of {}", target.display())))?;
    let mut buf = vec![0u8; len];
    let mut done = 0;
    while done < len {
        #[cfg(unix)]
        let read = file.read_at(&mut buf[done..], range.start + done as u64);
        #[cfg(windows)]
        let read = std::os::windows::fs::FileExt::seek_read(
            &file,
            &mut buf[done..],
            range.start + done as u64,
        );
        match read {
            Ok(0) => {
                let size = file.metadata().map_or(0, |m| m.len());
                return Err(Error::Store(format!(
                    "read {}: asked for bytes {}..{}, the file holds {size}",
                    target.display(),
                    range.start,
                    range.end
                )));
            }
            Ok(n) => done += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(io_err("read", target, &e)),
        }
    }
    Ok(Some(bytes::Bytes::from(buf)))
}

/// Creates `target` only if absent.
pub(crate) fn put_if_absent(target: &Path, body: &[u8]) -> Result<StoreVersion> {
    let _guard = lock(target)?;
    let staging = write_staging(target, body)?;
    let linked = std::fs::hard_link(&staging, target);
    let _ = std::fs::remove_file(&staging);
    match linked {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(Error::VersionConflict {
                expected: "<absent>".into(),
                found: current_token(target)?.map_or_else(|| "<absent>".into(), |v| v.0),
            })
        }
        Err(e) => return Err(io_err("link into", target, &e)),
    }
    sync_dir(parent_of(target))?;
    clean_staging(target);
    Ok(content_token(body))
}

/// Replaces `target` only if its content hash is `expected`.
pub(crate) fn put_if_version(
    target: &Path,
    body: &[u8],
    expected: &StoreVersion,
) -> Result<StoreVersion> {
    let _guard = lock(target)?;
    let current = current_token(target)?;
    if current.as_ref() != Some(expected) {
        return Err(Error::VersionConflict {
            expected: expected.0.clone(),
            found: current.map_or_else(|| "<absent>".into(), |v| v.0),
        });
    }
    put_overwrite(target, body)?;
    clean_staging(target);
    Ok(content_token(body))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn first_write_creates_nested_directories() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("new/index/root/manifest.json");
        put_if_absent(&target, b"m").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"m");
        // Idempotent on existing directories.
        create_dirs_durably(target.parent().unwrap()).unwrap();
        put_overwrite(&dir.path().join("new/index/root/segments/a.seg"), b"s").unwrap();
    }

    #[test]
    fn staging_names_are_unique_within_a_process() {
        let target = Path::new("/x/manifest.json");
        let names: BTreeSet<PathBuf> = (0..1000).map(|_| staging_name(target)).collect();
        assert_eq!(names.len(), 1000);
    }

    #[test]
    fn leftover_staging_files_are_cleaned_on_next_commit() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("manifest.json");
        let v1 = put_if_absent(&target, b"one").unwrap();
        // A crashed writer's staging file.
        std::fs::write(
            dir.path().join("manifest.json#12345678901234567890"),
            b"partial",
        )
        .unwrap();
        put_if_version(&target, b"two", &v1).unwrap();
        let mut names: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        // Only the target and its lock file remain.
        assert_eq!(names, ["manifest.json", "manifest.json.lock#0"]);
        assert_eq!(std::fs::read(&target).unwrap(), b"two");
        assert!(staging_name(&target)
            .to_string_lossy()
            .rsplit_once('#')
            .is_some_and(|(_, d)| d.len() == 20 && d.bytes().all(|b| b.is_ascii_digit())));
        assert!(!is_staging_of("manifest.json.lock#0", "manifest.json"));
        assert!(!is_staging_of("manifest.json#0", "manifest.json"));
    }

    #[test]
    fn lock_wait_is_bounded_and_conflicts_are_typed() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("m");
        let v1 = put_if_absent(&target, b"a").unwrap();
        assert!(matches!(
            put_if_absent(&target, b"b"),
            Err(Error::VersionConflict { .. })
        ));
        assert!(matches!(
            put_if_version(&target, b"b", &StoreVersion("nope".into())),
            Err(Error::VersionConflict { .. })
        ));
        // While another handle holds the lock, a writer waits, then proceeds
        // once it is released.
        let held = lock(&target).unwrap();
        let t = std::thread::spawn({
            let target = target.clone();
            move || put_if_version(&target, b"c", &v1)
        });
        std::thread::sleep(Duration::from_millis(50));
        assert!(!t.is_finished());
        drop(held);
        t.join().unwrap().unwrap();
    }

    #[test]
    fn cache_and_segment_writes_are_atomic_and_leave_no_staging_files() {
        let dir = tempfile::tempdir().unwrap();
        for rel in [
            "idx/cache/entry.bin",
            "idx/cache/entry.meta.json",
            "idx/segments/abc.seg",
            "ledger/rec.rec",
        ] {
            let target = dir.path().join(rel);
            put_content_addressed(rel, &target, b"first").unwrap();
            assert_eq!(std::fs::read(&target).unwrap(), b"first");
            put_content_addressed(rel, &target, b"second-longer").unwrap();
            assert_eq!(std::fs::read(&target).unwrap(), b"second-longer");
            let parent = target.parent().unwrap();
            let names: Vec<String> = std::fs::read_dir(parent)
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            assert!(
                names.iter().all(|n| !n.contains('#')),
                "no staging files left in {parent:?}: {names:?}"
            );
        }
        // `sync_dir_if_exists` succeeds whether `segments/` exists or not.
        sync_dir_if_exists(&dir.path().join("idx/segments")).unwrap();
        sync_dir_if_exists(&dir.path().join("absent/segments")).unwrap();
    }

    #[test]
    fn segments_defer_their_sync_only_where_a_directory_barrier_exists() {
        assert!(skips_file_sync("idx/cache/entry.bin"));
        assert!(skips_file_sync("idx/cache/entry.meta.json"));
        assert_eq!(skips_file_sync("idx/segments/abc.seg"), cfg!(unix));
        assert_eq!(skips_file_sync("idx/segments/abc.seg"), DIR_SYNC_BARRIER);
        assert!(!skips_file_sync("ledger/rec.rec"));
        assert!(!skips_file_sync("idx/manifests/00000000000000000001.json"));
    }
}
