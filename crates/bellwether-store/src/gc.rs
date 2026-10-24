//! Garbage collection of unreferenced segments (ADR 013), lock-free by
//! claim-then-delete.
//!
//! # Two windows, unrelated to each other
//!
//! - **Retention** is the snapshot-protection window. A segment is claimed
//!   only once its tombstone (the time it stopped being referenced) is older
//!   than retention, so a reader that opened a snapshot less than retention
//!   ago can still read every segment its snapshot references.
//! - **The claim lease** ([`GcOptions::claim_lease`], default one hour) is a
//!   liveness bound. A claim protects the run that made it and records that
//!   run's lease (`lease_ms`). Another run treats the claim as abandoned (its
//!   run died) and takes it over only once it is older than the **larger** of
//!   the recorded lease and its own, so runs configured with different
//!   leases stay safe: a short-lease run cannot take over a long-lease run's
//!   live claim. Retention never shortens or lengthens the lease.
//!
//! # A run
//!
//! Each run draws a random nonzero **token**.
//!
//! 1. **Claim.** One commit moves every unreferenced segment whose tombstone
//!    is older than retention from `segment_tombstones` into `deleting`,
//!    stamped with the run's time, token and lease, and takes over abandoned
//!    claims by re-stamping them. The claim set is recomputed from the
//!    winner's manifest on every rebase, so an id the winner re-referenced is
//!    never claimed. Claims already carrying the run's token count as the
//!    run's own: a claim commit that landed but was reported as failed is not
//!    orphaned.
//! 2. **Delete, fenced.** Files are deleted only after the claim commit
//!    succeeds, in batches ([`GcOptions::delete_batch`]).
//!    - **Fence.** Immediately before each delete the run checks whether
//!      half its lease has elapsed since just before the claim was stamped,
//!      on **either** clock: the monotonic clock, which excludes host sleep
//!      (`Instant` is `CLOCK_UPTIME_RAW` on macOS, and Linux's
//!      `CLOCK_MONOTONIC` excludes suspend too), or the wall clock, which
//!      includes it. If so it stops deleting ([`GcReport::fenced`]). A forward
//!      wall-clock step only stops a run early, which is safe. Half the lease
//!      leaves the other half as margin for clock skew between hosts and for
//!      the delete call itself.
//!    - **Re-read.** The manifest is re-read, and only ids still claimed
//!      with the run's token are deleted, at every batch boundary and before
//!      any delete once more than an eighth of the lease has passed since
//!      the last re-read (either clock). This is the second line of defence,
//!      for when the fence's margin is not enough. It acts only at those
//!      points: within a batch, between re-reads, a run deletes from its last
//!      reading. Losing a claim inside that window takes a takeover, and a
//!      takeover before the lease the holder recorded takes a taker clock
//!      skewed more than half a lease ahead (a full lease only for a holder that has just claimed), or a holder asleep for over half a
//!      lease (which the fence and the eighth-of-a-lease re-read catch on
//!      waking). `delete_batch: 1` re-checks before every delete, closing
//!      even the skew case at the cost of one manifest read per delete.
//! 3. **Scan.** Unless the run was fenced, the `segments/` listing is
//!    streamed one page at a time (ADR 018). A file with no reference,
//!    tombstone or claim is an orphan, for example a segment a builder has
//!    pre-uploaded and not committed yet. Orphans are never deleted on sight;
//!    at most [`GcOptions::max_orphans`] are processed per run and the rest
//!    are counted in [`GcReport::orphans_deferred`].
//! 4. **Release.** A final commit removes every claim carrying the run's
//!    token. Claimed ids the run did not delete (it was fenced, or a delete
//!    failed) get their original tombstone back, so a later run deletes them.
//!    Orphans found in the scan are tombstoned; a later run deletes them once
//!    retention has passed, unless a builder commits them first.
//!
//! If a delete or the scan fails, the run still releases its claims before
//! returning the error. If the claim commit itself fails with an unknown
//! outcome, the run attempts a release by token anyway.
//!
//! # Residual risk and the clock contract
//!
//! - **Pause between check and delete.** A run that pauses for longer than
//!   half the lease exactly between its fence check and its delete call can
//!   delete a segment after another run took the claim over, released it,
//!   and a writer re-added the segment. Leases without fencing tokens
//!   enforced by the store itself cannot close this window; the lease must be
//!   much longer than any plausible pause. The default of one hour is.
//! - **Clock skew.** Timestamps (`since`, `claimed_at`) are written by one
//!   host's wall clock and compared by another's. The contract requires skew
//!   between GC hosts to be well under half the lease. A taker whose clock
//!   runs more than half a lease ahead (a full lease only for a holder that has just claimed) can see a live claim as abandoned and take
//!   it over; that is outside the contract, and with the default batch size
//!   the holder can still delete, from its last reading, ids it lost within
//!   the current batch. `delete_batch: 1` closes even that. Skew also shifts
//!   the effective retention by the skew.
//! - **Crashes.** A run that dies after claiming leaves its ids blocked
//!   (commits referencing them are refused) until a later run finds the
//!   claims older than the lease.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use bellwether_types::{Hash, Result};

use crate::backend::{StoreExt, DEFAULT_PAGE_SIZE};
use crate::commit::{commit_inner, read_manifest, ClaimMode, CommitOptions, Rng, StagedSegments};
use crate::manifest::{
    join, now_rfc3339, parse_rfc3339, segment_path, DeleteClaim, Extra, Manifest, SegmentTombstone,
    SEGMENTS_DIR,
};

/// Default cap on orphans processed per run (ADR 018).
pub const DEFAULT_MAX_ORPHANS: usize = 10_000;
/// Default claim lease: how long a claim protects the run that made it.
pub const DEFAULT_CLAIM_LEASE: Duration = Duration::from_secs(3600);
/// Default number of ids deleted between manifest re-reads.
pub const DEFAULT_DELETE_BATCH: usize = 64;

/// Tunables for [`gc_with`].
#[derive(Clone, Copy, Debug)]
pub struct GcOptions {
    /// Listing page size.
    pub page_size: usize,
    /// Orphans tombstoned per run; the rest are deferred.
    pub max_orphans: usize,
    /// How long a claim protects the run that made it. It is recorded in the
    /// claim; another run takes the claim over only once it is older than
    /// the larger of the recorded lease and its own. A run stops deleting
    /// once half of its lease has elapsed since it claimed. A liveness bound,
    /// unrelated to retention. Zero makes the run fence before its first
    /// delete; it still cannot take over a live claim made with a longer
    /// lease.
    pub claim_lease: Duration,
    /// Ids deleted between manifest re-reads that confirm the run still
    /// holds its claims. A re-read also happens before any delete once more
    /// than an eighth of the lease has passed since the last one. `1`
    /// re-checks before every delete.
    pub delete_batch: usize,
}

impl Default for GcOptions {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            max_orphans: DEFAULT_MAX_ORPHANS,
            claim_lease: DEFAULT_CLAIM_LEASE,
            delete_batch: DEFAULT_DELETE_BATCH,
        }
    }
}

/// What a GC run did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Segment files deleted.
    pub deleted: usize,
    /// Claims older than the lease that this run took over.
    pub stale_claims_taken_over: usize,
    /// Unreferenced segments still inside the retention window.
    pub retained: usize,
    /// Orphan segment files newly tombstoned.
    pub tombstoned: usize,
    /// Orphans beyond [`GcOptions::max_orphans`], left for a later run.
    pub orphans_deferred: usize,
    /// The run stopped deleting because half the claim lease had elapsed
    /// since it claimed.
    pub fenced: bool,
    /// Claimed ids found claimed by another run at a re-read, so not deleted.
    pub claims_lost: usize,
    /// Claimed ids released without being deleted; their tombstones were
    /// restored for a later run.
    pub released_undeleted: usize,
}

/// Runs GC over the index at `root` with default options.
pub fn gc<S: StoreExt + ?Sized>(store: &S, root: &str, retention: Duration) -> Result<GcReport> {
    gc_with(store, root, retention, GcOptions::default())
}

/// Runs GC over the index at `root`; see the module docs.
pub fn gc_with<S: StoreExt + ?Sized>(
    store: &S,
    root: &str,
    retention: Duration,
    opts: GcOptions,
) -> Result<GcReport> {
    let Some((first, _)) = read_manifest(store, root)? else {
        return Ok(GcReport::default());
    };
    let run = Run::new(retention, opts.claim_lease)?;
    let mut report = GcReport::default();

    // Phase 1: claim.
    let claimed = match claim(store, root, &first, &run) {
        Ok(c) => c,
        Err(e) => {
            // The claim commit may have landed although it reported failure;
            // release by token so the ids are not blocked for a whole lease.
            let _ = release(
                store,
                root,
                &run,
                &Claimed::default(),
                &BTreeSet::new(),
                &BTreeSet::new(),
                &mut GcReport::default(),
            );
            return Err(e);
        }
    };
    report.stale_claims_taken_over = claimed.taken_over;

    // Phase 2: delete, fenced.
    let mut deleted = BTreeSet::new();
    let mut first_err = delete_claimed(
        store,
        root,
        &run,
        &opts,
        &claimed,
        &mut deleted,
        &mut report,
    )
    .err();

    // Phase 3: scan for orphans, unless the run must hurry to release.
    let mut orphans = BTreeSet::new();
    if !report.fenced && first_err.is_none() {
        first_err = scan(store, root, &opts, &mut orphans, &mut report).err();
    }

    // Phase 4: release this run's claims, restore what it did not delete,
    // tombstone orphans.
    let released = if claimed.ids.is_empty() && orphans.is_empty() {
        Ok(())
    } else {
        release(store, root, &run, &claimed, &deleted, &orphans, &mut report)
    };
    report.deleted = deleted.len();
    if let Some(e) = first_err {
        return Err(e);
    }
    released?;
    Ok(report)
}

/// One run's identity and clocks.
struct Run {
    token: u64,
    /// RFC 3339 stamp written into this run's claims.
    stamp: String,
    /// Monotonic time taken just before `stamp`, so the fence never measures
    /// less time than has passed since the claim was stamped.
    started: Instant,
    /// `stamp` parsed: the wall-clock start the fence also measures from.
    now: chrono::DateTime<chrono::Utc>,
    retention: Duration,
    lease: Duration,
}

/// Wall-clock time elapsed since `since`; zero if the clock stepped back.
fn wall_since(since: chrono::DateTime<chrono::Utc>) -> Duration {
    (chrono::Utc::now() - since).to_std().unwrap_or_default()
}

/// Elapsed time on whichever clock shows more: the monotonic clock
/// (`Instant`, which excludes host sleep: `CLOCK_UPTIME_RAW` on macOS, and
/// `CLOCK_MONOTONIC` on Linux excludes suspend too) or the wall clock (which
/// includes it). A forward wall-clock step can only make this larger, which
/// only makes a run stop or re-check earlier.
struct Stopwatch {
    mono: Instant,
    wall: chrono::DateTime<chrono::Utc>,
}

impl Stopwatch {
    fn start() -> Self {
        Self {
            mono: Instant::now(),
            wall: chrono::Utc::now(),
        }
    }

    fn elapsed(&self) -> Duration {
        self.mono.elapsed().max(wall_since(self.wall))
    }
}

impl Run {
    fn new(retention: Duration, lease: Duration) -> Result<Self> {
        let started = Instant::now();
        let stamp = now_rfc3339();
        let now = parse_rfc3339(&stamp)?;
        Ok(Self {
            // xorshift output from a nonzero state is never zero; `max`
            // keeps the "0 never matches" rule obvious.
            token: Rng::seeded().next_u64().max(1),
            stamp,
            started,
            now,
            retention,
            lease,
        })
    }

    /// Whether `ts` is at least `age` before this run's `now`. An unreadable
    /// timestamp is never old.
    fn older_than(&self, ts: &str, age: Duration) -> bool {
        parse_rfc3339(ts).is_ok_and(|t| (self.now - t).to_std().unwrap_or_default() >= age)
    }

    /// Whether this run must stop deleting: half its lease has elapsed on
    /// either clock. The monotonic clock alone is not enough: it excludes
    /// host sleep, so a holder suspended for longer than its lease would
    /// wake with an almost-zero fence. The wall clock includes the sleep.
    fn fenced(&self) -> bool {
        let half = self.lease / 2;
        self.started.elapsed() >= half || wall_since(self.now) >= half
    }

    /// This run's lease as recorded in its claims.
    fn lease_ms(&self) -> u64 {
        u64::try_from(self.lease.as_millis()).unwrap_or(u64::MAX)
    }

    /// Whether another run's claim is abandoned: older than the larger of
    /// the lease its holder recorded and this run's own lease. The holder
    /// fences at half its recorded lease, so a taker with a shorter lease
    /// can never take over a claim whose holder may still delete.
    fn is_abandoned(&self, c: &DeleteClaim) -> bool {
        let lease = Duration::from_millis(c.lease_ms).max(self.lease);
        self.older_than(&c.claimed_at, lease)
    }
}

/// The outcome of the claim phase.
#[derive(Default)]
struct Claimed {
    /// Ids claimed with this run's token in the committed manifest.
    ids: BTreeSet<Hash>,
    /// The tombstone `since` to restore for an id released undeleted.
    restore_since: BTreeMap<Hash, String>,
    /// Of `ids`, how many were taken over from an abandoned claim.
    taken_over: usize,
}

/// Phase 1: one commit claims expired tombstones and takes over claims older
/// than the lease, recomputed from the winner on every rebase. Idempotent:
/// claims already carrying this run's token are the run's own.
fn claim<S: StoreExt + ?Sized>(
    store: &S,
    root: &str,
    first: &Manifest,
    run: &Run,
) -> Result<Claimed> {
    let work_exists = first
        .segment_tombstones
        .iter()
        .any(|t| run.older_than(&t.since, run.retention))
        || first
            .deleting
            .iter()
            .any(|c| c.token != run.token && run.is_abandoned(c));
    if !work_exists {
        return Ok(Claimed::default());
    }
    // Kept across attempts: an attempt that landed but was reported as
    // failed recorded what to restore for the claims it made.
    let mut restore_since: BTreeMap<Hash, String> = BTreeMap::new();
    let mut taken: BTreeSet<Hash> = BTreeSet::new();
    let mut ids: BTreeSet<Hash> = BTreeSet::new();
    commit_inner(
        store,
        root,
        &StagedSegments::new(),
        CommitOptions::default(),
        ClaimMode::Manage,
        |m| {
            let mut m = m.clone();
            let live = m.segment_ids();
            let mut mine = BTreeSet::new();
            for c in &mut m.deleting {
                if c.token == run.token {
                    mine.insert(c.id);
                } else if run.is_abandoned(c) && !live.contains(&c.id) {
                    restore_since
                        .entry(c.id)
                        .or_insert_with(|| c.claimed_at.clone());
                    c.claimed_at.clone_from(&run.stamp);
                    c.token = run.token;
                    c.lease_ms = run.lease_ms();
                    taken.insert(c.id);
                    mine.insert(c.id);
                }
            }
            let held: BTreeSet<Hash> = m.deleting.iter().map(|c| c.id).collect();
            let mut keep = Vec::with_capacity(m.segment_tombstones.len());
            for t in std::mem::take(&mut m.segment_tombstones) {
                if run.older_than(&t.since, run.retention)
                    && !live.contains(&t.id)
                    && !held.contains(&t.id)
                {
                    restore_since.entry(t.id).or_insert_with(|| t.since.clone());
                    mine.insert(t.id);
                    m.deleting.push(DeleteClaim {
                        id: t.id,
                        claimed_at: run.stamp.clone(),
                        token: run.token,
                        lease_ms: run.lease_ms(),
                        extra: Extra::new(),
                    });
                } else {
                    keep.push(t);
                }
            }
            m.segment_tombstones = keep;
            ids = mine;
            Ok(m)
        },
    )?;
    let taken_over = ids.intersection(&taken).count();
    Ok(Claimed {
        ids,
        restore_since,
        taken_over,
    })
}

/// Phase 2: deletes the claimed ids in batches. The manifest is re-read, to
/// keep only ids still claimed with this run's token, at every batch
/// boundary and before any delete once more than an eighth of the lease has
/// passed since the last re-read on either clock (a host that slept mid-batch
/// re-checks on waking). The fence is checked immediately before each
/// delete.
fn delete_claimed<S: StoreExt + ?Sized>(
    store: &S,
    root: &str,
    run: &Run,
    opts: &GcOptions,
    claimed: &Claimed,
    deleted: &mut BTreeSet<Hash>,
    report: &mut GcReport,
) -> Result<()> {
    let reread_after = run.lease / 8;
    let ids: Vec<Hash> = claimed.ids.iter().copied().collect();
    for batch in ids.chunks(opts.delete_batch.max(1)) {
        if run.fenced() {
            report.fenced = true;
            return Ok(());
        }
        let mut ours = still_ours(store, root, run)?;
        let mut read_at = Stopwatch::start();
        for id in batch {
            if read_at.elapsed() > reread_after {
                ours = still_ours(store, root, run)?;
                read_at = Stopwatch::start();
            }
            if !ours.contains(id) {
                report.claims_lost += 1;
                continue;
            }
            if run.fenced() {
                report.fenced = true;
                return Ok(());
            }
            store.delete(&segment_path(root, id))?;
            deleted.insert(*id);
        }
    }
    Ok(())
}

/// The ids the current manifest shows as claimed with this run's token.
fn still_ours<S: StoreExt + ?Sized>(store: &S, root: &str, run: &Run) -> Result<BTreeSet<Hash>> {
    Ok(read_manifest(store, root)?
        .map(|(m, _)| {
            m.deleting
                .iter()
                .filter(|c| c.token == run.token)
                .map(|c| c.id)
                .collect()
        })
        .unwrap_or_default())
}

/// Phase 3: streams `segments/` and collects up to `max_orphans` orphans.
fn scan<S: StoreExt + ?Sized>(
    store: &S,
    root: &str,
    opts: &GcOptions,
    orphans: &mut BTreeSet<Hash>,
    report: &mut GcReport,
) -> Result<()> {
    let Some((current, _)) = read_manifest(store, root)? else {
        return Ok(());
    };
    let live = current.segment_ids();
    let known: BTreeSet<Hash> = current
        .segment_tombstones
        .iter()
        .map(|t| t.id)
        .chain(current.deleting.iter().map(|c| c.id))
        .collect();
    store.list_pages(&join(root, SEGMENTS_DIR), opts.page_size, &mut |page| {
        for meta in page {
            let Some(id) = segment_id_of(meta.location.as_ref()) else {
                continue;
            };
            if live.contains(&id) {
                continue;
            }
            if known.contains(&id) {
                if current.is_tombstoned(&id) {
                    report.retained += 1;
                }
            } else if orphans.len() < opts.max_orphans {
                orphans.insert(id);
            } else {
                report.orphans_deferred += 1;
            }
        }
        Ok(ControlFlow::Continue(()))
    })
}

/// Phase 4: removes every claim carrying this run's token, restores the
/// tombstone of each claimed id the run did not delete, and tombstones
/// orphans. Recomputed from the winner on every rebase.
fn release<S: StoreExt + ?Sized>(
    store: &S,
    root: &str,
    run: &Run,
    claimed: &Claimed,
    deleted: &BTreeSet<Hash>,
    orphans: &BTreeSet<Hash>,
    report: &mut GcReport,
) -> Result<()> {
    let mut restored = 0;
    let mut tombstoned = 0;
    commit_inner(
        store,
        root,
        &StagedSegments::new(),
        CommitOptions::default(),
        ClaimMode::Manage,
        |m| {
            let mut m = m.clone();
            let mut undeleted = Vec::new();
            m.deleting.retain(|c| {
                if c.token != run.token {
                    return true;
                }
                if !deleted.contains(&c.id) {
                    undeleted.push(c.id);
                }
                false
            });
            restored = 0;
            for id in undeleted {
                if !m.references(&id) && !m.is_tombstoned(&id) {
                    let since = claimed
                        .restore_since
                        .get(&id)
                        .cloned()
                        .unwrap_or_else(|| run.stamp.clone());
                    m.segment_tombstones.push(SegmentTombstone {
                        id,
                        since,
                        extra: Extra::new(),
                    });
                    restored += 1;
                }
            }
            tombstoned = 0;
            for id in orphans {
                if !m.references(id) && !m.is_tombstoned(id) && !m.is_claimed(id) {
                    m.segment_tombstones.push(SegmentTombstone::now(*id));
                    tombstoned += 1;
                }
            }
            Ok(m)
        },
    )?;
    report.released_undeleted = restored;
    report.tombstoned = tombstoned;
    Ok(())
}

/// Parses `.../segments/<hex>.seg` into its id.
fn segment_id_of(path: &str) -> Option<Hash> {
    let name = path.rsplit('/').next()?;
    Hash::from_hex(name.strip_suffix(".seg")?).ok()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    /// A run whose claim was stamped `ago` (wall clock) but whose monotonic
    /// clock starts now, as for a holder whose host slept for `ago`.
    fn run_stamped(ago: Duration, lease: Duration) -> Run {
        let now = chrono::Utc::now() - chrono::Duration::from_std(ago).unwrap();
        Run {
            token: 1,
            stamp: now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            started: Instant::now(),
            now,
            retention: Duration::ZERO,
            lease,
        }
    }

    /// The suspended-holder gap: `Instant` excludes host sleep, so after a
    /// long sleep the monotonic fence reads almost zero. The wall clock
    /// includes the sleep, so the run is fenced anyway.
    #[test]
    fn fence_counts_wall_clock_time_the_monotonic_clock_missed() {
        let lease = Duration::from_secs(3600);
        assert!(run_stamped(lease / 2, lease).fenced());
        assert!(run_stamped(lease * 2, lease).fenced());
        assert!(!run_stamped(lease / 4, lease).fenced());
        assert!(!run_stamped(Duration::ZERO, lease).fenced());
    }

    /// A backward wall-clock step reads as zero wall time; the monotonic
    /// clock still fences.
    #[test]
    fn fence_falls_back_to_the_monotonic_clock() {
        let lease = Duration::from_millis(20);
        let mut r = run_stamped(Duration::ZERO, lease);
        r.now = chrono::Utc::now() + chrono::Duration::hours(5);
        assert!(!r.fenced());
        r.started = Instant::now()
            .checked_sub(Duration::from_millis(15))
            .unwrap();
        assert!(r.fenced(), "15 ms monotonic >= half of a 20 ms lease");
    }

    #[test]
    fn stopwatch_takes_the_larger_clock() {
        let mut sw = Stopwatch::start();
        sw.wall = chrono::Utc::now() - chrono::Duration::seconds(90);
        assert!(sw.elapsed() >= Duration::from_secs(90));
        let mut sw = Stopwatch::start();
        sw.wall = chrono::Utc::now() + chrono::Duration::seconds(90);
        assert!(sw.elapsed() < Duration::from_secs(5));
    }
}
