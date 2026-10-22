//! Bounded write-behind SQLite page cache. SQL transactions commit to memory;
//! a dedicated worker publishes complete, atomic database-file snapshots.
//! A crash can lose recent commits, but never exposes a half-written snapshot.
use crate::memory::{Budget, Lease};
use sqlite_vfs::{DatabaseHandle, LockKind, OpenKind, OpenOptions, Vfs, WalDisabled};
use std::{
    collections::{BTreeSet, HashMap},
    fs::{File, OpenOptions as FsOptions},
    io::{self, ErrorKind},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

const PAGE: usize = 4096;
const COST: usize = PAGE + 512;
struct Page {
    data: [u8; PAGE],
    dirty: AtomicBool,
    _lease: Lease,
}
struct Cached {
    page: Arc<Page>,
    touched: u64,
}
struct State {
    file: Arc<File>,
    size: u64,
    pages: HashMap<u64, Cached>,
    lru: BTreeSet<(u64, u64)>,
    clock: u64,
    pending: HashMap<u64, Arc<Page>>,
    pending_size: u64,
    generation: u64,
    persisted: u64,
    flushing: bool,
    managed_lifecycle: bool,
    stop: bool,
    error: Option<String>,
}
pub struct Store {
    state: Mutex<State>,
    wake: Condvar,
    budget: Arc<Budget>,
    path: PathBuf,
    // Prevent two CCM writers using different memory views of the same file.
    _owner: File,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    vfs_name: std::sync::OnceLock<String>,
    snapshots: Mutex<HashMap<String, Arc<Snapshot>>>,
    active_readers: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    delay: std::sync::atomic::AtomicU64,
}

struct Snapshot {
    file: Arc<File>,
    size: u64,
    pages: HashMap<u64, Arc<Page>>,
    lease: Option<Lease>,
    store: std::sync::Weak<Store>,
}
impl Drop for Snapshot {
    fn drop(&mut self) {
        self.pages.clear();
        self.lease.take();
        if let Some(store) = self.store.upgrade() {
            let _guard = store.state.lock().unwrap();
            store.active_readers.fetch_sub(1, Ordering::Relaxed);
            store.wake.notify_all();
        }
    }
}

fn error(text: impl ToString) -> io::Error {
    io::Error::other(text.to_string())
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

impl Store {
    pub fn open(path: &Path, limit: usize) -> io::Result<Arc<Self>> {
        let absolute = std::path::absolute(path)?;
        let path = if absolute.exists() {
            std::fs::canonicalize(absolute)?
        } else {
            std::fs::canonicalize(
                absolute
                    .parent()
                    .ok_or_else(|| error("database has no parent"))?,
            )?
            .join(
                absolute
                    .file_name()
                    .ok_or_else(|| error("database has no filename"))?,
            )
        };
        let owner = FsOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(sidecar(&path, ".ccm-lock"))?;
        fs2::FileExt::try_lock_exclusive(&owner)?;
        // Recover any legacy SQLite journal/WAL before taking ownership of pages.
        {
            let conn = rusqlite::Connection::open(&path).map_err(error)?;
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")
                .map_err(error)?;
        }
        let file = Arc::new(File::open(&path)?);
        let size = file.metadata()?.len();
        let store = Arc::new(Self {
            state: Mutex::new(State {
                file,
                size,
                pages: HashMap::new(),
                lru: BTreeSet::new(),
                clock: 0,
                pending: HashMap::new(),
                pending_size: size,
                generation: 0,
                persisted: 0,
                flushing: false,
                managed_lifecycle: false,
                stop: false,
                error: None,
            }),
            wake: Condvar::new(),
            budget: Budget::new(limit),
            path,
            _owner: owner,
            worker: Mutex::new(None),
            vfs_name: std::sync::OnceLock::new(),
            snapshots: Mutex::new(HashMap::new()),
            active_readers: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            delay: std::sync::atomic::AtomicU64::new(0),
        });
        let weak = Arc::downgrade(&store);
        *store.worker.lock().unwrap() = Some(
            std::thread::Builder::new()
                .name("ccm-persistence".into())
                .spawn(move || {
                    while let Some(store) = weak.upgrade() {
                        let mut s = store.state.lock().unwrap();
                        if s.stop && s.generation == s.persisted {
                            break;
                        }
                        if s.generation == s.persisted {
                            let (next, _) = store
                                .wake
                                .wait_timeout(s, Duration::from_millis(100))
                                .unwrap();
                            drop(next);
                            continue;
                        }
                        // Fixed batching window; never reset it for each incoming token.
                        if !s.stop {
                            drop(s);
                            std::thread::sleep(Duration::from_millis(250));
                            s = store.state.lock().unwrap();
                        }
                        let pages = std::mem::take(&mut s.pending);
                        let size = s.pending_size;
                        let generation = s.generation;
                        let base = s.file.clone();
                        s.flushing = true;
                        drop(s);
                        #[cfg(test)]
                        std::thread::sleep(Duration::from_millis(
                            store.delay.load(Ordering::Relaxed),
                        ));
                        let result = store.publish(&base, &pages, size);
                        let mut s = store.state.lock().unwrap();
                        s.flushing = false;
                        match result {
                            Ok(file) => {
                                s.file = Arc::new(file);
                                s.persisted = generation;
                                for page in pages.values() {
                                    page.dirty.store(false, Ordering::Release);
                                }
                            }
                            Err(e) => {
                                s.error = Some(e.to_string());
                                tracing::error!(error=%e, "database persistence stopped");
                                store.wake.notify_all();
                                break;
                            }
                        }
                        drop(pages);
                        store.wake.notify_all();
                    }
                })?,
        );
        Ok(store)
    }

    fn publish(&self, base: &File, pages: &HashMap<u64, Arc<Page>>, size: u64) -> io::Result<File> {
        let temp = sidecar(&self.path, ".ccm-next");
        let mut out = FsOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp)?;
        out.set_permissions(base.metadata()?.permissions())?;
        // Independent FD position, no cache/state mutex held during disk work.
        let mut input = base.try_clone()?;
        use std::io::{Seek, SeekFrom};
        input.seek(SeekFrom::Start(0))?;
        // Reflink keeps snapshot publication proportional to changed pages on
        // supporting filesystems (including this deployment's btrfs).
        #[cfg(target_os = "linux")]
        let cloned = {
            use std::os::fd::AsRawFd;
            // SAFETY: both live descriptors name regular files; FICLONE takes an fd.
            unsafe {
                libc::ioctl(
                    std::os::fd::AsRawFd::as_raw_fd(&out),
                    0x40049409 as libc::c_ulong,
                    input.as_raw_fd(),
                ) == 0
            }
        };
        #[cfg(not(target_os = "linux"))]
        let cloned = false;
        if !cloned {
            io::copy(&mut input, &mut out)?;
        }
        out.set_len(size)?;
        for (index, page) in pages {
            let offset = index * PAGE as u64;
            if offset < size {
                let len = ((size - offset) as usize).min(PAGE);
                out.write_all_at(&page.data[..len], offset)?;
            }
        }
        out.sync_all()?;
        std::fs::rename(&temp, &self.path)?;
        if let Some(parent) = self.path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(out)
    }

    pub fn flush(&self) -> io::Result<()> {
        let mut s = self.state.lock().unwrap();
        let target = s.generation;
        self.wake.notify_all();
        while s.persisted < target {
            if let Some(e) = &s.error {
                return Err(error(e));
            }
            s = self.wake.wait(s).unwrap();
        }
        Ok(())
    }

    /// Await publication without blocking a Tokio worker. This is a durability
    /// barrier for callers that must not acknowledge a memory-visible commit
    /// before the corresponding database snapshot has reached disk.
    pub async fn flush_async(self: &Arc<Self>) -> Result<(), String> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.flush())
            .await
            .map_err(|error| format!("database flush task failed: {error}"))?
            .map_err(|error| error.to_string())
    }

    /// Mark this Store as owned by a longer-lived process lifecycle. In that mode,
    /// dropping a scoped Portfolio view must not stop the shared persistence worker.
    pub fn manage_lifecycle(&self) {
        self.state.lock().unwrap().managed_lifecycle = true;
    }

    /// Portfolio Drop calls this for both the root and scoped views. Preserve the
    /// legacy terminal behavior for standalone Stores, but only flush a process-
    /// managed shared Store until its owner explicitly calls `shutdown`.
    pub fn stop(&self) -> io::Result<()> {
        let managed = {
            let s = self.state.lock().unwrap();
            s.managed_lifecycle && !s.stop
        };
        if managed {
            self.flush()
        } else {
            self.shutdown()
        }
    }

    /// Fence new commits, persist every commit accepted before the fence, and join
    /// the persistence worker. The state mutex linearizes this against VFS sync:
    /// a sync before the fence advances `generation` and is flushed; a sync after
    /// the fence is rejected.
    pub fn shutdown(&self) -> io::Result<()> {
        {
            let mut s = self.state.lock().unwrap();
            s.stop = true;
            self.wake.notify_all();
        }
        let flush_result = self.flush();
        let join_result = self
            .worker
            .lock()
            .unwrap()
            .take()
            .map(|worker| {
                worker
                    .join()
                    .map_err(|_| error("persistence worker panicked"))
            })
            .unwrap_or(Ok(()));
        flush_result?;
        join_result?;
        Ok(())
    }

    pub fn metrics(&self) -> serde_json::Value {
        let s = self.state.lock().unwrap();
        serde_json::json!({"cache_bytes":self.budget.used(),"cache_limit_bytes":self.budget.limit(),
            "commit_generation":s.generation,"persisted_generation":s.persisted,
            "pending_pages":s.pending.len(),"flushing":s.flushing,"error":s.error})
    }

    fn allocate(&self) -> io::Result<Lease> {
        self.allocate_size(COST)
    }

    fn allocate_size(&self, bytes: usize) -> io::Result<Lease> {
        if bytes > self.budget.limit() {
            return Err(error("record exceeds database cache budget"));
        }
        loop {
            if let Some(lease) = self.budget.try_acquire(bytes) {
                return Ok(lease);
            }
            let mut s = self.state.lock().unwrap();
            if s.stop {
                return Err(error("database is shutting down"));
            }
            if let Some(e) = &s.error {
                return Err(error(e));
            }
            let oldest = s
                .lru
                .iter()
                .find(|(_, i)| {
                    s.pages.get(i).is_some_and(|p| {
                        !p.page.dirty.load(Ordering::Acquire) && Arc::strong_count(&p.page) == 1
                    })
                })
                .copied();
            if let Some((tick, index)) = oldest {
                s.lru.remove(&(tick, index));
                s.pages.remove(&index);
                continue;
            }
            if !s.flushing
                && s.pending.is_empty()
                && self.active_readers.load(Ordering::Relaxed) == 0
            {
                return Err(error("transaction exceeds database memory budget"));
            }
            // Only exhausted dirty capacity can exert disk backpressure.
            s = self.wake.wait(s).unwrap();
            drop(s);
        }
    }

    /// Called at a SQL operation boundary under the execution connection mutex.
    /// The mutex is released before any dashboard query runs on this immutable view.
    pub fn read_connection(self: &Arc<Self>) -> Result<rusqlite::Connection, String> {
        // Reserve for the largest possible index so concurrent cold readers
        // warming clean pages cannot grow a snapshot beyond its admission cost.
        let cost = 128 * 1024 + (self.budget.limit() / COST) * 64;
        let lease = self.allocate_size(cost).map_err(|e| e.to_string())?;
        let snapshot = {
            let s = self.state.lock().unwrap();
            self.active_readers.fetch_add(1, Ordering::Relaxed);
            Arc::new(Snapshot {
                file: s.file.clone(),
                size: s.size,
                pages: s.pages.iter().map(|(i, p)| (*i, p.page.clone())).collect(),
                lease: Some(lease),
                store: Arc::downgrade(self),
            })
        };
        let path = format!("{}.snapshot-{}", self.path.display(), uuid::Uuid::new_v4());
        self.snapshots
            .lock()
            .unwrap()
            .insert(path.clone(), snapshot);
        let result = rusqlite::Connection::open_with_flags_and_vfs(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            self.vfs_name.get().unwrap(),
        )
        .map_err(|e| e.to_string());
        self.snapshots.lock().unwrap().remove(&path);
        let conn = result?;
        conn.execute_batch(
            "PRAGMA cache_size=-64; PRAGMA temp_store=MEMORY; PRAGMA query_only=ON;",
        )
        .map_err(|e| e.to_string())?;
        Ok(conn)
    }

    fn page(&self, index: u64) -> io::Result<Arc<Page>> {
        {
            let mut s = self.state.lock().unwrap();
            s.clock += 1;
            let tick = s.clock;
            if let Some(p) = s.pages.get_mut(&index) {
                let old = p.touched;
                p.touched = tick;
                let page = p.page.clone();
                s.lru.remove(&(old, index));
                s.lru.insert((tick, index));
                return Ok(page);
            }
        }
        let lease = self.allocate()?;
        let mut data = [0; PAGE];
        let mut s = self.state.lock().unwrap();
        // Only persisted pages are evictable. A cold read never misses newer writes.
        let file_len = s.file.metadata()?.len();
        let offset = index * PAGE as u64;
        if offset < file_len {
            s.file.read_exact_at(
                &mut data[..((file_len - offset) as usize).min(PAGE)],
                offset,
            )?;
        }
        let page = Arc::new(Page {
            data,
            dirty: AtomicBool::new(false),
            _lease: lease,
        });
        let tick = s.clock;
        s.pages.insert(
            index,
            Cached {
                page: page.clone(),
                touched: tick,
            },
        );
        s.lru.insert((tick, index));
        Ok(page)
    }
}

struct MemoryVfs {
    store: std::sync::Weak<Store>,
}
pub struct Handle {
    store: Arc<Store>,
    lock: LockKind,
    writes: HashMap<u64, Arc<Page>>,
    snapshot: Option<Arc<Snapshot>>,
}

impl DatabaseHandle for Handle {
    type WalIndex = WalDisabled;
    fn size(&self) -> io::Result<u64> {
        Ok(self
            .snapshot
            .as_ref()
            .map(|s| s.size)
            .unwrap_or_else(|| self.store.state.lock().unwrap().size))
    }
    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let size = self.size()?;
        buf.fill(0);
        let available = size.saturating_sub(offset).min(buf.len() as u64) as usize;
        let mut done = 0;
        while done < available {
            let at = offset as usize + done;
            let start = at % PAGE;
            let n = (PAGE - start).min(available - done);
            let index = (at / PAGE) as u64;
            if let Some(snapshot) = &self.snapshot {
                if let Some(page) = snapshot.pages.get(&index) {
                    buf[done..done + n].copy_from_slice(&page.data[start..start + n]);
                } else {
                    // A cold history read can warm the shared cache, but must
                    // never substitute a newer page into an older snapshot.
                    let cached = {
                        let s = self.store.state.lock().unwrap();
                        if Arc::ptr_eq(&s.file, &snapshot.file) {
                            s.pages
                                .get(&index)
                                .filter(|p| !p.page.dirty.load(Ordering::Acquire))
                                .map(|p| p.page.clone())
                        } else {
                            None
                        }
                    };
                    if let Some(page) = cached {
                        buf[done..done + n].copy_from_slice(&page.data[start..start + n]);
                    } else {
                        let mut data = [0; PAGE];
                        let offset = index * PAGE as u64;
                        let count = snapshot
                            .file
                            .metadata()?
                            .len()
                            .saturating_sub(offset)
                            .min(PAGE as u64) as usize;
                        snapshot.file.read_exact_at(&mut data[..count], offset)?;
                        buf[done..done + n].copy_from_slice(&data[start..start + n]);
                        // Cache admission is optional; history readers do not
                        // wait for persistence merely to populate clean pages.
                        if let Some(lease) = self.store.budget.try_acquire(COST) {
                            let mut s = self.store.state.lock().unwrap();
                            if Arc::ptr_eq(&s.file, &snapshot.file) && !s.pages.contains_key(&index)
                            {
                                s.clock += 1;
                                let touched = s.clock;
                                s.pages.insert(
                                    index,
                                    Cached {
                                        page: Arc::new(Page {
                                            data,
                                            dirty: AtomicBool::new(false),
                                            _lease: lease,
                                        }),
                                        touched,
                                    },
                                );
                                s.lru.insert((touched, index));
                            }
                        }
                    }
                }
            } else {
                let page = self.store.page(index)?;
                buf[done..done + n].copy_from_slice(&page.data[start..start + n]);
            }
            done += n;
        }
        if available < buf.len() {
            Err(io::Error::from(ErrorKind::UnexpectedEof))
        } else {
            Ok(())
        }
    }
    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
        if self.snapshot.is_some() {
            return Err(error("read-only memory snapshot"));
        }
        let mut done = 0;
        while done < buf.len() {
            let at = offset as usize + done;
            let index = (at / PAGE) as u64;
            let start = at % PAGE;
            let n = (PAGE - start).min(buf.len() - done);
            let mut data = if start == 0 && n == PAGE {
                [0; PAGE]
            } else {
                self.store.page(index)?.data
            };
            data[start..start + n].copy_from_slice(&buf[done..done + n]);
            let page = Arc::new(Page {
                data,
                dirty: AtomicBool::new(true),
                _lease: self.store.allocate()?,
            });
            let mut s = self.store.state.lock().unwrap();
            if s.stop {
                return Err(error("database is shutting down"));
            }
            if let Some(e) = &s.error {
                return Err(error(e));
            }
            s.clock += 1;
            let touched = s.clock;
            if let Some(old) = s.pages.insert(
                index,
                Cached {
                    page: page.clone(),
                    touched,
                },
            ) {
                s.lru.remove(&(old.touched, index));
            }
            s.lru.insert((touched, index));
            self.writes.insert(index, page);
            s.size = s.size.max(offset + buf.len() as u64);
            done += n;
        }
        Ok(())
    }
    fn sync(&mut self, _data_only: bool) -> io::Result<()> {
        if self.snapshot.is_some() {
            return Ok(());
        }
        let mut s = self.store.state.lock().unwrap();
        if s.stop {
            return Err(error("database is shutting down"));
        }
        if let Some(e) = &s.error {
            return Err(error(e));
        }
        s.pending.extend(self.writes.drain());
        s.pending_size = s.size;
        s.generation += 1;
        self.store.wake.notify_all();
        Ok(())
    }
    fn set_len(&mut self, size: u64) -> io::Result<()> {
        if self.snapshot.is_some() {
            return Err(error("read-only memory snapshot"));
        }
        let mut s = self.store.state.lock().unwrap();
        if s.stop {
            return Err(error("database is shutting down"));
        }
        s.size = size;
        s.pages.retain(|i, _| i * (PAGE as u64) < size);
        s.lru.retain(|(_, i)| i * (PAGE as u64) < size);
        self.writes.retain(|i, _| i * (PAGE as u64) < size);
        Ok(())
    }
    fn lock(&mut self, lock: LockKind) -> io::Result<bool> {
        self.lock = lock;
        Ok(true)
    }
    fn reserved(&mut self) -> io::Result<bool> {
        Ok(matches!(
            self.lock,
            LockKind::Reserved | LockKind::Pending | LockKind::Exclusive
        ))
    }
    fn current_lock(&self) -> io::Result<LockKind> {
        Ok(self.lock)
    }
    fn wal_index(&self, _: bool) -> io::Result<WalDisabled> {
        Ok(WalDisabled)
    }
}

impl Vfs for MemoryVfs {
    type Handle = Handle;
    fn open(&self, db: &str, opts: OpenOptions) -> io::Result<Handle> {
        if opts.kind != OpenKind::MainDb {
            return Err(error(
                "only main database files supported; journals/temp must use memory",
            ));
        }
        let store = self
            .store
            .upgrade()
            .ok_or_else(|| error("closed storage"))?;
        let snapshot = store.snapshots.lock().unwrap().get(db).cloned();
        if snapshot.is_none() && Path::new(db) != store.path {
            return Err(error("unknown memory database"));
        }
        Ok(Handle {
            store,
            snapshot,
            lock: LockKind::None,
            writes: HashMap::new(),
        })
    }
    fn delete(&self, _: &str) -> io::Result<()> {
        Err(error("cannot delete live database"))
    }
    fn exists(&self, db: &str) -> io::Result<bool> {
        Ok(self.store.upgrade().is_some_and(|s| {
            Path::new(db) == s.path || s.snapshots.lock().unwrap().contains_key(db)
        }))
    }
    fn temporary_name(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }
    fn random(&self, buffer: &mut [i8]) {
        for chunk in buffer.chunks_mut(16) {
            for (dst, src) in chunk.iter_mut().zip(uuid::Uuid::new_v4().as_bytes()) {
                *dst = *src as i8;
            }
        }
    }
    fn sleep(&self, duration: Duration) -> Duration {
        let start = Instant::now();
        std::thread::sleep(duration);
        start.elapsed()
    }
}

pub fn connection(path: &Path, limit: usize) -> Result<(rusqlite::Connection, Arc<Store>), String> {
    let store = Store::open(path, limit).map_err(|e| e.to_string())?;
    let name = format!("ccm-memory-{}", uuid::Uuid::new_v4());
    sqlite_vfs::register(
        &name,
        MemoryVfs {
            store: Arc::downgrade(&store),
        },
        false,
    )
    .map_err(|e| e.to_string())?;
    let _ = store.vfs_name.set(name.clone());
    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        &store.path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        &name,
    )
    .map_err(|e| e.to_string())?;
    conn.execute_batch("PRAGMA journal_mode=MEMORY; PRAGMA synchronous=FULL; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-1024;").map_err(|e|e.to_string())?;
    Ok((conn, store))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn path() -> PathBuf {
        std::env::temp_dir().join(format!("ccm-memory-{}.db", uuid::Uuid::new_v4()))
    }
    fn cleanup(path: &Path) {
        for p in [
            path.to_path_buf(),
            path.with_extension("db.ccm-lock"),
            path.with_extension("db.ccm-next"),
        ] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn slow_disk_does_not_delay_memory_commits_and_publishes_whole_snapshots() {
        let path = path();
        let (conn, store) = connection(&path, 8 * 1024 * 1024).unwrap();
        conn.execute_batch("CREATE TABLE values_test(n INTEGER PRIMARY KEY, text TEXT);")
            .unwrap();
        store.flush().unwrap();
        store.delay.store(1500, Ordering::Relaxed);
        conn.execute("INSERT INTO values_test VALUES(0,'first')", [])
            .unwrap();
        let until = Instant::now();
        while !store.state.lock().unwrap().flushing {
            assert!(until.elapsed() < Duration::from_secs(3));
            std::thread::sleep(Duration::from_millis(5));
        }
        let start = Instant::now();
        for n in 1..500 {
            conn.execute("INSERT INTO values_test VALUES(?1,'memory')", [n])
                .unwrap();
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(1000),
            "memory operations waited for disk: {elapsed:?}"
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM values_test", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            500
        );
        let disk = rusqlite::Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        assert_eq!(
            disk.query_row("SELECT COUNT(*) FROM values_test", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        drop(disk);
        store.delay.store(0, Ordering::Relaxed);
        store.flush().unwrap();
        let disk = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            disk.query_row("SELECT COUNT(*) FROM values_test", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            500
        );
        assert_eq!(
            disk.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        eprintln!("500 memory commits/read with 1500ms disk delay: {elapsed:?}");
        drop(disk);
        drop(conn);
        store.shutdown().unwrap();
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn bounded_eviction_reads_old_history_and_rollback_survives_reopen() {
        let path = path();
        let limit = 128 * 1024;
        let (conn, store) = connection(&path, limit).unwrap();
        conn.execute_batch("CREATE TABLE history(n INTEGER PRIMARY KEY, text TEXT);")
            .unwrap();
        let text = "x".repeat(4096);
        for n in 0..100 {
            conn.execute(
                "INSERT INTO history VALUES(?1,?2)",
                rusqlite::params![n, text],
            )
            .unwrap();
            if n % 10 == 0 {
                store.flush().unwrap();
            }
            assert!(store.budget.used() <= limit);
        }
        conn.execute_batch("BEGIN; DELETE FROM history; ROLLBACK;")
            .unwrap();
        store.flush().unwrap();
        conn.execute_batch("PRAGMA cache_size=-8;").unwrap();
        assert_eq!(
            conn.query_row("SELECT length(text) FROM history WHERE n=0", [], |r| r
                .get::<_, usize>(0))
                .unwrap(),
            4096
        );
        assert!(store.budget.used() <= limit);
        drop(conn);
        store.shutdown().unwrap();
        drop(store);
        let disk = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            disk.query_row("SELECT COUNT(*) FROM history", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            100
        );
        assert_eq!(
            disk.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        drop(disk);
        cleanup(&path);
    }

    #[test]
    fn shutdown_flushes_accepted_commit_and_rejects_a_late_writer_commit() {
        let path = path();
        let (conn, store) = connection(&path, 4 * 1024 * 1024).unwrap();
        conn.execute_batch("CREATE TABLE writes(n INTEGER PRIMARY KEY, value TEXT NOT NULL);")
            .unwrap();
        store.flush().unwrap();

        // Dropping a scoped Portfolio calls stop; that is a flush, not the
        // terminal fence, because other views still share this Store.
        store.manage_lifecycle();
        store.stop().unwrap();

        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (late_commit_tx, late_commit_rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            conn.execute(
                "INSERT INTO writes VALUES(1,'accepted before shutdown')",
                [],
            )
            .unwrap();
            conn.execute_batch("BEGIN; INSERT INTO writes VALUES(2,'after shutdown barrier');")
                .unwrap();
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            let rejected = conn.execute_batch("COMMIT").is_err();
            late_commit_tx.send(rejected).unwrap();
        });

        ready_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        store.shutdown().unwrap();
        release_tx.send(()).unwrap();
        assert!(late_commit_rx.recv_timeout(Duration::from_secs(3)).unwrap());
        writer.join().unwrap();
        drop(store);

        let disk = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            disk.query_row("SELECT n FROM writes", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1,
            "the pre-barrier commit must persist and the late commit must be rejected"
        );
        assert_eq!(
            disk.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        drop(disk);
        cleanup(&path);
    }

    #[test]
    fn dashboard_snapshot_is_memory_fresh_immutable_and_does_not_lock_execution() {
        let path = path();
        let (conn, store) = connection(&path, 4 * 1024 * 1024).unwrap();
        conn.execute_batch("CREATE TABLE live(n); INSERT INTO live VALUES(1);")
            .unwrap();
        store.flush().unwrap();
        store.delay.store(500, Ordering::Relaxed);
        conn.execute("INSERT INTO live VALUES(2)", []).unwrap();
        let view = store.read_connection().unwrap();
        assert_eq!(
            view.query_row("SELECT COUNT(*) FROM live", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        conn.execute("INSERT INTO live VALUES(3)", []).unwrap();
        assert_eq!(
            view.query_row("SELECT COUNT(*) FROM live", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM live", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            3
        );
        assert!(view.execute("DELETE FROM live", []).is_err());
        store.delay.store(0, Ordering::Relaxed);
        store.flush().unwrap();
        assert_eq!(
            view.query_row("SELECT COUNT(*) FROM live", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        drop(view);
        drop(conn);
        store.shutdown().unwrap();
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn persistence_failure_is_reported_and_previous_snapshot_stays_valid() {
        let path = path();
        let (conn, store) = connection(&path, 1024 * 1024).unwrap();
        conn.execute_batch("CREATE TABLE test(n); INSERT INTO test VALUES(1);")
            .unwrap();
        store.flush().unwrap();
        assert!(
            connection(&path, 1024 * 1024).is_err(),
            "second memory writer must be excluded"
        );
        std::fs::create_dir(path.with_extension("db.ccm-next")).unwrap();
        conn.execute("INSERT INTO test VALUES(2)", []).unwrap();
        assert!(store.flush().is_err());
        assert!(store.metrics()["error"].is_string());
        let disk = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            disk.query_row("SELECT COUNT(*) FROM test", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        drop(disk);
        drop(conn);
        let _ = store.shutdown();
        drop(store);
        std::fs::remove_dir(path.with_extension("db.ccm-next")).unwrap();
        cleanup(&path);
    }

    #[test]
    fn crash_writer_child() {
        let Ok(path) = std::env::var("CCM_TEST_CRASH_DB") else {
            return;
        };
        let (conn, store) = connection(Path::new(&path), 8 * 1024 * 1024).unwrap();
        conn.execute_batch("CREATE TABLE crash_test(n INTEGER, payload TEXT);")
            .unwrap();
        store.flush().unwrap();
        loop {
            conn.execute_batch("BEGIN;").unwrap();
            for n in 0..10 {
                conn.execute(
                    "INSERT INTO crash_test VALUES(?1,?2)",
                    rusqlite::params![n, "z".repeat(1000)],
                )
                .unwrap();
            }
            conn.execute_batch("COMMIT;").unwrap();
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn abrupt_process_loss_recovers_a_complete_transaction_prefix() {
        let path = path();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::tests::crash_writer_child",
                "--nocapture",
            ])
            .env("CCM_TEST_CRASH_DB", &path)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let start = Instant::now();
        let mut observed = false;
        while start.elapsed() < Duration::from_secs(10) {
            if let Ok(conn) = rusqlite::Connection::open_with_flags(
                &path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            ) {
                if conn
                    .query_row("SELECT COUNT(*) FROM crash_test", [], |r| {
                        r.get::<_, i64>(0)
                    })
                    .unwrap_or(0)
                    > 0
                {
                    observed = true;
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(observed, "child never published a snapshot");
        let (conn, store) = connection(&path, 4 * 1024 * 1024).unwrap();
        let count = conn
            .query_row("SELECT COUNT(*) FROM crash_test", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap();
        assert!(count > 0);
        assert_eq!(count % 10, 0, "partial SQL transaction survived crash");
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        drop(conn);
        store.shutdown().unwrap();
        drop(store);
        cleanup(&path);
    }
}
