//! The cluster key that keys the secret files' HMACs (spec 22.3).
//!
//! One key per cluster: an explicit initialisation ([`init`], once, when the
//! cluster is set up) generates it and keeps it in a file that only its owner
//! can read. Loading never makes a key: a missing key is an error (and one
//! that says plainly "restore it" when the state already holds data made under
//! it), because a silently new key would make every existing bundle and
//! worker fail its HMAC check; a worker gets a copy when it is
//! enrolled (S10) and keeps it the same way. The file holds 64 lower-case hex
//! characters (32 random bytes). A key file that is a symlink, not a regular
//! file, not owned by this user or readable by anyone else is refused, never
//! "fixed"; so is a directory that others can write to.

use super::plan::{hex_decode, hex_encode};
use super::vault::{sweep_stale_tmp, sync_dir, STALE_TMP_AGE};
use super::{Result, SecretError};
use local_ci::source::MIN_SECRET_KEY;
use rustix::fs::{Mode, OFlags};
use rustix::io::Errno;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::Path;

const KEY_BYTES: usize = 32;
// Generating a key writes 65 bytes; retain support for larger existing keys,
// but never allocate/read an unbounded owner-controlled file during startup.
const MAX_KEY_FILE_BYTES: u64 = 4096;

#[derive(Clone, PartialEq, Eq)]
pub struct ClusterKey(Vec<u8>);

/// Never shows the key.
impl std::fmt::Debug for ClusterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterKey(<redacted>)")
    }
}

impl ClusterKey {
    /// A key from bytes. At least local-ci's minimum, so that a capture never
    /// fails on the key's length.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        if bytes.len() < MIN_SECRET_KEY {
            return Err(SecretError::Format(format!(
                "cluster key is {} bytes; at least {MIN_SECRET_KEY} are needed",
                bytes.len()
            )));
        }
        Ok(ClusterKey(bytes))
    }

    pub fn generate() -> Result<Self> {
        let mut b = vec![0u8; KEY_BYTES];
        getrandom::fill(&mut b).map_err(|e| {
            SecretError::Format(format!("no random bytes for the cluster key: {e}"))
        })?;
        Ok(ClusterKey(b))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    fn to_file_text(&self) -> String {
        format!("{}\n", hex_encode(&self.0))
    }

    fn from_file_text(text: &str) -> Result<Self> {
        let bytes = hex_decode(text.trim_end_matches('\n'))
            .ok_or_else(|| SecretError::Format("cluster key file is not lower-case hex".into()))?;
        ClusterKey::from_bytes(bytes)
    }
}

fn refuse(path: &Path) -> SecretError {
    SecretError::Insecure {
        what: "cluster key file",
        path: path.to_path_buf(),
    }
}

fn missing(path: &Path, state_in_use: bool) -> SecretError {
    let base = format!(
        "the cluster key {} does not exist; it is made only by an explicit init, never by loading",
        path.display()
    );
    SecretError::KeyMissing(if state_in_use {
        format!(
            "{base}. This state already holds secret bundles, worker registrations or other data made under a key: a new key would not match them, so restore the original key (or enrol the cluster again) instead of making one"
        )
    } else {
        base
    })
}

/// A directory that holds a key: a real directory (not a link) of this user
/// that group and others cannot write to (they could swap the key file).
fn check_dir(dir: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(dir)
        .map_err(|e| SecretError::io(format!("directory {}", dir.display()), e))?;
    if !meta.file_type().is_dir()
        || meta.mode() & 0o022 != 0
        || meta.uid() != rustix::process::geteuid().as_raw()
    {
        return Err(SecretError::Insecure {
            what: "cluster key directory",
            path: dir.to_path_buf(),
        });
    }
    Ok(())
}

/// Read the key at `path`: a regular file (not a link) of this user with no
/// access for group or others, in a directory only this user can write. The
/// checks are made on the opened descriptor (`O_NOFOLLOW`, then `fstat`), so
/// nothing can be swapped between the check and the read.
pub fn load(path: &Path) -> Result<ClusterKey> {
    load_checked(path, false)
}

/// [`load`], and when the key is missing, an error that says so plainly if
/// `state_dir` already holds anything (a vault, an index, a database): the
/// key must be restored, not made anew.
pub fn load_in_state(state_dir: &Path, path: &Path) -> Result<ClusterKey> {
    let in_use = fs::read_dir(state_dir).is_ok_and(|rd| {
        rd.flatten()
            .any(|e| e.path() != path && e.path() != path.parent().unwrap_or(path))
    });
    load_checked(path, in_use)
}

fn load_checked(path: &Path, state_in_use: bool) -> Result<ClusterKey> {
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        match fs::symlink_metadata(dir) {
            Err(e) if e.kind() == ErrorKind::NotFound => return Err(missing(path, state_in_use)),
            _ => check_dir(dir)?,
        }
    }
    let fd = match rustix::fs::openat(
        rustix::fs::CWD,
        path,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(Errno::NOENT) => return Err(missing(path, state_in_use)),
        Err(Errno::LOOP) => return Err(refuse(path)),
        Err(e) => {
            return Err(SecretError::io(
                format!("cluster key {}", path.display()),
                e.into(),
            ))
        }
    };
    let mut f = fs::File::from(fd);
    load_opened(path, &mut f)
}

/// Validate and read the supplied descriptor without reopening its path.
/// Bootstrap uses this after its stricter path-component checks.
pub(crate) fn load_opened(path: &Path, f: &mut fs::File) -> Result<ClusterKey> {
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        check_dir(dir)?;
    }
    let meta = f
        .metadata()
        .map_err(|e| SecretError::io(format!("cluster key {}", path.display()), e))?;
    if !meta.file_type().is_file()
        || meta.nlink() != 1
        || meta.mode() & 0o077 != 0
        || meta.uid() != rustix::process::geteuid().as_raw()
    {
        return Err(refuse(path));
    }
    let mut text = String::new();
    f.take(MAX_KEY_FILE_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(|e| SecretError::io(format!("cluster key {}", path.display()), e))?;
    if text.len() as u64 > MAX_KEY_FILE_BYTES {
        return Err(SecretError::Format(
            "cluster key file exceeds 4096 bytes".into(),
        ));
    }
    ClusterKey::from_file_text(&text)
}

/// Make the cluster key at `path` (the explicit initialisation, run once when
/// the cluster is set up) and return it; a key that is already there is
/// checked and returned, never replaced. Two processes that race both end with
/// the same key: the file is published with an atomic no-replace rename,
/// which fails for the loser, who then reads the winner's. Loading never makes a key: see
/// [`load`].
pub fn init(path: &Path) -> Result<ClusterKey> {
    match fs::symlink_metadata(path) {
        Ok(_) => return load(path),
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => {
            return Err(SecretError::io(
                format!("cluster key {}", path.display()),
                e,
            ))
        }
    }
    let dir = path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .ok_or_else(|| SecretError::Format("cluster key path has no directory".into()))?;
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .map_err(|e| SecretError::io(format!("directory {}", dir.display()), e))?;
    check_dir(dir)?;
    // a crash before publication leaves a temporary key copy behind
    sweep_stale_tmp(dir, STALE_TMP_AGE)?;
    let key = ClusterKey::generate()?;
    let tmp = dir.join(format!(".cluster-key.{}.tmp", ulid::Ulid::new()));
    let io = |e| SecretError::io(format!("cluster key {}", path.display()), e);
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)
        .map_err(io)?;
    let wrote = f
        .write_all(key.to_file_text().as_bytes())
        .and_then(|()| f.sync_all());
    drop(f);
    let linked = wrote
        .and_then(|()| {
            rustix::fs::renameat_with(
                rustix::fs::CWD,
                &tmp,
                rustix::fs::CWD,
                path,
                rustix::fs::RenameFlags::NOREPLACE,
            )
            .map_err(std::io::Error::from)
        })
        .and_then(|()| sync_dir(dir));
    let _ = fs::remove_file(&tmp);
    match linked {
        Ok(()) => Ok(key),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => load(path),
        Err(e) => Err(io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn a_missing_key_is_created_with_mode_0600_and_read_back() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("state/cluster.key");
        let k = init(&p).unwrap();
        assert_eq!(k.as_bytes().len(), 32);
        assert_eq!(fs::metadata(&p).unwrap().mode() & 0o777, 0o600);
        assert_eq!(
            fs::metadata(p.parent().unwrap()).unwrap().mode() & 0o777,
            0o700
        );
        assert_eq!(init(&p).unwrap(), k);
        assert_eq!(load(&p).unwrap(), k);
        // no temp file is left behind
        let names: Vec<_> = fs::read_dir(p.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names.len(), 1);
    }

    #[test]
    fn key_read_is_bounded_including_sparse_files_and_size_boundary() {
        use std::io::Seek;
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("key");
        init(&path).unwrap();
        fs::write(&path, "ab".repeat(MAX_KEY_FILE_BYTES as usize / 2)).unwrap();
        assert_eq!(
            load(&path).unwrap().as_bytes().len(),
            MAX_KEY_FILE_BYTES as usize / 2
        );
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"\n")
            .unwrap();
        assert!(load(&path).unwrap_err().to_string().contains("exceeds"));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(1 << 34).unwrap();
        assert!(load_opened(&path, &mut file)
            .unwrap_err()
            .to_string()
            .contains("exceeds"));
        assert_eq!(file.stream_position().unwrap(), MAX_KEY_FILE_BYTES + 1);
    }

    #[test]
    fn opened_key_is_not_reopened_after_path_is_replaced_by_symlink() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("key");
        let key = init(&path).unwrap();
        let mut file = fs::File::open(&path).unwrap();
        let original = d.path().join("original");
        fs::rename(&path, &original).unwrap();
        let other = d.path().join("other");
        init(&other).unwrap();
        fs::write(&other, "invalid").unwrap();
        std::os::unix::fs::symlink(&other, &path).unwrap();
        assert_eq!(load_opened(&path, &mut file).unwrap(), key);
        assert!(matches!(load(&path), Err(SecretError::Insecure { .. })));
        fs::remove_file(&path).unwrap();
        fs::hard_link(&original, &path).unwrap();
        assert!(matches!(
            load_opened(&original, &mut file),
            Err(SecretError::Insecure { .. })
        ));
    }

    #[test]
    fn two_keys_differ_and_debug_hides_the_key() {
        let a = ClusterKey::generate().unwrap();
        let b = ClusterKey::generate().unwrap();
        assert_ne!(a, b);
        let shown = format!("{a:?}");
        assert!(!shown.contains(&hex_encode(a.as_bytes())));
        assert!(shown.contains("redacted"));
    }

    #[test]
    fn racing_creators_all_end_with_one_key() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("cluster.key");
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let p = p.clone();
                std::thread::spawn(move || init(&p).unwrap())
            })
            .collect();
        let keys: Vec<ClusterKey> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(keys.iter().all(|k| *k == keys[0]));
        assert_eq!(load(&p).unwrap(), keys[0]);
    }

    #[test]
    fn a_key_file_others_can_read_is_refused() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("cluster.key");
        let k = init(&p).unwrap();
        for mode in [0o640, 0o604, 0o644, 0o666] {
            fs::set_permissions(&p, fs::Permissions::from_mode(mode)).unwrap();
            assert!(
                matches!(load(&p), Err(SecretError::Insecure { .. })),
                "{mode:o}"
            );
            assert!(matches!(init(&p), Err(SecretError::Insecure { .. })));
        }
        fs::set_permissions(&p, fs::Permissions::from_mode(0o400)).unwrap();
        assert_eq!(load(&p).unwrap(), k);
    }

    #[test]
    fn a_symlink_or_a_directory_is_not_a_key_file() {
        let d = tempfile::tempdir().unwrap();
        let real = d.path().join("real.key");
        let k = init(&real).unwrap();
        let link = d.path().join("link.key");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(matches!(load(&link), Err(SecretError::Insecure { .. })));
        assert!(matches!(init(&link), Err(SecretError::Insecure { .. })));
        let dir = d.path().join("dir.key");
        fs::create_dir(&dir).unwrap();
        assert!(matches!(load(&dir), Err(SecretError::Insecure { .. })));
        assert_eq!(load(&real).unwrap(), k);
    }

    #[test]
    fn a_damaged_or_short_key_is_an_error_not_a_new_key() {
        let d = tempfile::tempdir().unwrap();
        for (i, text) in ["zz\n", "ab\n", "", "ABCDEF0123456789ABCDEF0123456789\n"]
            .iter()
            .enumerate()
        {
            let p = d.path().join(format!("k{i}"));
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&p)
                .unwrap();
            f.write_all(text.as_bytes()).unwrap();
            assert!(matches!(init(&p), Err(SecretError::Format(_))), "{text:?}");
        }
        assert!(ClusterKey::from_bytes(vec![0; 15]).is_err());
        assert!(ClusterKey::from_bytes(vec![0; 16]).is_ok());
    }

    #[test]
    fn missing_file_and_a_path_without_a_directory() {
        let d = tempfile::tempdir().unwrap();
        assert!(matches!(
            load(&d.path().join("none")),
            Err(SecretError::KeyMissing(_))
        ));
        assert!(matches!(
            init(Path::new("cluster.key")),
            Err(SecretError::Format(_))
        ));
        // the parent is a file
        let f = d.path().join("f");
        fs::write(&f, b"x").unwrap();
        assert!(init(&f.join("k")).is_err());
    }

    #[test]
    fn loading_never_makes_a_key_and_a_used_state_is_told_to_restore_it() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("cluster.key");
        let err = load(&p).unwrap_err();
        assert!(matches!(err, SecretError::KeyMissing(_)));
        assert!(!p.exists(), "load must not create the key");
        // a missing directory is the same error, and creates nothing
        let deep = d.path().join("no/such/cluster.key");
        assert!(matches!(load(&deep), Err(SecretError::KeyMissing(_))));
        assert!(!d.path().join("no").exists());
        // an empty state: the plain message
        let plain = load_in_state(d.path(), &p).unwrap_err().to_string();
        assert!(!plain.contains("restore"), "{plain}");
        // a state that holds data: the message says to restore
        fs::create_dir(d.path().join("secrets")).unwrap();
        let used = load_in_state(d.path(), &p).unwrap_err();
        assert!(matches!(used, SecretError::KeyMissing(_)));
        assert!(used.to_string().contains("restore"), "{used}");
        assert!(!p.exists());
        // once it was initialised, the state loads it
        let k = init(&p).unwrap();
        assert_eq!(load_in_state(d.path(), &p).unwrap(), k);
    }

    #[test]
    fn a_key_directory_others_can_write_to_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let d = tempfile::tempdir().unwrap();
        let sub = d.path().join("keys");
        let p = sub.join("cluster.key");
        let k = init(&p).unwrap();
        for mode in [0o770, 0o707, 0o777] {
            fs::set_permissions(&sub, fs::Permissions::from_mode(mode)).unwrap();
            assert!(
                matches!(load(&p), Err(SecretError::Insecure { .. })),
                "{mode:o}"
            );
            assert!(matches!(init(&p), Err(SecretError::Insecure { .. })));
        }
        // an existing directory that is writable by others is refused for a new key too
        let other = d.path().join("open");
        fs::create_dir(&other).unwrap();
        fs::set_permissions(&other, fs::Permissions::from_mode(0o777)).unwrap();
        assert!(matches!(
            init(&other.join("cluster.key")),
            Err(SecretError::Insecure { .. })
        ));
        assert!(!other.join("cluster.key").exists());
        fs::set_permissions(&sub, fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(load(&p).unwrap(), k);
        // a directory that is a link is not a key directory
        let link = d.path().join("link");
        std::os::unix::fs::symlink(&sub, &link).unwrap();
        assert!(matches!(
            load(&link.join("cluster.key")),
            Err(SecretError::Insecure { .. })
        ));
    }

    #[test]
    fn a_stale_temporary_key_copy_is_swept_by_init_and_a_fresh_one_is_kept() {
        let d = tempfile::tempdir().unwrap();
        let sub = d.path().join("k");
        fs::create_dir(&sub).unwrap();
        let old = sub.join(".cluster-key.OLD.tmp");
        let fresh = sub.join(".cluster-key.FRESH.tmp");
        fs::write(&old, b"x").unwrap();
        fs::write(&fresh, b"x").unwrap();
        let long_ago = std::time::SystemTime::now() - std::time::Duration::from_secs(7200);
        fs::File::options()
            .write(true)
            .open(&old)
            .unwrap()
            .set_modified(long_ago)
            .unwrap();
        init(&sub.join("cluster.key")).unwrap();
        assert!(!old.exists());
        assert!(fresh.exists());
    }
}
