use std::{fs, io, path::Path};

pub fn prune_standalone_backups(root: &Path, retention: usize) -> io::Result<usize> {
    if retention == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "backup retention must be greater than zero",
        ));
    }
    let mut backups = fs::read_dir(root)?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_str()?;
            if !name.starts_with("standalone-") || !entry.file_type().ok()?.is_dir() {
                return None;
            }
            Some((name.to_owned(), entry.path()))
        })
        .collect::<Vec<_>>();
    backups.sort_by(|left, right| left.0.cmp(&right.0));
    let remove = backups.len().saturating_sub(retention);
    for (_, path) in backups.into_iter().take(remove) {
        fs::remove_dir_all(path)?;
    }
    Ok(remove)
}

pub fn prune_etcd_backups(root: &Path, retention: usize) -> io::Result<usize> {
    if retention == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "backup retention must be greater than zero",
        ));
    }
    let mut backups = fs::read_dir(root)?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_str()?;
            if !name.starts_with("rcaster-")
                || !name.ends_with(".db")
                || !entry.file_type().ok()?.is_file()
            {
                return None;
            }
            Some((name.to_owned(), entry.path()))
        })
        .collect::<Vec<_>>();
    backups.sort_by(|left, right| left.0.cmp(&right.0));
    let remove = backups.len().saturating_sub(retention);
    for (_, path) in backups.into_iter().take(remove) {
        fs::remove_file(path)?;
    }
    Ok(remove)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{prune_etcd_backups, prune_standalone_backups};

    #[test]
    fn prunes_only_old_standalone_backups() {
        let root = temp_dir();
        for name in [
            "standalone-0000000001-000000000",
            "standalone-0000000002-000000000",
            "standalone-0000000003-000000000",
            "manual",
        ] {
            fs::create_dir(root.join(name)).unwrap();
        }
        assert_eq!(prune_standalone_backups(&root, 2).unwrap(), 1);
        assert!(!root.join("standalone-0000000001-000000000").exists());
        assert!(root.join("standalone-0000000002-000000000").exists());
        assert!(root.join("standalone-0000000003-000000000").exists());
        assert!(root.join("manual").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prunes_only_old_etcd_snapshots() {
        let root = temp_dir();
        for name in [
            "rcaster-0000000001.db",
            "rcaster-0000000002.db",
            "rcaster-0000000003.db",
            "manual.db",
            "rcaster-current.tmp",
        ] {
            fs::write(root.join(name), b"snapshot").unwrap();
        }
        assert_eq!(prune_etcd_backups(&root, 2).unwrap(), 1);
        assert!(!root.join("rcaster-0000000001.db").exists());
        assert!(root.join("rcaster-0000000002.db").exists());
        assert!(root.join("rcaster-0000000003.db").exists());
        assert!(root.join("manual.db").exists());
        assert!(root.join("rcaster-current.tmp").exists());
        let _ = fs::remove_dir_all(root);
    }

    fn temp_dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "rcaster-backup-retention-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
