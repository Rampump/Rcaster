use std::{
    fs, io,
    path::{Path, PathBuf},
};

pub fn load(path: &Path) -> io::Result<Option<bool>> {
    match fs::read_to_string(path) {
        Ok(value) => match value.trim() {
            "true" => Ok(Some(true)),
            "false" => Ok(Some(false)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "maintenance state must be true or false",
            )),
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

pub fn save(path: &Path, maintenance: bool) -> io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let temporary = temporary_path(path);
    fs::write(
        &temporary,
        if maintenance {
            &b"true\n"[..]
        } else {
            &b"false\n"[..]
        },
    )?;
    replace_file(&temporary, path)
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    PathBuf::from(temporary)
}

fn replace_file(source: &Path, target: &Path) -> io::Result<()> {
    #[cfg(windows)]
    if target.exists() {
        let backup = target.with_extension("old");
        let _ = fs::remove_file(&backup);
        fs::rename(target, &backup)?;
        match fs::rename(source, target) {
            Ok(()) => {
                let _ = fs::remove_file(backup);
                return Ok(());
            }
            Err(error) => {
                let _ = fs::rename(backup, target);
                return Err(error);
            }
        }
    }
    fs::rename(source, target)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{load, save};

    #[test]
    fn maintenance_state_survives_reopen_and_rejects_invalid_content() {
        let path =
            std::env::temp_dir().join(format!("rcaster-maintenance-{}.state", std::process::id()));
        let _ = fs::remove_file(&path);
        assert_eq!(load(&path).unwrap(), None);
        save(&path, true).unwrap();
        assert_eq!(load(&path).unwrap(), Some(true));
        save(&path, false).unwrap();
        assert_eq!(load(&path).unwrap(), Some(false));
        fs::write(&path, "invalid").unwrap();
        assert!(load(&path).is_err());
        let _ = fs::remove_file(path);
    }
}
