use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Config {
    pub dir: Option<PathBuf>,
    pub dbfilename: Option<String>,
}

impl Config {
    pub fn db_file(&self) -> Option<PathBuf> {
        match (&self.dir, &self.dbfilename) {
            (None, Some(f)) => Some(Path::new(".").join(f).to_owned()),
            (Some(p), Some(f)) => Some(p.join(f).to_owned()),
            (Some(p), None) => Some(p.join("dump.rdb").to_owned()),
            _ => None,
        }
    }
}
