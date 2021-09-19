use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::io::SeekFrom;
use std::path;
use std::sync::Arc;

use tracing::debug;
use tracing::warn;

use crate::error::BoxError;
use crate::error::IoError;
use crate::error::SpawnError;
use crate::log::LogEntry;
use crate::log::LogKeeping;
use crate::CoordNum;
use crate::RoundNum;

use super::io;

const MAGIC_BYTES: u32 = 0x70617861;
const APPLY: u32 = 0x41504C59;

#[derive(Debug)]
pub struct WorkingDir<R: RoundNum> {
    path: path::PathBuf,
    file: fs::File,
    log_keeping: LogKeeping,
    current_applied_entry_log: Option<R>,
    applied_entry_logs: BTreeMap<R, (path::PathBuf, fs::File)>,
    applied_entry_index: HashMap<R, (R, u64)>,
}

impl<R: RoundNum> WorkingDir<R> {
    pub fn init(working_dir: path::PathBuf, log_keeping: LogKeeping) -> Result<Self, SpawnError> {
        let working_dir_path = fs::canonicalize(&working_dir)
            .map_err(|e| IoError::new(format!("Failed to canonicalize `{:?}`.", working_dir), e))?;

        if !working_dir_path.is_dir() {
            return Err(SpawnError::InvalidWorkingDir(
                working_dir_path,
                "not a directory".into(),
            ));
        }

        let mut indicator_file = working_dir_path.clone();
        indicator_file.push(".paxakos");

        if !indicator_file.is_file() {
            if io::read_dir(&working_dir_path)?.next().is_some() {
                return Err(SpawnError::InvalidWorkingDir(
                    working_dir_path,
                    "no indicator file and not empty".into(),
                ));
            }

            fs::File::create(&indicator_file).map_err(|e| {
                IoError::new(format!("Failed to create `{:?}`.", indicator_file), e)
            })?;
        }

        let mut working_dir_file =
            fs::File::open(&working_dir_path).map_err(io::to_failed_to_open(&working_dir_path))?;
        io::sync(&working_dir_path, &mut working_dir_file)?;

        let mut working_dir = WorkingDir {
            path: working_dir_path,
            file: working_dir_file,
            log_keeping,
            current_applied_entry_log: None,
            applied_entry_logs: BTreeMap::new(),
            applied_entry_index: HashMap::new(),
        };

        working_dir.load_and_index_applied_entries();

        Ok(working_dir)
    }

    fn load_and_index_applied_entries(&mut self) {
        let mut errors = Vec::new();

        let logs = match self.find_applied_entry_logs() {
            Ok(logs) => logs,
            Err(err) => {
                return warn!("Failed to find applied entry logs: {:?}", err);
            }
        };

        for (round_num, path, mut file) in logs {
            match Self::index_applied_entries_of(&path, &mut std::io::BufReader::new(&mut file)) {
                Ok(results) => {
                    if results.get(0).map(|(r, _o)| *r) == Some(round_num) {
                        self.applied_entry_logs.insert(round_num, (path, file));
                        self.applied_entry_index
                            .extend(results.into_iter().map(|(r, o)| (r, (round_num, o))));
                    } else {
                        warn!("The content of applied entry log `{}` is inconsistent with its name and is ignored.", path.display());
                    }
                }
                Err(err) => errors.push(err),
            }
        }

        if !errors.is_empty() {
            warn!("Encountered errors while indexing applied entry logs.");

            for error in errors {
                warn!(" - {:?}", error);
            }
        }

        debug!(
            "Indexed `{}` applied entries across `{}` files.",
            self.applied_entry_index.len(),
            self.applied_entry_logs.len()
        );
    }

    pub fn find_applied_entry_logs(&self) -> Result<Vec<(R, path::PathBuf, fs::File)>, IoError> {
        let prefix = "applied_entry.log.";

        io::read_dir(&self.path)?
            .map(|entry| {
                let entry = entry.map_err(|e| {
                    IoError::new(
                        format!(
                            "Failed to enumerate directory entries of `{}`.",
                            self.path.display()
                        ),
                        e,
                    )
                })?;

                let file_name = entry.file_name();
                let file_name = match file_name.to_str() {
                    Some(n) => n,
                    None => return Ok(None),
                };

                if file_name.len() != prefix.len() + U128_BASE62_LEN
                    || !file_name.starts_with(prefix)
                {
                    return Ok(None);
                }

                let suffix = &file_name[prefix.len()..];
                let round_num = match u128_from_base62(suffix) {
                    Some(n) => n,
                    None => return Ok(None),
                };

                let round_num = io::recover_round_num(round_num)?;
                let path = entry.path();
                let file = fs::File::open(&path).map_err(io::to_failed_to_open(&path))?;

                Ok(Some((round_num, path, file)))
            })
            .filter_map(Result::transpose)
            .collect::<Result<_, _>>()
    }

    fn index_applied_entries_of(
        path: &path::Path,
        mut file: &mut impl Read,
    ) -> Result<Vec<(R, u64)>, IoError> {
        debug!("Indexing applied log entries in `{}`.", path.display());

        let bytes = io::try_read_u32_from(path, file)?;

        if bytes != Some(MAGIC_BYTES) {
            return Err(IoError::invalid_data(
                format!("invalid applied entries log file `{}`", path.display()),
                format!("expected `0x{:08x?}`, got `0x{:08x?}`.", MAGIC_BYTES, bytes),
            ));
        }

        let mut offset = 4;
        let mut results = Vec::new();

        loop {
            let kind = io::try_read_u32_from(path, file)?;

            match kind {
                Some(APPLY) => {
                    let size = io::try_read_u32_from(path, file)?;
                    let size = match size {
                        Some(size) => size,
                        None => break,
                    };

                    let mut checksumming_read =
                        io::Checksumming::from(file.take(u64::from(size) + (128 / 8)));

                    let round_num = io::try_read_u128_from(path, &mut checksumming_read)?;
                    let round_num = match round_num {
                        Some(round_num) => round_num,
                        None => break,
                    };

                    io::copy_from_to(
                        path.display(),
                        &mut checksumming_read,
                        "<void>",
                        &mut std::io::sink(),
                    )?;

                    let (take, expected_checksum) = checksumming_read.into_inner();
                    file = take.into_inner();

                    if !io::try_read_and_verify_checksum_from(path, file, &expected_checksum)? {
                        break;
                    }

                    let round_num = io::recover_round_num(round_num)?;

                    offset += 4 + (128 / 8);
                    results.push((round_num, offset));
                    offset += u64::from(size);
                }
                Some(unexpected) => {
                    return Err(IoError::invalid_data(
                        format!("Log file `{}` was corrupted.", path.display()),
                        format!("Read unexcected value `{}`.", unexpected),
                    ));
                }
                None => {
                    break;
                }
            }
        }

        Ok(results)
    }

    pub fn log_entry_application<C: CoordNum>(
        &mut self,
        round_num: R,
        coord_num: C,
        log_entry: &impl LogEntry,
    ) {
        if self.current_applied_entry_log.is_some()
            && self.current_applied_entry_log
                <= crate::util::try_usize_sub(round_num, self.log_keeping.entry_limit)
        {
            // implicitly start a new log
            self.current_applied_entry_log = None;

            while !self.applied_entry_logs.is_empty()
                && self.applied_entry_logs.len() >= self.log_keeping.logs_kept
            {
                let (_handle, (path, _file)) = self
                    .applied_entry_logs
                    .pop_first()
                    .expect("oldest applied log entry");

                if let Err(err) = fs::remove_file(&path) {
                    warn!(
                        "Failed to delete applied entry log `{}`: {:?}",
                        path.display(),
                        err
                    );
                }
            }
        }

        if self.log_keeping.logs_kept == 0 {
            return;
        }

        let (handle, path, file) = if let Some(current) = self.current_applied_entry_log {
            self.applied_entry_logs
                .iter_mut()
                .filter(|(r, _)| **r == current)
                .map(|(r, (p, f))| (*r, &*p, f))
                .next()
                .expect("current applied entry log")
        } else {
            match self.open_applied_entry_log_for(round_num) {
                Ok(res) => res,
                Err(err) => {
                    return warn!(
                        "Failed to open applied entry log for round `{}`: {:?}",
                        round_num, err
                    );
                }
            }
        };

        let offset =
            match Self::append_applied_entry_log_entry(path, file, round_num, coord_num, log_entry)
            {
                Ok(offset) => offset,
                Err(err) => {
                    return warn!(
                    "Failed to append entry `{:?}` for round `{}` to applied entry log `{}`: {:?}",
                    log_entry.id(),
                    round_num,
                    path.display(),
                    err
                );
                }
            };

        self.applied_entry_index.insert(round_num, (handle, offset));
    }

    fn append_applied_entry_log_entry<C: CoordNum>(
        path: &path::Path,
        file: &mut fs::File,
        round_num: R,
        coord_num: C,
        log_entry: &impl LogEntry,
    ) -> Result<u64, IoError> {
        let offset = io::seek_in(path, file, SeekFrom::End(0))?;

        io::write_u32_to(path, file, APPLY)?;
        let serialized = rmp_serde::to_vec_named(&log_entry)
            .map_err(|err| IoError::invalid_data("Could not serialize log entry.", err))?;
        io::write_usize_as_u32_to(path, file, serialized.len())?;

        let mut checksumming_write = io::Checksumming::from(file);

        io::write_u128_to(path, &mut checksumming_write, round_num)?;
        io::write_u128_to(path, &mut checksumming_write, coord_num)?;

        io::copy_from_to(
            format!("log-entry://{:?}", log_entry.id()),
            &mut std::io::Cursor::new(serialized),
            path.display(),
            &mut checksumming_write,
        )?;

        let (file, checksum) = checksumming_write.into_inner();
        io::write_u256_to(path, file, &checksum)?;

        Ok(offset + 4 + 4 + (128 / 8))
    }

    fn open_applied_entry_log_for(
        &mut self,
        round_num: R,
    ) -> Result<(R, &path::PathBuf, &mut fs::File), IoError> {
        let mut path = self.path.clone();
        path.push(format!("applied_entry.log.{}", Base62Of(round_num.into())));

        let mut file = fs::File::with_options()
            .create(true)
            .write(true)
            .open(&path)
            .map_err(io::to_failed_to_open(&path))?;

        io::write_u32_to(&path, &mut file, MAGIC_BYTES)?;

        let (path, file) = self
            .applied_entry_logs
            .entry(round_num)
            .or_insert((path, file));

        self.current_applied_entry_log = Some(round_num);

        Ok((round_num, &*path, file))
    }

    pub fn try_get_entry_applied_in<C: CoordNum, E: LogEntry>(
        &mut self,
        round_num: R,
    ) -> Option<(C, Arc<E>)> {
        let (log_handle, offset) = match self.applied_entry_index.get(&round_num) {
            Some(v) => v,
            None => {
                return None;
            }
        };

        let (path, file) = self
            .applied_entry_logs
            .get_mut(&log_handle)
            .expect("applied entry log given handle");

        match Self::read_applied_entry(path, file, *offset) {
            Ok(log_entry) => Some(log_entry),
            Err(err) => {
                warn!(
                    "Failed to load log entry from offset `{}` in `{}`: {:?}",
                    offset,
                    path.display(),
                    err
                );

                None
            }
        }
    }

    fn read_applied_entry<C: CoordNum, E: LogEntry>(
        path: &path::Path,
        file: &mut fs::File,
        offset: u64,
    ) -> Result<(C, Arc<E>), IoError> {
        // TODO calculate and compare the checksum
        io::seek_in(path, file, SeekFrom::Start(offset))?;
        let coord_num = io::try_read_u128_from(path, file)?.ok_or_else(|| {
            IoError::new(
                "Could not read coordination number.",
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("Applied entry log `{}` is truncated.", path.display()),
                ),
            )
        })?;
        let coord_num = io::recover_coord_num(coord_num)?;
        let log_entry = io::recover_log_entry(
            rmp_serde::from_read(file).map_err::<BoxError, _>(|e| Box::new(e)),
        )?;

        Ok((coord_num, Arc::new(log_entry)))
    }
}

const U128_BASE62_LEN: usize = 22;
const BASE62_DIGITS: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

fn u128_from_base62(val: &str) -> Option<u128> {
    let mut result = 0;

    for digit in val.chars() {
        let digit = BASE62_DIGITS.iter().position(|d| char::from(*d) == digit)?;

        result = result * 62 + digit as u128;
    }

    Some(result)
}

struct Base62Of(u128);

impl std::fmt::Display for Base62Of {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut val = self.0;

        let mut result = String::with_capacity(U128_BASE62_LEN);

        while val > 0 {
            let digit = val % 62;
            let digit = char::from(BASE62_DIGITS[digit as usize]);

            result.push(digit);

            val /= 62;
        }

        write!(
            f,
            "{:0>width$}",
            result.chars().rev().collect::<String>(),
            width = U128_BASE62_LEN
        )
    }
}
