use std::convert::TryInto;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path;

use crate::error::{BoxError, IoError};
use crate::log::LogEntry;
use crate::{CoordNum, RoundNum};

pub fn read_dir(dir: impl AsRef<path::Path>) -> Result<fs::ReadDir, IoError> {
    let dir = dir.as_ref();

    dir.read_dir()
        .map_err(|e| IoError::new(format!("Failed to read directory `{:?}`.", dir), e))
}

pub fn to_failed_to_open(path: impl AsRef<path::Path>) -> impl FnOnce(std::io::Error) -> IoError {
    move |e| {
        IoError::new(
            format!("Failed to open `{:?}`.", path.as_ref().display()),
            e,
        )
    }
}

pub fn seek_in(
    path: impl AsRef<path::Path>,
    seekable: &mut impl Seek,
    target_position: SeekFrom,
) -> Result<u64, IoError> {
    seekable.seek(target_position).map_err(|e| {
        IoError::new(
            format!("Failed to seek in `{}`.", path.as_ref().display(),),
            e,
        )
    })
}

pub fn try_read_and_verify_checksum_from(
    path: impl AsRef<path::Path>,
    reader: &mut impl Read,
    expected_checksum: &[u8; 32],
) -> Result<bool, IoError> {
    let path = path.as_ref();
    let checksum = try_read_u256_from(path, reader)?;
    let checksum = match checksum {
        Some(checksum) => checksum,
        None => return Ok(false),
    };

    if checksum == *expected_checksum {
        Ok(true)
    } else {
        Err(IoError::invalid_data(
            format!("Log file `{}` was corrupted.", path.display()),
            format!(
                "Checksum mismatch `{:?} != {:?}`.",
                checksum, expected_checksum
            ),
        ))
    }
}

pub fn try_read_u32_from(
    path: impl AsRef<path::Path>,
    reader: &mut impl Read,
) -> Result<Option<u32>, IoError> {
    const SIZE: usize = std::mem::size_of::<u32>();
    let mut buf = [0; SIZE];

    let read = try_read_exact_from(path, reader, &mut buf)?;

    if read == SIZE {
        Ok(Some(u32::from_be_bytes(buf)))
    } else {
        Ok(None)
    }
}

pub fn try_read_u128_from(
    path: impl AsRef<path::Path>,
    reader: &mut impl Read,
) -> Result<Option<u128>, IoError> {
    const SIZE: usize = std::mem::size_of::<u128>();
    let mut buf = [0; SIZE];

    let read = try_read_exact_from(path, reader, &mut buf)?;

    if read == SIZE {
        Ok(Some(u128::from_be_bytes(buf)))
    } else {
        Ok(None)
    }
}

pub fn try_read_u256_from(
    path: impl AsRef<path::Path>,
    reader: &mut impl Read,
) -> Result<Option<[u8; 32]>, IoError> {
    const SIZE: usize = 256 / 8;
    let mut buf = [0; SIZE];

    let read = try_read_exact_from(path, reader, &mut buf)?;

    if read == SIZE {
        Ok(Some(buf))
    } else {
        Ok(None)
    }
}

pub fn try_read_exact_from(
    path: impl AsRef<path::Path>,
    reader: &mut impl Read,
    buf: &mut [u8],
) -> Result<usize, IoError> {
    let mut read = 0;

    while read < buf.len() {
        match reader.read(&mut buf[read..]).map_err(|e| {
            IoError::new(
                format!("Failed to read from `{}`.", path.as_ref().display()),
                e,
            )
        })? {
            0 => {
                break;
            }
            n => {
                read += n;
            }
        }
    }

    Ok(read)
}

pub fn recover_round_num<R: RoundNum>(round_num: u128) -> Result<R, IoError> {
    R::try_from(round_num).map_err(|_| {
        IoError::invalid_data(
            "Log file has become incompatible.",
            format!("`{}` is no longer a valid round number.", round_num),
        )
    })
}

pub fn recover_coord_num<C: CoordNum>(coord_num: u128) -> Result<C, IoError> {
    C::try_from(coord_num).map_err(|_| {
        IoError::invalid_data(
            "Log file has become incompatible.",
            format!("`{}` is no longer a valid coordination number.", coord_num),
        )
    })
}

pub fn recover_log_entry<E: LogEntry>(read_result: Result<E, BoxError>) -> Result<E, IoError> {
    read_result
        .map_err(|e| IoError::invalid_data(format!("Log entry could not be deserialized."), e))
}

pub fn write_usize_as_u32_to(
    path: impl AsRef<path::Path>,
    writer: &mut impl Write,
    v: impl Into<usize>,
) -> Result<(), IoError> {
    let size = v.into();
    let size: u32 = size.try_into().map_err(|_| {
        IoError::invalid_data(
            format!("Failed to write to log file `{}`.", path.as_ref().display()),
            format!("Unsupported size of `{}`.", size),
        )
    })?;
    write_u32_to(path, writer, size)
}

pub fn write_u32_to(
    path: impl AsRef<path::Path>,
    writer: &mut impl Write,
    v: impl Into<u32>,
) -> Result<(), IoError> {
    write_all_to(path, writer, &v.into().to_be_bytes())
}

pub fn write_u128_to(
    path: impl AsRef<path::Path>,
    writer: &mut impl Write,
    v: impl Into<u128>,
) -> Result<(), IoError> {
    write_all_to(path, writer, &v.into().to_be_bytes())
}

pub fn write_u256_to(
    path: impl AsRef<path::Path>,
    writer: &mut impl Write,
    v: &[u8; 32],
) -> Result<(), IoError> {
    write_all_to(path, writer, v.as_ref())
}

pub fn copy_from_to(
    source: impl std::fmt::Display,
    reader: &mut impl Read,
    destination: impl std::fmt::Display,
    writer: &mut impl Write,
) -> Result<u64, IoError> {
    std::io::copy(reader, writer).map_err(|e| {
        IoError::new(
            format!("Failed to copy from `{}` to `{}`.", source, destination),
            e,
        )
    })
}

pub fn write_all_to(
    path: impl AsRef<path::Path>,
    writer: &mut impl Write,
    buf: &[u8],
) -> Result<(), IoError> {
    writer.write_all(buf).map_err(|e| {
        IoError::new(
            format!("Failed to write to `{}`.", path.as_ref().display()),
            e,
        )
    })
}

pub fn sync(path: impl AsRef<path::Path>, file: &mut fs::File) -> Result<(), IoError> {
    file.flush()
        .map_err(|e| IoError::new(format!("Failed to flush `{}`.", path.as_ref().display()), e))?;

    file.sync_all()
        .map_err(|e| IoError::new(format!("Failed to sync `{}`.", path.as_ref().display()), e))
}

// TODO consider using something faster (ZFS uses fletcher)
pub struct Checksumming<I> {
    inner: I,
    hasher: blake3::Hasher,
}

impl<I> Checksumming<I> {
    pub fn into_inner(self) -> (I, [u8; 32]) {
        let hash = self.hasher.finalize();

        (self.inner, hash.into())
    }
}

impl<I> From<I> for Checksumming<I> {
    fn from(inner: I) -> Self {
        Self {
            inner,
            hasher: blake3::Hasher::new(),
        }
    }
}

impl<R: Read> Read for Checksumming<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let result = self.inner.read(buf);

        if let Ok(read) = result {
            self.hasher.update(&buf[0..read]);
        }

        result
    }
}

impl<W: Write> Write for Checksumming<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let result = self.inner.write(buf);

        if let Ok(written) = result {
            self.hasher.update(&buf[0..written]);
        }

        result
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
