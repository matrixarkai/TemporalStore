// SPDX-License-Identifier: Apache-2.0

use std::fs::File;
use std::io;
use std::path::Path;

use tempfile::{Builder, NamedTempFile};

pub(super) enum StagedBlob {
    Named(NamedTempFile),
    #[cfg(target_os = "linux")]
    Unnamed(File),
}

impl StagedBlob {
    pub(super) fn new(parent: &Path) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            use rustix::fs::{open, Mode, OFlags};

            // A process exit discards the unnamed file without leaving a partial blob.
            if let Ok(fd) = open(
                parent,
                OFlags::TMPFILE | OFlags::RDWR | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            ) {
                return Ok(Self::Unnamed(File::from(fd)));
            }
        }
        named_builder().tempfile_in(parent).map(Self::Named)
    }

    pub(super) fn file_mut(&mut self) -> &mut File {
        match self {
            Self::Named(file) => file.as_file_mut(),
            #[cfg(target_os = "linux")]
            Self::Unnamed(file) => file,
        }
    }

    pub(super) fn publish(self, path: &Path, _parent: &Path) -> io::Result<()> {
        match self {
            Self::Named(file) => file.persist(path).map(|_| ()).map_err(|error| error.error),
            #[cfg(target_os = "linux")]
            Self::Unnamed(file) => publish_unnamed(file, path, _parent),
        }
    }
}

fn named_builder() -> Builder<'static, 'static> {
    let mut builder = Builder::new();
    builder.prefix(".blob-upload-");
    builder
}

#[cfg(target_os = "linux")]
fn publish_unnamed(mut file: File, path: &Path, parent: &Path) -> io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    use std::os::fd::AsRawFd;

    use rustix::fs::{linkat, AtFlags, CWD};
    use rustix::io::Errno;

    // Following the descriptor through procfs permits linking as an unprivileged user.
    let source = format!("/proc/self/fd/{}", file.as_raw_fd());
    let link = |destination: &Path| {
        linkat(
            CWD,
            source.as_str(),
            CWD,
            destination,
            AtFlags::SYMLINK_FOLLOW,
        )
    };
    match link(path) {
        Ok(()) => return Ok(()),
        Err(Errno::EXIST) => {
            // Replacement needs a temporary name for the atomic rename.
            if let Ok(staged) =
                named_builder().make_in(parent, |name| link(name).map_err(io::Error::from))
            {
                staged.persist(path).map_err(|error| error.error)?;
                return Ok(());
            }
        }
        Err(_) => {}
    }

    // Some mounts or sandboxes allow anonymous files but reject linking them.
    // Preserve the normal upload behavior after the request body has been read.
    file.seek(SeekFrom::Start(0))?;
    let mut staged = named_builder().tempfile_in(parent)?;
    io::copy(&mut file, staged.as_file_mut())?;
    staged.as_file_mut().flush()?;
    staged
        .persist(path)
        .map(|_| ())
        .map_err(|error| error.error)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn blob_staging_copies_when_the_temporary_file_cannot_be_linked() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("published");
        std::fs::write(&target, b"old").unwrap();
        let (mut file, temporary_path) = named_builder()
            .tempfile_in(directory.path())
            .unwrap()
            .into_parts();
        temporary_path.close().unwrap();
        file.write_all(b"complete").unwrap();

        publish_unnamed(file, &target, directory.path()).unwrap();

        assert_eq!(std::fs::read(target).unwrap(), b"complete");
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }
}
