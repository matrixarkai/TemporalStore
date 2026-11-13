// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::io::{self, BufRead, Read};

pub(crate) struct ChunkedReader<R> {
    reader: R,
    remaining: usize,
    terminator_due: bool,
    done: bool,
}

impl<R: BufRead> ChunkedReader<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self {
            reader,
            remaining: 0,
            terminator_due: false,
            done: false,
        }
    }

    fn line(&mut self) -> io::Result<Vec<u8>> {
        let mut line = Vec::new();
        self.reader
            .by_ref()
            .take(8193)
            .read_until(b'\n', &mut line)?;
        if line.len() > 8192 || !line.ends_with(b"\r\n") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid chunk framing line",
            ));
        }
        line.truncate(line.len() - 2);
        Ok(line)
    }
}

impl<R: BufRead> Read for ChunkedReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() || self.done {
            return Ok(0);
        }
        if self.remaining == 0 {
            if self.terminator_due {
                let mut terminator = [0; 2];
                self.reader.read_exact(&mut terminator)?;
                if terminator != *b"\r\n" {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid chunk terminator",
                    ));
                }
            }
            let line = self.line()?;
            let size = line.split(|byte| *byte == b';').next().unwrap_or_default();
            if size.is_empty() || !size.iter().all(u8::is_ascii_hexdigit) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid chunk size",
                ));
            }
            self.remaining = usize::from_str_radix(std::str::from_utf8(size).unwrap(), 16)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "chunk size overflow"))?;
            if self.remaining == 0 {
                let mut trailer_bytes = 0usize;
                loop {
                    let trailer = self.line()?;
                    trailer_bytes += trailer.len() + 2;
                    if trailer_bytes > 8192 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "chunk trailers too large",
                        ));
                    }
                    if trailer.is_empty() {
                        break;
                    }
                    if !trailer.contains(&b':') {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid chunk trailer",
                        ));
                    }
                }
                self.done = true;
                return Ok(0);
            }
            self.terminator_due = true;
        }
        let limit = bytes.len().min(self.remaining);
        let read = self.reader.read(&mut bytes[..limit])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete chunk body",
            ));
        }
        self.remaining -= read;
        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor};

    #[test]
    fn chunked_reader_streams_extensions_and_trailers() {
        let wire = b"2;test=yes\r\n{\"\r\n5\r\na\":1}\r\n0\r\nX-Result: ok\r\n\r\nnext";
        let mut reader = ChunkedReader::new(BufReader::with_capacity(1, Cursor::new(wire)));
        let mut decoded = String::new();
        reader.read_to_string(&mut decoded).unwrap();
        assert_eq!(decoded, "{\"a\":1}");
        let mut rest = String::new();
        reader.reader.read_to_string(&mut rest).unwrap();
        assert_eq!(rest, "next");
    }

    #[test]
    fn chunked_reader_rejects_truncated_and_invalid_frames() {
        for wire in [
            b"3\r\na".as_slice(),
            b"z\r\n",
            b"1\r\naXX0\r\n\r\n",
            b"0\r\n",
        ] {
            let mut reader = ChunkedReader::new(Cursor::new(wire));
            assert!(reader.read_to_end(&mut Vec::new()).is_err());
        }
        let mut reader = ChunkedReader::new(Cursor::new(vec![b'f'; 8193]));
        assert!(reader.read_to_end(&mut Vec::new()).is_err());
    }
}
