use std::io::{self, BufRead, Write};

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<RespValue>),
}

impl RespValue {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.write_to(&mut out).expect("vec write cannot fail");
        out
    }

    fn write_to(&self, writer: &mut impl Write) -> io::Result<()> {
        match self {
            RespValue::SimpleString(value) => write!(writer, "+{value}\r\n"),
            RespValue::Error(value) => write!(writer, "-{value}\r\n"),
            RespValue::Integer(value) => write!(writer, ":{value}\r\n"),
            RespValue::Bulk(Some(value)) => {
                write!(writer, "${}\r\n", value.len())?;
                writer.write_all(value)?;
                writer.write_all(b"\r\n")
            }
            RespValue::Bulk(None) => writer.write_all(b"$-1\r\n"),
            RespValue::Array(values) => {
                write!(writer, "*{}\r\n", values.len())?;
                for value in values {
                    value.write_to(writer)?;
                }
                Ok(())
            }
        }
    }
}

pub fn read_command(reader: &mut impl BufRead) -> io::Result<Option<Vec<Vec<u8>>>> {
    let mut first = Vec::new();
    let bytes = reader.read_until(b'\n', &mut first)?;
    if bytes == 0 {
        return Ok(None);
    }
    trim_crlf(&mut first);
    if first.is_empty() {
        return Ok(Some(Vec::new()));
    }
    if first[0] != b'*' {
        return Ok(Some(split_inline(&first)));
    }
    let count = parse_prefixed_number(&first, b'*')?;
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        let mut len_line = Vec::new();
        reader.read_until(b'\n', &mut len_line)?;
        trim_crlf(&mut len_line);
        let len = parse_prefixed_number(&len_line, b'$')?;
        let mut value = vec![0; len + 2];
        reader.read_exact(&mut value)?;
        if &value[len..] != b"\r\n" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad bulk string terminator",
            ));
        }
        value.truncate(len);
        args.push(value);
    }
    Ok(Some(args))
}

fn parse_prefixed_number(line: &[u8], prefix: u8) -> io::Result<usize> {
    if !line.starts_with(&[prefix]) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected RESP prefix",
        ));
    }
    std::str::from_utf8(&line[1..])
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad RESP length"))
}

fn split_inline(line: &[u8]) -> Vec<Vec<u8>> {
    String::from_utf8_lossy(line)
        .split_whitespace()
        .map(|part| part.as_bytes().to_vec())
        .collect()
}

fn trim_crlf(value: &mut Vec<u8>) {
    while value
        .last()
        .is_some_and(|byte| *byte == b'\n' || *byte == b'\r')
    {
        value.pop();
    }
}
