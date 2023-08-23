use std::io::{self, Read};

use crate::value::Value::{self, *};

const BUF_SIZE: usize = 256;

pub trait RedisParser<R: Read>: Send {
    fn parse(&self, input: &mut R) -> Result<Value, io::Error>;
}

pub struct RespParser;

unsafe impl Send for RespParser {}

impl RespParser {
    pub fn new() -> Self {
        Self {}
    }
}

impl<R: Read> RedisParser<R> for RespParser {
    fn parse(&self, input: &mut R) -> Result<Value, io::Error> {
        let mut buf = [0u8; BUF_SIZE];
        let key = input.bytes().next();
        if let None = key {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        let key = key.unwrap()?;
        match key {
            b'*' => self.parse_array(input, &mut buf),
            b':' => self.parse_integer(input),
            b'+' => self.parse_simple_string(input),
            b'$' => self.parse_bulk_string(input, &mut buf),
            b'-' => self.parse_error(input),
            _ => Err(io::Error::from(io::ErrorKind::InvalidInput)),
        }
    }
}

impl RespParser {
    fn parse_integer(&self, stream: &mut impl Read) -> Result<Value, io::Error> {
        let integer = self.read_until_crlf(stream)?;
        let integer = integer.parse::<i64>();

        match integer {
            Ok(integer) => Ok(Integer(integer)),
            Err(_) => Err(io::Error::from(io::ErrorKind::InvalidInput)),
        }
    }

    fn parse_simple_string(&self, stream: &mut impl Read) -> Result<Value, io::Error> {
        let string = self.read_until_crlf(stream)?;
        Ok(SimpleString(string))
    }

    fn parse_bulk_string(
        &self,
        stream: &mut impl Read,
        buf: &mut [u8; BUF_SIZE],
    ) -> Result<Value, io::Error> {
        let len = self.parse_len(stream.bytes(), buf)?;
        if len == -1 {
            return Ok(NullBulkString);
        }
        let mut len = len as usize;
        let mut string = String::with_capacity(len as usize);
        let mut read_count = len / BUF_SIZE;
        while read_count > 0 {
            stream.read_exact(buf)?;
            string.push_str(&String::from_utf8(buf.to_vec()).unwrap());
            len -= BUF_SIZE;
            read_count -= 1;
        }
        if len > 0 {
            stream.read_exact(&mut buf[..len])?;
            string.push_str(&String::from_utf8(buf[..len].to_vec()).unwrap());
        }
        self.skip_crlf(stream);
        Ok(BulkString(string))
    }

    fn parse_array<R: Read>(
        &self,
        stream: &mut R,
        buf: &mut [u8; BUF_SIZE],
    ) -> Result<Value, io::Error> {
        let len = self.parse_len(stream.bytes(), buf)?;
        if len == -1 {
            return Ok(NullArray);
        }
        let mut vec = Vec::with_capacity(len as usize);

        for _ in 0..len {
            vec.push(self.parse(stream)?)
        }
        Ok(Array(vec))
    }

    fn parse_error(&self, stream: &mut impl Read) -> Result<Value, io::Error> {
        let string = self.parse_simple_string(stream)?;
        match string {
            SimpleString(string) => Ok(Error(string)),
            _ => Err(io::Error::from(io::ErrorKind::InvalidInput)),
        }
    }

    fn parse_len<T>(
        &self,
        mut stream: std::io::Bytes<T>,
        buf: &mut [u8; BUF_SIZE],
    ) -> Result<i32, io::Error>
    where
        T: io::Read,
    {
        let mut len = 0usize;
        while let Some(byte) = stream.next() {
            let byte = byte?;
            if matches!(byte, b'0'..=b'9' | b'-') {
                buf[len] = byte;
                len += 1;
            } else {
                break;
            }
        }
        stream.next();

        let array_len_str_rep = String::from_utf8(buf[..len].to_vec()).unwrap();
        match array_len_str_rep.parse() {
            Ok(len) => Ok(len),
            Err(_) => Err(io::Error::from(io::ErrorKind::InvalidInput)),
        }
    }

    fn read_until_crlf(&self, stream: &mut impl Read) -> Result<String, io::Error> {
        let mut result = String::new();
        let mut stream = stream.bytes();
        let mut found_cr = false;

        while let Some(byte) = stream.next() {
            let byte = byte?;
            if !found_cr && byte == b'\r' {
                found_cr = true;
                continue;
            }
            if found_cr && byte == b'\n' {
                break;
            }
            if found_cr {
                result.push('\r');
                found_cr = false;
            }
            result.push(byte as char);
        }
        Ok(result)
    }

    fn skip_crlf(&self, stream: &mut impl Read) {
        stream.bytes().next();
        stream.bytes().next();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn parse_integer() {
        let mut input = Cursor::new(":15\r\n");
        let result = RespParser::new().parse(&mut input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Integer(15));
    }

    #[test]
    fn parse_simple_string() {
        let mut input = Cursor::new("+hello world\r\n");
        let result = RespParser::new().parse(&mut input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SimpleString(String::from("hello world")));
    }

    #[test]
    fn parse_bulk_string() {
        let mut input = Cursor::new("$11\r\nhello world\r\n");
        let result = RespParser::new().parse(&mut input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), BulkString(String::from("hello world")));
    }

    #[test]
    fn parse_error() {
        let mut input = Cursor::new("-Error\r\n");
        let result = RespParser::new().parse(&mut input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Error(String::from("Error")));
    }

    #[test]
    fn parse_array() {
        let mut input = Cursor::new("*3\r\n$5\r\nhello\r\n$5\r\nworld\r\n:-150\r\n");
        let result = RespParser::new().parse(&mut input);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Array(vec![
                BulkString(String::from("hello")),
                BulkString(String::from("world")),
                Integer(-150)
            ])
        );
    }

    #[test]
    fn parse_empty_array() {
        let mut input = Cursor::new("*0\r\n");
        let result = RespParser::new().parse(&mut input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Array(vec![]));
    }

    #[test]
    fn parse_empty_string() {
        let mut input = Cursor::new("$0\r\n\r\n");
        let result = RespParser::new().parse(&mut input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), BulkString(String::from("")));
    }

    #[test]
    fn parse_null_string() {
        let mut input = Cursor::new("$-1\r\n");
        let result = RespParser::new().parse(&mut input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), NullBulkString);
    }

    #[test]
    fn parse_null_array() {
        let mut input = Cursor::new("*-1\r\n");
        let result = RespParser::new().parse(&mut input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), NullArray);
    }
}
