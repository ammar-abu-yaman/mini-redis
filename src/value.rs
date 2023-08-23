use std::fmt::Display;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    Array(Vec<Value>),
    Integer(i64),
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    NullArray,
    Error(String),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer(integer) => write!(f, ":{integer}\r\n"),
            Self::SimpleString(string) => write!(f, "+{string}\r\n"),
            Self::BulkString(string) => write!(f, "${}\r\n{}\r\n", string.len(), string),
            Self::Error(err) => write!(f, "-{err}\r\n"),
            Self::NullBulkString => write!(f, "$-1\r\n"),
            Self::NullArray => write!(f, "*-1\r\n"),
            Self::Array(tokens) => {
                write!(f, "*{}\r\n", tokens.len())?;
                for token in tokens {
                    write!(f, "{token}")?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_integer() {
        let token = Value::Integer(15232);
        let disp = format!("{token}");
        assert_eq!(disp, ":15232\r\n");
    }

    #[test]
    fn write_simple_string() {
        let token = Value::SimpleString(String::from("hello world"));
        let disp = format!("{token}");
        assert_eq!(disp, "+hello world\r\n");
    }
    #[test]
    fn write_error() {
        let token = Value::Error(String::from("hello world"));
        let disp = format!("{token}");
        assert_eq!(disp, "-hello world\r\n");
    }

    #[test]
    fn write_bluk_string() {
        let token = Value::BulkString(String::from("hello"));
        let disp = format!("{token}");
        assert_eq!(disp, "$5\r\nhello\r\n");
    }

    #[test]
    fn write_null_bluk_string() {
        let token = Value::NullBulkString;
        let disp = format!("{token}");
        assert_eq!(disp, "$-1\r\n");
    }

    #[test]
    fn write_null_array() {
        let token = Value::NullArray;
        let disp = format!("{token}");
        assert_eq!(disp, "*-1\r\n");
    }

    #[test]
    fn write_array() {
        let token = Value::Array(vec![
            Value::SimpleString(String::from("hello world")),
            Value::BulkString(String::from("hello")),
            Value::Integer(15232),
            Value::NullBulkString,
        ]);
        let disp = format!("{token}");
        assert_eq!(
            disp,
            "*4\r\n+hello world\r\n$5\r\nhello\r\n:15232\r\n$-1\r\n"
        );
    }
}
