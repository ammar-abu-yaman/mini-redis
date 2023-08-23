use std::time::Duration;

use crate::value::Value;

#[derive(Debug)]
pub enum Operation {
    Ping,
    Echo(String),
    Get(String),
    Set(String, String, SetOptions),
    Invalid(String),
}

#[derive(Debug)]
pub struct SetOptions {
    pub expiration: Option<Duration>,
}

pub trait OperationDeducer: Send {
    fn deduce_operation(&self, value: &Value) -> Operation;
}

pub struct StandardOperationDeducer;

unsafe impl Send for StandardOperationDeducer {}

impl StandardOperationDeducer {
    pub fn new() -> Self {
        Self {}
    }
}

impl OperationDeducer for StandardOperationDeducer {
    fn deduce_operation(&self, input: &Value) -> Operation {
        if let Value::Array(tokens) = input {
            let op = if let Some(Value::BulkString(s)) = tokens.get(0) {
                s
            } else {
                return Operation::Invalid(String::from("Error: Invalid or corrupt input"));
            }
            .to_lowercase();
            match &op[..] {
                "ping" => Operation::Ping,
                "echo" => self.deduce_echo(tokens),
                "get" => self.deduce_get(tokens),
                "set" => self.deduce_set(tokens),
                _ => Operation::Invalid(format!("Error: Unkown operation {op}")),
            }
        } else {
            return Operation::Invalid(String::from("Error: Invalid or corrupt input"));
        }
    }
}

impl StandardOperationDeducer {
    fn deduce_echo(&self, tokens: &[Value]) -> Operation {
        if let Some(Value::BulkString(s)) = tokens.get(1) {
            Operation::Echo(s.clone())
        } else {
            Operation::Invalid(String::from("Error: Invalid or corrupt input"))
        }
    }

    fn deduce_get(&self, tokens: &[Value]) -> Operation {
        match tokens {
            [Value::BulkString(_), Value::BulkString(key)] => Operation::Get(key.clone()),
            _ => Operation::Invalid(String::from("Invalid syntax for GET operation")),
        }
    }

    fn deduce_set(&self, tokens: &[Value]) -> Operation {
        match tokens {
            [Value::BulkString(_), Value::BulkString(key), Value::BulkString(val)] => {
                Operation::Set(key.clone(), val.clone(), SetOptions { expiration: None })
            }
            [Value::BulkString(_), Value::BulkString(key), Value::BulkString(val), Value::BulkString(ex_op), Value::BulkString(duration)]
                if (ex_op.eq_ignore_ascii_case("ex") || ex_op.eq_ignore_ascii_case("px"))
                    && duration.parse::<u64>().is_ok() =>
            {
                let expiration = if ex_op.eq_ignore_ascii_case("ex") {
                    Duration::from_secs(duration.parse().unwrap())
                } else {
                    Duration::from_millis(duration.parse().unwrap())
                };
                Operation::Set(
                    key.clone(),
                    val.clone(),
                    SetOptions {
                        expiration: Some(expiration),
                    },
                )
            }
            _ => Operation::Invalid(String::from("Invalid syntax for SET operation")),
        }
    }
}
