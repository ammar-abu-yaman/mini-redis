use std::io;
use std::io::Cursor;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net;
use rand;

use crate::dataframe::DataFrame;
use crate::operation::Operation;
use crate::operation::OperationDeducer;
use crate::operation::SetOptions;
use crate::operation::StandardOperationDeducer;
use crate::parse::RedisParser;
use crate::parse::RespParser;
use crate::store::ConcurrentHashtable;
use crate::store::Store;
use crate::value::Value;

const CLEANER_TASK_FREQUENCY: Duration = Duration::from_millis(10);
const CLEANER_TASK_SAMPLE_SIZE: usize = 20;
const CLEANER_TASK_SUCCESS_FACTOR: usize = 4;

struct Context<P, D, S> {
    parser: Arc<P>,
    deducer: Arc<D>,
    store: Arc<S>,
}

unsafe impl<P, D, S> Send for Context<P, D, S>
where
    P: Send,
    D: Send,
    S: Send,
{
}

pub struct Server<
    P = RespParser,
    D = StandardOperationDeducer,
    S = ConcurrentHashtable<String, DataFrame<String>>,
> {
    port: String,
    parser: Arc<P>,
    deducer: Arc<D>,
    store: Arc<S>,
}

impl Server<RespParser, StandardOperationDeducer, ConcurrentHashtable<String, DataFrame<String>>> {
    pub fn new(port: impl Into<String>) -> Self {
        Self {
            port: port.into(),
            parser: Arc::new(RespParser::new()),
            deducer: Arc::new(StandardOperationDeducer::new()),
            store: Arc::new(ConcurrentHashtable::with_shards(100000)),
        }
    }
}

impl<P, D, S> Server<P, D, S>
where
    P: RedisParser<Cursor<String>> + 'static + Sync,
    D: OperationDeducer + 'static + Sync,
    S: Store<String, DataFrame<String>> + 'static + Sync,
{
    pub async fn listen(&self) {
        self.spawn_expiration_cleaner_task(CLEANER_TASK_FREQUENCY).await;
        let port = &self.port;
        let addr = format!("localhost:{port}");
        let listener = net::TcpListener::bind(addr).await;
        match listener {
            Ok(listener) => loop {
                let stream = listener.accept().await;

                let context = Context {
                    parser: Arc::clone(&self.parser),
                    deducer: Arc::clone(&self.deducer),
                    store: Arc::clone(&self.store),
                };
                tokio::task::spawn(async move {
                    Self::serve(context, stream).await;
                });
            },
            Err(err) => println!("Error starting server: {}", err.to_string()),
        }
    }

    async fn serve(
        context: Context<P, D, S>,
        stream: Result<(net::TcpStream, std::net::SocketAddr), io::Error>,
    ) {
        match stream {
            Ok((mut stream, _)) => loop {
                let input = Self::read_stream(&mut stream).await.unwrap();
                let mut input = Cursor::new(input);
                let token = context.parser.as_ref().parse(&mut input);
                match token {
                    Ok(token) => Self::handle_input(&context, token, &mut stream).await,
                    Err(_) => break,
                };
            },
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    async fn handle_input(context: &Context<P, D, S>, value: Value, stream: &mut net::TcpStream) {
        let op = context.deducer.deduce_operation(&value);
        let mut buf = vec![];
        match op {
            Operation::Ping => write!(buf, "{}", Value::SimpleString(String::from("PONG")))
                .expect("Error while handling request"),
            Operation::Echo(msg) => {
                write!(buf, "{}", Value::BulkString(msg)).expect("Error while handling request")
            }
            Operation::Get(key) => Self::handle_get(&context, key, &mut buf)
                .await
                .expect("Error while handling get"),
            Operation::Set(key, val, options) => {
                Self::handle_set(context, key, val, options, &mut buf)
                    .await
                    .expect("Error while handling set")
            }
            Operation::Invalid(msg) => {
                write!(buf, "{}", Value::Error(msg)).expect("Error while handling request")
            }
        };
        stream.write_all(&buf).await.unwrap();
    }

    async fn handle_get(
        context: &Context<P, D, S>,
        key: String,
        buf: &mut Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let result = context.store.get(key.clone());
        match result {
            None => write!(buf, "{}", Value::NullBulkString),
            Some(df) => {
                if df.has_expired() {
                    context.store.remove(key.clone());
                    return write!(buf, "{}", Value::NullBulkString);
                }
                match df {
                    DataFrame::Plain(data)
                    | DataFrame::Expiring {
                        data,
                        expiration: _,
                        timestamp: _,
                    } => write!(buf, "{}", Value::BulkString(data)),
                    DataFrame::Empty => panic!("_"), // should nevere happen
                }
            }
        }
    }

    async fn handle_set(
        context: &Context<P, D, S>,
        key: String,
        val: String,
        options: SetOptions,
        buf: &mut Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let df = match options.expiration {
            Some(expiration) => DataFrame::with_expiration(val, expiration),
            None => DataFrame::Plain(val),
        };

        context.store.set(key, df);
        write!(buf, "{}", Value::SimpleString(String::from("OK")))
    }

    async fn read_stream(stream: &mut net::TcpStream) -> Result<String, io::Error> {
        let mut buf = [0u8; 512];
        stream.read(&mut buf).await?;
        match String::from_utf8(buf.to_vec()) {
            Ok(s) => Ok(s),
            Err(_) => Err(std::io::Error::from(std::io::ErrorKind::InvalidData)),
        }
    }

    async fn spawn_expiration_cleaner_task(&self, duration: Duration) {
        use tokio::time::interval;
        let context = Context {
            parser: Arc::clone(&self.parser),
            deducer: Arc::clone(&self.deducer),
            store: Arc::clone(&self.store),
        };
        tokio::task::spawn(async move {
            let mut ticker = interval(duration);
            loop {
                ticker.tick().await;
                Self::clean_expired(&context).await;
            }
        }); 
    } 

    async fn clean_expired(context: &Context<P, D, S>) {
        let mut is_done = false;
        while ! is_done {

            use rand::prelude::*;
            let mut expired_keys = vec![];
            context.store.for_each(|k, v| {
                if let DataFrame::Expiring { data: _, expiration, timestamp } = v {
                    expired_keys.push((k.clone(), expiration.clone(), timestamp.clone()))
                }
            });
            let mut rng = thread_rng();
            let sampled_keys = expired_keys 
                .into_iter()
                .choose_multiple(&mut rng, CLEANER_TASK_SAMPLE_SIZE);
                
            if sampled_keys.len() < CLEANER_TASK_SAMPLE_SIZE {
                return;
            }
            let mut removed_count: usize = 0;
            for (key, expiration, timestamp) in sampled_keys {
                if expiration > (Instant::now() - timestamp) {
                    continue;
                }
                removed_count += context.store.remove(key) as usize;
            }
            is_done = removed_count <= CLEANER_TASK_SAMPLE_SIZE / CLEANER_TASK_SUCCESS_FACTOR;
        }

    }

}
