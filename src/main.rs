use std::{net::TcpListener, io::Read};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream ) => {
                let mut buf = String::new();
                stream.read_to_string(&mut buf).unwrap();
                println!("Recieved {buf}");
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
