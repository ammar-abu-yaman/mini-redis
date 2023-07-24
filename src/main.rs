use std::{net::TcpListener, io::Read};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream ) => {
                println!("connected");
                let mut buf = [0u8; 4];
                stream.read_exact(&mut buf);
                println!("Recieved {}", String::from_utf8(buf.to_vec()).unwrap());
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
