use std::{net::{TcpListener, TcpStream}, io::{Read, Write}};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Started server successfully");    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream ) => {
                // println!("Established a new Connection");
                // let input = parse_array(&mut stream);
                // if input[0] == "PING" {
                //     write!(stream, "+PONG\r\n").unwrap();
                // }
                write!(stream, "+PONG\r\n").unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn parse_array(stream: &mut TcpStream) -> Vec<String> {
    let mut result = vec![];
    let mut buf = [0u8; 256];
    stream.bytes().next();
    let array_len: i32 = parse_len(stream.bytes(), &mut buf);
    for _ in 0..array_len {
        let string = {
            stream.bytes().next();
            let string_len = parse_len(stream.bytes(), &mut buf) as usize;
            stream.read_exact(&mut buf[..string_len]).unwrap();
            stream.bytes().next();
            stream.bytes().next();
            String::from_utf8(buf[..string_len].to_vec())
        };
        result.push(string.unwrap());
    }
    result
}

fn parse_len(mut stream: std::io::Bytes<&mut TcpStream>, buf: &mut [u8; 256]) -> i32 {
    let mut len = 0usize;
    while let Some(byte) = stream.next() {
        let byte = byte.unwrap();
        if matches!(byte, b'0'..=b'9' | b'-') {
            buf[len] = byte;
            len += 1;
        } else {
            break;
        }
    }
    stream.next();

    let array_len_str_rep = String::from_utf8(buf[..len].to_vec()).unwrap();
    array_len_str_rep.parse().unwrap()
}