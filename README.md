# tcp_service_lib
Learning project originally based on https://github.com/hjr3/mob

Implements a non-blocking tcp server with one dedicated I/O thread and configurable number of worker threads.
Currently the message framing isn't configurable. The library expects incoming messages to be prepended by their length in bytes.
Specifically, first 8 bytes of a message should contain a big endian, unsigned 64 bit integer specifying how many bytes follow in the complete message.

Here's a basic example program implementing a server which reverses and echoes strings sent by the client:
```rust
extern crate tcp_service_lib;

use std::net::SocketAddr;
use tcp_service_lib::MessageHandler;
use tcp_service_lib::errors as lib_errors;

struct Reverser{}

impl MessageHandler for Reverser {
    type Req = String;
    type Resp = String;

    fn process(&self, msg: String) -> lib_errors::Result<String> {
        let msg = msg.chars().rev().collect::<String>();
        Ok(msg)
    }

    fn serialize(&self, msg: String) -> lib_errors::Result<Vec<u8>> {
        Ok(msg.as_bytes().to_vec())
    }

    fn deserialize(&self, buf: Vec<u8>) -> lib_errors::Result<String> {
        match String::from_utf8(buf) {
            Ok(msg) => Ok(msg),
            Err(_) => Err("couldn't build string".into())
        }
    }
}

static HANDLER: Reverser = Reverser{};

fn main() {
    let addr: SocketAddr = "127.0.0.1:7777".parse().expect("couldn't parse listen address");
    let num_workers = 4;

    if let Ok(_sd) = tcp_service_lib::bootstrap(addr, num_workers, &HANDLER) {    
        loop {}
    } else {
        panic!("couldn't start server");
    }
    
}
```
