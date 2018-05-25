#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;
#[macro_use] 
extern crate log;
extern crate crossbeam_channel as channel;
extern crate byteorder;
extern crate mio;
extern crate slab;
extern crate crossbeam;


mod errors {
    // Create the Error, ErrorKind, ResultExt, and Result types
    error_chain! {
        foreign_links {
            Fmt(::std::fmt::Error);
            Io(::std::io::Error) #[cfg(unix)];
            Net(::std::net::AddrParseError);
            ChanRecv(::channel::TryRecvError);
            ChanSend(::channel::TrySendError<Vec<u8>>);
        }
    }
}

mod worker;
mod connection;
mod server;

use std::net::SocketAddr;
use channel::{Sender,Receiver};
use worker::MsgBuf;
use mio::Poll;
use mio::net::TcpListener;
use errors::*;
use worker::Worker;
use server::Server;

/*
learning project based on https://github.com/hjr3/mob
Single threaded non blocking echo server which frames messages by prepending the message length
todo is add threads per cpu,
and maybe crossbeam-channel to pass stuff between threads?
*/

/*
fn main() {
    if let Err(ref e) = run() {
        println!("error: {}", e);

        for e in e.iter().skip(1) {
            println!("caused by: {}", e);
        }

        if let Some(backtrace) = e.backtrace() {
            println!("backtrace: {:?}", backtrace);
        }

        ::std::process::exit(1);
    }
}



pub fn run() -> Result<()> {
    env_logger::init();

    let matches = clap_app!(myapp =>
        (version: "0.1.0")
        (author: "pete.matern@nike.com")
        (about: "service proxy with fancy networking behavior")
        (@arg listen_addr: -a --listen_address +takes_value "address to bind to")
        (@arg port: -p --port +takes_value "port to listen on")
    ).get_matches();

    let mut listen_addr = String::from(matches.value_of("listen_addr").unwrap_or("127.0.0.1"));
    listen_addr.push_str(":");
    listen_addr.push_str(matches.value_of("port").unwrap_or("7777"));
        
    let addr: SocketAddr = listen_addr.parse()?;
    let sock = TcpListener::bind(&addr)?;
    let (ready_tx, ready_rx) = channel::unbounded();
    let (register_tx, register_rx) = channel::unbounded();

    crossbeam::scope(|s| {
        let mut worker = Worker::new(read_rx, register_tx);

        info!("worker starting");

        worker.run().expect("failed to start worker");
    });
    
    let mut poll = Poll::new()?;

    let mut server = Acceptor::new(poll, sock, ready_tx, register_rx);

    info!("acceptor starting on {}", listen_addr);

    server.run().expect("failed to start server");

    Ok(())
}
*/


pub trait MessageHandler: Sync {
    type Req;
    type Resp;
    fn process(&self, msg: Self::Req) -> Result<Self::Resp>;
    fn serialize(&self, msg: Self::Resp) -> Result<Vec<u8>>;
    fn deserialize(&self, buf: Vec<u8>) -> Result<Self::Req>;
}

pub struct Shutdown {
    read_tx: Sender<MsgBuf>,
    read_rx: Receiver<MsgBuf>,
    write_tx: Sender<MsgBuf>,
    write_rx: Receiver<MsgBuf>,
}

impl Shutdown {
    fn new(read_tx: Sender<MsgBuf>, read_rx: Receiver<MsgBuf>,
            write_tx: Sender<MsgBuf>, write_rx: Receiver<MsgBuf>) -> Shutdown {
        Shutdown {
            read_tx: read_tx,
            read_rx: read_rx,
            write_tx: write_tx,
            write_rx: write_rx,
        }    
    }

    pub fn shutdown(&self) {
        self.read_tx.disconnect();
        self.write_tx.disconnect();
        self.read_rx.disconnect();
        self.write_rx.disconnect();
    }
}

//todo return shutdown handle which will close channels and shut it all down
pub fn bootstrap<I, O>(listen_addr: SocketAddr, num_workers: u16, 
        handler: &MessageHandler<Req=I, Resp=O>) -> Result<(Shutdown)> {
    assert!(num_workers >= 2, "num_wokers must be at least two");

    let sock = TcpListener::bind(&listen_addr)?;
    let (read_tx, read_rx) = channel::unbounded();
    let (write_tx, write_rx) = channel::unbounded();

    for _ in 0..num_workers-1 {
        crossbeam::scope(|s| {
            s.spawn(|| {
                let mut worker = Worker::new(handler, &read_rx, &write_tx);
                info!("worker starting");
                worker.run().expect("failed to start worker");
            });
        });
    }

    let server_read_tx = read_tx.clone();
    let server_write_rx = write_rx.clone();

    crossbeam::scope(|s| {
        s.spawn(|| {
            let mut poll = Poll::new().expect("Failed to create poll");
            let mut server = Server::new(sock, server_read_tx, server_write_rx);

            info!("server starting on {}", listen_addr);
            server.run(&mut poll).expect("failed to start server");
        });
    });

    Ok(Shutdown::new(read_tx, read_rx, write_tx, write_rx))
}




#[cfg(test)]
mod tests {

    use ::*;
    use ::std::net::SocketAddr;

    struct Reverser{}

    impl MessageHandler for Reverser {
        type Req = String;
        type Resp = String;

        fn process(&self, msg: String) -> Result<String> {
            let msg = msg.chars().rev().collect::<String>();
            Ok(msg)
        }

        fn serialize(&self, msg: String) -> Result<Vec<u8>> {
            Ok(msg.as_bytes().to_vec())
        }

        fn deserialize(&self, buf: Vec<u8>) -> Result<String> {
            match String::from_utf8(buf) {
                Ok(msg) => Ok(msg),
                Err(_) => Err("couldn't build string".into())
            }
        }
    }

    #[test]
    fn boot() {
        let addr: SocketAddr = "127.0.0.1:7866".parse().expect("couldn't parse address string");
        let num_workers = 4;
        let handler = Reverser{};
        if let Ok(sd) = ::bootstrap(addr, num_workers, &handler) {
             sd.shutdown();
        }
    }
}

