#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;
#[macro_use] 
extern crate log;
extern crate byteorder;
extern crate mio;
extern crate slab;

mod worker;
mod connection;
mod server;

pub mod errors {
    use worker::MsgBuf;

    // Create the Error, ErrorKind, ResultExt, and Result types
    error_chain! {
        foreign_links {
            Fmt(::std::fmt::Error);
            Io(::std::io::Error) #[cfg(unix)];
            Net(::std::net::AddrParseError);
            TryChanRecv(::mpsc::TryRecvError);
            TryChanSend(::mpsc::TrySendError<MsgBuf>);
            ChanRecv(::mpsc::RecvError);
            ChanSend(::mpsc::SendError<MsgBuf>);

        }
    }
}

use std::net::SocketAddr;
use mio::Poll;
use mio::net::TcpListener;
use errors::*;
use worker::{Worker,MsgBuf};
use server::Server;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::thread;


pub trait MessageHandler: Sync {
    type Req;
    type Resp;
    fn process(&self, msg: Self::Req) -> Result<Self::Resp>;
    fn serialize(&self, msg: Self::Resp) -> Result<Vec<u8>>;
    fn deserialize(&self, buf: Vec<u8>) -> Result<Self::Req>;
}

pub struct Shutdown {
    read_tx: Vec<Sender<MsgBuf>>,
    write_tx: Sender<MsgBuf>,
}

impl Shutdown {
    pub fn shutdown(&self) -> Result<()> {
        for tx in &self.read_tx {
            tx.send(MsgBuf::shutdown_msg())?;
        }
        self.write_tx.send(MsgBuf::shutdown_msg())?;

        Ok(())
    }
}

pub fn bootstrap<I, O>(listen_addr: SocketAddr, num_workers: u16, 
    handler: &'static MessageHandler<Req=I, Resp=O>) -> Result<(Shutdown)> {
    assert!(num_workers >= 2, "num_wokers must be at least two");

    let sock = TcpListener::bind(&listen_addr)?;
    let (write_tx, write_rx) = mpsc::channel();
    let mut all_read_tx : Vec<Sender<MsgBuf>> = vec!{};

    let sd = Shutdown {
        read_tx: all_read_tx.to_owned(),
        write_tx: write_tx.clone(),
    };
    
    let (source, sink) = worker::write_pipeline(write_tx, write_rx);

    for _ in 0..num_workers-1 {
        let (read_tx, read_rx) = mpsc::channel();
        all_read_tx.push(read_tx);
        let worker_sink = sink.clone();

        thread::spawn(move || {
            let mut worker = Worker::new(handler, read_rx, worker_sink);
            info!("worker starting");
            worker.run().expect("failed to start worker");
        });
    }

    thread::spawn(move || {
        let mut poll = Poll::new().expect("Failed to create poll");
        let mut server = Server::new(sock, all_read_tx, source);

        info!("server starting on {}", listen_addr);
        server.run(&mut poll).expect("failed to start server");
    });
    
    Ok(sd)
}




#[cfg(test)]
mod tests {

    use std::net::SocketAddr;
    use ::MessageHandler;
    use ::errors::*;
    
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

    static HANDLER: Reverser = Reverser{};

    #[test]
    fn boot() {
        let addr: SocketAddr = "127.0.0.1:7866".parse().expect("couldn't parse address string");
        let num_workers = 4;
        if let Ok(sd) = ::bootstrap(addr, num_workers, &HANDLER) {
            match sd.shutdown() {
                Ok(()) => {},
                Err(_e) => {
                    panic!("had trouble shutting down.");
                }
            }
        }
    }
}

