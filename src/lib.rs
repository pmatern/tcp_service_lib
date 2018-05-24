#![recursion_limit = "1024"]

// #[macro_use]
// extern crate clap;
#[macro_use]
extern crate error_chain;
#[macro_use] 
extern crate log;
extern crate crossbeam_channel as channel;
extern crate env_logger;
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

//todo return shutdown handle which will close channels and shut it all down
pub fn bootstrap<I, O>(listen_addr: SocketAddr, num_workers: u16, 
        handler: &MessageHandler<Req=I, Resp=O>) -> Result<()> {

    let sock = TcpListener::bind(&listen_addr)?;
    let (read_tx, read_rx) = channel::unbounded();
    let (write_tx, write_rx) = channel::unbounded();

    for _ in 0..num_workers {
        crossbeam::scope(|s| {
            s.spawn(|| {
                let mut worker = Worker::new(handler, &read_rx, &write_tx);
                info!("worker starting");
                worker.run().expect("failed to start worker");
            });
        });
    }
    let mut poll = Poll::new().expect("Failed to create poll");
    let mut server = Server::new(sock, read_tx, write_rx);
    info!("server starting on {}", listen_addr);
    server.run(&mut poll).expect("failed to start server");

    Ok(())
}




#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

