use mio::{Poll, Events, Token, PollOpt, Ready};
use mio::net::{TcpListener, TcpStream};
use mio::unix::UnixReady;
use channel::{Sender, Receiver}; 
use slab::Slab;
use connection::Connection;
use worker::MsgBuf;
use errors::*;

use std::io::ErrorKind;

pub struct Server {
    conns: Slab<Connection>,
    sock: TcpListener,
    token: Token,
    events: Events,
    read: Sender<MsgBuf>,
    write: Receiver<MsgBuf>,
}

impl Server {
    pub fn new(sock: TcpListener, read: Sender<MsgBuf>, write: Receiver<MsgBuf>) -> Server {
        Server {
            conns: Slab::with_capacity(128),
            sock: sock,
            token: Token(10_000_000),
            events: Events::with_capacity(1024),
            read: read,
            write: write,
        }
    }

    pub fn run(&mut self, poll: &mut Poll) -> Result<()> {
        poll.register(&self.sock, self.token, Ready::readable(), PollOpt::edge())?;

        loop {
            let cnt = poll.poll(&mut self.events, None)?;
            debug!("processing {} events", cnt);

            #[allow(deprecated)]
            for i in 0..cnt {    
                if let Some(evt) = self.events.get(i) { //index based loop to appease borrow checker
                    //todo handle a return value that says when to break
                    match self.handle_event(evt.token(), evt.readiness(), poll) {
                        Ok(()) => {},
                        Err(e) => {
                            warn!("error processing event: {:?}", e);
                        }
                    }
                }
            }

            self.handle_writes();
        }
    }

    fn handle_writes(&mut self) {
        let write_tx = self.write.clone();
        
        for msg in write_tx.try_iter() {
            match self.lookup_conn(msg.conn_idx).send_message(msg.buf) {
                Ok(_) => {},
                Err(e) => {
                    error!("failed to send message to connection {:?}", e);
                }
            }
        }
    }

    fn handle_event(&mut self, token: Token, event: Ready, poll: &mut Poll) -> Result<()> {
        debug!("{:?} event = {:?}", token, event);
        let conn_idx = usize::from(token);

        if self.token != token && !self.conns.contains(conn_idx) {
            warn!("unable to find connection for token {:?}", token);
            return Ok(());
        }

        let uevent = UnixReady::from(event);
        if uevent.is_error()|| uevent.is_hup() {
            warn!("error signaled for connection {:?}", token);
            self.remove_conn(conn_idx);
            return Ok(());
        }

        if event.is_writable() {
            assert!(self.token != token, "received writable event for server");
            match self.lookup_conn(conn_idx).handle_write() {
                Ok(()) => {},
                Err(e) => {
                    warn!("write event failed for connection {:?} due to error {:?}", token, e);
                    self.conns.remove(conn_idx);
                    return Ok(());
                }
            }     
        } 
        
        if event.is_readable() {
            if self.token == token {
                self.accept(poll);
            } else {
                match self.dispatch_messages(conn_idx) {
                    Ok(()) => {},
                    Err(e) => {
                        warn!("failed to dispatch messages for connection {:?} due to error {:?}", token, e);
                    }
                }    
            }
        }

        if self.token != token {
            match self.lookup_conn(conn_idx).register(poll, false) {
                Ok(()) => {},
                Err(e) => {
                    warn!("unable to reregister connection {:?} due to error {:?}", token, e);
                    self.remove_conn(conn_idx);
                }
            }
        }

        Ok(())
    }

    fn accept(&mut self, poll: &mut Poll) {
        loop {
            let sock = match self.sock.accept() {
                Ok((sock, _)) => sock,
                Err(e) => {
                    if e.kind() != ErrorKind::WouldBlock {
                        error!("failed to accept new socket, {:?}", e);
                    }
                    return;
                }
            };

            let conn_idx = self.add_conn(sock);
            let token = Token::from(conn_idx);

            debug!("registering {:?} with poller", token);
            match self.lookup_conn(conn_idx).register(poll, true) {
                Ok(_) => {},
                Err(e) => {
                    error!("failed to register {:?} connection with poller, {:?}", token, e);
                    self.remove_conn(conn_idx);
                }
            }
        }
    }

    fn dispatch_messages(&mut self, conn_idx: usize) -> Result<()> {
        let read_sender = self.read.clone();
        let conn = self.lookup_conn(conn_idx);
        
        while let Some(message) = conn.handle_read()? {
            match read_sender.send(MsgBuf::new(conn_idx, message)) {
                Ok(()) => {},
                Err(e) => {
                    warn!("unable to dispatch message for connection {} due to {:?}", conn_idx, e);
                }
            }
        }

        Ok(())
    }

    fn add_conn(&mut self, sock: TcpStream) -> usize {
        let entry = self.conns.vacant_entry();
        let conn_idx = entry.key();
        entry.insert(Connection::new(sock, Token::from(conn_idx)));
        conn_idx
    }

    fn remove_conn(&mut self, conn_idx: usize) {
       self.conns.remove(conn_idx);
    }

    fn lookup_conn(&mut self, conn_idx: usize) -> &mut Connection {
        return self.conns.get_mut(conn_idx).expect("nonexistent connection");
    }
}