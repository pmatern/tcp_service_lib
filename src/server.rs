use mio::{Poll, Events, Token, PollOpt, Ready};
use mio::net::{TcpListener, TcpStream};
use mio::unix::UnixReady;
use std::sync::mpsc::{Sender, Receiver}; 
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
    read: Vec<Sender<MsgBuf>>,
    write: Receiver<MsgBuf>,
    read_idx: usize,
}

impl Server {
    pub fn new(sock: TcpListener, read: Vec<Sender<MsgBuf>>, write: Receiver<MsgBuf>) -> Server {
        Server {
            conns: Slab::with_capacity(128),
            sock: sock,
            token: Token(10_000_000),
            events: Events::with_capacity(1024),
            read: read,
            write: write,
            read_idx: 0,
        }
    }

    pub fn run(&mut self, poll: &mut Poll) -> Result<()> {
        poll.register(&self.sock, self.token, Ready::readable(), PollOpt::edge())?;
        
        loop {
            self.handle_writes();
            
            //write events coming in need to wake up this poller. may need to implement Evented for the write receiver after all
            let cnt = poll.poll(&mut self.events, None)?;
            debug!("processing {} events", cnt);

            #[allow(deprecated)]
            for i in 0..cnt {    
                if let Some(evt) = self.events.get(i) { //index based loop to appease borrow checker
                    match self.handle_event(evt.token(), evt.readiness(), poll) {
                        Ok(true) => {},
                        Ok(false) => {
                            info!("exiting server loop");
                            return Ok(());
                        }
                        Err(e) => {
                            warn!("error processing event: {:?}", e);
                        }
                    }
                }
            }
        }
    }

    fn handle_writes(&mut self) {
        let mut new_writes = Vec::new();

        for msg in self.write.try_iter() {
            new_writes.push(msg);
        }

        for msg in new_writes {
            if let Some(conn) = self.lookup_conn(msg.conn_idx) {
                match conn.send_message(msg.buf) {
                    Ok(_) => {},
                    Err(e) => {
                        error!("failed to send message to connection {:?}", e);
                    }
                }
            }
        }
    }

    fn handle_event(&mut self, token: Token, event: Ready, poll: &mut Poll) -> Result<(bool)> {
        debug!("{:?} event = {:?}", token, event);
        let conn_idx = usize::from(token);

        if self.token != token && !self.conns.contains(conn_idx) {
            warn!("unable to find connection for token {:?}", token);
            return Ok(true);
        }

        let uevent = UnixReady::from(event);
        if uevent.is_error()|| uevent.is_hup() {
            warn!("error signaled for connection {:?}", token);
            self.remove_conn(conn_idx);
            return Ok(true);
        }

        if event.is_writable() {
            assert!(self.token != token, "received writable event for server");
            let mut write_fail = false;

            if let Some(conn) = self.lookup_conn(conn_idx) {
                match conn.handle_write() {
                    Ok(()) => {},
                    Err(e) => {
                        warn!("write event failed for connection {:?} due to error {:?}", token, e);
                        write_fail = true;
                    }
                }  
            } 

            if write_fail {
                self.conns.remove(conn_idx);
                return Ok(true);
            } 
        } 
        
        if event.is_readable() {
            if self.token == token {
                self.accept(poll);
            } else {
                match self.dispatch_messages(conn_idx) {
                    Ok(true) => {},
                    Ok(false) => return Ok(false),
                    Err(e) => {
                        warn!("failed to dispatch messages for connection {:?} due to error {:?}", token, e);
                    }
                }    
            }
        }

        if self.token != token {
            let mut remove = false;
            if let Some(conn) = self.lookup_conn(conn_idx) {
                match conn.register(poll, false) {
                    Ok(()) => {},
                    Err(e) => {
                        warn!("unable to reregister connection {:?} due to error {:?}", token, e);
                        remove = true;
                    }
                }
            }

            if remove {
                self.remove_conn(conn_idx);
            }
        }

        Ok(true)
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
            let mut remove = false;

            if let Some(conn) = self.lookup_conn(conn_idx) {
                match conn.register(poll, true) {
                    Ok(_) => {},
                    Err(e) => {
                        error!("failed to register {:?} connection with poller, {:?}", token, e);
                        remove = true;
                    }
                }
            }

            if remove {
                self.remove_conn(conn_idx);
            }
        }
    }

    fn dispatch_messages(&mut self, conn_idx: usize) -> Result<(bool)> {
        let read_idx = self.read_idx;
        self.read_idx+=1 % self.read.len();

        let mut new_msgs = Vec::new();

        if let Some(conn) = self.lookup_conn(conn_idx) {
            while let Some(message) = conn.handle_read()? {
                new_msgs.push(message);   
            }
        }

        for message in new_msgs {
            match self.read[read_idx].send(MsgBuf::new(conn_idx, message)) {
                Ok(()) => {},
                Err(e) => {
                    info!("unable to dispatch message for connection {} due to {:?}. presuming shutdown", conn_idx, e);
                    return Ok(false);
                }
            }
        }

        Ok(true)
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

    fn lookup_conn(&mut self, conn_idx: usize) -> Option<&mut Connection> {
        match self.conns.get_mut(conn_idx) {
            Some(conn) => Some(conn),
            None => {
                info!("unable to look up connection {}", conn_idx);
                None
            }
        }
    }
}