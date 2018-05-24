use std::collections::VecDeque;

use mio::{Token, Ready, Poll, PollOpt};
use mio::net::{TcpStream};
use mio::unix::UnixReady;

use errors::*;
use std::io::prelude::*;
use std::io::ErrorKind;

use byteorder::{ByteOrder, BigEndian};

pub struct Connection {    
    pub token: Token,

    sock: TcpStream,
    interest: Ready,
    send_queue: VecDeque<Vec<u8>>,
    left_to_read: Option<u64>,
    partial_write: bool,
}

impl Connection {
    pub fn new(sock: TcpStream, token: Token) -> Connection {
        Connection {
            token: token,
            sock: sock,
            interest: Ready::from(UnixReady::hup()),
            send_queue: VecDeque::with_capacity(32),
            left_to_read: None,
            partial_write: false,
        }
    }

    pub fn send_message(&mut self, message: Vec<u8>) -> Result<()> {
        if self.send_queue.is_empty() {
            self.write_message(message)?;
        } else {
            self.send_queue.push_back(message);
        }

        if !self.send_queue.is_empty() && !self.interest.is_writable() {
            self.interest.insert(Ready::writable());
        }

        Ok(())
    }

    pub fn handle_read(&mut self) -> Result<(Option<Vec<u8>>)> {
        let msg_len = match self.read_message_length()? {
            None => { 
                return Ok(None); 
            },
            Some(n) => n,
        };

        if msg_len == 0 {
            return Ok(None);
        }

        let msg_len = msg_len as usize;
        debug!("Expected message length is {}", msg_len);

        // Here we allocate and set the length with unsafe code. The risks of this are discussed
        // at https://stackoverflow.com/a/30979689/329496 and are mitigated as recv_buf is
        // abandoned below if we don't read msg_leg bytes from the socket
        let mut recv_buf: Vec<u8> = Vec::with_capacity(msg_len);
        unsafe { 
            recv_buf.set_len(msg_len); 
        }

        // UFCS: resolve "multiple applicable items in scope [E0034]" error
        let sock_ref = <TcpStream as Read>::by_ref(&mut self.sock);

        match sock_ref.take(msg_len as u64).read(&mut recv_buf) {
            Ok(n) => {
                debug!("read {} bytes", n);
                if n < msg_len as usize {
                    return Err("Did not read enough bytes".into());
                }

                self.left_to_read = None;

                Ok(Some(recv_buf.to_vec()))
            },
            Err(e) => {
                if e.kind() == ErrorKind::WouldBlock {
                    self.left_to_read = Some(msg_len as u64);
                    Ok(None)
                }
                else {
                    Err(e.into())
                }
            },
        }
    }
    
    fn read_message_length(&mut self) -> Result<Option<u64>> {
        if let Some(n) = self.left_to_read {
            return Ok(Some(n));
        }

        let mut buf = [0u8; 8];

        let bytes = match self.sock.read(&mut buf) {
            Ok(n) => n,
            Err(e) => {
                if e.kind() == ErrorKind::WouldBlock {
                    return Ok(None);
                } else {
                    return Err(e.into());
                }
            },
        };

        if bytes < 8 {
            return Err("Invalid message length".into());
        }

        let msg_len = BigEndian::read_u64(buf.as_ref());
        Ok(Some(msg_len))
    }

    pub fn handle_write(&mut self) -> Result<()> {
        self.send_queue.pop_front()
            .ok_or("Send queue is empty on write".into())
            .and_then(|buf|{
                self.write_message(buf)
            })?;

        if self.send_queue.is_empty() {
            self.interest.remove(Ready::writable());
        }

        Ok(())
    }

    fn write_message(&mut self, buf: Vec<u8>) -> Result<()> {
        match self.write_message_length(&buf) {
            Ok(None) => {
                self.send_queue.push_front(buf);
                return Ok(());
            }, 
            Ok(Some(())) => {
                ()
            },
            Err(e) => {
                return Err(e.into());
            },
        }

        let len = buf.len();
        match self.sock.write(&buf) {
            Ok(n) => {
                if n < len {
                    let remaining = buf[n..].to_vec();
                    self.send_queue.push_front(remaining);
                    self.partial_write = true;
                } else {
                    self.partial_write = false;
                }
                Ok(())
            },
            Err(e) => {
                if e.kind() == ErrorKind::WouldBlock {
                    self.send_queue.push_front(buf);
                    self.partial_write = true;
                    Ok(())
                } else {
                    Err(e.into())
                }
                
            },
        }
    }

    fn write_message_length(&mut self, buf: &Vec<u8>) -> Result<Option<()>> {
        if self.partial_write {
            return Ok(Some(()));
        }

        let mut len_buf = [0u8; 8];
        BigEndian::write_u64(&mut len_buf, buf.len() as u64);

        let len = len_buf.len();

        match self.sock.write(&len_buf) {
            Ok(n) => {
                if n < len {
                    Err("Failed to write message length".into())
                } else {
                    Ok(Some(()))
                }
            },
            Err(e) => {
                if e.kind() == ErrorKind::WouldBlock {
                    Ok(None)
                } else {
                    Err(e.into())
                }
            },
        }
    }

    pub fn register(&mut self, poll: &mut Poll, initial: bool) -> Result<()> {
        if initial {
            self.interest.insert(Ready::readable());
        }
        match poll.register(&self.sock, self.token, self.interest, PollOpt::edge()) {
            Ok(()) => Ok(()),
            Err(e) => Err(e.into())
        }
    }
}