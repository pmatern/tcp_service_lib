use ::MessageHandler;
use channel::{Sender, Receiver};
use errors::*;

pub struct MsgBuf {
    pub conn_idx: usize,
    pub buf: Vec<u8>,
}

impl MsgBuf {
    pub fn new(conn_idx: usize, buf: Vec<u8>) -> MsgBuf {
        MsgBuf {
            conn_idx: conn_idx,
            buf: buf,
        }
    }
}

pub struct Worker<'a, I: 'a, O: 'a> {
    handler: &'a MessageHandler<Req=I, Resp=O>,
    read_rx: &'a Receiver<MsgBuf>,
    write_tx: &'a Sender<MsgBuf>,
}

impl<'a, I, O> Worker<'a, I, O> {
    pub fn new(handler: &'a MessageHandler<Req=I, Resp=O>, read_rx: &'a Receiver<MsgBuf>, 
            write_tx: &'a Sender<MsgBuf>) -> Worker<'a, I, O> {
        Worker {
            handler: handler,
            read_rx: read_rx,
            write_tx: write_tx
        }
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            if !self.readloop() {
                info!("worker exiting");
                return Ok(());
            }
        }
    }

    fn readloop(&self) -> bool {
        match self.read_rx.recv() {
            Ok(buf) => {
                self.handle_input(buf)
            },
            Err(e) => {
                if self.read_rx.is_disconnected() {
                    info!("input channel disconnected");
                    false
                } else {
                    warn!("error receiving input in worker: {:?}", e);
                    true
                }
            }
        }
    }

    fn handle_input(&self, buf: MsgBuf) -> bool {
        match self.handler.deserialize(buf.buf) {
            Ok(req) => {
                self.process_and_reply(buf.conn_idx, req)
            },
            Err(e) => {
                warn!("unable to deserialize message: {:?}", e);
                true
            }
        }
    }


    fn process_and_reply(&self, conn_idx: usize, req: I) -> bool {
        match self.handler.process(req) {
            Ok(resp) => {
                self.serialize_and_write(conn_idx, resp)
            },
            Err(e) => {
                warn!("unable to process message: {:?}", e);
                true
            }
        }
    }

    fn serialize_and_write(&self, conn_idx: usize, resp: O) -> bool {
        match self.handler.serialize(resp) {
            Ok(buf) => {
                self.write_response(MsgBuf::new(conn_idx, buf))
            },
            Err(e) => {
                warn!("unable to serialize response: {:?}", e);
                true
            }
        }
    }

    fn write_response(&self, buf: MsgBuf) -> bool {
        match self.write_tx.send(buf) {
            Ok(()) => true,
            Err(e) => {
                if self.write_tx.is_disconnected() {
                    info!("output channel disconnected");
                    false
                } else {
                    warn!("error sending output in worker: {:?}", e);
                    true
                }
            }
        }
    }
}