use ::MessageHandler;
use std::sync::mpsc::{Sender, Receiver,TryIter};
use errors::*;
use mio::{Evented, Poll, Token, Ready, PollOpt, Registration,SetReadiness};
use std::io::Result as IOResult;

pub fn write_pipeline(sender: Sender<MsgBuf>, receiver: Receiver<MsgBuf>) -> (MessageSource, MessageSink) {
    let (registration, set_readiness) = Registration::new2();
    (MessageSource{registration: registration, receiver: receiver}, MessageSink{sender: sender, set_readiness: set_readiness})
}

#[derive(Debug, Clone)]
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

    pub fn shutdown_msg() -> MsgBuf {
        MsgBuf {
            conn_idx: 111_111,
            buf: vec!{},
        }

    }
}

#[derive(Debug, Clone)]
pub struct MessageSink {
    sender: Sender<MsgBuf>,
    set_readiness: SetReadiness,
}

impl MessageSink {
    pub fn send_message(&self, msg: MsgBuf) -> Result<()> {
        self.sender.send(msg)?;
        self.set_readiness.set_readiness(Ready::writable())?;
        Ok(())
    }
}


pub struct MessageSource {
    receiver: Receiver<MsgBuf>,
    registration: Registration,
}

impl MessageSource {
    pub fn try_iter(&self) -> TryIter<MsgBuf> {
        self.receiver.try_iter()
    }
}

impl Evented for MessageSource {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> IOResult<()> {
        self.registration.register(poll, token, interest, opts)
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> IOResult<()> {
        self.registration.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> IOResult<()> {
        #[allow(deprecated)]
        self.registration.deregister(poll)
    }
}

pub struct Worker<'a, I: 'a, O: 'a> {
    handler: &'a MessageHandler<Req=I, Resp=O>,
    read_rx: Receiver<MsgBuf>,
    sink: MessageSink,
}

impl<'a, I, O> Worker<'a, I, O> {
    pub fn new(handler: &'a MessageHandler<Req=I, Resp=O>, read_rx: Receiver<MsgBuf>, 
            sink: MessageSink) -> Worker<'a, I, O> {
        Worker {
            handler: handler,
            read_rx: read_rx,
            sink: sink,
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
                info!("error receiving input in worker: {:?}. presuming shutdown", e);
                false
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
        match self.sink.send_message(buf) {
            Ok(()) => true,
            Err(e) => {
                info!("error sending output in worker: {:?}. presuming shutdown", e);
                false
            }
        }
    }
}