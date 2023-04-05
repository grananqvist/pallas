use std::marker::PhantomData;

use pallas_codec::Fragment;
use pallas_multiplexer::agents::{Channel, ChannelBuffer};

use super::protocol::{Error, Message, State, TxBody, TxIdAndSize};

pub enum Request<TxId> {
    TxIds(u16, u16),
    TxIdsNonBlocking(u16, u16),
    Txs(Vec<TxId>),
}

pub struct Client<H, TxId>(State, ChannelBuffer<H>, PhantomData<TxId>)
where
    H: Channel,
    Message<TxId>: Fragment;

impl<H, TxId> Client<H, TxId>
where
    H: Channel,
    Message<TxId>: Fragment,
{
    pub fn new(channel: H) -> Self {
        Self(State::Init, ChannelBuffer::new(channel), PhantomData {})
    }

    pub fn state(&self) -> &State {
        &self.0
    }

    pub fn is_done(&self) -> bool {
        self.0 == State::Done
    }

    fn has_agency(&self) -> bool {
        !matches!(self.state(), State::Idle)
    }

    fn assert_agency_is_ours(&self) -> Result<(), Error> {
        if !self.has_agency() {
            Err(Error::AgencyIsTheirs)
        } else {
            Ok(())
        }
    }

    fn assert_agency_is_theirs(&self) -> Result<(), Error> {
        if self.has_agency() {
            Err(Error::AgencyIsOurs)
        } else {
            Ok(())
        }
    }

    /// As a client in a specific state, am I allowed to send this message?
    fn assert_outbound_state(&self, msg: &Message<TxId>) -> Result<(), Error> {
        match (&self.0, msg) {
            (State::Init, Message::Init) => Ok(()),
            (State::TxIdsBlocking, Message::ReplyTxIds(..)) => Ok(()),
            (State::TxIdsBlocking, Message::Done) => Ok(()),
            (State::TxIdsNonBlocking, Message::ReplyTxIds(..)) => Ok(()),
            (State::Txs, Message::ReplyTxs(..)) => Ok(()),
            _ => Err(Error::InvalidOutbound),
        }
    }

    /// As a client in a specific state, am I allowed to receive this message?
    fn assert_inbound_state(&self, msg: &Message<TxId>) -> Result<(), Error> {
        match (&self.0, msg) {
            (State::Idle, Message::RequestTxIds(..)) => Ok(()),
            (State::Idle, Message::RequestTxs(..)) => Ok(()),
            _ => Err(Error::InvalidInbound),
        }
    }

    pub fn send_message(&mut self, msg: &Message<TxId>, assert_state: bool) -> Result<(), Error> {
        if assert_state {
            self.assert_agency_is_ours()?;
            self.assert_outbound_state(msg)?;
        }
        self.1.send_msg_chunks(msg).map_err(Error::ChannelError)?;

        Ok(())
    }

    pub fn recv_message(&mut self) -> Result<Message<TxId>, Error> {
        self.assert_agency_is_theirs()?;
        let msg = self.1.recv_full_msg().map_err(Error::ChannelError)?;
        self.assert_inbound_state(&msg)?;

        Ok(msg)
    }

    pub fn send_init(&mut self) -> Result<(), Error> {
        let msg = Message::Init;
        self.send_message(&msg, true)?;
        self.0 = State::Idle;

        Ok(())
    }

    pub fn reply_tx_ids(&mut self, ids: Vec<TxIdAndSize<TxId>>) -> Result<(), Error> {
        let msg = Message::ReplyTxIds(ids);
        self.send_message(&msg, true)?;
        self.0 = State::Idle;

        Ok(())
    }

    pub fn reply_txs(&mut self, txs: Vec<TxBody>) -> Result<(), Error> {
        let msg = Message::ReplyTxs(txs);
        self.send_message(&msg, false)?;
        self.0 = State::Idle;

        Ok(())
    }

    pub fn set_idle(&mut self) -> Result<(), Error> {
        self.0 = State::Idle;
        Ok(())
    }

    pub fn next_request(&mut self) -> Result<Request<TxId>, Error> {
        match self.recv_message()? {
            Message::RequestTxIds(blocking, ack, req) => {
                self.0 = State::TxIdsBlocking;

                match blocking {
                    true => Ok(Request::TxIds(ack, req)),
                    false => Ok(Request::TxIdsNonBlocking(ack, req)),
                }
            }
            Message::RequestTxs(x) => {
                self.0 = State::Txs;
                Ok(Request::Txs(x))
            }
            _ => Err(Error::InvalidInbound),
        }
    }

    pub fn send_done(&mut self) -> Result<(), Error> {
        let msg = Message::Done;
        self.send_message(&msg, true)?;
        self.0 = State::Done;

        Ok(())
    }
}
