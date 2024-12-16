use std::ops::AddAssign;

use mqtt::packet::{PublishPacket, SubscribePacket, UnsubscribePacket};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Default)]
pub struct LegId(u64);

impl AddAssign<u64> for LegId {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum LegControl {
    Subscribe(SubscribePacket),
    Unsubscribe(UnsubscribePacket),
    Publish(PublishPacket),
}

#[derive(Debug, PartialEq, Eq)]
pub enum LegOutput {
    Publish(PublishPacket),
}

pub struct Leg {
    id: LegId,
    control_tx: UnboundedSender<(LegId, LegControl)>,
    event_rx: UnboundedReceiver<LegOutput>,
}

impl Leg {
    pub(crate) fn new(id: LegId, control_tx: UnboundedSender<(LegId, LegControl)>, event_rx: UnboundedReceiver<LegOutput>) -> Self {
        Self { id, control_tx, event_rx }
    }

    pub fn id(&self) -> LegId {
        self.id
    }

    pub async fn subscribe(&mut self, sub: SubscribePacket) {
        self.control_tx.send((self.id, LegControl::Subscribe(sub))).expect("should send to main loop");
    }

    pub async fn unsubscribe(&mut self, unsub: UnsubscribePacket) {
        self.control_tx.send((self.id, LegControl::Unsubscribe(unsub))).expect("should send to main loop");
    }

    pub async fn publish(&mut self, pkt: PublishPacket) -> Option<()> {
        self.control_tx.send((self.id, LegControl::Publish(pkt))).expect("should send to main loop");
        None
    }

    pub async fn recv(&mut self) -> Option<LegOutput> {
        self.event_rx.recv().await
    }
}
