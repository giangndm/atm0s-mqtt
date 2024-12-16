use futures::{SinkExt, StreamExt};
use mqtt::packet::{
    ConnackPacket, ConnectPacket, MqttCodec, PingrespPacket, PubackPacket, PublishPacket, SubackPacket, SubscribePacket, UnsubackPacket, UnsubscribePacket, VariablePacket, VariablePacketError,
};
use thiserror::Error;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};
use tokio_util::codec::{FramedRead, FramedWrite};

#[derive(Debug)]
pub enum Output {
    Continue,
    Connect(ConnectPacket),
    Subscribe(SubscribePacket),
    Unsubscribe(UnsubscribePacket),
    Publish(PublishPacket),
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("codec error: {0}")]
    Codec(#[from] VariablePacketError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct Session {
    reader: FramedRead<OwnedReadHalf, MqttCodec>,
    writer: FramedWrite<OwnedWriteHalf, MqttCodec>,
}

impl Session {
    pub fn new(tcp_stream: TcpStream) -> Session {
        let (reader, writer) = tcp_stream.into_split();
        Session {
            reader: FramedRead::new(reader, MqttCodec::new()),
            writer: FramedWrite::new(writer, MqttCodec::new()),
        }
    }

    pub async fn conn_ack(&mut self, ack: ConnackPacket) -> Result<(), Error> {
        self.writer.send(mqtt::packet::VariablePacket::ConnackPacket(ack)).await?;
        Ok(())
    }

    pub async fn sub_ack(&mut self, ack: SubackPacket) -> Result<(), Error> {
        self.writer.send(mqtt::packet::VariablePacket::SubackPacket(ack)).await?;
        Ok(())
    }

    pub async fn unsub_ack(&mut self, ack: UnsubackPacket) -> Result<(), Error> {
        self.writer.send(mqtt::packet::VariablePacket::UnsubackPacket(ack)).await?;
        Ok(())
    }

    pub async fn pub_ack(&mut self, ack: PubackPacket) -> Result<(), Error> {
        self.writer.send(mqtt::packet::VariablePacket::PubackPacket(ack)).await?;
        Ok(())
    }

    pub async fn publish(&mut self, pkt: PublishPacket) -> Result<(), Error> {
        self.writer.send(mqtt::packet::VariablePacket::PublishPacket(pkt)).await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Option<Output>, Error> {
        let item = match self.reader.next().await {
            Some(Ok(item)) => item,
            Some(Err(err)) => return Err(Error::Codec(err)),
            None => return Ok(None),
        };
        match item {
            mqtt::packet::VariablePacket::ConnectPacket(connect_packet) => {
                log::info!("connect packet: {:#?}", connect_packet);
                Ok(Some(Output::Connect(connect_packet)))
            }
            mqtt::packet::VariablePacket::PingreqPacket(_) => {
                self.writer.send(VariablePacket::PingrespPacket(PingrespPacket::new())).await?;
                Ok(Some(Output::Continue))
            }
            mqtt::packet::VariablePacket::PublishPacket(publish_packet) => {
                log::info!("publish packet: {:#?}", publish_packet);
                Ok(Some(Output::Publish(publish_packet)))
            }
            mqtt::packet::VariablePacket::SubscribePacket(subscribe_packet) => {
                log::info!("subscribe packet: {:#?}", subscribe_packet);
                Ok(Some(Output::Subscribe(subscribe_packet)))
            }
            mqtt::packet::VariablePacket::UnsubscribePacket(unsubscribe_packet) => {
                log::info!("unsubscribe packet: {:#?}", unsubscribe_packet);
                Ok(Some(Output::Unsubscribe(unsubscribe_packet)))
            }
            mqtt::packet::VariablePacket::DisconnectPacket(disconnect_packet) => {
                log::info!("disconnect packet: {:#?}", disconnect_packet);
                Ok(None)
            }
            _ => Ok(Some(Output::Continue)),
        }
    }
}
