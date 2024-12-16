use std::net::SocketAddr;

use atm0s_small_p2p::P2pService;
use mqtt::{
    control::ConnectReturnCode,
    packet::{ConnackPacket, PubackPacket, PublishPacket, SubackPacket, UnsubackPacket},
};
use tokio::{net::TcpListener, select};

use crate::hub::{self, LegId, LegOutput};

mod session;

pub struct MqttBroker {
    tcp_listener: TcpListener,
    hub: hub::Hub,
}

impl MqttBroker {
    pub async fn new(listen: SocketAddr, hub_service: P2pService, kv_service: P2pService) -> Result<MqttBroker, std::io::Error> {
        let tcp_listener = TcpListener::bind(listen).await?;
        log::info!("[MqttBroker] listening on {}", listen);
        let hub = hub::Hub::new(hub_service, kv_service);
        Ok(MqttBroker { tcp_listener, hub })
    }

    pub async fn publish(&mut self, pkt: PublishPacket) -> Result<(), hub::HubPublishError> {
        self.hub.publish(pkt).await
    }

    pub async fn recv(&mut self) -> Result<(), std::io::Error> {
        select! {
            e = self.tcp_listener.accept() => {
                let (socket, remote) = e?;
                log::info!("[MqttBroker] new connection {remote}");
                let session = session::Session::new(socket);
                let leg = self.hub.create_leg();
                let mut runner = SessionRunner::new(session, leg);
                tokio::spawn(async move {
                    log::info!("[MqttBroker] session {:?} runner started", runner.id());
                    runner.run_loop().await;
                    log::info!("[MqttBroker] session {:?} runner stopped", runner.id());
                });
                Ok(())
            }
            _ = self.hub.recv() => {
                Ok(())
            }
        }
    }
}

struct SessionRunner {
    session: session::Session,
    leg: hub::Leg,
}

impl SessionRunner {
    pub fn new(session: session::Session, leg: hub::Leg) -> SessionRunner {
        SessionRunner { session, leg }
    }

    pub fn id(&self) -> LegId {
        self.leg.id()
    }

    pub async fn run_loop(&mut self) {
        loop {
            select! {
                out = self.session.recv() => match out {
                    Ok(Some(out)) => match out {
                        session::Output::Continue => {}
                        session::Output::Connect(_connect) => {
                            let ack = ConnackPacket::new(true, ConnectReturnCode::ConnectionAccepted);
                            self.session.conn_ack(ack).await.unwrap(); //TODO avoid unwrap
                        }
                        session::Output::Subscribe(subscribe) => {
                            let ack = SubackPacket::new(subscribe.packet_identifier(), subscribe.subscribes().iter().map(|(_topic, qos)| (*qos).into()).collect());
                            self.leg.subscribe(subscribe).await;
                            self.session.sub_ack(ack).await.unwrap(); //TODO avoid unwrap
                        }
                        session::Output::Unsubscribe(unsubscribe) => {
                            let ack = UnsubackPacket::new(unsubscribe.packet_identifier());
                            self.leg.unsubscribe(unsubscribe).await;
                            self.session.unsub_ack(ack).await.unwrap(); //TODO avoid unwrap
                        }
                        session::Output::Publish(publish) => {
                            let (_qos, pkid) = publish.qos().split();
                            self.leg.publish(publish).await;
                            if let Some(pkid) = pkid {
                                let ack = PubackPacket::new(pkid);
                                self.session.pub_ack(ack).await.unwrap(); //TODO avoid unwrap
                            }
                        }
                    },
                    Ok(None) => {
                        log::info!("[MqttBroker] connection closed");
                        break;
                    }
                    Err(err) => {
                        log::error!("[MqttBroker] connection error: {}", err)
                    }
                },
                event = self.leg.recv() => match event {
                    Some(event) => match event {
                        LegOutput::Publish(pkt) => {
                            self.session.publish(pkt).await.unwrap(); //TODO avoid unwrap
                        },
                    },
                    None => {
                        log::warn!("[MqttBroker] leg closed");
                        break;
                    },
                }
            }
        }
    }
}
