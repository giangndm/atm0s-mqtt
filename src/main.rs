use std::{net::SocketAddr, str::FromStr};

use atm0s_mqtt::MqttBroker;
use atm0s_small_p2p::{P2pNetwork, P2pNetworkConfig, PeerAddress, SharedKeyHandshake};
use clap::Parser;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio::select;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const HUB_SERVICE_ID: u16 = 100;
const KV_SERVICE: u16 = 101;

pub const DEFAULT_CLUSTER_CERT: &[u8] = include_bytes!("../dev-certs/dev.cluster.cert");
pub const DEFAULT_CLUSTER_KEY: &[u8] = include_bytes!("../dev-certs/dev.cluster.key");

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// UDP/TCP port for serving QUIC/TCP connection for SDN network
    #[arg(env, long)]
    sdn_peer_id: u64,

    /// UDP/TCP port for serving QUIC/TCP connection for SDN network
    #[arg(env, long, default_value = "0.0.0.0:11111")]
    sdn_listener: SocketAddr,

    /// Seeds
    #[arg(env, long, value_delimiter = ',')]
    sdn_seeds: Vec<String>,

    /// Allow it broadcast address to other peers
    /// This allows other peer can active connect to this node
    /// This option is useful with high performance relay node
    #[arg(env, long)]
    sdn_advertise_address: Option<SocketAddr>,

    /// Sdn secure code
    #[arg(env, long, default_value = "insecure")]
    sdn_secure_code: String,

    /// Mqtt tcp listen
    #[arg(env, long, default_value = "0.0.0.0:1883")]
    mqtt_listen: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider().install_default().expect("should install ring as default");
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info");
    }
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        std::env::set_var("RUST_BACKTRACE", "1");
    }
    let args: Args = Args::parse();
    tracing_subscriber::registry().with(fmt::layer()).with(EnvFilter::from_default_env()).init();

    let priv_key = PrivatePkcs8KeyDer::from(DEFAULT_CLUSTER_KEY.to_vec());
    let cert = CertificateDer::from(DEFAULT_CLUSTER_CERT.to_vec());

    let mut p2p = P2pNetwork::new(P2pNetworkConfig {
        peer_id: args.sdn_peer_id.into(),
        listen_addr: args.sdn_listener,
        advertise: args.sdn_advertise_address.map(|a| a.into()),
        priv_key,
        cert,
        tick_ms: 100,
        seeds: args.sdn_seeds.into_iter().map(|s| PeerAddress::from_str(s.as_str()).expect("should parse address")).collect::<Vec<_>>(),
        secure: SharedKeyHandshake::from(args.sdn_secure_code.as_str()),
    })
    .await
    .expect("should create network");

    let hub_service = p2p.create_service(HUB_SERVICE_ID.into());
    let kv_service = p2p.create_service(KV_SERVICE.into());

    let mut mqtt_broker = MqttBroker::new(args.mqtt_listen, hub_service, kv_service).await?;

    loop {
        select! {
            _ = p2p.recv() => {}
            _ = mqtt_broker.recv() => {}
        }
    }
}
