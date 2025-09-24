use clap::{Parser, Subcommand};
use tracing::info;

mod tun;
mod p2p;
mod routing;
mod ipam;
mod acl;
mod config;

#[derive(Debug, Parser)]
#[command(name = "coconet", version, about = "Decentralized P2P VPN")] 
struct Cli {
    /// RUST_LOG style filter, e.g. info,coconet=debug
    #[arg(long, default_value = "info,coconet=info")] 
    log: String,

    /// Bootstrap multiaddrs to join the DHT/swarm
    #[arg(long)]
    bootstrap: Vec<String>,

    /// Virtual CIDR to use for IPAM (e.g., 10.99.0.0/16 or fd00:coco::/64)
    #[arg(long, default_value = "10.99.0.0/16")]
    cidr: String,

    /// Interface name for TUN (optional; auto if omitted)
    #[arg(long)]
    ifname: Option<String>,

    /// TUN interface MTU (inner IP). If omitted, uses OS-specific default.
    #[arg(long)]
    mtu: Option<u16>,

    /// Run as a relay-capable node
    #[arg(long)]
    relay: bool,

    /// Listen UDP port for QUIC (0 = random)
    #[arg(long, default_value_t = 0)]
    listen_port: u16,

    /// Subcommands
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Print derived virtual IP for this peer
    Whoami,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // init logging
    std::env::set_var("RUST_LOG", std::env::var("RUST_LOG").unwrap_or_default());
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(cli.log)
        .with_target(false)
        .compact()
        .init();

    match cli.command {
        Some(Commands::Whoami) => {
            let local_id = p2p::derive_local_peer_id();
            let ip = ipam::derive_ip_from_peerid(&cli.cidr, &local_id)?;
            println!("peer_id={} ip={}", local_id, ip);
            return Ok(());
        }
        None => {}
    }

    info!("starting CocoNet node…");
    let keys = p2p::generate_identity();
    let local_id = p2p::peer_id_from(&keys);
    let virtual_ip = ipam::derive_ip_from_peerid(&cli.cidr, &local_id)?;

    // Initialize TUN
    // 计算前缀并传入完整 CIDR（例如 10.99.0.42/16），便于内核识别本地地址与直连路由
    let cidr_net: ipnetwork::Ipv4Network = cli.cidr.parse()?;
    let full_addr = format!("{}/{}", virtual_ip, cidr_net.prefix());
    let (tun_dev, tun_reader, tun_writer) = tun::open_tun(cli.ifname.clone(), cli.mtu, full_addr).await?;
    info!("tun ready ifname={} ip={}", tun_dev.ifname(), virtual_ip);

    // Initialize P2P
    let mut node = p2p::P2pNode::new(keys, cli.relay, cli.listen_port, cli.bootstrap.clone()).await?;

    // Spawn tasks: TUN->P2P and P2P->TUN
    let p2p_rx = node.subscribe_packets();
    let tun_mtu = tun_dev.mtu();
    tokio::spawn(async move {
        if let Err(e) = routing::pump_p2p_to_tun(p2p_rx, tun_writer, Some(tun_dev.ifname().to_string()), tun_mtu).await { eprintln!("p2p->tun error: {e}"); }
    });

    let p2p_tx = node.sender();
    tokio::spawn(async move {
        if let Err(e) = routing::pump_tun_to_p2p(tun_reader, p2p_tx).await { eprintln!("tun->p2p error: {e}"); }
    });

    // Run swarm/event loop
    node.run().await?;
    Ok(())
}
