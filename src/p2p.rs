use anyhow::Result;
use bytes::Bytes;
use futures::{future, StreamExt};
use libp2p::{
    gossipsub, identify, kad,
    identity,
    multiaddr::Multiaddr,
    ping,
    swarm::{NetworkBehaviour, SwarmEvent, ConnectionId},
    core::connection::ConnectedPoint,
    PeerId, SwarmBuilder,
    relay,
    noise, tls, yamux,
};
use libp2p::request_response::{self, ProtocolSupport, Event as RrEvent, Message as RrMessage, Codec};
use serde::{Deserialize, Serialize};
use libp2p::swarm::behaviour::toggle::Toggle;
use std::str::FromStr;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use bytes::{BytesMut, BufMut};

pub struct P2pNode {
    pub local: PeerId,
    tx_to_swarm: UnboundedSender<Bytes>,
    rx_to_tun: UnboundedReceiver<Bytes>,
}

pub fn generate_identity() -> identity::Keypair { identity::Keypair::generate_ed25519() }
pub fn peer_id_from(k: &identity::Keypair) -> PeerId { PeerId::from(k.public()) }
pub fn derive_local_peer_id() -> PeerId { peer_id_from(&generate_identity()) }

#[derive(NetworkBehaviour)]
struct NetBehaviour {
    gsub: gossipsub::Behaviour<gossipsub::IdentityTransform>,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    kad: kad::Behaviour<kad::store::MemoryStore>,
    dcutr: libp2p::dcutr::Behaviour,
    autonat: libp2p::autonat::Behaviour,
    relay: relay::client::Behaviour,
    relay_server: Toggle<relay::Behaviour>,
    mdns: libp2p::mdns::tokio::Behaviour,
    data_rr: request_response::Behaviour<DataCodec>,
}

// ================= Direct Data Protocol over request-response =================
#[derive(Clone, Debug)] struct DataProtocol;
impl AsRef<str> for DataProtocol { fn as_ref(&self) -> &str { "/coconet/data/1" } }
#[derive(Clone, Default)] struct DataCodec;
#[async_trait::async_trait]
impl Codec for DataCodec {
    type Protocol = DataProtocol;
    type Request = Vec<u8>;
    type Response = Vec<u8>; // 空响应
    async fn read_request<T>(&mut self, _: &DataProtocol, io: &mut T) -> std::io::Result<Self::Request>
    where T: futures::AsyncRead + Unpin + Send {
        use futures::AsyncReadExt; let mut len_buf=[0u8;4]; io.read_exact(&mut len_buf).await?; let len=u32::from_be_bytes(len_buf) as usize; let mut buf=vec![0u8;len]; io.read_exact(&mut buf).await?; Ok(buf)
    }
    async fn read_response<T>(&mut self, _: &DataProtocol, io: &mut T) -> std::io::Result<Self::Response>
    where T: futures::AsyncRead + Unpin + Send { use futures::AsyncReadExt; let mut len_buf=[0u8;4]; io.read_exact(&mut len_buf).await?; let len=u32::from_be_bytes(len_buf) as usize; let mut buf=vec![0u8;len]; if len>0 { io.read_exact(&mut buf).await?; } Ok(buf) }
    async fn write_request<T>(&mut self, _: &DataProtocol, io: &mut T, req: Self::Request) -> std::io::Result<()>
    where T: futures::AsyncWrite + Unpin + Send { use futures::AsyncWriteExt; let len=req.len() as u32; io.write_all(&len.to_be_bytes()).await?; io.write_all(&req).await?; io.flush().await }
    async fn write_response<T>(&mut self, _: &DataProtocol, io: &mut T, resp: Self::Response) -> std::io::Result<()>
    where T: futures::AsyncWrite + Unpin + Send { use futures::AsyncWriteExt; let len=resp.len() as u32; io.write_all(&len.to_be_bytes()).await?; if !resp.is_empty() { io.write_all(&resp).await?; } io.flush().await }
}

impl P2pNode {
    pub async fn new(keys: identity::Keypair, relay_server_enable: bool, listen_port: u16, bootstrap: Vec<String>) -> Result<Self> {
        let local = PeerId::from(keys.public());

        // Channels: TUN -> P2P (tx_to_swarm), P2P -> TUN (tx_to_tun)
        let (tx_to_swarm, mut rx_from_tun) = mpsc::unbounded_channel::<Bytes>();
        let (tx_to_tun, rx_to_tun) = mpsc::unbounded_channel::<Bytes>();

        // Build a QUIC + Relay-capable swarm.
        let local_peer = PeerId::from(keys.public());

        let mut swarm = SwarmBuilder::with_existing_identity(keys.clone())
            .with_tokio()
            .with_quic()
            .with_relay_client((tls::Config::new, noise::Config::new), yamux::Config::default)?
            .with_behaviour(|id: &identity::Keypair, relay_client: relay::client::Behaviour| {
                // gossipsub
                let gcfg = gossipsub::ConfigBuilder::default()
                    .validation_mode(gossipsub::ValidationMode::Permissive)
                    .build()
                    .expect("gossipsub cfg");
                // 使用 Anonymous 以避免对每个数据面包做 Ed25519 签名的 CPU 开销（性能显著提升）
                let gsub = gossipsub::Behaviour::<gossipsub::IdentityTransform>::new(
                    gossipsub::MessageAuthenticity::Anonymous,
                    gcfg,
                )
                .expect("gossipsub behaviour");

                // identify
                let icfg = identify::Config::new("coconet/0.1.0".into(), id.public());
                let identify = identify::Behaviour::new(icfg);

                // ping
                let ping = ping::Behaviour::default();

                // kademlia
                let store = kad::store::MemoryStore::new(PeerId::from(id.public()));
                let kad = kad::Behaviour::new(local_peer, store);

                // DCUtR (Direct Connection Upgrade through Relay): 先经由中继建立连接，再尝试打洞直连
                let dcutr = libp2p::dcutr::Behaviour::new(local_peer);
                // AutoNAT: 自动判断内外网可达性，帮助选择地址
                let autonat = libp2p::autonat::Behaviour::new(local_peer, Default::default());

                let relay = relay_client;
                // relay server（可选）
                let relay_server = if relay_server_enable {
                    Toggle::from(Some(relay::Behaviour::new(local_peer, Default::default())))
                } else {
                    Toggle::from(None)
                };

                // mDNS for LAN peer discovery
                let mdns = libp2p::mdns::tokio::Behaviour::new(libp2p::mdns::Config::default(), local_peer)
                    .expect("mdns behaviour");

                // direct data-plane (multi-substream) via request-response
                let protocols = std::iter::once((DataProtocol, ProtocolSupport::Full));
                let data_rr = request_response::Behaviour::<DataCodec>::new(protocols, request_response::Config::default());

                NetBehaviour { gsub, identify, ping, kad, dcutr, autonat, relay, relay_server, mdns, data_rr }
            })?
            .build();

        // Listen on QUIC any-address, configurable port (0 = random)
        let listen_addr: Multiaddr = if listen_port == 0 {
            "/ip4/0.0.0.0/udp/0/quic-v1".parse().expect("valid multiaddr")
        } else {
            format!("/ip4/0.0.0.0/udp/{}/quic-v1", listen_port).parse().expect("valid multiaddr")
        };
        if let Err(e) = swarm.listen_on(listen_addr) {
            tracing::warn!(error = %e, "listen_on failed");
        }

        // Topics: data-plane + control-plane(announce)
        let data_topic = gossipsub::IdentTopic::new("coconet/packets");
        let ann_topic = gossipsub::IdentTopic::new("coconet/announce");
        if let Err(e) = swarm.behaviour_mut().gsub.subscribe(&data_topic) {
            tracing::warn!(error = %e, "gossipsub subscribe data failed");
        }
        if let Err(e) = swarm.behaviour_mut().gsub.subscribe(&ann_topic) {
            tracing::warn!(error = %e, "gossipsub subscribe announce failed");
        }

        // Try dialing bootstrap peers if provided.
        for addr_str in bootstrap {
            match addr_str.parse::<Multiaddr>() {
                Ok(ma) => {
                    if let Err(e) = swarm.dial(ma.clone()) {
                        tracing::warn!(%ma, error = %e, "dial bootstrap failed");
                    } else {
                        tracing::info!(%ma, "dialing bootstrap");
                    }
                }
                Err(e) => tracing::warn!(addr = %addr_str, error = %e, "invalid bootstrap address"),
            }
        }

        // Drive swarm + bridge channel I/O in background.
    tokio::spawn(async move {
            use std::collections::{HashMap, HashSet};
            use std::time::{Duration, Instant};
            let mut relayed_conns: HashMap<PeerId, Vec<ConnectionId>> = HashMap::new();
            let mut direct_peers: HashSet<PeerId> = HashSet::new();
            // 对直连拨号做去重与冷却，避免 Identify 反复触发拨号造成连接抖动
            let mut recent_dials: HashMap<PeerId, HashMap<Multiaddr, Instant>> = HashMap::new();
            let dial_cooldown = Duration::from_secs(30);
            // 我的可公告地址集合与公告冷却
            let mut my_addrs: HashSet<Multiaddr> = HashSet::new();
            let mut last_announce: Option<Instant> = None;
            let announce_cooldown = Duration::from_secs(30);
            let mut announced_once: bool = false;
            let mut kad_bootstrapped = false;

            #[derive(Debug, Serialize, Deserialize)]
            struct AnnounceMsg {
                peer: String,
                addrs: Vec<String>,
                ts: u64,
            }

            fn is_public_ipv4(ip: std::net::Ipv4Addr) -> bool {
                let octets = ip.octets();
                let a = octets[0];
                let b = octets[1];
                // 私网/保留网段过滤
                // 0.0.0.0/8, 10.0.0.0/8, 100.64.0.0/10, 127.0.0.0/8, 169.254.0.0/16,
                // 172.16.0.0/12, 192.0.0.0/24, 192.0.2.0/24, 192.168.0.0/16,
                // 198.18.0.0/15, 198.51.100.0/24, 203.0.113.0/24, 224.0.0.0/4(组播), 240.0.0.0/4(保留)
                if a == 0
                    || a == 10
                    || (a == 100 && (b & 0b1100_0000) == 0b0100_0000) // 100.64.0.0/10
                    || a == 127
                    || (a == 169 && b == 254)
                    || (a == 172 && (16..=31).contains(&b))
                    || (a == 192 && b == 0)
                    || (a == 192 && b == 168)
                    || (a == 192 && b == 2)
                    || (a == 198 && (b == 18 || b == 19))
                    || (a == 198 && b == 51)
                    || (a == 203 && b == 0)
                    || (224..=255).contains(&a)
                { return false; }
                true
            }

            fn is_public_udp_quic(addr: &Multiaddr) -> bool {
                use libp2p::multiaddr::Protocol;
                let mut ip4: Option<std::net::Ipv4Addr> = None;
                let mut udp = false;
                let mut quic = false;
                for p in addr.iter() {
                    match p {
                        Protocol::Ip4(a) => ip4 = Some(a),
                        Protocol::Udp(_) => udp = true,
                        Protocol::QuicV1 => quic = true,
                        Protocol::P2pCircuit => return false,
                        _ => {}
                    }
                }
                match (ip4, udp, quic) { (Some(ip), true, true) => is_public_ipv4(ip), _ => false }
            }

            fn rebuild_udp_quic(addr: &Multiaddr) -> Option<Multiaddr> {
                // 从含 /p2p 等后缀的地址提取 /ip4/..../udp/..../quic-v1
                if let Some((ip, port)) = udp_ip_port(addr) {
                    let s = format!("/ip4/{}/udp/{}/quic-v1", ip, port);
                    return s.parse::<Multiaddr>().ok();
                }
                None
            }

            fn publish_announce(
                swarm: &mut libp2p::Swarm<NetBehaviour>,
                my_addrs: &std::collections::HashSet<Multiaddr>,
                last_announce: &mut Option<Instant>,
                announce_cooldown: Duration,
                local_peer: &PeerId,
                ann_topic: &gossipsub::IdentTopic,
            ) {
                let now = Instant::now();
                if last_announce.map(|t| now.duration_since(t) < announce_cooldown).unwrap_or(false) {
                    return;
                }
                let addrs: Vec<Multiaddr> = my_addrs.iter().cloned().filter(|a| is_public_udp_quic(a)).collect();
                if addrs.is_empty() { return; }
                let msg = AnnounceMsg {
                    peer: local_peer.to_string(),
                    addrs: addrs.iter().map(|a| a.to_string()).collect(),
                    ts: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
                };
                if let Ok(json) = serde_json::to_vec(&msg) {
                    if swarm.behaviour_mut().gsub.publish(ann_topic.clone(), json).is_ok() {
                        *last_announce = Some(now);
                        tracing::info!(count = addrs.len(), addrs = ?addrs, "announced public addrs");
                    }
                }
            }
            // 批处理聚合：将多个 IP 包封装到单个 gossipsub 消息，减少协议与调度开销
            // 支持通过环境变量覆盖：
            //  - COCONET_BATCH_MAX_BYTES (默认 49152)
            //  - COCONET_BATCH_MAX_PKTS  (默认 128)
            //  - COCONET_BATCH_INTERVAL_MS (默认 2)
            fn env_usize(key: &str, default: usize) -> usize {
                std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
            }
            fn env_u64(key: &str, default: u64) -> u64 {
                std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
            }
            const HDR_LEN: usize = 5; // 'C''O' 0x01 count(u16)
            let max_batch_bytes: usize = env_usize("COCONET_BATCH_MAX_BYTES", 48 * 1024);
            let max_batch_pkts: usize = env_usize("COCONET_BATCH_MAX_PKTS", 128);
            let base_interval_ms: u64 = env_u64("COCONET_BATCH_INTERVAL_MS", 2);

            struct BatchBuilder {
                buf: BytesMut,     // 带 header
                pkt_count: u16,
            }
            impl BatchBuilder {
                fn with_capacity(cap: usize) -> Self {
                    let mut buf = BytesMut::with_capacity(cap.max(HDR_LEN));
                    // 预写 header 占位，count 两字节后面回填
                    buf.put_slice(b"CO");
                    buf.put_u8(0x01); // 版本
                    buf.put_u8(0);    // 高字节占位
                    buf.put_u8(0);    // 低字节占位
                    Self { buf, pkt_count: 0 }
                }
                #[inline] fn is_empty(&self) -> bool { self.pkt_count == 0 }
                #[inline] fn payload_size(&self) -> usize { self.buf.len() - HDR_LEN }
                #[inline] fn remaining_mut(&self) -> usize { self.buf.capacity() - self.buf.len() }
                fn add(&mut self, pkt: &Bytes) -> bool {
                    if self.pkt_count == u16::MAX { return false; }
                    let needed = 2 + pkt.len();
                    if self.remaining_mut() < needed { self.buf.reserve(needed); }
                    self.buf.put_u16(pkt.len() as u16);
                    self.buf.put_slice(pkt);
                    self.pkt_count += 1;
                    true
                }
                fn take(&mut self) -> Bytes {
                    if self.pkt_count == 0 { return Bytes::new(); }
                    // 回填 count（大端） 位于 buf[3..5)
                    let hi = (self.pkt_count >> 8) as u8;
                    let lo = (self.pkt_count & 0xFF) as u8;
                    // buf 布局: 'C','O',0x01,hi,lo,<payload...>
                    // 之前我们用的是 4 字节 header，这里扩展到 5，需要在初始化时写 5 字节，但为了兼容旧格式保持 4 + count:u16。
                    // 因为初始化写了: 'C','O',0x01,0,0 => 5 字节, 修正 HDR_LEN=5 更合理；为避免大范围改动，这里直接插入逻辑调整。
                    // 为保持与旧解析兼容（旧解析期待长度>=5 并读 data[3], data[4]），我们直接写入。
                    if self.buf.len() >= 5 { self.buf[3] = hi; self.buf[4] = lo; }
                    let out = self.buf.split().freeze();
                    // 重建 header 供后续复用 capacity
                    self.buf.put_slice(b"CO"); self.buf.put_u8(0x01); self.buf.put_u8(0); self.buf.put_u8(0);
                    self.pkt_count = 0;
                    out
                }
            }

            let mut batch = BatchBuilder::with_capacity(max_batch_bytes + 4096);
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(base_interval_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            fn flush_batch(swarm: &mut libp2p::Swarm<NetBehaviour>, topic: &gossipsub::IdentTopic, batch: &mut BatchBuilder, direct_peers: &std::collections::HashSet<PeerId>) {
                if batch.is_empty() { return; }
                let pkt_count = batch.pkt_count;
                let data = batch.take();
                if data.is_empty() { return; }
                if direct_peers.is_empty() {
                    if let Err(e) = swarm.behaviour_mut().gsub.publish(topic.clone(), data) {
                        tracing::debug!(?e, count=pkt_count, "gossipsub publish (batch) failed");
                    } else {
                        tracing::trace!(count=pkt_count, "published batch via gossipsub");
                    }
                } else {
                    for peer in direct_peers.iter() {
                        let id = swarm.behaviour_mut().data_rr.send_request(peer, data.clone().to_vec());
                        tracing::trace!(%peer, ?id, count=pkt_count, bytes=data.len(), "direct rr batch sent");
                    }
                }
            }

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        flush_batch(&mut swarm, &data_topic, &mut batch, &direct_peers);
                    }
                    maybe_pkt = rx_from_tun.recv() => {
                        match maybe_pkt {
                            Some(pkt) => {
                                let pkt_len = pkt.len();
                                // 超过一半批容量的大包直接单发，避免拖慢聚合
                                if pkt_len > (max_batch_bytes / 2) {
                                    flush_batch(&mut swarm, &data_topic, &mut batch, &direct_peers);
                                    if let Err(e) = swarm.behaviour_mut().gsub.publish(data_topic.clone(), pkt) {
                                        tracing::debug!(?e, "gossipsub publish (large single) failed");
                                    }
                                    continue;
                                }
                                if batch.pkt_count as usize >= max_batch_pkts || batch.payload_size() + pkt_len + 2 >= max_batch_bytes {
                                    flush_batch(&mut swarm, &data_topic, &mut batch, &direct_peers);
                                }
                                batch.add(&pkt);
                            }
                            None => {
                                flush_batch(&mut swarm, &data_topic, &mut batch, &direct_peers);
                                break;
                            }
                        }
                    }
                    // Swarm events -> forward inbound data-plane messages to TUN
                    event = swarm.select_next_some() => {
                        match event {
                            SwarmEvent::Behaviour(NetBehaviourEvent::Gsub(gossipsub::Event::Message { message, .. })) => {
                                // 若消息来源就是本地，则跳过（避免潜在的重复处理）
                                if message.source.as_ref() == Some(&local_peer) { continue; }
                                if message.topic == data_topic.hash() {
                                    let data = message.data;
                                    if data.len() >= 5 && &data[0..2] == b"CO" && data[2] == 0x01 {
                                        let count = (((data[3] as u16) << 8) | data[4] as u16) as usize;
                                        let mut idx = 5;
                                        let mut delivered = 0usize;
                                        for _ in 0..count {
                                            if idx + 2 > data.len() { break; }
                                            let len = u16::from_be_bytes([data[idx], data[idx+1]]) as usize; idx += 2;
                                            if idx + len > data.len() { break; }
                                            let slice = &data[idx..idx+len];
                                            idx += len;
                                            let _ = tx_to_tun.send(Bytes::copy_from_slice(slice));
                                            delivered += 1;
                                        }
                                        tracing::trace!(raw_count=count, delivered, "unpacked batch packets");
                                    } else {
                                        let _ = tx_to_tun.send(Bytes::from(data));
                                    }
                                } else if message.topic == ann_topic.hash() {
                                    if let Ok(parsed) = serde_json::from_slice::<AnnounceMsg>(&message.data) {
                                        if let Ok(peer_id) = PeerId::from_str(&parsed.peer) {
                                            if peer_id != local_peer {
                                                // 记录并尝试直连
                                                for s in parsed.addrs {
                                                    if let Ok(ma) = s.parse::<Multiaddr>() {
                                                        if !is_public_udp_quic(&ma) { continue; }
                                                        // 冷却判断
                                                        let now = Instant::now();
                                                        let addr_recent = recent_dials
                                                            .entry(peer_id)
                                                            .or_default()
                                                            .entry(ma.clone())
                                                            .or_insert(Instant::now() - dial_cooldown * 2);
                                                        if now.duration_since(*addr_recent) < dial_cooldown { continue; }
                                                        *addr_recent = now;
                                                        swarm.behaviour_mut().kad.add_address(&peer_id, ma.clone());
                                                        if let Err(e) = swarm.dial(ma.clone()) {
                                                            tracing::trace!(%peer_id, %ma, error=%e, "announce direct dial failed");
                                                        } else {
                                                            tracing::info!(%peer_id, %ma, "announce: attempting direct dial");
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            SwarmEvent::Behaviour(NetBehaviourEvent::Identify(ev)) => {
                                tracing::debug!(?ev, "identify");
                                if let identify::Event::Received { peer_id, info } = ev {
                                    // 若已经存在直连，则不再尝试直连拨号
                                    let already_direct = direct_peers.contains(&peer_id);
                                    // 清理过期的拨号记录
                                    if let Some(map) = recent_dials.get_mut(&peer_id) {
                                        let now = Instant::now();
                                        map.retain(|_, ts| now.duration_since(*ts) < dial_cooldown);
                                    }
                                    // 准备本次要尝试的地址集合
                                    for addr in info.listen_addrs {
                                        // 加入 DHT 地址本地缓存
                                        swarm.behaviour_mut().kad.add_address(&peer_id, addr.clone());
                                        let is_circuit = addr.iter().any(|p| matches!(p, libp2p::multiaddr::Protocol::P2pCircuit));
                                        if already_direct {
                                            continue;
                                        }
                                        if is_circuit {
                                            // 作为回退路径，尝试经中继拨号（依赖 relay client 支持）
                                            let now = Instant::now();
                                            let addr_recent = recent_dials
                                                .entry(peer_id)
                                                .or_default()
                                                .entry(addr.clone())
                                                .or_insert(Instant::now() - dial_cooldown * 2);
                                            if now.duration_since(*addr_recent) < dial_cooldown { continue; }
                                            *addr_recent = now;
                                            if let Err(e) = swarm.dial(addr.clone()) {
                                                tracing::trace!(%peer_id, addr=%addr, error=%e, "relay-circuit dial attempt failed");
                                            } else {
                                                tracing::info!(%peer_id, addr=%addr, "attempting relay-circuit dial");
                                            }
                                            continue;
                                        }
                                        // 非中继地址：规范化为 /ip4/..../udp/..../quic-v1，并过滤仅公网地址
                                        let canonical = rebuild_udp_quic(&addr).unwrap_or(addr.clone());
                                        if !is_public_udp_quic(&canonical) { continue; }
                                        // 冷却判断
                                        let now = Instant::now();
                                        let addr_recent = recent_dials
                                            .entry(peer_id)
                                            .or_default()
                                            .entry(canonical.clone())
                                            .or_insert(Instant::now() - dial_cooldown * 2);
                                        if now.duration_since(*addr_recent) < dial_cooldown { continue; }
                                        *addr_recent = now;
                                        // 尝试直接拨号（即便当前可能存在经中继连接），有助于切换为直连
                                        if let Err(e) = swarm.dial(canonical.clone()) {
                                            tracing::trace!(%peer_id, addr=%canonical, error=%e, "direct dial attempt failed");
                                        } else {
                                            tracing::debug!(%peer_id, addr=%canonical, "attempting direct dial");
                                        }
                                    }
                                    // 将对端看到的本端 observed_addr 加入外部地址候选
                                    let _ = swarm.add_external_address(info.observed_addr.clone());
                                    // 触发一次公告（经冷却）
                                    publish_announce(&mut swarm, &my_addrs, &mut last_announce, announce_cooldown, &local_peer, &ann_topic);
                                    if !kad_bootstrapped {
                                        if let Err(e) = swarm.behaviour_mut().kad.bootstrap() {
                                            tracing::debug!(error = %e, "kad bootstrap not ready");
                                        } else {
                                            kad_bootstrapped = true;
                                            tracing::info!(%peer_id, "kad bootstrap started");
                                        }
                                    }
                                }
                            }
                            SwarmEvent::Behaviour(NetBehaviourEvent::Ping(ev)) => {
                                tracing::trace!(?ev, "ping");
                            }
                            SwarmEvent::Behaviour(NetBehaviourEvent::Mdns(ev)) => {
                                match ev {
                                    libp2p::mdns::Event::Discovered(list) => {
                                        for (peer, addr) in list {
                                            // 将 mDNS 发现的局域网地址加入 DHT 缓存，并尝试拨号（允许私网）
                                            swarm.behaviour_mut().kad.add_address(&peer, addr.clone());
                                            // 仅拨打 IPv4 UDP/quic-v1 或 TCP (quic 上层) 的地址
                                            if let Some(ma) = rebuild_udp_quic(&addr) { 
                                                if let Err(e) = swarm.dial(ma.clone()) {
                                                    tracing::trace!(%peer, addr=%ma, error=%e, "mdns direct dial failed");
                                                } else {
                                                    tracing::info!(%peer, addr=%ma, "mdns: attempting direct dial");
                                                }
                                            } else {
                                                // 若无法标准化，直接尝试该地址（可能是 /ip4/.../tcp/...）
                                                if let Err(e) = swarm.dial(addr.clone()) {
                                                    tracing::trace!(%peer, addr=%addr, error=%e, "mdns direct dial failed");
                                                } else {
                                                    tracing::info!(%peer, addr=%addr, "mdns: attempting direct dial");
                                                }
                                            }
                                        }
                                    }
                                    libp2p::mdns::Event::Expired(list) => {
                                        for (peer, addr) in list {
                                            tracing::debug!(%peer, %addr, "mdns expired");
                                        }
                                    }
                                }
                            }
                            SwarmEvent::Behaviour(NetBehaviourEvent::Kad(ev)) => {
                                tracing::trace!(?ev, "kad");
                            }
                            SwarmEvent::Behaviour(NetBehaviourEvent::Dcutr(ev)) => {
                                tracing::info!(?ev, "dcutr");
                            }
                            SwarmEvent::Behaviour(NetBehaviourEvent::Relay(ev)) => {
                                tracing::debug!(?ev, "relay-client");
                            }
                            SwarmEvent::Behaviour(NetBehaviourEvent::RelayServer(ev)) => {
                                tracing::info!(?ev, "relay-server");
                            }
                            SwarmEvent::Behaviour(NetBehaviourEvent::Autonat(ev)) => {
                                tracing::info!(?ev, "autonat");
                                // 状态变化为 Public 时，也尝试广播一次自身地址（依赖 ExternalAddrConfirmed 收集地址）
                                publish_announce(&mut swarm, &my_addrs, &mut last_announce, announce_cooldown, &local_peer, &ann_topic);
                            }
                            SwarmEvent::Behaviour(NetBehaviourEvent::DataRr(ev)) => {
                                match ev {
                                    RrEvent::Message { peer, message } => match message {
                                        RrMessage::Request { request, channel, .. } => {
                                            // 解包同 batch 格式
                                            if request.len() >= 5 && &request[0..2] == b"CO" && request[2] == 0x01 {
                                                let count = (((request[3] as u16) << 8) | request[4] as u16) as usize;
                                                let mut idx = 5; let mut delivered = 0usize;
                                                for _ in 0..count { if idx + 2 > request.len() { break; } let len = u16::from_be_bytes([request[idx], request[idx+1]]) as usize; idx += 2; if idx + len > request.len() { break; } let slice=&request[idx..idx+len]; idx+=len; let _ = tx_to_tun.send(Bytes::copy_from_slice(slice)); delivered+=1; }
                                                tracing::trace!(%peer, raw_count=count, delivered, "unpacked direct rr batch packets");
                                            } else {
                                                let _ = tx_to_tun.send(Bytes::from(request));
                                            }
                                            // 发送空 ACK
                                            if let Err(e) = swarm.behaviour_mut().data_rr.send_response(channel, Vec::new()) {
                                                tracing::debug!(%peer, ?e, "send ack failed");
                                            }
                                        }
                                        RrMessage::Response { .. } => { /* 忽略 ACK */ }
                                    },
                                    RrEvent::OutboundFailure { peer, error, request_id } => {
                                        tracing::debug!(%peer, ?request_id, ?error, "direct rr outbound failed");
                                    }
                                    RrEvent::InboundFailure { peer, error, request_id } => {
                                        tracing::debug!(%peer, ?request_id, ?error, "direct rr inbound failed");
                                    }
                                    RrEvent::ResponseSent { peer, request_id } => {
                                        tracing::trace!(%peer, ?request_id, "direct rr response sent");
                                    }
                                }
                            }
                            SwarmEvent::NewListenAddr { address, .. } => {
                                tracing::info!(%address, "listening");
                                // 新监听地址也加入外部地址池，便于对端发现/拨入
                                let _ = swarm.add_external_address(address.clone());
                                // 记录可公告地址（去除 /p2p 等后缀，且仅保留公网 UDP/QUIC）
                                if let Some(ma) = rebuild_udp_quic(&address) {
                                    let mut to_insert = Vec::new();
                                    if is_public_udp_quic(&ma) { to_insert.push(ma); }
                                    for a in to_insert { my_addrs.insert(a); }
                                }
                                publish_announce(&mut swarm, &my_addrs, &mut last_announce, announce_cooldown, &local_peer, &ann_topic);
                                // 尝试 UPnP/NAT-PMP 端口映射（仅针对 IPv4 UDP）
                                if let Some((ip, port)) = udp_ip_port(&address) {
                                    tokio::spawn(async move {
                                        if let Err(e) = try_upnp_map(ip, port).await {
                                            tracing::debug!(error=%e, "upnp map failed");
                                        }
                                    });
                                }
                            }
                            SwarmEvent::ExternalAddrConfirmed { address } => {
                                tracing::info!(%address, "external address confirmed");
                                if let Some(ma) = rebuild_udp_quic(&address) {
                                    let mut to_insert = Vec::new();
                                    if is_public_udp_quic(&ma) { to_insert.push(ma); }
                                    for a in to_insert { my_addrs.insert(a); }
                                }
                                publish_announce(&mut swarm, &my_addrs, &mut last_announce, announce_cooldown, &local_peer, &ann_topic);
                            }
                            SwarmEvent::ExternalAddrExpired { address } => {
                                tracing::info!(%address, "external address expired");
                            }
                            SwarmEvent::ListenerClosed { addresses, .. } => {
                                tracing::warn!(?addresses, "listener closed");
                            }
                            SwarmEvent::IncomingConnection { .. } => {}
                            SwarmEvent::ConnectionEstablished { peer_id, connection_id, endpoint, .. } => {
                                // 判断连接是否经由中继（地址中包含 p2p-circuit）
                                let remote_addr = match &endpoint {
                                    ConnectedPoint::Dialer { address, .. } => address,
                                    ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr,
                                };
                                let is_relayed = remote_addr.iter().any(|p| matches!(p, libp2p::multiaddr::Protocol::P2pCircuit));
                                tracing::debug!(%peer_id, %remote_addr, is_relayed, "connection established");
                                // 首次连接建立后（不论与谁）尝试进行一次公告，满足“连接上服务器后广播”的语义
                                if !announced_once { announced_once = true; publish_announce(&mut swarm, &my_addrs, &mut last_announce, announce_cooldown, &local_peer, &ann_topic); }
                                if is_relayed {
                                    if direct_peers.contains(&peer_id) {
                                        tracing::info!(%peer_id, cid=?connection_id, "close relayed conn (already have direct)");
                                        let _ = swarm.close_connection(connection_id);
                                    } else {
                                        relayed_conns.entry(peer_id).or_default().push(connection_id);
                                    }
                                } else {
                                    // 标记已有直连；首次变为直连时打印一次 info
                                    let first_direct = direct_peers.insert(peer_id);
                                    if first_direct {
                                        tracing::info!(%peer_id, %remote_addr, "direct connectivity established");
                                    }
                                    // 关闭之前的中继连接以强制走直连路径
                                    if let Some(list) = relayed_conns.get_mut(&peer_id) {
                                        for rid in list.drain(..) {
                                            tracing::info!(%peer_id, cid=?rid, "close previous relayed conn (switched to direct)");
                                            let _ = swarm.close_connection(rid);
                                        }
                                    }
                                }
                            }
                            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                                tracing::debug!(%peer_id, "connection closed");
                            }
                            _ => {}
                        }
                    }
                }
            }
        });

        Ok(Self { local, tx_to_swarm, rx_to_tun })
    }

    pub async fn run(&mut self) -> Result<()> {
        // Placeholder: in future, drive the libp2p swarm here.
        future::pending::<()>().await;
        // unreachable
        #[allow(unreachable_code)]
        Ok(())
    }

    pub fn sender(&self) -> UnboundedSender<Bytes> { self.tx_to_swarm.clone() }
    pub fn subscribe_packets(&mut self) -> UnboundedReceiver<Bytes> { std::mem::replace(&mut self.rx_to_tun, mpsc::unbounded_channel().1) }
}

fn udp_ip_port(addr: &Multiaddr) -> Option<(std::net::Ipv4Addr, u16)> {
    use libp2p::multiaddr::Protocol;
    let mut ip: Option<std::net::Ipv4Addr> = None;
    let mut port: Option<u16> = None;
    for p in addr.iter() {
        match p {
            Protocol::Ip4(a) => ip = Some(a),
            Protocol::Udp(p) => port = Some(p),
            Protocol::QuicV1 => break,
            _ => {}
        }
    }
    match (ip, port) { (Some(i), Some(p)) => Some((i, p)), _ => None }
}

async fn try_upnp_map(local: std::net::Ipv4Addr, port: u16) -> anyhow::Result<()> {
    use igd::{search_gateway, PortMappingProtocol};
    use std::net::SocketAddrV4;
    // 运行时开关：COCONET_UPNP=0 可禁用（默认启用）
    if std::env::var("COCONET_UPNP").map(|v| v == "0" || v.eq_ignore_ascii_case("false")).unwrap_or(false) {
        tracing::info!("UPnP disabled by env COCONET_UPNP=0");
        return Ok(());
    }
    let gw = match search_gateway(Default::default()) {
        Ok(g) => g,
        Err(e) => { tracing::trace!(error=%e, "no igd gateway found"); return Ok(()); }
    };
    // 尝试添加/刷新 UDP 端口映射
    let desc = "coconet-quic";
    let local_sock = SocketAddrV4::new(local, port);
    match gw.add_port(PortMappingProtocol::UDP, port, local_sock, 3600, desc) {
        Ok(()) => tracing::info!(port=port, "UPnP mapped UDP port"),
        Err(e) => tracing::debug!(error=%e, port=port, "UPnP map failed"),
    }
    Ok(())
}

