use anyhow::Result;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, ReadHalf, WriteHalf};
use tokio::io::DuplexStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use std::net::{Ipv4Addr, Ipv6Addr};

#[derive(Debug, Clone)]
pub struct PacketMeta {
    pub src: String,
    pub dst: String,
    pub version: &'static str, // "ipv4" | "ipv6"
    pub proto: u8,             // IPv4: protocol; IPv6: next header
}

pub fn parse_ip_meta(pkt: &[u8]) -> Option<PacketMeta> {
    if pkt.len() < 1 { return None; }
    let ver = pkt[0] >> 4;
    match ver {
        4 => {
            if pkt.len() < 20 { return None; }
            let ihl = (pkt[0] & 0x0f) as usize * 4;
            if pkt.len() < ihl + 20 { /* minimal guard */ }
            let proto = pkt[9];
            if pkt.len() < 20 { return None; }
            let src = Ipv4Addr::new(pkt[12], pkt[13], pkt[14], pkt[15]);
            let dst = Ipv4Addr::new(pkt[16], pkt[17], pkt[18], pkt[19]);
            Some(PacketMeta { src: src.to_string(), dst: dst.to_string(), version: "ipv4", proto })
        }
        6 => {
            if pkt.len() < 40 { return None; }
            let next = pkt[6];
            let src = Ipv6Addr::from(<[u8;16]>::try_from(&pkt[8..24]).ok()?);
            let dst = Ipv6Addr::from(<[u8;16]>::try_from(&pkt[24..40]).ok()?);
            Some(PacketMeta { src: src.to_string(), dst: dst.to_string(), version: "ipv6", proto: next })
        }
        _ => None,
    }
}

pub async fn pump_tun_to_p2p(mut reader: ReadHalf<DuplexStream>, tx: UnboundedSender<Bytes>) -> Result<()> {
    let mut buf = vec![0u8; 65536];
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 { break; }
        if let Some(meta) = parse_ip_meta(&buf[..n]) {
            tracing::debug!(
                version = meta.version,
                proto = meta.proto,
                src = %meta.src,
                dst = %meta.dst,
                len = n,
                forward = "tun->p2p",
                "packet"
            );
        } else {
            tracing::trace!(len = n, forward = "tun->p2p", "non-ip or short packet");
        }
        tx.send(Bytes::copy_from_slice(&buf[..n]))?;
    }
    Ok(())
}

pub async fn pump_p2p_to_tun(
    mut rx: UnboundedReceiver<Bytes>,
    mut writer: WriteHalf<DuplexStream>,
    #[allow(unused_variables)] ifname: Option<String>,
    mtu: u16,
) -> Result<()> {
    #[cfg(target_os = "windows")]
    use std::collections::HashSet;
    #[cfg(target_os = "windows")]
    let mut routed_v4_hosts: HashSet<String> = HashSet::new();

    while let Some(pkt) = rx.recv().await {
        if let Some(meta) = parse_ip_meta(&pkt) {
            tracing::debug!(
                version = meta.version,
                proto = meta.proto,
                src = %meta.src,
                dst = %meta.dst,
                len = pkt.len(),
                forward = "p2p->tun",
                "packet"
            );
            #[cfg(target_os = "windows")]
            if meta.version == "ipv4" {
                // 为目标 IP 添加 /32 主机路由，避免 Windows 对 /16 on-link 进行 ARP 导致延迟/丢包
                if let Some(ref ifn) = ifname {
                    if routed_v4_hosts.insert(meta.dst.clone()) {
                        let ifn = ifn.clone();
                        let dst = meta.dst.clone();
                        tokio::spawn(async move {
                            use tokio::process::Command;
                            let prefix = format!("{}/32", dst);
                            // 查询接口索引
                            let out = Command::new("powershell")
                                .args(["-NoProfile", "-Command", &format!("(Get-NetIPInterface -InterfaceAlias '{}' -AddressFamily IPv4).InterfaceIndex", ifn)])
                                .output().await;
                            if let Ok(out) = out {
                                if out.status.success() {
                                    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
                                    if let Ok(idx) = s.parse::<u32>() {
                                        let ps_add = format!(
                                            "Try {{ New-NetRoute -InterfaceIndex {} -DestinationPrefix '{}' -NextHop 0.0.0.0 -RouteMetric 5 -ErrorAction SilentlyContinue }} Catch {{ $_ }}",
                                            idx, prefix
                                        );
                                        let _ = Command::new("powershell").args(["-NoProfile", "-Command", &ps_add]).status().await;
                                    }
                                }
                            }
                            tracing::info!(%dst, ifname=%ifn, "installed host route /32 for peer");
                        });
                    }
                }
            }
            // 进行 MTU 检查与 IPv4 分片（仅下行：p2p->tun）
            if meta.version == "ipv4" {
                let respect_df = std::env::var("COCONET_RESPECT_DF")
                    .ok()
                    .and_then(|v| {
                        let v = v.to_ascii_lowercase();
                        match v.as_str() { "1"|"true"|"yes"|"on" => Some(true), "0"|"false"|"no"|"off" => Some(false), _ => None }
                    })
                    .unwrap_or(false);
                if let Some(frags) = ipv4_fragment_if_needed(&pkt, mtu as usize, respect_df) {
                    for f in frags {
                        tokio::io::AsyncWriteExt::write_all(&mut writer, &f).await?;
                    }
                    continue; // 已写入分片
                }
            } else if meta.version == "ipv6" {
                // IPv6 中间节点不允许分片；如超过 MTU，这里直接写入由内核处理（或丢弃）。
                // 也可以选择丢弃并记录。
                if pkt.len() > mtu as usize {
                    tracing::warn!(dst=%meta.dst, len=pkt.len(), mtu, "IPv6 packet exceeds MTU; not fragmented");
                }
            }
        } else {
            tracing::trace!(len = pkt.len(), forward = "p2p->tun", "non-ip or short packet");
        }
        tokio::io::AsyncWriteExt::write_all(&mut writer, &pkt).await?;
    }
    Ok(())
}

// --- IPv4 fragmentation helpers ---

fn ipv4_checksum(header: &mut [u8]) -> u16 {
    // IPv4 header checksum over 16-bit words with checksum field zeroed
    header[10] = 0;
    header[11] = 0;
    let mut sum: u32 = 0;
    for chunk in header.chunks(2) {
        let word = if chunk.len() == 2 {
            u16::from_be_bytes([chunk[0], chunk[1]]) as u32
        } else {
            (chunk[0] as u32) << 8
        };
        sum = sum.wrapping_add(word);
    }
    while (sum >> 16) != 0 {
        sum = (sum & 0xFFFF) + (sum >> 16);
    }
    !(sum as u16)
}

fn ipv4_fragment_if_needed(pkt: &[u8], mtu: usize, respect_df: bool) -> Option<Vec<Vec<u8>>> {
    if pkt.len() <= 20 { return None; }
    if (pkt[0] >> 4) != 4 { return None; }
    let ihl = ((pkt[0] & 0x0f) as usize) * 4;
    if pkt.len() < ihl || ihl < 20 { return None; }
    // If packet already within MTU, no fragmentation needed
    if pkt.len() <= mtu { return None; }

    // Respect DF (Don't Fragment); if DF set, don't fragment here
    let flags_fragment = u16::from_be_bytes([pkt[6], pkt[7]]);
    let df_set = (flags_fragment & 0x4000) != 0;
    if df_set && respect_df { return None; }

    // Compute max fragment payload per fragment, must be multiple of 8 bytes
    if mtu <= ihl { return None; }
    let mut max_payload = mtu - ihl;
    max_payload &= !7; // align to 8 bytes
    if max_payload == 0 { return None; }

    let total_len = u16::from_be_bytes([pkt[2], pkt[3]]) as usize;
    let payload_len = total_len.saturating_sub(ihl);
    if payload_len == 0 { return None; }
    if ihl > pkt.len() { return None; }
    let payload = &pkt[ihl..ihl+payload_len.min(pkt.len().saturating_sub(ihl))];

    let id = u16::from_be_bytes([pkt[4], pkt[5]]);
    let proto = pkt[9];
    let ttl = pkt[8];
    let src_dst = &pkt[12..20];
    let tos = pkt[1];
    let version_ihl = pkt[0];

    let mut frags: Vec<Vec<u8>> = Vec::new();
    let mut offset = 0usize; // in bytes
    while offset < payload.len() {
        let remaining = payload.len() - offset;
        let frag_payload_len = if remaining > max_payload { max_payload } else { remaining };
        let more_fragments = offset + frag_payload_len < payload.len();

        let frag_total_len = ihl + frag_payload_len;
        let mut buf = vec![0u8; frag_total_len];
        // Build header
        buf[0] = version_ihl; // version + IHL
        buf[1] = tos;
        buf[2..4].copy_from_slice(&(frag_total_len as u16).to_be_bytes());
        buf[4..6].copy_from_slice(&id.to_be_bytes());
        let frag_off_field: u16 = {
            let off_units = (offset / 8) as u16;
            let flags = if more_fragments { 0x2000 } else { 0x0000 }; // MF flag
            flags | (off_units & 0x1FFF)
        };
        buf[6..8].copy_from_slice(&frag_off_field.to_be_bytes());
    buf[8] = ttl; // preserve original TTL
        buf[9] = proto;
        // checksum zero for now
        buf[12..20].copy_from_slice(src_dst);
        // Copy header options if any
        if ihl > 20 {
            buf[20..ihl].copy_from_slice(&pkt[20..ihl]);
        }
        // Copy payload slice
        buf[ihl..ihl+frag_payload_len].copy_from_slice(&payload[offset..offset+frag_payload_len]);
        // Compute checksum
        let csum = ipv4_checksum(&mut buf[..ihl]);
        buf[10..12].copy_from_slice(&csum.to_be_bytes());

        frags.push(buf);
        offset += frag_payload_len;
    }

    Some(frags)
}
