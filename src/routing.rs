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

pub async fn pump_p2p_to_tun(mut rx: UnboundedReceiver<Bytes>, mut writer: WriteHalf<DuplexStream>) -> Result<()> {
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
        } else {
            tracing::trace!(len = pkt.len(), forward = "p2p->tun", "non-ip or short packet");
        }
        tokio::io::AsyncWriteExt::write_all(&mut writer, &pkt).await?;
    }
    Ok(())
}
