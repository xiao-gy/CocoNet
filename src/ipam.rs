use anyhow::{Result, bail};
use ipnetwork::Ipv4Network;
use libp2p::PeerId;
use std::net::Ipv4Addr;

pub fn derive_ip_from_peerid(cidr: &str, peer: &PeerId) -> Result<Ipv4Addr> {
    // currently only IPv4; extend to IPv6 later.
    let net: Ipv4Network = cidr.parse().map_err(|e| anyhow::anyhow!("invalid CIDR: {e}"))?;
    let prefix = net.prefix();
    let host_bits = 32 - prefix;
    if host_bits < 2 { bail!("CIDR too small for host allocation (/31 or /32)"); }
    let base = u32::from(net.network());
    let bcast = u32::from(net.broadcast());
    let usable = (bcast - base).saturating_sub(1); // number of usable addresses excluding network
    if usable == 0 { bail!("no usable host addresses"); }
    let hash = xxhash_rust::xxh3::xxh3_64(peer.to_bytes().as_slice());
    let offset = (hash as u32 % usable) + 1; // skip network .0, avoid broadcast by modulo
    let ip_u32 = base + offset;
    Ok(Ipv4Addr::from(ip_u32))
}
