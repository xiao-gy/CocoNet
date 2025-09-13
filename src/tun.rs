use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::DuplexStream;

use tun::Configuration;
use tun::Device as _;

pub struct TunHandle {
    #[allow(dead_code)]
    ifname: String,
}

pub async fn open_tun(ifname: Option<String>, ip_cidr: String) -> Result<(TunHandle, tokio::io::ReadHalf<DuplexStream>, tokio::io::WriteHalf<DuplexStream>)> {
    {
    let mut config = Configuration::default();
    if let Some(name) = ifname.clone() { config.name(&name); }
        config.mtu(1400);
        config.up();
        // NOTE: IP assignment is platform-specific; we only open device here.
        let dev = tun::create_as_async(&config).map_err(|e| anyhow::anyhow!(e))?;
        // 获取实际 ifname（需要将 tun::Device trait 引入作用域）；失败则回退到传入名或默认名
        let name = dev
            .get_ref()
            .name()
            .unwrap_or_else(|_| ifname.clone().unwrap_or_else(|| "coconet0".to_string()));

        // 配置平台对应的地址与路由（失败不致命，仅记录日志）
        if let Err(e) = configure_iface_addr(&name, &ip_cidr).await {
            tracing::warn!(ifname=%name, error=%e, "failed to configure TUN address");
        }

        // 用 DuplexStream 提供跨平台的字节通道，将 dev 与另一端桥接
        let (s1, s2) = tokio::io::duplex(64 * 1024);
        let (r, w) = tokio::io::split(s1);
        // Spawn a forwarding task between dev and s2 using raw read/write
        tokio::spawn(async move {
            let mut sock = s2;
            let mut dev = dev;
            let mut buf1 = vec![0u8; 65536];
            let mut buf2 = vec![0u8; 65536];
            loop {
                tokio::select! {
                    r = dev.read(&mut buf1) => {
                        match r { Ok(n) if n>0 => { if sock.write_all(&buf1[..n]).await.is_err() { break; } }, _ => break }
                    }
                    r = sock.read(&mut buf2) => {
                        match r { Ok(n) if n>0 => { if dev.write_all(&buf2[..n]).await.is_err() { break; } }, _ => break }
                    }
                }
            }
        });

        let handle = TunHandle { ifname: name };
        return Ok((handle, r, w));
    }
}

impl TunHandle {
    pub fn ifname(&self) -> &str { &self.ifname }
}

#[cfg(target_os = "linux")]
async fn configure_iface_addr(ifname: &str, ip_cidr: &str) -> Result<()> {
    use tokio::process::Command;
    use ipnetwork::Ipv4Network;
    // 解析出网段前缀，便于添加路由
    let net: Ipv4Network = ip_cidr.parse()?;
    let cidr_str = format!("{}/{}", net.network(), net.prefix());
    // 添加地址（幂等；失败不致命）
    let _ = Command::new("ip").args(["addr", "add", ip_cidr, "dev", ifname]).status().await?;
    // 置 up（即便前面 config.up()，这里再确保一次）
    let _ = Command::new("ip").args(["link", "set", "dev", ifname, "up"]).status().await?;
    // 添加直连路由到整个虚拟网段（replace 保证幂等）
    let _ = Command::new("ip").args(["route", "replace", &cidr_str, "dev", ifname]).status().await?;
    Ok(())
}

#[cfg(target_os = "windows")]
async fn configure_iface_addr(ifname: &str, ip_cidr: &str) -> Result<()> {
    use tokio::process::Command;
    use ipnetwork::Ipv4Network;
    let net: Ipv4Network = ip_cidr.parse()?; // ip/prefix
    let ip = net.ip();
    let prefix = net.prefix();
    let dest = format!("{}/{}", net.network(), prefix);

    // 使用 PowerShell 配置地址与直连路由
    // 1) 设置 IPv4 地址（无网关）
    let ps_addr = format!(
        "Try {{ New-NetIPAddress -InterfaceAlias \"{}\" -IPAddress {} -PrefixLength {} -AddressFamily IPv4 -ErrorAction Stop }} Catch {{ $_ }}",
        ifname, ip, prefix
    );
    let _ = Command::new("powershell").args(["-NoProfile", "-Command", &ps_addr]).status().await?;

    // 2) 添加直连路由，下一跳 0.0.0.0（本地直连）
    let ps_route = format!(
        "Try {{ New-NetRoute -InterfaceAlias \"{}\" -DestinationPrefix \"{}\" -NextHop 0.0.0.0 -ErrorAction Stop }} Catch {{ $_ }}",
        ifname, dest
    );
    let _ = Command::new("powershell").args(["-NoProfile", "-Command", &ps_route]).status().await?;

    Ok(())
}
