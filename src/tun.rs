use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::DuplexStream;

#[cfg(not(target_os = "windows"))]
use tun::Configuration;
#[cfg(not(target_os = "windows"))]
use tun::Device as _;

pub struct TunHandle {
    #[allow(dead_code)]
    ifname: String,
    #[allow(dead_code)]
    mtu: u16,
}

pub async fn open_tun(ifname: Option<String>, mtu: Option<u16>, ip_cidr: String) -> Result<(TunHandle, tokio::io::ReadHalf<DuplexStream>, tokio::io::WriteHalf<DuplexStream>)> {
    // 决定 MTU
    let chosen_mtu: u16 = match mtu { Some(m) => m, None => {
        #[cfg(target_os = "windows")] { 1300 }
        #[cfg(not(target_os = "windows"))] { 1400 }
    }};
    tracing::info!(mtu = chosen_mtu, "configuring TUN MTU");

    #[cfg(not(target_os = "windows"))]
    {
        let mut config = Configuration::default();
        if let Some(name) = ifname.clone() { config.name(&name); }
        config.mtu(chosen_mtu as i32);
        config.up();
        let dev = tun::create_as_async(&config).map_err(|e| anyhow::anyhow!(e))?;
        let name = dev
            .get_ref()
            .name()
            .unwrap_or_else(|_| ifname.clone().unwrap_or_else(|| "coconet0".to_string()));
        if let Err(e) = configure_iface_addr(&name, chosen_mtu, &ip_cidr).await {
            tracing::warn!(ifname=%name, error=%e, "failed to configure TUN address");
        }
    let (s1, s2) = tokio::io::duplex(64 * 1024);
    let (r, w) = tokio::io::split(s1);
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
        let handle = TunHandle { ifname: name, mtu: chosen_mtu };
        return Ok((handle, r, w));
    }

    #[cfg(target_os = "windows")]
    {
        use std::sync::{Arc, mpsc};
        use std::thread;
    use wintun::{Adapter, Session};

        // 加载 Wintun.dll
        let wintun = unsafe { wintun::load() }.map_err(|e| anyhow::anyhow!(format!("failed to load wintun.dll: {e}")))?;
        let adapter_name = ifname.clone().unwrap_or_else(|| "wintun".to_string());
        // 打开或创建适配器
        let adapter = match Adapter::open(&wintun, &adapter_name) {
            Ok(a) => a,
            Err(_) => Adapter::create(&wintun, &adapter_name, "CocoNet", None)
                .map_err(|e| anyhow::anyhow!(format!("create wintun adapter failed: {e}")))?,
        };

        // 启动会话（环形缓冲容量 4MiB）
        let session = Arc::new(adapter.start_session(4 * 1024 * 1024)
            .map_err(|e| anyhow::anyhow!(format!("start wintun session failed: {e}")))?);

        // 配置地址/路由/MTU（使用适配器友好名）
        if let Err(e) = configure_iface_addr(&adapter_name, chosen_mtu, &ip_cidr).await {
            tracing::warn!(ifname=%adapter_name, error=%e, "failed to configure Wintun address");
        }

        // 建立与上层的字节通道
    let (s1, s2) = tokio::io::duplex(64 * 1024);
    let (r, w) = tokio::io::split(s1);
    let (s2r, s2w) = tokio::io::split(s2);

        // Wintun -> sock: 使用阻塞接收线程 + async 写入
        let (tx_in, mut rx_in) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        tokio::spawn(async move {
            let mut s2w = s2w;
            while let Some(buf) = rx_in.recv().await {
                if s2w.write_all(&buf).await.is_err() { break; }
            }
        });

        {
            let session_rx = Arc::clone(&session);
            thread::spawn(move || {
                loop {
                    match session_rx.receive_blocking() {
                        Ok(pkt) => {
                            let bytes = pkt.bytes();
                            let mut v = Vec::with_capacity(bytes.len());
                            v.extend_from_slice(bytes);
                            // 交由 Drop 释放接收包（wintun 0.4 Packet 实现 Drop）
                            let _ = tx_in.send(v);
                        }
                        Err(_) => break,
                    }
                }
            });
        }

        // sock -> Wintun: async 读 + 阻塞发送线程
        let (tx_out, rx_out) = mpsc::sync_channel::<Vec<u8>>(1024);
        {
            let session_tx = Arc::clone(&session);
            thread::spawn(move || {
                while let Ok(buf) = rx_out.recv() {
                    let len = (buf.len().min(u16::MAX as usize)) as u16;
                    if let Ok(mut pkt) = session_tx.allocate_send_packet(len) {
                        pkt.bytes_mut().copy_from_slice(&buf);
                        let _ = session_tx.send_packet(pkt);
                    }
                }
            });
        }
        tokio::spawn(async move {
            let mut s2r = s2r;
            let mut buf2 = vec![0u8; 65536];
            loop {
                match s2r.read(&mut buf2).await {
                    Ok(n) if n>0 => { if tx_out.send(buf2[..n].to_vec()).is_err() { break; } },
                    _ => break,
                }
            }
        });

    let handle = TunHandle { ifname: adapter_name, mtu: chosen_mtu };
    return Ok((handle, r, w));
    }
}

impl TunHandle {
    pub fn ifname(&self) -> &str { &self.ifname }
    pub fn mtu(&self) -> u16 { self.mtu }
}

#[cfg(target_os = "linux")]
async fn configure_iface_addr(ifname: &str, mtu: u16, ip_cidr: &str) -> Result<()> {
    use tokio::process::Command;
    use ipnetwork::Ipv4Network;
    // 解析出网段前缀，便于添加路由
    let net: Ipv4Network = ip_cidr.parse()?;
    let cidr_str = format!("{}/{}", net.network(), net.prefix());
    // 添加地址（幂等；失败不致命）
    let _ = Command::new("ip").args(["addr", "add", ip_cidr, "dev", ifname]).status().await?;
    // 置 up（即便前面 config.up()，这里再确保一次）
    let _ = Command::new("ip").args(["link", "set", "dev", ifname, "up"]).status().await?;
    // 设置 MTU（幂等）
    let _ = Command::new("ip").args(["link", "set", "dev", ifname, "mtu", &mtu.to_string()]).status().await?;
    // 添加直连路由到整个虚拟网段（replace 保证幂等）
    let _ = Command::new("ip").args(["route", "replace", &cidr_str, "dev", ifname]).status().await?;
    Ok(())
}

#[cfg(target_os = "macos")]
async fn configure_iface_addr(ifname: &str, mtu: u16, ip_cidr: &str) -> Result<()> {
    use tokio::process::Command;
    use ipnetwork::Ipv4Network;
    // 解析出 IP 和网段前缀
    let net: Ipv4Network = ip_cidr.parse()?;
    let ip = net.ip();
    let netmask = net.mask();
    let cidr_str = format!("{}/{}", net.network(), net.prefix());
    
    // 添加地址（macOS 使用 ifconfig）
    let _ = Command::new("ifconfig")
        .args([ifname, &ip.to_string(), &netmask.to_string()])
        .status()
        .await?;
    
    // 设置 MTU
    let _ = Command::new("ifconfig")
        .args([ifname, "mtu", &mtu.to_string()])
        .status()
        .await?;
    
    // 确保接口 up
    let _ = Command::new("ifconfig")
        .args([ifname, "up"])
        .status()
        .await?;
    
    // 添加直连路由到整个虚拟网段
    let _ = Command::new("route")
        .args(["-n", "add", "-net", &cidr_str, "-interface", ifname])
        .status()
        .await?;
    
    Ok(())
}

#[cfg(target_os = "windows")]
async fn configure_iface_addr(ifname: &str, mtu: u16, ip_cidr: &str) -> Result<()> {
    use tokio::process::Command;
    use ipnetwork::Ipv4Network;
    let net: Ipv4Network = ip_cidr.parse()?; // ip/prefix
    let ip = net.ip();
    let prefix = net.prefix();
    let dest = format!("{}/{}", net.network(), prefix);

    // 查询接口索引，避免别名歧义
    async fn get_ifindex(alias: &str) -> Result<u32> {
        let out = Command::new("powershell")
            .args([
                "-NoProfile",
                "-Command",
                &format!("(Get-NetIPInterface -InterfaceAlias '{}' -AddressFamily IPv4).InterfaceIndex", alias),
            ])
            .output()
            .await?;
        if !out.status.success() { return Err(anyhow::anyhow!("Get-NetIPInterface failed for alias={}", alias)); }
        let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let idx: u32 = s.parse().map_err(|_| anyhow::anyhow!("invalid InterfaceIndex: {}", s))?;
        Ok(idx)
    }
    let ifindex = get_ifindex(ifname).await?;

    // 1) 设置 IPv4 地址（无网关），若已存在忽略错误
    let ps_addr = format!(
        "Try {{ New-NetIPAddress -InterfaceIndex {} -IPAddress {} -PrefixLength {} -AddressFamily IPv4 -ErrorAction Stop }} Catch {{ $_ }}",
        ifindex, ip, prefix
    );
    let _ = Command::new("powershell").args(["-NoProfile", "-Command", &ps_addr]).status().await?;

    // 2) 添加 /16 路由（低 metric），若已存在静默跳过
    let ps_route = format!(
        "Try {{ New-NetRoute -InterfaceIndex {} -DestinationPrefix '{}' -NextHop 0.0.0.0 -RouteMetric 5 -ErrorAction SilentlyContinue }} Catch {{ $_ }}",
        ifindex, dest
    );
    let _ = Command::new("powershell").args(["-NoProfile", "-Command", &ps_route]).status().await?;

    // 3) 设置 MTU：PowerShell（IPv4/IPv6），失败不致命；再用 netsh 兜底 IPv4
    let ps_mtu_v4 = format!(
        "Try {{ Set-NetIPInterface -InterfaceIndex {} -AddressFamily IPv4 -NlMtuBytes {} -ErrorAction Stop }} Catch {{ $_ }}",
        ifindex, mtu
    );
    let _ = Command::new("powershell").args(["-NoProfile", "-Command", &ps_mtu_v4]).status().await?;
    let ps_mtu_v6 = format!(
        "Try {{ Set-NetIPInterface -InterfaceIndex {} -AddressFamily IPv6 -NlMtuBytes {} -ErrorAction Stop }} Catch {{ $_ }}",
        ifindex, mtu
    );
    let _ = Command::new("powershell").args(["-NoProfile", "-Command", &ps_mtu_v6]).status().await?;
    let _ = Command::new("netsh").args(["interface", "ipv4", "set", "subinterface", ifname, &format!("mtu={}", mtu), "store=active"]).status().await?;

    // 4) 固定接口度量（InterfaceMetric）较低，禁用自动度量
    let ps_metric = format!(
        "Try {{ Set-NetIPInterface -InterfaceIndex {} -AutomaticMetric Disabled -InterfaceMetric 5 -ErrorAction Stop }} Catch {{ $_ }}",
        ifindex
    );
    let _ = Command::new("powershell").args(["-NoProfile", "-Command", &ps_metric]).status().await?;

    Ok(())
}
