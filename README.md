# CocoNet

一个完全去中心化的 P2P 虚拟组网（VPN）工具。CocoNet 通过 libp2p 构建覆盖网络，使用 TUN 设备承载二层/三层数据平面，并提供多种打洞方案（AutoNAT、DCUtR、UPnP）与中继能力（Relay Client/Server）以尽可能实现端到端直连，同时在直连不可达时自动回退到中继路径。

> 当前目标平台：Linux / macOS / Windows（TUN 已适配；Windows 需管理员权限创建/配置 TUN 与路由）。

## 特性

- TUN 虚拟网卡：将本地 IP 流量与 P2P 数据平面桥接
- 去中心化发现：Kademlia DHT + Identify
- 发布/订阅数据面：Gossipsub 传输封装的 IP 包
- 可达性与打洞：AutoNAT、DCUtR（通过中继升级为直连）、UPnP/NAT-PMP（端口映射）
- 中继能力：
	- Relay Client（默认启用）
	- Relay Server（`--relay` 开启，为其他节点提供 p2p-circuit 中继）
- 公网地址广播：通过 `coconet/announce` 主动宣告自身公网 UDP/QUIC 地址，促进端侧直连
- 直连优先策略：一旦直连成功，自动关闭中继连接，避免走中继数据面
- 详细日志：收/发包方向、五元组摘要、直连/中继切换、NAT 状态等

## 架构概览
 libp2p 0.53（gossipsub/identify/kad/relay/autonat/dcutr/quic 等）
 tokio、tracing、serde、igd（UPnP，默认启用）
 tun（异步）
	- Announce：在 `coconet/announce` 主题上定期广播自身可达公网 UDP/QUIC 地址
- 数据平面：
	- TUN -> Gossipsub：从内核 TUN 读出 IP 包，封装为 PubSub 消息
	- Gossipsub -> TUN：收到的消息写回 TUN，完成虚拟网络互通
- 连接管理：
	- 优先直连拨号；当仅有 `/p2p-circuit` 地址时使用中继拨号

## 安装与构建

### 先决条件

- Rust 1.75+（建议使用 rustup 安装）
- Linux 内核支持 TUN（通常默认具备）

### 构建（GNU glibc）

```bash
cd CocoNet
cargo build --release
```

### 构建（musl 静态链接，便于跨发行版运行）

项目已包含 `x86_64-unknown-linux-musl` 目标产物目录；如需本机重建：

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
```

二进制位于：

- `target/release/CocoNet`（glibc）
- `target/x86_64-unknown-linux-musl/release/CocoNet`（musl）

## 快速开始（两台主机）

假设 A 作为“中继服务器 + 引导点”，B 作为“客户端”。

1) 在 A 上启动中继服务器（监听指定端口，推荐公网主机）：

```bash
RUST_LOG=info,coconet=trace ./target/x86_64-unknown-linux-musl/release/CocoNet \
	--relay \
	--listen-port 36826
```

2) 在 B 上指向 A 的地址进行引导：

```bash
RUST_LOG=info,coconet=trace ./target/x86_64-unknown-linux-musl/release/CocoNet \
	--bootstrap /ip4/<A_PUBLIC_IP>/udp/36826/quic-v1
```

3) 观察日志：

- 中继路径建立时：
	- B 侧出现 `attempting relay-circuit dial`
	- A 侧出现 `relay-server` 事件（Reservation/Circuit）
- 直连打通后：
	- B 侧出现 `direct connectivity established`
	- 随后 `close previous relayed conn (switched to direct)`

4) 网络连通性测试：

两端均会分配到 10.99.0.0/16 网段的虚拟 IP（从 PeerId 确定性派生）。可在对端之间互相 ping 虚拟 IP 验证联通。

## CLI 参考

```text
--log <FILTER>              RUST_LOG 风格的日志过滤，默认 info,coconet=info
--strap <MA>...         引导多地址（multiaddr），可重复；例 /ip4/1.2.3.4/udp/36826/quic-v1
--cidr <CIDR>               虚拟网段（默认 10.99.0.0/16）
--ifname <NAME>             TUN 设备名（可选，不填自动分配）
--relay                     启用中继服务器（默认仅客户端）
--listen-port <PORT>        QUIC 监听端口（0=随机）

子命令：
whoami                      输出本节点 PeerId 与派生虚拟 IP
```

示例：

```bash
./CocoNet --bootstrap /ip4/47.122.60.162/udp/36826/quic-v1
./CocoNet --relay --listen-port 36826
./CocoNet whoami
```

## 日志与观测

- 关键日志：
	- `direct connectivity established`：获得直连
	- `relay-client`：中继客户端事件
	- `relay-server`：中继服务器事件（Reservation/Circuit）
	- `announced public addrs`：已广播自身公网 UDP/QUIC 地址
	- `external address confirmed`：对外可达地址被确认
- 包级日志（可在源码 `routing.rs` 中查看解析逻辑）：
	- 收/发方向、IPv4/IPv6、长度、协议号、源/目的 IP/端口

建议设置：`RUST_LOG=info,coconet=trace` 以获得更细粒度的组件日志。

## 原理与策略细节

- 地址筛选：
	- 仅对公网 IPv4 UDP/QUIC 地址进行直连拨号；跳过 `/p2p-circuit` 与内网/保留地址
- 拨号冷却与去重：
	- 针对（PeerId, Address）维护冷却时间窗口，避免抖动与打满连接
- 中继回退：
	- 若仅有中继地址（`/p2p-circuit`），在未直连时尝试经中继拨号；保持连通
- 直连优先：
	- 直连成功后关闭中继连接，降低时延与成本
- UPnP/NAT-PMP：
	- 在监听到 IPv4 UDP 地址后尝试映射；失败会记录 debug 级日志但不影响主流程

## 常见问题（FAQ）

1) 为什么我仍然走中继？
	 - 对端未能从你的日志中确认 `external address confirmed`，或双方都在对称 NAT 后面，DCUtR 未成功
	 - 等待一段时间（announce/identify/kad/自动拨号）后可能会升级为直连
	 - 尝试在路由器上开启 UPnP，或手工做 UDP 端口映射

2) Announce 有什么作用？
	 - 主动广播自身可达公网地址，加速其他节点对你发起直连拨号

3) 需要防火墙策略吗？
	 - 建议放行所用 UDP 端口（`--listen-port`）的入站；走中继时对外暴露度较小

4) 如何确认直连？
	 - 查看 `direct connectivity established`；也可在 `ss -u` 看到直连的对端公网 IP:port

## 开发说明

### 代码结构

- `src/main.rs`：CLI 与节点启动流程
- `src/tun.rs`：TUN 打开、地址配置、路由设置
- `src/ipam.rs`：从 PeerId 派生虚拟 IP（避免冲突；跳过网络/广播地址）
- `src/routing.rs`：TUN 与 P2P 的双向数据泵与包级日志
- `src/p2p.rs`：libp2p 行为（gossipsub/identify/ping/kad/dcutr/autonat/relay）、拨号策略、announce
- `src/acl.rs`：访问控制（预留/示例）
- `src/config.rs`：配置（预留）

### 依赖与特性

- libp2p 0.53（gossipsub/identify/kad/relay/autonat/dcutr/quic 等）
- tokio、tracing、serde、igd（UPnP）
- tun（异步）

### 构建与测试

```bash
cargo build
cargo build --release --target x86_64-unknown-linux-musl
```

你可以使用 Linux netns 或多虚拟机场景来进行本地化联通性测试，避免路由冲突。

## 打包与分发

- 已提供 GitHub Actions 多平台构建工作流，提交到 main 或打 v* tag 会生成 Linux、macOS、Windows 构建工件以及 Linux musl 静态产物。
- 本地打包与分发建议（musl 静态、Windows 安装包、systemd 模板等），见 docs/packaging.md。

## 许可

本项目采用 Apache-2.0 或 MIT 双许可。


## 备注：MTU 与分片

- 默认在 Windows 使用 Wintun，在 Linux/macOS 使用 TUN。
- 默认 MTU：Windows 1300，其他平台 1400，可通过 `--mtu` 指定。
- 为避免超过 MTU 的数据包在注入 TUN 时被丢弃，p2p -> TUN 下行路径实现了用户态 IPv4 分片（RFC 791）：
	- 当 IPv4 包长度大于 TUN MTU 且未设置 DF 位时，会自动进行 8 字节对齐的分片并重算头部校验和；
	- 若 DF 位已设置，则不会分片，包将按原样写入（可能被内核丢弃）；
	- IPv6 中间分片不被允许，超过 MTU 的 IPv6 包会记录告警（后续可扩展 ICMPv6 Packet Too Big）。

