Cross-platform packaging guide

This document explains how to build and package CocoNet for Linux, macOS, and Windows.

Prerequisites
- Rust stable (MSVC toolchain on Windows)
- Git, C toolchain
- On Linux: musl-tools for static builds; iproute2 for runtime
- On macOS: Xcode Command Line Tools
- On Windows: PowerShell (Administrator for TUN), Visual Studio Build Tools

Build locally
- Debug (all OS): cargo build
- Release (native): cargo build --release

Linux static (musl)
- Install: sudo apt-get install musl-tools pkg-config
- Add target: rustup target add x86_64-unknown-linux-musl
- Build: cargo build --release --target x86_64-unknown-linux-musl

macOS
- Build: cargo build --release
- Codesign/notarize as needed for distribution

Windows
- Toolchain: rustup default stable-x86_64-pc-windows-msvc
- Build: cargo build --release
- The executable needs Administrator to create/configure TUN
- Wintun driver is bundled by the tun crate; no manual install required

Installer options
- Linux: tar.gz with README and systemd service template
- macOS: zip or pkg (optional notarization)
- Windows: zip or NSIS/Inno Setup installer (add Run as administrator note)

CI builds
- GitHub Actions workflow in .github/workflows/build.yml builds artifacts for Linux, macOS, Windows, plus a musl static Linux build. Download from the Actions run artifacts.

Systemd template (optional)
[Unit]
Description=CocoNet
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/CocoNet --bootstrap <peer_multiaddr> --cidr 10.99.0.0/24
Restart=on-failure
AmbientCapabilities=CAP_NET_ADMIN
CapabilityBoundingSet=CAP_NET_ADMIN

[Install]
WantedBy=multi-user.target

Troubleshooting
- glibc not found: use musl build
- Permission denied creating TUN: run with sudo (Linux) or as Administrator (Windows)
- No connectivity: check firewall/NAT, ensure ports are open or use relay mode
