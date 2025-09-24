#!/bin/bash
TARGETS=(
    "x86_64-unknown-linux-gnu"
    "aarch64-unknown-linux-gnu"
    "x86_64-pc-windows-gnu"
    # "x86_64-apple-darwin"
    # "aarch64-apple-darwin"
)

for target in "${TARGETS[@]}"; do
    echo "编译目标平台: $target"
    cargo build --release --target $target
done