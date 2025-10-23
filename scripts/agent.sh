RUST_LOG=info \
cargo run --manifest-path ../Cargo.toml --release --no-default-features --features agent,cli \
--bin o3-agent -p o3 -- \
-d -m 127.0.0.1:1901 -s 127.0.0.1 -p 1900 -c "../cert/client.crt" -k "../cert/client.key" listen 0.0.0.0:1024
