RUST_LOG=info \
cargo run --manifest-path ../Cargo.toml --release --no-default-features --features o3,cli \
--bin o3 -p o3 -- \
-m 127.0.0.1:1902 -d -p 1900 -c "../cert/server.crt" \
-k "../cert/server.key" \
redirect 127.0.0.1:12948
