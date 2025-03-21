dev:
    cargo fmt
    cargo clippy

server:
    RUST_BACKTRACE=full \
    RUST_LOG=server=debug \
    cargo run --release --bin server

client:
    RUST_BACKTRACE=full \
    RUST_LOG=client=debug \
    cargo run --release --bin client \
    -- target/sample.bin \
    | tee client.log

mkdata:
    dd if=/dev/urandom of=target/sample.bin bs=64M count=16 iflag=fullblock
    mkdir -p target/data
