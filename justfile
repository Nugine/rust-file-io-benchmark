dev:
    cargo fmt
    cargo clippy

server:
    RUST_BACKTRACE=full \
    RUST_LOG=fio=debug,server=debug \
    cargo run --release --bin server

client:
    RUST_BACKTRACE=full \
    RUST_LOG=fio=debug,client=debug \
    cargo run --release --bin client \
    -- data/sample.bin \
    | tee client.log

mkdata:
    mkdir -p data
    dd if=/dev/urandom of=data/sample.bin bs=64M count=16 iflag=fullblock
