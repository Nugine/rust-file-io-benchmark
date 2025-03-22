dev:
    cargo fmt
    cargo clippy

server:
    RUST_BACKTRACE=full \
    RUST_LOG=rust_upload_file_benchmark=debug,server=debug \
    cargo run --release --bin server

client:
    RUST_BACKTRACE=full \
    RUST_LOG=rust_upload_file_benchmark=debug,client=debug \
    cargo run --release --bin client \
    -- target/sample.bin \
    | tee client.log

mkdata:
    mkdir -p target/data
    dd if=/dev/urandom of=target/sample.bin bs=64M count=16 iflag=fullblock
