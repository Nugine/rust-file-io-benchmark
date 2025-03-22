use std::ops::Not;
use std::process::Stdio;

use axum::http::HeaderValue;
use rand::seq::SliceRandom as _;
use reqwest::header::CONTENT_LENGTH;
use tokio_util::codec::BytesCodec;
use tokio_util::codec::FramedRead;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let base_url = "http://localhost:8080";

    let file_path = std::env::args().nth(1).expect("expected a path to a file");
    let file_length = std::fs::metadata(&file_path)?.len();
    let sample_sha256 = sha256sum(&file_path)?;

    let mut api_paths = rust_upload_file_benchmark::routes::API_PATHS.to_owned();

    let client = reqwest::Client::new();

    let max_rounds = 3;
    for round in 1..=max_rounds {
        println!("round {round}:");
        api_paths.shuffle(&mut rand::rng());
        for &api in &api_paths {
            let file = tokio::fs::File::open(&file_path).await?;
            let body = reqwest::Body::wrap_stream(FramedRead::with_capacity(
                file,
                BytesCodec::new(),
                128 * 1024,
            ));

            let t0 = std::time::Instant::now();

            let resp = client
                .put(format!("{base_url}{api}"))
                .header(CONTENT_LENGTH, HeaderValue::from(file_length))
                .body(body)
                .send()
                .await?;

            let t1 = std::time::Instant::now();

            let duration = t1 - t0;
            let speed = file_length as f64 / (1024.0 * 1024.0) / duration.as_secs_f64(); // wall time
            println!(
                "{:<30}: {:.6}s, {:>12.6} MiB/s",
                api,
                duration.as_secs_f64(),
                speed
            );
            if resp.status().is_success().not() {
                println!("{:?}", resp);
                println!("{:?}", resp.text().await)
            }

            let dst_file = format!("{}{}", rust_upload_file_benchmark::routes::DATA_DIR, api);
            let dst_sha256 = sha256sum(&dst_file)?;
            if sample_sha256 != dst_sha256 {
                println!("sha256 mismatch: {} != {}", dst_sha256, sample_sha256);
            }
        }
        println!("--------");
    }
    println!("all sha256 match");

    Ok(())
}

fn sha256sum(path: &str) -> anyhow::Result<String> {
    let output = std::process::Command::new("sha256sum")
        .arg(path)
        .stdout(Stdio::piped())
        .output()?;

    let line = String::from_utf8(output.stdout)?;
    Ok(line.split_once(' ').unwrap().0.to_owned())
}
