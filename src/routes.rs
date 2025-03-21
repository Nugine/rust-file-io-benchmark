use std::io::Write as _;
use std::path::PathBuf;
use std::sync::LazyLock;

use crate::error::AppError;

use axum::Router;
use axum::body::Body;
use axum::routing::put;
use bytes::Bytes;
use futures::StreamExt;
use futures::TryStreamExt;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::io::StreamReader;
use tracing::debug;

#[tracing::instrument(err)]
async fn put_v1(body: Body) -> Result<(), AppError> {
    debug!("start");

    let path = "target/data/put_v1";
    let mut file = tokio::fs::File::create(path).await?;

    let mut data = StreamReader::new(body.into_data_stream().map_err(std::io::Error::other));
    tokio::io::copy(&mut data, &mut file).await?;

    file.sync_all().await?;

    debug!("end");

    Ok(())
}

#[tracing::instrument(err)]
async fn put_v2(body: Body) -> Result<(), AppError> {
    debug!("start");

    let path = "target/data/put_v2";

    let (tx, mut rx) = mpsc::channel::<Bytes>(32);

    let task = tokio::task::spawn_blocking(move || {
        debug!("fs start");
        let mut file = std::fs::File::create(path)?;
        while let Some(bytes) = rx.blocking_recv() {
            file.write_all(&bytes)?;
        }
        file.sync_all()?;
        debug!("fs end");
        Ok::<_, std::io::Error>(())
    });

    let mut stream = body.into_data_stream();
    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        tx.send(bytes).await?;
    }
    drop(tx);

    task.await??;

    debug!("end");

    Ok(())
}

enum Operation {
    PutFile(
        PathBuf,
        mpsc::Receiver<Bytes>,
        oneshot::Sender<Result<(), AppError>>,
    ),
}

static IO_URING: LazyLock<mpsc::Sender<Operation>> = LazyLock::new(|| {
    let (tx, mut rx) = mpsc::channel::<Operation>(32);

    std::thread::spawn(move || {
        tokio_uring::start(async move {
            while let Some(op) = rx.recv().await {
                match op {
                    Operation::PutFile(path, data_rx, ans_tx) => tokio_uring::spawn(async move {
                        let res = io_uring_put_file(path, data_rx).await;
                        let _ = ans_tx.send(res);
                    }),
                };
            }
            Ok::<_, anyhow::Error>(())
        })
    });

    tx
});

async fn io_uring_put_file(path: PathBuf, mut rx: mpsc::Receiver<Bytes>) -> Result<(), AppError> {
    let file = tokio_uring::fs::File::create(path).await?;
    {
        let mut pos: u64 = 0;
        while let Some(bytes) = rx.recv().await {
            let buf_len = bytes.len();
            let (res, _) = file.write_all_at(bytes, pos).await;
            res?;
            pos += buf_len as u64;
        }
    }
    file.sync_all().await?;
    file.close().await?;
    Ok(())
}

#[tracing::instrument(err)]
async fn put_v3(body: Body) -> Result<(), AppError> {
    debug!("start");

    let path = "target/data/put_v3";

    let mut stream = body.into_data_stream();
    let (data_tx, data_rx) = mpsc::channel::<Bytes>(32);
    let (ans_tx, ans_rx) = oneshot::channel::<Result<(), AppError>>();

    let op = Operation::PutFile(PathBuf::from(path), data_rx, ans_tx);

    IO_URING.send(op).await?;

    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        data_tx.send(bytes).await?;
    }
    drop(data_tx);

    ans_rx.await??;

    debug!("end");

    Ok(())
}

#[tracing::instrument(err)]
async fn put_v4(body: Body) -> Result<(), AppError> {
    debug!("start");

    let path = "target/data/put_v4";
    let mut file = tokio::fs::File::create(path).await?;

    let mut stream = body.into_data_stream();
    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        file.write_all(&bytes).await?;
    }

    file.sync_all().await?;

    debug!("end");

    Ok(())
}

pub fn router() -> Router {
    Router::new()
        .route("/put/v1", put(put_v1))
        .route("/put/v2", put(put_v2))
        .route("/put/v3", put(put_v3))
        .route("/put/v4", put(put_v4))
}

pub const API_PATHS: &[&str] = &["/put/v1", "/put/v2", "/put/v3", "/put/v4"];

pub const DST_FILES: &[&str] = &[
    "target/data/put_v1",
    "target/data/put_v2",
    "target/data/put_v3",
    "target/data/put_v4",
];
