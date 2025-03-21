use crate::error::AppError;

use std::io::Write as _;
use std::path::PathBuf;
use std::sync::LazyLock;

use axum::Router;
use axum::body::Body;
use axum::routing::put;
use bytes::Bytes;
use compio::io::AsyncWriteAtExt as _;
use futures::StreamExt;
use futures::TryStreamExt;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::io::StreamReader;
use tracing::debug;

#[macro_export]
macro_rules! function_name {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        let name = name
            .strip_suffix("::{{closure}}::{{closure}}::{{closure}}::f")
            .unwrap();
        name.rsplit_once("::").unwrap().1
    }};
}

#[tracing::instrument(err)]
async fn put_std_fs_stream(body: Body) -> Result<(), AppError> {
    debug!("start");

    let path = format!("{DATA_DIR}/{}", function_name!());

    let (tx, mut rx) = mpsc::channel::<Bytes>(DATA_CHAN_SIZE);

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

#[tracing::instrument(err)]
async fn put_tokio_fs_iocopy(body: Body) -> Result<(), AppError> {
    debug!("start");

    let path = format!("{DATA_DIR}/{}", function_name!());

    let mut file = tokio::fs::File::create(path).await?;

    let mut data = StreamReader::new(body.into_data_stream().map_err(std::io::Error::other));
    tokio::io::copy(&mut data, &mut file).await?;

    file.sync_all().await?;

    debug!("end");

    Ok(())
}

#[tracing::instrument(err)]
async fn put_tokio_fs_stream(body: Body) -> Result<(), AppError> {
    debug!("start");

    let path = format!("{DATA_DIR}/{}", function_name!());

    let mut file = tokio::fs::File::create(path).await?;

    let mut stream = body.into_data_stream();
    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        // debug!(bytes_len = %(bytes.len()));
        file.write_all(&bytes).await?;
    }

    file.sync_all().await?;

    debug!("end");

    Ok(())
}

enum Operation {
    PutFileAllSequential(
        PathBuf,
        mpsc::Receiver<Bytes>,
        oneshot::Sender<Result<(), AppError>>,
    ),
    PutFileBatchSequential(
        PathBuf,
        mpsc::Receiver<Bytes>,
        oneshot::Sender<Result<(), AppError>>,
    ),
}

static TOKIO_URING: LazyLock<mpsc::Sender<Operation>> = LazyLock::new(|| {
    let (tx, mut rx) = mpsc::channel::<Operation>(32);

    std::thread::spawn(move || {
        tokio_uring::start(async move {
            while let Some(op) = rx.recv().await {
                match op {
                    Operation::PutFileAllSequential(path, mut data_rx, ans_tx) => {
                        tokio_uring::spawn(async move {
                            let future = async move {
                                let file = tokio_uring::fs::File::create(path).await?;
                                {
                                    let mut pos: u64 = 0;
                                    while let Some(bytes) = data_rx.recv().await {
                                        let buf_len = bytes.len();
                                        let (res, _) = file.write_all_at(bytes, pos).await;
                                        res?;
                                        pos += buf_len as u64;
                                    }
                                }
                                file.sync_all().await?;
                                file.close().await?;
                                Ok::<(), AppError>(())
                            };
                            let _ = ans_tx.send(future.await);
                        })
                    }
                    Operation::PutFileBatchSequential(path, mut data_rx, ans_tx) => {
                        tokio_uring::spawn(async move {
                            let future = async move {
                                let file = tokio_uring::fs::File::create(path).await?;
                                {
                                    let mut vec = Vec::with_capacity(DATA_CHAN_SIZE);
                                    let mut pos: u64 = 0;
                                    while let Some(bytes) = data_rx.recv().await {
                                        vec.clear();
                                        vec.push(bytes);
                                        while let Ok(bytes) = data_rx.try_recv() {
                                            vec.push(bytes);
                                        }
                                        let buf_len: usize = vec.iter().map(|b| b.len()).sum();
                                        let res;
                                        (res, vec) = file.writev_at_all(vec, Some(pos)).await;
                                        res?;
                                        pos += buf_len as u64;
                                    }
                                }
                                file.sync_all().await?;
                                file.close().await?;
                                Ok::<(), AppError>(())
                            };
                            let _ = ans_tx.send(future.await);
                        })
                    }
                };
            }
            Ok::<_, anyhow::Error>(())
        })
    });

    tx
});

#[tracing::instrument(err)]
async fn put_tokio_uring_all_seq(body: Body) -> Result<(), AppError> {
    debug!("start");

    let path = format!("{DATA_DIR}/{}", function_name!());

    let mut stream = body.into_data_stream();
    let (data_tx, data_rx) = mpsc::channel::<Bytes>(DATA_CHAN_SIZE);
    let (ans_tx, ans_rx) = oneshot::channel::<Result<(), AppError>>();

    let op = Operation::PutFileAllSequential(PathBuf::from(path), data_rx, ans_tx);

    TOKIO_URING.send(op).await?;

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
async fn put_tokio_uring_batch_seq(body: Body) -> Result<(), AppError> {
    debug!("start");

    let path = format!("{DATA_DIR}/{}", function_name!());

    let mut stream = body.into_data_stream();
    let (data_tx, data_rx) = mpsc::channel::<Bytes>(DATA_CHAN_SIZE);
    let (ans_tx, ans_rx) = oneshot::channel::<Result<(), AppError>>();

    let op = Operation::PutFileBatchSequential(PathBuf::from(path), data_rx, ans_tx);

    TOKIO_URING.send(op).await?;

    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        data_tx.send(bytes).await?;
    }
    drop(data_tx);

    ans_rx.await??;

    debug!("end");

    Ok(())
}

static COMPIO: LazyLock<mpsc::Sender<Operation>> = LazyLock::new(|| {
    let (tx, mut rx) = mpsc::channel::<Operation>(32);

    std::thread::spawn(move || {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            while let Some(op) = rx.recv().await {
                match op {
                    Operation::PutFileAllSequential(path, mut data_rx, ans_tx) => {
                        compio::runtime::spawn(async move {
                            let future = async move {
                                let mut file = compio::fs::File::create(path).await?;
                                {
                                    let mut pos: u64 = 0;
                                    while let Some(bytes) = data_rx.recv().await {
                                        let buf_len = bytes.len();
                                        let compio::BufResult(res, _) =
                                            file.write_all_at(bytes, pos).await;
                                        res?;
                                        pos += buf_len as u64;
                                    }
                                }
                                file.sync_all().await?;
                                file.close().await?;
                                Ok(())
                            };
                            let _ = ans_tx.send(future.await);
                        })
                    }
                    Operation::PutFileBatchSequential(path, mut data_rx, ans_tx) => {
                        compio::runtime::spawn(async move {
                            let future = async move {
                                let mut file = compio::fs::File::create(path).await?;
                                {
                                    let mut vec = Vec::with_capacity(DATA_CHAN_SIZE);
                                    let mut pos: u64 = 0;
                                    while let Some(bytes) = data_rx.recv().await {
                                        vec.clear();
                                        vec.push(bytes);
                                        while let Ok(bytes) = data_rx.try_recv() {
                                            vec.push(bytes);
                                        }
                                        let buf_len: usize = vec.iter().map(|b| b.len()).sum();
                                        let res = file.write_vectored_all_at(vec, pos).await;
                                        res.0?;
                                        vec = res.1;
                                        pos += buf_len as u64;
                                    }
                                }
                                file.sync_all().await?;
                                file.close().await?;
                                Ok(())
                            };
                            let _ = ans_tx.send(future.await);
                        })
                    }
                }
                .detach();
            }
        });
    });

    tx
});

#[tracing::instrument(err)]
async fn put_compio_all_seq(body: Body) -> Result<(), AppError> {
    debug!("start");

    let path = format!("{DATA_DIR}/{}", function_name!());

    let mut stream = body.into_data_stream();
    let (data_tx, data_rx) = mpsc::channel::<Bytes>(DATA_CHAN_SIZE);
    let (ans_tx, ans_rx) = oneshot::channel::<Result<(), AppError>>();

    let op = Operation::PutFileAllSequential(PathBuf::from(path), data_rx, ans_tx);

    COMPIO.send(op).await?;

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
async fn put_compio_batch_seq(body: Body) -> Result<(), AppError> {
    debug!("start");

    let path = format!("{DATA_DIR}/{}", function_name!());

    let mut stream = body.into_data_stream();
    let (data_tx, data_rx) = mpsc::channel::<Bytes>(DATA_CHAN_SIZE);
    let (ans_tx, ans_rx) = oneshot::channel::<Result<(), AppError>>();

    let op = Operation::PutFileBatchSequential(PathBuf::from(path), data_rx, ans_tx);

    COMPIO.send(op).await?;

    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        data_tx.send(bytes).await?;
    }
    drop(data_tx);

    ans_rx.await??;

    debug!("end");

    Ok(())
}

const DATA_CHAN_SIZE: usize = 64;

pub const DATA_DIR: &str = "./target/data";

pub const API_PATHS: &[&str] = &[
    concat!("/", stringify!(put_std_fs_stream)),
    concat!("/", stringify!(put_tokio_fs_iocopy)),
    concat!("/", stringify!(put_tokio_fs_stream)),
    concat!("/", stringify!(put_tokio_uring_all_seq)),
    concat!("/", stringify!(put_tokio_uring_batch_seq)),
    concat!("/", stringify!(put_compio_all_seq)),
    concat!("/", stringify!(put_compio_batch_seq)),
];

pub fn router() -> Router {
    Router::new()
        .route(API_PATHS[0], put(put_std_fs_stream))
        .route(API_PATHS[1], put(put_tokio_fs_iocopy))
        .route(API_PATHS[2], put(put_tokio_fs_stream))
        .route(API_PATHS[3], put(put_tokio_uring_all_seq))
        .route(API_PATHS[4], put(put_tokio_uring_batch_seq))
        .route(API_PATHS[5], put(put_compio_all_seq))
        .route(API_PATHS[6], put(put_compio_batch_seq))
}
