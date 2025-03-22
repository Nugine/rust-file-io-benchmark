use clap::Parser;

#[derive(Debug, Parser)]
struct Opt {
    #[arg(long, default_value = "localhost")]
    host: String,

    #[arg(long, default_value = "8080")]
    port: u16,
}

fn setup_tracing() {
    use std::io::IsTerminal;
    use tracing_subscriber::EnvFilter;

    let env_filter = EnvFilter::from_default_env();
    let enable_color = std::io::stdout().is_terminal();

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(env_filter)
        .with_ansi(enable_color)
        .init();
}

#[tokio::main]
#[tracing::instrument]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    setup_tracing();

    let app = fio::routes::router();

    let listener = tokio::net::TcpListener::bind((opt.host, opt.port)).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
