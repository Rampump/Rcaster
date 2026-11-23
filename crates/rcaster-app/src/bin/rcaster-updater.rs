use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let command = args.first();
    let config = args
        .windows(2)
        .find(|pair| pair[0] == "--config")
        .map(|pair| pair[1].clone())
        .map(PathBuf::from);
    let result = match (command.map(String::as_str), config) {
        (Some("supervise"), Some(config)) => rcaster_app::updater::run_supervisor(config).await,
        _ => Err("usage: rcaster-updater supervise --config <updater.json>".into()),
    };
    if let Err(error) = result {
        eprintln!("rcaster-updater: {error}");
        std::process::exit(1);
    }
}
