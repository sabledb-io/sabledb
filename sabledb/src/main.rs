use libsabledb::{SableError, Server, ServerOptions, Transport, WorkerManager, WorkerMessage};
use std::net::TcpListener;
use std::sync::Arc;
use tracing::{debug, error, info};

fn main() -> Result<(), SableError> {
    // configure our tracing subscriber
    let options = if let Some(config_file) = std::env::args().nth(1) {
        ServerOptions::from_config(config_file)?
    } else {
        ServerOptions::default()
    };

    let fmtr = tracing_subscriber::fmt::fmt()
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_max_level(options.general_settings.log_level);

    if let Some(logdir) = &options.general_settings.logdir {
        fmtr.with_writer(tracing_appender::rolling::hourly(logdir, "sabledb.log"))
            .with_ansi(false) // No need for colours when using file
            .init();
    } else {
        fmtr.init();
    }

    // install our custom panic! handler
    std::panic::set_hook(Box::new(|e| {
        let errmsg = format!("{}", e);
        let lines = errmsg.split('\n');
        for line in lines.into_iter() {
            tracing::error!("{}", line);
        }
    }));

    // Open the storage
    let mut store = libsabledb::StorageAdapter::default();
    store.open(options.open_params.clone())?;

    info!("Server configuration:\n{:#?}", options);
    let workers_count = WorkerManager::default_workers_count(options.general_settings.workers);

    let address = format!(
        "{}:{}",
        options.general_settings.listen_ip, options.general_settings.port
    );
    info!("TLS enabled: {:?}", options.use_tls());

    let server = Arc::new(Server::new(options, store.clone(), workers_count)?);
    info!("Successfully created {} workers", workers_count);

    // Bind the listener to the address
    let listener = TcpListener::bind(address.clone())
        .unwrap_or_else(|_| panic!("failed to bind address {}", address));
    info!("Server started on port address: {}", address);

    let server_state_clone = server.state();
    let _ = ctrlc::set_handler(move || {
        info!("Received Ctrl-C");
        server_state_clone.shutdown();
        info!("Flushing data");
        if let Err(e) = store.flush() {
            error!("failed to flush. {:?}", e);
        }
        info!("Bye");
        std::process::exit(0);
    });

    loop {
        debug!("Waiting for new connections..");
        match listener.accept() {
            Ok((socket, addr)) => {
                // handle the connection
                debug!("Accepted connection from {:?}", addr);
                Transport::prepare_std_tcp_stream(&socket);

                // pick the worker to use (simple round robin)
                let worker = server.get_worker();
                debug!("Connection passed to {:?}", worker);
                if let Err(e) = worker.send(WorkerMessage::NewConnection(socket)) {
                    error!("failed to send message to worker thread! {:?}", e);
                }
            }
            Err(e) => {
                libsabledb::error_with_throttling!(300, "error accepting connection. {:?}", e);
            }
        }
    }
}
