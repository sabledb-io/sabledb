use clap::Parser;
use libsabledb::{
    utils::IpPort, CommandLineArgs, SableError, Server, ServerOptions, Transport, WorkerManager,
    WorkerMessage,
};
use std::net::TcpListener;
use std::sync::{Arc, RwLock as StdRwLock};
use tracing::{debug, error, info};

const OPTIONS_LOCK_ERR: &str = "Failed to obtain read lock on ServerOptions";

fn main() -> Result<(), SableError> {
    // configure our tracing subscriber
    let args = CommandLineArgs::parse();
    let options = if let Some(config_file) = args.config_file() {
        Arc::new(StdRwLock::new(ServerOptions::from_config(config_file)?))
    } else {
        Arc::new(StdRwLock::<ServerOptions>::default())
    };

    args.apply(options.clone());

    // Override values passed to the command line
    let fmtr = tracing_subscriber::fmt::fmt()
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_max_level(
            options
                .read()
                .expect(OPTIONS_LOCK_ERR)
                .general_settings
                .log_level,
        );

    if let Some(logdir) = &options
        .read()
        .expect(OPTIONS_LOCK_ERR)
        .general_settings
        .logdir
    {
        if logdir.to_string_lossy().is_empty() {
            // Use stdout
            fmtr.init();
        } else {
            fmtr.with_writer(tracing_appender::rolling::hourly(logdir, "sabledb.log"))
                .with_ansi(false) // No need for colours when using file
                .init();
        }
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
    store.open(options.read().expect(OPTIONS_LOCK_ERR).open_params.clone())?;

    info!(
        "Server configuration:\n{:#?}",
        options.read().expect(OPTIONS_LOCK_ERR)
    );
    let workers_count = WorkerManager::default_workers_count(
        options
            .read()
            .expect(OPTIONS_LOCK_ERR)
            .general_settings
            .workers,
    );

    info!(
        "TLS enabled: {:?}",
        options.read().expect(OPTIONS_LOCK_ERR).use_tls()
    );

    // Allocate / load NodeID for this instance
    let address = options
        .read()
        .expect(OPTIONS_LOCK_ERR)
        .general_settings
        .public_address
        .clone();

    let server = Arc::new(Server::new(options.clone(), store.clone(), workers_count)?);
    info!("Successfully created {} workers", workers_count);

    // Bind the listener to the address
    let listener = TcpListener::bind(address.clone())
        .unwrap_or_else(|_| panic!("failed to bind address {}", address));
    info!("Server started on port address: {}", address);

    // load the persistent state of this server from the disk
    let server_state_clone = Server::state();
    server_state_clone
        .persistent_state()
        .initialise(options.clone());

    if let Some(shard_name) = args.shard_name() {
        server_state_clone
            .persistent_state()
            .set_shard_name(shard_name);
        server_state_clone.persistent_state().save();
    }

    info!(
        "NodeID is set to: {}",
        server_state_clone.persistent_state().id()
    );

    info!(
        "Node slots are: {}",
        server_state_clone.persistent_state().slots().to_string()
    );

    info!(
        "This node is member of shard: '{}'",
        server_state_clone.persistent_state().shard_name()
    );

    // Notify the replicator thread to start
    server_state_clone.notify_replicator_init_done_sync()?;

    // If this node is a replica, trigger a "REPLICAOF" command
    if server_state_clone.persistent_state().is_replica() {
        let addr: IpPort = server_state_clone
            .persistent_state()
            .primary_address()
            .parse()?;
        info!("Restoring server replica mode (Primary: {:?})", addr);
        Server::state().connect_to_primary_sync(addr.ip.clone(), addr.port)?;
    }

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
