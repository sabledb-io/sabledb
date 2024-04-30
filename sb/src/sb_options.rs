use clap::Parser;

#[derive(Parser, Debug, Clone)]
pub struct Options {
    /// Total number of connections
    #[arg(short, long, default_value = "512")]
    pub connections: usize,

    /// Number of threads to use.
    /// Each thread will run `connections / threads` connections
    #[arg(long, default_value = "1")]
    pub threads: usize,

    /// Host address
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Host port
    #[arg(short, long, default_value = "6379")]
    pub port: usize,

    /// test suits to run. Possible values are:
    /// `set`, `get`, `lpush`, `lpop`, `incr`, `rpop`, `rpush`, `ping`, `hset`
    #[arg(short, long, default_value = "set")]
    pub test: String,

    /// Payload data size
    #[arg(short, long, default_value = "256")]
    pub data_size: usize,

    /// Key size
    #[arg(short, long, default_value = "10")]
    pub key_size: usize,

    /// Number of unique keys in the benchmark
    #[arg(long, default_value = "1000000")]
    pub key_range: usize,

    /// Total number of requests
    #[arg(short, long, default_value = "1000000")]
    pub num_requests: usize,

    /// Log level
    #[arg(short, long)]
    pub log_level: Option<tracing::Level>,

    /// use TLS
    #[arg(long, default_value = "false")]
    pub tls: bool,

    /// Pipeline size
    #[arg(long, default_value = "1")]
    pub pipeline: usize,
}

impl Options {
    /// Finalise the values provided by the user
    pub fn finalise(&mut self) {
        if self.threads == 0 {
            self.threads = 1;
        }

        if self.connections == 0 {
            self.connections = 1;
        }

        if self.num_requests == 0 {
            self.num_requests = 1000;
        }
    }

    /// Return the number of connections per thread to start
    pub fn thread_clients(&self) -> usize {
        self.connections.saturating_div(self.threads)
    }

    /// Number of requests per client
    #[allow(dead_code)]
    pub fn client_requests(&self) -> usize {
        self.num_requests.saturating_div(self.connections)
    }
}
