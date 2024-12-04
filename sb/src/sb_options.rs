use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[clap(disable_help_flag = true)]
pub struct Options {
    /// Print this help message and exit
    #[arg(long, action = clap::ArgAction::HelpLong)]
    help: Option<bool>,

    /// Total number of connections
    #[arg(short, long, default_value = "512")]
    pub connections: usize,

    /// Number of threads to use.
    /// Each thread will run `connections / threads` connections
    #[arg(long, default_value = "1")]
    pub threads: usize,

    /// Host address
    #[arg(short, long, default_value = "127.0.0.1")]
    pub host: String,

    /// Host port
    #[arg(short, long, default_value = "6379")]
    pub port: usize,

    /// test suits to run. Possible values are:
    /// `set`, `get`, `lpush`, `lpop`, `incr`, `rpop`, `rpush`, `ping`, `hset`, `setget`.
    /// Note when the test is `setget`, you can control the ratio by passing: `--setget-ratio`
    #[arg(short, long, default_value = "set")]
    pub test: String,

    /// Payload data size
    #[arg(short, long, default_value = "256")]
    pub data_size: usize,

    /// Key size
    #[arg(short, long, default_value = "10")]
    pub key_size: usize,

    /// Number of unique keys in the benchmark
    #[arg(short = 'r', long, default_value = "1000000")]
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

    /// use SSL (this is similar to passing --tls)
    #[arg(long, default_value = "false")]
    pub ssl: bool,

    /// Pipeline
    #[arg(long, default_value = "1")]
    pub pipeline: usize,

    /// The ratio between set:get when test is "setget"
    /// For example, passing "1:4" means: execute 1 set for every 4 get calls
    #[arg(long, default_value = "1:4")]
    pub setget_ratio: Option<String>,
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

    /// Return the number of connections ("tasks") per thread to start
    pub fn tasks_per_thread(&self) -> usize {
        self.connections.saturating_div(self.threads)
    }

    /// Number of requests per client
    pub fn client_requests(&self) -> usize {
        self.num_requests.saturating_div(self.connections)
    }

    /// Return true if should be using TLS connection
    pub fn tls_enabled(&self) -> bool {
        self.ssl || self.tls
    }

    /// If the test requested is "setget" return the ratio between
    /// the two: SET_COUNT:GET_COUNT, e.g. (1,4) -> perform 1 set for every 4 get calls
    pub fn get_setget_ratio(&self) -> Option<(f32, f32)> {
        if !self.test.eq("setget") {
            return None;
        }

        if let Some(ratio) = &self.setget_ratio {
            let parts: Vec<&str> = ratio.split(':').collect();
            let (Some(setcalls), Some(getcalls)) = (parts.first(), parts.get(1)) else {
                return Some((1.0, 4.0));
            };

            let setcalls = setcalls.parse::<f32>().unwrap_or(1.0);
            let getcalls = getcalls.parse::<f32>().unwrap_or(4.0);
            Some((setcalls, getcalls))
        } else {
            Some((1.0, 4.0))
        }
    }
}
