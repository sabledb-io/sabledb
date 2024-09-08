use crate::{ini_bool, ini_read_prop, ini_usize, parse_number, SableError, StorageOpenParams};
use clap::Parser;
use ini::Ini;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, RwLock as StdRwLock};

#[derive(Clone, Debug)]
pub struct GeneralSettings {
    /// Clients are connecting to this port
    pub public_address: String,
    /// Internal IP, usually used internally for communicating between SableDB nodes
    pub private_address: String,
    /// Cluster address used for managing the replication / cluster
    pub cluster_address: Option<String>,
    /// Server workers count. set to 0 to let SableDB decide
    pub workers: usize,
    /// Database log level
    pub log_level: tracing::Level,
    /// Path to the service certification path
    /// NOTE: when both `cert` and `key` are provided TLS is enabled
    pub cert: Option<PathBuf>,
    /// Path to the service key
    /// NOTE: when both `cert` and `key` are provided TLS is enabled
    pub key: Option<PathBuf>,
    /// Configuration files directory. Default: current process working directory
    pub config_dir: Option<PathBuf>,
    /// Log directory. If set to `None`, logs are written into `stdout`
    /// SableDB uses an hourly rotating logs
    pub logdir: Option<PathBuf>,
}

impl Default for GeneralSettings {
    fn default() -> Self {
        GeneralSettings {
            public_address: "127.0.0.1:6379".to_string(),
            workers: 0,
            log_level: tracing::Level::INFO,
            cert: None,
            key: None,
            config_dir: None,
            private_address: "127.0.0.1:7379".to_string(),
            logdir: None,
            cluster_address: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReplicationLimits {
    /// A single replication message update can not exceed this value (in bytes)
    pub single_update_buffer_size: usize,
    /// A single replication response can hold up to `num_updates_per_message`
    /// database updates
    pub num_updates_per_message: usize,
    /// If there are changes queued for replication, they are sent immediately.
    /// However, when there are no changes to send to the replica, the replication task
    /// suspend itself for `check_for_updates_interval_ms` milliseconds.
    pub check_for_updates_interval_ms: usize,
}

impl Default for ReplicationLimits {
    fn default() -> Self {
        ReplicationLimits {
            single_update_buffer_size: 50 << 20, // 50mb
            num_updates_per_message: 10_000,
            check_for_updates_interval_ms: 50,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientLimits {
    /// Build up to `response_buffer_size` bytes in memory before flushing
    /// to the network
    pub client_response_buffer_size: usize,
}

impl Default for ClientLimits {
    fn default() -> Self {
        ClientLimits {
            client_response_buffer_size: 1 << 20, // 1mb
        }
    }
}

#[derive(Clone, Debug)]
pub struct CronSettings {
    /// Purge orphan records every N seconds
    pub evict_orphan_records_secs: usize,
    /// Delete of a single item is always `O(1)` regardless of its type. If the type has children
    /// they are purged by the evictor background thread
    pub instant_delete: bool,
    /// Scan the database and collect statistics every `scan_keys_secs` seconds
    pub scan_keys_secs: usize,
}

impl Default for CronSettings {
    fn default() -> Self {
        CronSettings {
            evict_orphan_records_secs: 60, // 1 minute
            instant_delete: true,
            scan_keys_secs: 30,
        }
    }
}

/// Allow user to override configuration file parameters by passing them directly in the command line
#[derive(Parser, Debug, Clone, Default)]
pub struct CommandLineArgs {
    #[arg(long)]
    /// Public address used by clients
    pub public_address: Option<String>,

    #[arg(long)]
    /// Private address. Used internally for communicating between SableDB nodes
    pub private_address: Option<String>,

    #[arg(long)]
    /// Cluster DB address. If provided, SableDB enables advanced cluster / shard features such as "auto-failover"
    pub cluster_address: Option<String>,

    #[arg(short, long)]
    /// Path to the storage directory (you may also set to special devices, e.g. /deb/shm/sabledb.db)
    pub db_path: Option<String>,

    #[arg(long)]
    /// Log verbosity (can be one of: info, warn, error, trace, debug)
    pub log_level: Option<String>,

    #[arg(long)]
    /// Log directory
    pub logdir: Option<String>,

    #[arg(short, long)]
    /// Server workers count. set to 0 to let sabledb decide which defaults to `(number of CPUs / 2)`
    pub workers: Option<usize>,

    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub parameters: Vec<String>,
}

impl CommandLineArgs {
    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = Some(workers);
        self
    }

    pub fn with_public_address(mut self, public_address: &str) -> Self {
        self.public_address = Some(public_address.into());
        self
    }

    pub fn with_private_address(mut self, private_address: &str) -> Self {
        self.private_address = Some(private_address.into());
        self
    }

    pub fn with_cluster_address(mut self, cluster_address: &str) -> Self {
        self.cluster_address = Some(cluster_address.into());
        self
    }

    pub fn with_db_path(mut self, db_path: &str) -> Self {
        self.db_path = Some(db_path.into());
        self
    }

    pub fn with_log_level(mut self, log_level: &str) -> Self {
        self.log_level = Some(log_level.into());
        self
    }

    pub fn with_log_dir(mut self, logdir: &str) -> Self {
        self.logdir = Some(logdir.into());
        self
    }

    pub fn to_vec(&self) -> Vec<String> {
        let mut args = Vec::<String>::new();
        if let Some(public_address) = &self.public_address {
            args.push("--public-address".into());
            args.push(public_address.into());
        }

        if let Some(private_address) = &self.private_address {
            args.push("--private-address".into());
            args.push(private_address.into());
        }

        if let Some(cluster_address) = &self.cluster_address {
            args.push("--cluster-address".into());
            args.push(cluster_address.into());
        }

        if let Some(db_path) = &self.db_path {
            args.push("--db-path".into());
            args.push(db_path.into());
        }

        if let Some(log_level) = &self.log_level {
            args.push("--log-level".into());
            args.push(log_level.into());
        }

        if let Some(logdir) = &self.logdir {
            args.push("--logdir".into());
            args.push(logdir.into());
        }

        if let Some(workers) = &self.workers {
            args.push("--workers".into());
            args.push(format!("{}", workers));
        }
        args
    }

    /// Apply the command line arguments on the server options
    pub fn apply(&self, options: Arc<StdRwLock<ServerOptions>>) {
        if let Some(public_address) = &self.public_address {
            options
                .write()
                .expect("poisoned mutex")
                .general_settings
                .public_address = public_address.to_string();
        }

        if let Some(private_address) = &self.private_address {
            options
                .write()
                .expect("poisoned mutex")
                .general_settings
                .private_address = private_address.to_string();
        }

        if let Some(cluster_address) = &self.cluster_address {
            options
                .write()
                .expect("poisoned mutex")
                .general_settings
                .cluster_address = Some(cluster_address.to_string());
        }

        if let Some(db_path) = &self.db_path {
            options.write().expect("poisoned mutex").open_params.db_path = PathBuf::from(db_path);
        }

        if let Some(logdir) = &self.logdir {
            options
                .write()
                .expect("poisoned mutex")
                .general_settings
                .logdir = Some(PathBuf::from(logdir));
        }

        if let Some(log_level) = &self.log_level {
            if let Ok(log_level) = tracing::Level::from_str(log_level) {
                options
                    .write()
                    .expect("poisoned mutex")
                    .general_settings
                    .log_level = log_level;
            }
        }

        if let Some(workers) = &self.workers {
            options
                .write()
                .expect("poisoned mutex")
                .general_settings
                .workers = *workers;
        }
    }

    /// Return the configuration file passed (if any)
    pub fn config_file(&self) -> Option<String> {
        self.parameters.first().cloned()
    }
}

#[derive(Default, Debug, Clone)]
pub struct ServerOptions {
    pub general_settings: GeneralSettings,
    pub open_params: StorageOpenParams,
    pub replication_limits: ReplicationLimits,
    pub client_limits: ClientLimits,
    pub cron: CronSettings,
}

impl ServerOptions {
    pub fn use_tls(&self) -> bool {
        self.general_settings.key.is_some() && self.general_settings.cert.is_some()
    }

    /// Read values from INI configuration file and return `ServerOptions` structure
    pub fn from_config(config_file: String) -> Result<Self, SableError> {
        let ini_file = Ini::load_from_file(config_file)?;
        let mut options = ServerOptions::default();

        // [rocksdb] section
        Self::read_usize(
            &ini_file,
            "rocksdb",
            "max_background_jobs",
            &mut options.open_params.rocksdb.max_background_jobs,
        )?;
        Self::read_usize_with_unit(
            &ini_file,
            "rocksdb",
            "max_write_buffer_number",
            &mut options.open_params.rocksdb.max_write_buffer_number,
        )?;
        Self::read_usize_with_unit(
            &ini_file,
            "rocksdb",
            "write_buffer_size",
            &mut options.open_params.rocksdb.write_buffer_size,
        )?;
        Self::read_usize_with_unit(
            &ini_file,
            "rocksdb",
            "wal_ttl_seconds",
            &mut options.open_params.rocksdb.wal_ttl_seconds,
        )?;
        Self::read_bool(
            &ini_file,
            "rocksdb",
            "compression_enabled",
            &mut options.open_params.rocksdb.compression_enabled,
        )?;
        Self::read_bool(
            &ini_file,
            "rocksdb",
            "disable_wal",
            &mut options.open_params.rocksdb.disable_wal,
        )?;
        Self::read_bool(
            &ini_file,
            "rocksdb",
            "manual_wal_flush",
            &mut options.open_params.rocksdb.manual_wal_flush,
        )?;
        Self::read_bool(
            &ini_file,
            "rocksdb",
            "enable_pipelined_write",
            &mut options.open_params.rocksdb.enable_pipelined_write,
        )?;
        Self::read_usize_with_unit(
            &ini_file,
            "rocksdb",
            "manual_wal_flush_interval_ms",
            &mut options.open_params.rocksdb.manual_wal_flush_interval_ms,
        )?;

        Self::read_usize_with_unit(
            &ini_file,
            "rocksdb",
            "bloom_filter_bits_per_key",
            &mut options.open_params.rocksdb.bloom_filter_bits_per_key,
        )?;

        Self::read_isize_with_unit(
            &ini_file,
            "rocksdb",
            "max_open_files",
            &mut options.open_params.rocksdb.max_open_files,
        )?;

        // [general] section
        Self::read_path_buf(
            &ini_file,
            "general",
            "db_path",
            &mut options.open_params.db_path,
        )?;
        Self::read_path_buf_opt(
            &ini_file,
            "general",
            "config_dir",
            &mut options.general_settings.config_dir,
        )?;
        Self::read_string(
            &ini_file,
            "general",
            "public_address",
            &mut options.general_settings.public_address,
        )?;
        Self::read_string(
            &ini_file,
            "general",
            "private_address",
            &mut options.general_settings.private_address,
        )?;
        Self::read_string_opt(
            &ini_file,
            "general",
            "cluster_address",
            &mut options.general_settings.cluster_address,
        )?;
        Self::read_usize(
            &ini_file,
            "general",
            "workers",
            &mut options.general_settings.workers,
        )?;

        Self::read_log_level(
            &ini_file,
            "general",
            "log_level",
            &mut options.general_settings.log_level,
        )?;

        Self::read_path_buf_opt(
            &ini_file,
            "general",
            "logdir",
            &mut options.general_settings.logdir,
        )?;

        Self::read_path_buf_opt(
            &ini_file,
            "general",
            "cert",
            &mut options.general_settings.cert,
        )?;

        Self::read_path_buf_opt(
            &ini_file,
            "general",
            "key",
            &mut options.general_settings.key,
        )?;

        // [replication_limits]
        Self::read_usize_with_unit(
            &ini_file,
            "replication_limits",
            "single_update_buffer_size",
            &mut options.replication_limits.single_update_buffer_size,
        )?;

        Self::read_usize_with_unit(
            &ini_file,
            "replication_limits",
            "num_updates_per_message",
            &mut options.replication_limits.num_updates_per_message,
        )?;

        Self::read_usize_with_unit(
            &ini_file,
            "replication_limits",
            "check_for_updates_interval_ms",
            &mut options.replication_limits.check_for_updates_interval_ms,
        )?;

        // [client_limits]
        Self::read_usize_with_unit(
            &ini_file,
            "client_limits",
            "client_response_buffer_size",
            &mut options.client_limits.client_response_buffer_size,
        )?;

        // [cron]
        Self::read_usize_with_unit(
            &ini_file,
            "cron",
            "evict_orphan_records_secs",
            &mut options.cron.evict_orphan_records_secs,
        )?;

        Self::read_bool(
            &ini_file,
            "cron",
            "instant_delete",
            &mut options.cron.instant_delete,
        )?;

        Self::read_usize_with_unit(
            &ini_file,
            "cron",
            "scan_keys_secs",
            &mut options.cron.scan_keys_secs,
        )?;
        Ok(options)
    }

    /// =========-------------------------------------
    /// Helper methods
    /// =========-------------------------------------

    fn read_usize(
        ini_file: &Ini,
        section_name: &str,
        directive_name: &str,
        target: &mut usize,
    ) -> Result<(), SableError> {
        let val = ini_read_prop!(ini_file, section_name, directive_name);
        *target = ini_usize!(val);
        Ok(())
    }

    fn read_isize_with_unit(
        ini_file: &Ini,
        section_name: &str,
        directive_name: &str,
        target: &mut isize,
    ) -> Result<(), SableError> {
        let val = ini_read_prop!(ini_file, section_name, directive_name);
        *target = parse_number!(val, isize);
        Ok(())
    }

    fn read_usize_with_unit(
        ini_file: &Ini,
        section_name: &str,
        directive_name: &str,
        target: &mut usize,
    ) -> Result<(), SableError> {
        let val = ini_read_prop!(ini_file, section_name, directive_name);
        *target = parse_number!(val, usize);
        Ok(())
    }

    fn read_bool(
        ini_file: &Ini,
        section_name: &str,
        directive_name: &str,
        target: &mut bool,
    ) -> Result<(), SableError> {
        let val = ini_read_prop!(ini_file, section_name, directive_name);
        *target = ini_bool!(val);
        Ok(())
    }

    fn read_string(
        ini_file: &Ini,
        section_name: &str,
        directive_name: &str,
        target: &mut String,
    ) -> Result<(), SableError> {
        let val = ini_read_prop!(ini_file, section_name, directive_name);
        *target = val.to_string();
        Ok(())
    }

    fn read_string_opt(
        ini_file: &Ini,
        section_name: &str,
        directive_name: &str,
        target: &mut Option<String>,
    ) -> Result<(), SableError> {
        *target = None;
        let val = ini_read_prop!(ini_file, section_name, directive_name);
        *target = Some(val.to_string());
        Ok(())
    }

    fn read_path_buf_opt(
        ini_file: &Ini,
        section_name: &str,
        directive_name: &str,
        target: &mut Option<PathBuf>,
    ) -> Result<(), SableError> {
        *target = None;
        let val = ini_read_prop!(ini_file, section_name, directive_name);
        *target = Some(PathBuf::from(val));
        Ok(())
    }

    fn read_path_buf(
        ini_file: &Ini,
        section_name: &str,
        directive_name: &str,
        target: &mut PathBuf,
    ) -> Result<(), SableError> {
        let val = ini_read_prop!(ini_file, section_name, directive_name);
        *target = PathBuf::from(val);
        Ok(())
    }

    fn read_log_level(
        ini_file: &Ini,
        section_name: &str,
        directive_name: &str,
        target: &mut tracing::Level,
    ) -> Result<(), SableError> {
        *target = tracing::Level::INFO;
        let val = ini_read_prop!(ini_file, section_name, directive_name);
        let Ok(log_level) = tracing::Level::from_str(val) else {
            return Ok(());
        };
        *target = log_level;
        Ok(())
    }
}

//  _    _ _   _ _____ _______      _______ ______  _____ _______ _____ _   _  _____
// | |  | | \ | |_   _|__   __|    |__   __|  ____|/ ____|__   __|_   _| \ | |/ ____|
// | |  | |  \| | | |    | |    _     | |  | |__  | (___    | |    | | |  \| | |  __|
// | |  | | . ` | | |    | |   / \    | |  |  __|  \___ \   | |    | | | . ` | | |_ |
// | |__| | |\  |_| |_   | |   \_/    | |  | |____ ____) |  | |   _| |_| |\  | |__| |
//  \____/|_| \_|_____|  |_|          |_|  |______|_____/   |_|  |_____|_| \_|\_____|
//
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, RwLock as StdRwLock};

    #[test]
    fn test_override_values_from_cli() {
        let options = Arc::new(StdRwLock::<ServerOptions>::default());
        let cli_args = CommandLineArgs::default()
            .with_workers(42)
            .with_public_address("public:1234")
            .with_private_address("private:1234")
            .with_log_level("warn")
            .with_db_path("mydbpath");

        cli_args.apply(options.clone());

        let opts = options.read().unwrap();
        assert_eq!(opts.open_params.db_path, PathBuf::from("mydbpath"));
        assert_eq!(opts.general_settings.workers, 42);
        assert_eq!(opts.general_settings.log_level, tracing::Level::WARN);
        assert_eq!(
            opts.general_settings.public_address,
            "public:1234".to_string()
        );
        assert_eq!(
            opts.general_settings.private_address,
            "private:1234".to_string()
        );
    }
}
