use crate::{
    ini_bool, ini_usize, parse_number, replication::ReplicationConfig, SableError,
    StorageOpenParams,
};
use ini::Ini;
use std::path::PathBuf;

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
            check_for_updates_interval_ms: 100,
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

    /// Load the replication configuration from disk
    pub fn load_replication_config(&self) -> ReplicationConfig {
        ReplicationConfig::load(self)
    }

    /// Read values from INI configuration file and return `ServerOptions` structure
    pub fn from_config(config_file: String) -> Result<Self, SableError> {
        let ini_file = Ini::load_from_file(config_file)?;
        let mut options = ServerOptions::default();
        // parse rocksdb section
        if let Some(properties) = ini_file.section(Some("rocksdb")) {
            for (key, value) in properties.iter() {
                match key {
                    "max_background_jobs" => {
                        options.open_params.rocksdb.max_background_jobs = ini_usize!(value)
                    }
                    "max_write_buffer_number" => {
                        options.open_params.rocksdb.max_write_buffer_number =
                            parse_number!(value, usize)
                    }
                    "write_buffer_size" => {
                        options.open_params.rocksdb.write_buffer_size = parse_number!(value, usize)
                    }
                    "wal_ttl_seconds" => {
                        options.open_params.rocksdb.wal_ttl_seconds = parse_number!(value, usize)
                    }
                    "compression_enabled" => {
                        options.open_params.rocksdb.compression_enabled = ini_bool!(value)
                    }
                    "disable_wal" => options.open_params.rocksdb.disable_wal = ini_bool!(value),
                    "manual_wal_flush" => {
                        options.open_params.rocksdb.manual_wal_flush = ini_bool!(value)
                    }
                    "manual_wal_flush_interval_ms" => {
                        options.open_params.rocksdb.manual_wal_flush_interval_ms =
                            parse_number!(value, usize)
                    }
                    "enable_pipelined_write" => {
                        options.open_params.rocksdb.enable_pipelined_write = ini_bool!(value)
                    }
                    "bloom_filter_bits_per_key" => {
                        options.open_params.rocksdb.bloom_filter_bits_per_key =
                            parse_number!(value, usize)
                    }
                    "max_open_files" => {
                        options.open_params.rocksdb.max_open_files = parse_number!(value, isize)
                    }
                    _ => {}
                }
            }
        }

        if let Some(properties) = ini_file.section(Some("general")) {
            for (key, value) in properties.iter() {
                match key {
                    "db_path" => options.open_params.db_path = PathBuf::from(value),
                    "config_dir" => {
                        options.general_settings.config_dir = Some(PathBuf::from(value))
                    }
                    "public_address" => options.general_settings.public_address = value.to_string(),
                    "private_address" => {
                        options.general_settings.private_address = value.to_string()
                    }
                    "cluster_address" => {
                        options.general_settings.cluster_address = Some(value.to_string())
                    }
                    "workers" => options.general_settings.workers = ini_usize!(value),
                    "log_level" => {
                        options.general_settings.log_level = match value.to_lowercase().as_str() {
                            "info" => tracing::Level::INFO,
                            "debug" => tracing::Level::DEBUG,
                            "trace" => tracing::Level::TRACE,
                            "warn" => tracing::Level::WARN,
                            "error" => tracing::Level::ERROR,
                            _ => {
                                tracing::warn!(
                                    "invalid log level `{}`. Using default `info`",
                                    value
                                );
                                tracing::Level::INFO
                            }
                        }
                    }
                    "logdir" => options.general_settings.logdir = Some(PathBuf::from(value)),
                    "cert" => options.general_settings.cert = Some(PathBuf::from(value)),
                    "key" => options.general_settings.key = Some(PathBuf::from(value)),
                    _ => {}
                }
            }
        }

        if let Some(properties) = ini_file.section(Some("replication_limits")) {
            for (key, value) in properties.iter() {
                match key {
                    "single_update_buffer_size" => {
                        options.replication_limits.single_update_buffer_size =
                            parse_number!(value, usize);
                    }
                    "num_updates_per_message" => {
                        options.replication_limits.num_updates_per_message =
                            parse_number!(value, usize);
                    }
                    "check_for_updates_interval_ms" => {
                        options.replication_limits.check_for_updates_interval_ms =
                            parse_number!(value, usize);
                    }
                    _ => {}
                }
            }
        }

        if let Some(properties) = ini_file.section(Some("client_limits")) {
            for (key, value) in properties.iter() {
                if key == "client_response_buffer_size" {
                    options.client_limits.client_response_buffer_size = parse_number!(value, usize);
                }
            }
        }

        if let Some(properties) = ini_file.section(Some("cron")) {
            for (key, value) in properties.iter() {
                match key {
                    "evict_orphan_records_secs" => {
                        options.cron.evict_orphan_records_secs = parse_number!(value, usize);
                    }
                    "instant_delete" => {
                        options.cron.instant_delete = ini_bool!(value);
                    }
                    "scan_keys_secs" => {
                        options.cron.scan_keys_secs = parse_number!(value, usize);
                    }
                    _ => {}
                }
            }
        }
        Ok(options)
    }
}
