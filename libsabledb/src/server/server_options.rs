use crate::{ini_bool, ini_read_prop, ini_usize, parse_number, SableError, StorageOpenParams};
use clap::Parser;
use ini::Ini;
use std::path::PathBuf;
use std::str::FromStr;

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

    #[arg(short, long)]
    /// Path to the storage directory (you may also set to special devices, e.g. /deb/shm/sabledb.db)
    pub db_path: Option<String>,

    #[arg(long)]
    /// Log verbosity (can be one of: info, warn, error, trace, debug)
    pub log_level: Option<String>,

    #[arg(short, long)]
    /// Server workers count. set to 0 to let sabledb decide which defaults to `(number of CPUs / 2)`
    pub workers: Option<usize>,

    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub parameters: Vec<String>,
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

    /// Override values read from the configuration file from the
    pub fn apply_command_line_args(&mut self, cli_args: &CommandLineArgs) {
        if let Some(public_address) = &cli_args.public_address {
            self.general_settings.public_address = public_address.to_string();
        }

        if let Some(private_address) = &cli_args.private_address {
            self.general_settings.private_address = private_address.to_string();
        }

        if let Some(db_path) = &cli_args.db_path {
            self.open_params.db_path = PathBuf::from(db_path);
        }

        if let Some(log_level) = &cli_args.log_level {
            if let Ok(log_level) = tracing::Level::from_str(log_level) {
                self.general_settings.log_level = log_level;
            }
        }

        if let Some(workers) = &cli_args.workers {
            self.general_settings.workers = *workers;
        }
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
        let mut cli_args = CommandLineArgs::default();
        cli_args.db_path = Some("mydbpath".into());
        cli_args.workers = Some(42);
        cli_args.log_level = Some("warn".into());
        cli_args.public_address = Some("public:1234".into());
        cli_args.private_address = Some("private:1234".into());

        options.write().unwrap().apply_command_line_args(&cli_args);

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
