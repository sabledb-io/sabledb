use crate::{replication::ServerRole, storage::StorageMetadata, Server};

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::RwLock;

thread_local! {
    pub static WORKER_TELEMETRY: RefCell<Telemetry> = RefCell::new(Telemetry::default());
}

lazy_static::lazy_static! {
    /// Replication info goes into a separate data structure
    static ref REPLICATION_INFO: RwLock<ReplicationTelemetry> = RwLock::<ReplicationTelemetry>::default();
    static ref STORAGE_MD: RwLock<StorageMetadata> = RwLock::<StorageMetadata>::default();
}

#[derive(Clone, Default, Debug)]
pub struct ReplicationTelemetry {
    pub last_change_sequence_number: u64,
    pub replica_telemetry: ReplicaTelemetry,
    pub primary_telemetry: PrimaryTelemetry,
}

#[derive(Clone, Default, Debug)]
pub struct ReplicaTelemetry {
    pub distance_from_primary: u64,
    pub last_change_sequence_number: u64,
}

impl std::fmt::Display for ReplicaTelemetry {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "distance_from_primary:{},last_change_sequence_number:{}",
            self.distance_from_primary, self.last_change_sequence_number
        )
    }
}

#[derive(Clone, Default, Debug)]
pub struct PrimaryTelemetry {
    pub replicas: HashMap<String, ReplicaTelemetry>,
}

impl ReplicationTelemetry {
    /// Replication: update the last change in the database
    pub fn set_last_change(seq_num: u64) {
        let mut d = REPLICATION_INFO.write().expect("poisoned mutex");
        d.last_change_sequence_number = seq_num;

        let role = Server::state().persistent_state().role();
        match role {
            ServerRole::Primary => {
                for info in &mut d.primary_telemetry.replicas.values_mut() {
                    info.distance_from_primary =
                        seq_num.saturating_sub(info.last_change_sequence_number);
                }
            }
            ServerRole::Replica => {}
        }
    }

    pub fn update_replica_info(replica_id: String, info: ReplicaTelemetry) {
        let mut replication_telemetry = REPLICATION_INFO.write().expect("poisoned mutex");
        if let Some(replica_data) = replication_telemetry
            .primary_telemetry
            .replicas
            .get_mut(&replica_id)
        {
            *replica_data = info.clone();
        } else {
            replication_telemetry
                .primary_telemetry
                .replicas
                .insert(replica_id, info);
        }
    }

    pub fn remove_replica(replica_id: &String) {
        let mut replication_telemetry = REPLICATION_INFO.write().expect("poisoned mutex");
        replication_telemetry
            .primary_telemetry
            .replicas
            .remove(replica_id);
    }

    /// Return the number of replicas connected to this instance
    pub fn connected_replicas() -> usize {
        REPLICATION_INFO
            .read()
            .expect("read lock error")
            .primary_telemetry
            .replicas
            .len()
    }
}

impl std::fmt::Display for ReplicationTelemetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut lines = Vec::<String>::new();

        let node_role = Server::state().persistent_state().role();
        lines.push("# Replication".to_string());
        lines.push(format!("role:{}", node_role));
        lines.push(format!(
            "node_id:{}",
            Server::state().persistent_state().id()
        ));
        lines.push(format!(
            "shard:{}",
            Server::state().persistent_state().shard_name()
        ));
        lines.push(format!(
            "slots:{}",
            Server::state().persistent_state().slots()
        ));
        lines.push(format!(
            "cluster_database:{}",
            Server::state()
                .options()
                .read()
                .expect("poisoned mutex")
                .general_settings
                .cluster_address
                .clone()
                .unwrap_or_default()
        ));
        lines.push(format!(
            "last_db_update_sequence_number:{}",
            self.last_change_sequence_number
        ));

        match node_role {
            ServerRole::Primary => {
                lines.push(format!(
                    "connected_replicas:{}",
                    self.primary_telemetry.replicas.len()
                ));

                for (replica_id, info) in &self.primary_telemetry.replicas {
                    lines.push(format!("replica:{},{}", replica_id, info));
                }
            }
            ServerRole::Replica => {
                lines.push(format!(
                    "primary_node_id:{}",
                    Server::state().persistent_state().primary_node_id()
                ));
            }
        }
        let as_str = lines.join("\n");
        write!(f, "{}", as_str)
    }
}

/// A struct representing telemetry collected by SableDB
///
/// Each worker holds its own telemetry object so no locking are taking place
/// while collection is done.
///
/// Every N seconds - where N is unique value per worker - a worker merges its
/// counters with the global counters - So expect delay (up to 3 seconds) when viewing statistics
#[derive(Clone, Default, Debug)]
pub struct Telemetry {
    /// Number of connections opened
    pub connections_opened: u128,
    /// Number of connections closed
    pub connections_closed: u128,
    /// Number of bytes read from the network
    pub net_bytes_read: u128,
    /// Number of bytes written to the network
    pub net_bytes_written: u128,
    /// Total number of database miss
    pub db_miss: u128,
    /// Total number of database hits
    pub db_hit: u128,
    /// Total number of commands processed
    pub total_commands_processed: u128,
    /// Total number of microseconds spent doing Disk IO
    pub total_io_duration: u128,
    /// Total number of IO write calls
    pub total_io_write_calls: u128,
    /// Total number of IO read calls
    pub total_io_read_calls: u128,
    /// Avg time spent, per call, doing IO
    pub avg_io_duration: u128,
    /// Contains information about replication (role, data sent etc)
    pub replication_info: ReplicationTelemetry,
}

impl Telemetry {
    /// Create a copy of the telemetry
    pub fn clone() -> Telemetry {
        WORKER_TELEMETRY.with(|telemetry| telemetry.borrow().clone())
    }

    /// Increase the number of commands processed by 1
    pub fn inc_total_commands_processed() {
        WORKER_TELEMETRY.with(|telemetry| {
            let new_val = telemetry
                .borrow()
                .total_commands_processed
                .saturating_add(1);
            telemetry.borrow_mut().total_commands_processed = new_val;
        });
    }

    /// Increase the number of IO writes
    pub fn inc_total_io_duration(duration_micros: u128) {
        WORKER_TELEMETRY.with(|telemetry| {
            let new_val = telemetry
                .borrow()
                .total_io_duration
                .saturating_add(duration_micros);
            telemetry.borrow_mut().total_io_duration = new_val;
        });
    }

    /// Increase the number of IO writes
    pub fn inc_total_io_write_calls() {
        WORKER_TELEMETRY.with(|telemetry| {
            let new_val = telemetry.borrow().total_io_write_calls.saturating_add(1);
            telemetry.borrow_mut().total_io_write_calls = new_val;
        });
    }

    /// Increase the number of IO writes
    pub fn inc_total_io_read_calls() {
        WORKER_TELEMETRY.with(|telemetry| {
            let new_val = telemetry.borrow().total_io_read_calls.saturating_add(1);
            telemetry.borrow_mut().total_io_read_calls = new_val;
        });
    }

    /// Increase the number of connections opened by 1
    pub fn inc_connections_opened() {
        WORKER_TELEMETRY.with(|telemetry| {
            let new_val = telemetry.borrow().connections_opened.saturating_add(1);
            telemetry.borrow_mut().connections_opened = new_val;
        });
    }

    /// Increase the number of connections closed by 1
    pub fn inc_connections_closed() {
        WORKER_TELEMETRY.with(|telemetry| {
            let new_val = telemetry.borrow().connections_closed.saturating_add(1);
            telemetry.borrow_mut().connections_closed = new_val;
        });
    }

    /// Increase the number of connections opened by 1
    pub fn inc_db_hit() {
        WORKER_TELEMETRY.with(|telemetry| {
            let new_val = telemetry.borrow().db_hit.saturating_add(1);
            telemetry.borrow_mut().db_hit = new_val;
        });
    }

    /// Increase the number of connections closed by 1
    pub fn inc_db_miss() {
        WORKER_TELEMETRY.with(|telemetry| {
            let new_val = telemetry.borrow().db_miss.saturating_add(1);
            telemetry.borrow_mut().db_miss = new_val;
        });
    }

    /// Increase the number of network bytes read by `count`
    pub fn inc_net_bytes_read(count: u128) {
        WORKER_TELEMETRY.with(|telemetry| {
            let new_val = telemetry.borrow().net_bytes_read.saturating_add(count);
            telemetry.borrow_mut().net_bytes_read = new_val;
        });
    }

    /// Increase the number of network bytes read by `count`
    pub fn inc_net_bytes_written(count: u128) {
        WORKER_TELEMETRY.with(|telemetry| {
            let new_val = telemetry.borrow().net_bytes_written.saturating_add(count);
            telemetry.borrow_mut().net_bytes_written = new_val;
        });
    }

    pub fn set_database_info(db_info: StorageMetadata) {
        let mut data = STORAGE_MD.write().expect("write lock error");
        *data = db_info;
    }

    pub fn db_key_count(db_id: u16) -> usize {
        let data = STORAGE_MD.read().expect("read lock error");
        data.db_keys(db_id)
    }

    /// Clear the telemetry object
    pub fn clear() {
        WORKER_TELEMETRY.with(|telemetry| {
            telemetry.borrow_mut().connections_closed = 0;
            telemetry.borrow_mut().connections_opened = 0;
            telemetry.borrow_mut().net_bytes_read = 0;
            telemetry.borrow_mut().net_bytes_written = 0;
            telemetry.borrow_mut().db_miss = 0;
            telemetry.borrow_mut().db_hit = 0;
            telemetry.borrow_mut().total_commands_processed = 0;
            telemetry.borrow_mut().total_io_read_calls = 0;
            telemetry.borrow_mut().total_io_write_calls = 0;
            telemetry.borrow_mut().total_io_duration = 0;
        });
    }

    /// merge `worker_telemetry` into `self`
    pub fn merge_worker_telemetry(&mut self, worker_telemetry: Telemetry) {
        self.connections_opened = self
            .connections_opened
            .saturating_add(worker_telemetry.connections_opened);
        self.connections_closed = self
            .connections_closed
            .saturating_add(worker_telemetry.connections_closed);
        self.net_bytes_written = self
            .net_bytes_written
            .saturating_add(worker_telemetry.net_bytes_written);
        self.net_bytes_read = self
            .net_bytes_read
            .saturating_add(worker_telemetry.net_bytes_read);
        self.db_miss = self.db_miss.saturating_add(worker_telemetry.db_miss);
        self.db_hit = self.db_hit.saturating_add(worker_telemetry.db_hit);
        self.total_commands_processed = self
            .total_commands_processed
            .saturating_add(worker_telemetry.total_commands_processed);
        self.total_io_write_calls = self
            .total_io_write_calls
            .saturating_add(worker_telemetry.total_io_write_calls);
        self.total_io_read_calls = self
            .total_io_read_calls
            .saturating_add(worker_telemetry.total_io_read_calls);
        self.total_io_duration = self
            .total_io_duration
            .saturating_add(worker_telemetry.total_io_duration);
    }
}

impl std::fmt::Display for Telemetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut total_connections = self.connections_opened;
        total_connections = total_connections.saturating_sub(self.connections_closed);
        let mut avg_io_per_command = 0f64;
        if self.total_commands_processed > 0 {
            avg_io_per_command =
                self.total_io_duration as f64 / self.total_commands_processed as f64;
        }
        let mut lines = Vec::<String>::new();

        lines.push("# Commands".to_string());
        lines.push(format!(
            "total_commands_processed:{}",
            self.total_commands_processed
        ));

        lines.push("\n# Network".to_string());
        lines.push(format!("total_connections:{}", total_connections));
        lines.push(format!("net_bytes_written:{}", self.net_bytes_written));
        lines.push(format!("net_bytes_read:{}", self.net_bytes_read));
        lines.push("\n# Disk I/O".to_string());
        lines.push(format!(
            "total_io_write_calls:{}",
            self.total_io_write_calls
        ));
        lines.push(format!("total_io_read_calls:{}", self.total_io_read_calls));
        lines.push(format!("total_io_duration:{}", self.total_io_duration));
        lines.push(format!("avg_io_per_command_micros:{}", avg_io_per_command));

        lines.push("\n# Statistics".to_string());
        lines.push(format!("db_miss:{}", self.db_miss));
        lines.push(format!("db_hit:{}", self.db_hit));

        lines.push("\n# Keyspace".to_string());
        lines.push(format!(
            "keys:{}",
            STORAGE_MD.read().expect("read lock").total_key_count()
        ));
        lines.push(format!(
            "databases:{}",
            STORAGE_MD.read().expect("read lock").db_count()
        ));

        lines.push("\n".to_string());
        let as_str = lines.join("\n");

        write!(
            f,
            "{}\n{}",
            as_str,
            REPLICATION_INFO.read().expect("poisoned mutex")
        )
    }
}
