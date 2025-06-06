use crate::{
    impl_builder_with_fn, replication::ServerRole, CommandLineArgs, SableError, SlotBitmap,
};
use divide_range::RangeDivisions;
use std::cell::RefCell;
use std::ops::Range;
use std::process::Command;
use std::rc::Rc;
use std::str::FromStr;

#[cfg(debug_assertions)]
const TARGET_CONFIG: &str = "debug";

#[cfg(not(debug_assertions))]
const TARGET_CONFIG: &str = "release";

#[derive(Default)]
pub struct Instance {
    /// Command line arguments
    pub args: CommandLineArgs,
    /// Handle to SableDB process
    pub hproc: Option<std::process::Child>,
    /// SableDB working directory
    pub working_dir: String,
}

impl std::fmt::Debug for Instance {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Instance")
            .field("id", &self.node_id())
            .field("working_dir", &self.working_dir)
            .field("role", &self.role())
            .field("public_address", &self.address())
            .field("private_address", &self.private_address())
            .finish()
    }
}

impl Instance {
    pub fn info_property(&self, name: &str) -> Result<String, SableError> {
        let mut conn = self.connect_with_retries()?;
        let info: redis::InfoDict = redis::cmd("INFO").query(&mut conn)?;
        Ok(info.get(name).unwrap_or_default())
    }

    pub fn with_working_dir(mut self, working_dir: &str) -> Self {
        self.working_dir = working_dir.into();
        self
    }

    pub fn with_args(mut self, args: CommandLineArgs) -> Self {
        self.args = args;
        self
    }

    /// Return the role of the instance based on the INFO output
    pub fn role(&self) -> Result<ServerRole, SableError> {
        let role = self.info_property("role")?;
        ServerRole::from_str(&role)
    }

    /// Return client facing address
    pub fn address(&self) -> String {
        self.args.public_address.as_deref().unwrap().to_string()
    }

    /// Return internal address (visible by other instances)
    pub fn private_address(&self) -> String {
        self.args.private_address.as_deref().unwrap().to_string()
    }

    /// Connect to the instance
    ///
    /// Returns the connection
    pub fn connect(&self) -> Result<redis::Connection, SableError> {
        let connect_string = format!("redis://{}", self.address());
        let client = redis::Client::open(connect_string.as_str())?;
        let conn = client.get_connection()?;
        Ok(conn)
    }

    /// Connect to SableDB with retries (up to 20 retries)
    ///
    /// Returns the connection
    pub fn connect_with_retries(&self) -> Result<redis::Connection, SableError> {
        let connect_string = format!("redis://{}", self.address());
        let client = redis::Client::open(connect_string.as_str())?;
        let mut retries = 20usize;
        while retries > 0 {
            if let Ok(conn) = client.get_connection() {
                return Ok(conn);
            };
            std::thread::sleep(std::time::Duration::from_millis(100));
            retries = retries.saturating_sub(1);
        }

        Err(SableError::OtherError(format!(
            "Could not connect to SableDB@{}",
            self.address()
        )))
    }

    /// Terminate the current instance
    pub fn terminate(&mut self) {
        if let Some(hproc) = &mut self.hproc {
            println!("Terminating process {}", hproc.id());
            let _ = hproc.kill();
            // wait for the process to exit
            let _ = hproc.wait();
            self.hproc = None;
        }
    }

    /// Launch SableDB (using the same attributes as this instance was created with)
    pub fn run(&mut self) -> Result<(), SableError> {
        self.hproc = Some(run_instance(&self.working_dir, &self.args, false)?);
        Ok(())
    }

    /// Launch SableDB
    pub fn build(mut self) -> Result<Self, SableError> {
        self.hproc = Some(run_instance(&self.working_dir, &self.args, true)?);
        Ok(self)
    }

    /// Return true if this instance of SableDB is running
    pub fn is_running(&self) -> bool {
        self.hproc.is_some()
    }

    /// Return the process ID of this instance of SableDB
    pub fn pid(&self) -> Option<u32> {
        self.hproc.as_ref().map(|hproc| hproc.id())
    }

    pub fn node_id(&self) -> String {
        self.info_property("node_id").unwrap_or_default()
    }

    pub fn primary_node_id(&self) -> String {
        self.info_property("primary_node_id").unwrap_or_default()
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        let keep_dir = if let Ok(val) = std::env::var("SABLEDB_KEEP_GARBAGE") {
            val.eq("1")
        } else {
            false
        };

        self.terminate();
        if !keep_dir {
            let _ = std::fs::remove_dir_all(&self.working_dir);
        }
    }
}

type InstanceRefCell = Rc<RefCell<Instance>>;

#[allow(dead_code)]
#[derive(Default, Clone)]
/// Represents a collection of `Instances` that forms a a "Shard"
/// In a Shard we have:
/// - 1 cluster database
/// - 1 primary instance
/// - N replicas
pub struct Shard {
    instances: Vec<InstanceRefCell>,
    primary: Option<InstanceRefCell>,
    replicas: Option<Vec<InstanceRefCell>>,
    name: String,
    slots: String,
    cluster_db_address: String,
}

/// Needed for the macro below
type InstanceRefCellVec = Vec<InstanceRefCell>;

#[derive(Default)]
pub struct ShardBuilder {
    instances: Vec<InstanceRefCell>,
    name: String,
    slots: String,
    cluster_db_address: String,
}

impl ShardBuilder {
    impl_builder_with_fn!(instances, InstanceRefCellVec);
    impl_builder_with_fn!(name, String);
    impl_builder_with_fn!(slots, String);
    impl_builder_with_fn!(cluster_db_address, String);

    pub fn build(self) -> Shard {
        Shard {
            instances: self.instances,
            slots: self.slots,
            name: self.name,
            cluster_db_address: self.cluster_db_address,
            ..Default::default()
        }
    }
}

impl std::fmt::Debug for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut ds = f.debug_struct("Shard");
        ds.field("name", &self.name)
            .field("slots", &self.slots)
            .field("cluster_db", &self.cluster_db_address)
            .field("stabilised", &self.is_stabilised());
        let mut counter = 0usize;
        for inst in &self.instances {
            ds.field(&format!("instance-{}", counter), &inst.borrow());
            counter = counter.saturating_add(1);
        }
        ds.finish()
    }
}

impl Shard {
    /// Run shard wide INFO command and populate the replicas / primary
    /// lists. Return true on success (i.e. exactly 1 primary was found and 1+ replicas)
    /// false otherwise
    fn try_discover(&mut self) -> Result<bool, SableError> {
        let mut primary = Vec::<InstanceRefCell>::new();
        let mut replicas = Vec::<InstanceRefCell>::new();

        for inst in &self.instances {
            match inst.borrow().role()? {
                ServerRole::Primary => primary.push(inst.clone()),
                ServerRole::Replica => {
                    if inst.borrow().primary_node_id().is_empty() {
                        primary.push(inst.clone());
                    } else {
                        replicas.push(inst.clone());
                    }
                }
            }
        }

        if primary.len() == 1 && !replicas.is_empty() {
            let primary = primary
                .first()
                .ok_or(SableError::InternalError("No primary?".into()))?
                .clone();

            // Make sure that the primary ID is the correct one for every replica
            let expected_primary_id = primary.borrow().node_id();
            for repl in &replicas {
                if repl.borrow().primary_node_id().ne(&expected_primary_id) {
                    return Ok(false);
                }
            }

            self.primary = Some(primary);
            self.replicas = Some(replicas);
            Ok(true)
        } else {
            self.replicas = None;
            self.primary = None;
            Ok(false)
        }
    }

    /// Return true if the cluster is stabilised (we have 1 primary and N replicas)
    pub fn is_stabilised(&self) -> bool {
        let Some(replicas) = &self.replicas else {
            return false;
        };

        if self.primary.is_none() {
            return false;
        };
        replicas.len() == self.instances.len().saturating_sub(1)
    }

    /// Return the shard's replicas
    pub fn replicas(&self) -> Result<Vec<InstanceRefCell>, SableError> {
        let Some(replicas) = &self.replicas else {
            return Err(SableError::InvalidState("Shard is in invalid state".into()));
        };
        Ok(replicas.clone())
    }

    /// Return the shard's primary
    pub fn primary(&self) -> Result<InstanceRefCell, SableError> {
        let Some(primary) = &self.primary else {
            return Err(SableError::InvalidState("Shard is in invalid state".into()));
        };
        Ok(primary.clone())
    }

    pub fn instances(&self) -> Vec<InstanceRefCell> {
        self.instances.clone()
    }

    /// Create the replication group by connecting the replicas to the primary
    fn create_replication_group(&mut self) -> Result<(), SableError> {
        let mut primary_address = Vec::<String>::new();
        let mut index = 0usize;
        for inst in &self.instances {
            if index == 0 {
                println!(
                    "Node {} (Private:{}) : REPLICAOF NO ONE",
                    inst.borrow().address(),
                    inst.borrow().private_address()
                );
                primary_address = inst
                    .borrow()
                    .private_address()
                    .split(':')
                    .map(|s| s.to_string())
                    .collect();
            } else {
                let command = format!("REPLICAOF {} {}", primary_address[0], primary_address[1]);
                println!("Node {}: {}", inst.borrow().address(), command);
                let mut conn = inst.borrow_mut().connect_with_retries()?;
                let mut cmd = redis::cmd("REPLICAOF");
                for arg in &primary_address {
                    cmd.arg(arg);
                }
                let result = cmd.query(&mut conn)?;
                match result {
                    redis::Value::Okay => {
                        println!("OK");
                    }
                    other => {
                        return Err(SableError::OtherError(format!(
                            "Failed to execute command '{}'. {:?}",
                            command, other
                        )));
                    }
                };
            }
            index = index.saturating_add(1);
        }
        Ok(())
    }

    /// Query the shard until we get a valid shard (1 primary + N replicas)
    pub fn wait_for_shard_to_stabilise(&mut self) -> Result<(), SableError> {
        loop {
            if self.try_discover()? {
                // after stabilization, let give it some time really stabilise
                std::thread::sleep(std::time::Duration::from_secs(3));
                break;
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        Ok(())
    }

    /// Terminate and remove the primary node from the cluster
    pub fn terminate_and_remove_primary(&mut self) -> Result<InstanceRefCell, SableError> {
        if !self.is_stabilised() {
            return Err(SableError::InvalidState(
                "Can not remove Primary from an unstabled shard".into(),
            ));
        }

        let Some(primary) = &self.primary else {
            return Err(SableError::NotFound);
        };

        let primary = primary.clone();
        let primary_id = primary.borrow().node_id();

        // Remove the primary from the "all instances" list
        let mut instances: Vec<InstanceRefCell> = self
            .instances
            .iter()
            .filter(|inst| inst.borrow().node_id().ne(&primary_id))
            .cloned()
            .collect();

        std::mem::swap(&mut instances, &mut self.instances);

        // terminate the primary and clear it from this shard
        primary.borrow_mut().terminate();
        self.primary = None;

        Ok(primary)
    }

    /// Re-add to the shard the primary that was terminated.
    /// `inst` - the terminated primary that was returned from a previous call to `terminate_and_remove_primary`
    pub fn add_terminated_primary(&mut self, inst: InstanceRefCell) {
        self.instances.push(inst);
    }
}

#[derive(Clone)]
pub struct Cluster {
    pub shards: Vec<Shard>,
    pub cluster_db_instance: InstanceRefCell,
}

impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("shards", &self.shards)
            .field("cluster_db", &self.cluster_db_instance.borrow())
            .finish()
    }
}

impl Cluster {
    /// Create and start a new cluster with `num_of_shards` shards, each shard with `shard_size` instances
    /// (`shard_size - 1` replicas). The slots are split between the shards evenly
    pub fn new(
        num_of_shards: usize,
        shard_size: usize,
        cluster_name: &str,
    ) -> Result<Self, SableError> {
        let cluster_db_instance = create_db_instance(cluster_name, SlotBitmap::new_all_set())?;
        let mut shards = Vec::<Shard>::with_capacity(num_of_shards);

        let range: Range<u16> = 0..16384; // 0-16384 (excluding)
        let it = range.divide_evenly_into(num_of_shards);
        let mut shard_counter = 0usize;
        for slot_range in it {
            let shard_slots = SlotBitmap::try_from(&slot_range)?;
            let shard = start_shard(
                cluster_db_instance.clone(),
                shard_size,
                cluster_name,
                format!("{}.shard-{}", cluster_name, shard_counter).as_str(),
                shard_slots,
            )?;
            shards.push(shard);
            shard_counter = shard_counter.saturating_add(1);
        }

        Ok(Cluster {
            shards,
            cluster_db_instance,
        })
    }

    /// Return a connection to shard at a given index
    pub fn connection_to_shard(&self, index: usize) -> Result<redis::Connection, SableError> {
        let shard = self
            .shards
            .get(index)
            .ok_or(SableError::IndexOutOfRange("Shard index invalid".into()))?;
        let primary = shard.primary()?;
        let primary = primary.borrow();
        primary.connect()
    }

    /// Return the primary node of shard at a given index
    pub fn shard_primary(&self, index: usize) -> Result<InstanceRefCell, SableError> {
        self.shards
            .get(index)
            .ok_or(SableError::IndexOutOfRange("Shard index invalid".into()))?
            .primary
            .clone()
            .ok_or(SableError::NotFound)
    }
}

fn create_sabledb_args(
    cluster_address: Option<String>,
    cluster_name: &str,
    shard_name: &str,
    public_port: u16,
    private_port: u16,
    slots: SlotBitmap,
) -> (String, CommandLineArgs) {
    let mut db_dir = std::env::temp_dir();
    db_dir.push("sabledb_tests");
    db_dir.push("instances");
    db_dir.push(format!("instance.{}", public_port));
    db_dir.push(format!("db.{}", public_port));

    let public_address = format!("127.0.0.1:{}", public_port);
    let private_address = format!("127.0.0.1:{}", private_port);

    let mut args = CommandLineArgs::default()
        .with_workers(2)
        .with_log_level("debug")
        .with_db_path(db_dir.to_str().unwrap())
        .with_public_address(public_address.as_str())
        .with_private_address(private_address.as_str())
        .with_log_dir("logs")
        .with_shard_name(shard_name)
        .with_slots(&slots)
        .with_cluster_name(cluster_name);

    if let Some(cluster_address) = &cluster_address {
        args = args.with_cluster_address(cluster_address);
    }

    db_dir.pop();
    let working_dir = db_dir.display().to_string();
    (working_dir, args)
}

/// Launch a SableDB instance and return a handle to its process
fn run_instance(
    working_dir: &String,
    args: &CommandLineArgs,
    clear_before: bool,
) -> Result<std::process::Child, SableError> {
    let Some(mut rootdir) = super::findup("LICENSE")? else {
        return Err(SableError::NotFound);
    };
    rootdir.push("target");
    rootdir.push(TARGET_CONFIG);
    rootdir.push("sabledb");

    // Remove any old instance of this folder
    if clear_before {
        let _ = std::fs::remove_dir_all(working_dir);
    }
    // Ensure that the folder exists
    let _ = std::fs::create_dir_all(working_dir);

    let shard_args_as_vec = args.to_vec();
    let shard_args: Vec<&str> = shard_args_as_vec.iter().map(|s| s.as_str()).collect();
    let proc = Command::new(rootdir.to_string_lossy().to_string())
        .args(&shard_args)
        .current_dir(working_dir)
        .spawn()?;

    Ok(proc)
}

fn pick_ports_for_instance() -> (u16, u16) {
    let public_port = portpicker::pick_unused_port().expect("no free ports!");
    let private_port = portpicker::pick_unused_port().expect("no free ports!");
    (public_port, private_port)
}

/// Create the cluster database instance
pub fn create_db_instance(
    cluster_name: &str,
    slots: SlotBitmap,
) -> Result<InstanceRefCell, SableError> {
    let (public_port, private_port) = pick_ports_for_instance();
    let (wd, cluster_db_args) = create_sabledb_args(
        None,
        "",
        format!("CLUSTER_DB.{}", cluster_name).as_str(),
        public_port,
        private_port,
        slots,
    );

    let cluster_inst = Instance::default()
        .with_args(cluster_db_args)
        .with_working_dir(&wd)
        .build()?;

    // Make sure that the host is reachable
    let _conn = cluster_inst.connect_with_retries()?;
    drop(_conn);
    Ok(Rc::new(RefCell::new(cluster_inst)))
}

/// Start a shard of count instances. `count` must be greater than `2` (1 primary 1 replica)
/// The shard will consist of:
/// - `count - 1` replicas
/// - 1 primary
pub fn start_shard(
    cluster_inst: InstanceRefCell,
    instance_count: usize,
    cluster_name: &str,
    name: &str,
    slots: SlotBitmap,
) -> Result<Shard, SableError> {
    if instance_count < 2 {
        return Err(SableError::InvalidArgument(
            "shard instance count must be greater or equal to 2".into(),
        ));
    }

    let mut instances = Vec::<InstanceRefCell>::new();
    let cluster_address = cluster_inst.borrow().address();
    for _ in 0..instance_count {
        let (public_port, private_port) = pick_ports_for_instance();
        let (wd, args) = create_sabledb_args(
            Some(cluster_address.clone()),
            cluster_name,
            name,
            public_port,
            private_port,
            slots.clone(),
        );
        let inst = Instance::default()
            .with_args(args)
            .with_working_dir(&wd)
            .build()?;

        // Make sure that the host is reachable
        let _conn = inst.connect_with_retries()?;
        drop(_conn);

        instances.push(Rc::new(RefCell::new(inst)));
    }

    let mut shard = ShardBuilder::default()
        .with_instances(instances)
        .with_name(name.into())
        .with_slots(slots.to_string())
        .with_cluster_db_address(cluster_address)
        .build();

    // Assign roles to each instance
    shard.create_replication_group()?;

    // Wait for the instances to connect each other
    shard.wait_for_shard_to_stabilise()?;
    Ok(shard)
}

#[cfg(test)]
mod test {
    use super::*;
    use redis::Commands;
    use std::collections::HashSet;

    #[test]
    #[serial_test::serial]
    #[ntest_timeout::timeout(300_000)] // 5 minutes
    fn test_shard_args_are_unique() {
        let public_port = portpicker::pick_unused_port().expect("no free ports!");
        let private_port = portpicker::pick_unused_port().expect("no free ports!");
        let args_vec = create_sabledb_args(
            None,
            "",
            "test_shard_args_are_unique",
            public_port,
            private_port,
            SlotBitmap::new_all_set(),
        )
        .1
        .to_vec();
        let args: HashSet<&String> = args_vec.iter().collect();

        println!("{:?}", args_vec);
        assert_eq!(args_vec.len(), args.len());
    }

    #[test]
    #[serial_test::serial]
    #[ntest_timeout::timeout(300_000)] // 5 minutes
    fn test_start_shard() {
        // Start shard of 1 primary 2 replicas
        let cluster_inst =
            create_db_instance("test_start_shard", SlotBitmap::new_all_set()).unwrap();
        let shard = start_shard(
            cluster_inst.clone(),
            3,
            "",
            "test_start_shard",
            SlotBitmap::new_all_set(),
        )
        .unwrap();

        let all_instances = shard.instances();
        assert_eq!(all_instances.len(), 3);

        // At this point, we should have a primary instance
        let primary = shard.primary().unwrap();

        let mut conn = primary.borrow().connect_with_retries().unwrap();
        let res: redis::Value = conn.set("hello", "world").unwrap();
        assert_eq!(res, redis::Value::Okay);

        let primary_node_id = primary.borrow().node_id();
        println!("Primary node ID: {}", primary_node_id);
        let replicas = shard.replicas().unwrap();
        for replica in &replicas {
            // refresh the instance information
            let mut conn = replica.borrow().connect_with_retries().unwrap();

            // Read-only replica -> we expect error here
            let res = conn
                .set::<&str, &str, redis::Value>("hello", "world")
                .unwrap_err();
            assert_eq!(res.kind(), redis::ErrorKind::ReadOnly);
            println!(
                "Replica {}: Primary node ID is: {}",
                replica.borrow().node_id(),
                replica.borrow().primary_node_id()
            );
            assert_eq!(replica.borrow().primary_node_id(), primary_node_id);
        }
    }

    #[test]
    #[serial_test::serial]
    #[ntest_timeout::timeout(300_000)] // 5 minutes
    fn test_restart() {
        let cluster_inst = create_db_instance("test_restart", SlotBitmap::new_all_set()).unwrap();
        let shard = start_shard(
            cluster_inst.clone(),
            2,
            "",
            "test_restart",
            SlotBitmap::new_all_set(),
        )
        .unwrap();
        assert!(shard.primary().unwrap().borrow().is_running());
        println!(
            "Server started with PID: {}",
            shard.primary().unwrap().borrow().pid().unwrap()
        );
        shard.primary().unwrap().borrow_mut().terminate();
        assert!(!shard.primary().unwrap().borrow().is_running());
        assert!(shard.primary().unwrap().borrow_mut().run().is_ok());

        println!(
            "Server started with PID: {}",
            shard.primary().unwrap().borrow().pid().unwrap()
        );
    }

    #[test]
    #[serial_test::serial]
    #[ntest_timeout::timeout(300_000)] // 5 minutes
    fn test_auto_failover() {
        // start a shard consisting of 1 primary, 2 replicas and 1 cluster database
        let inst_count = 3usize;
        let cluster_inst =
            create_db_instance("test_auto_failover", SlotBitmap::new_all_set()).unwrap();
        let mut shard = start_shard(
            cluster_inst.clone(),
            inst_count,
            "",
            "test_auto_failover",
            SlotBitmap::new_all_set(),
        )
        .unwrap();
        println!("Initial state: {:?}", shard);
        let primary_node_id = shard.primary().unwrap().borrow().node_id();
        assert!(!primary_node_id.is_empty());

        let replicas = shard.replicas().unwrap();
        for replica in &replicas {
            let replica_id = replica.borrow().node_id();
            let replica_primary_id = replica.borrow().primary_node_id();

            assert_eq!(replica_primary_id, primary_node_id);
            assert!(!replica_id.is_empty());
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // terminate the primary node
        let old_primary_id = shard.primary().unwrap().borrow().node_id();
        let old_primary = shard.terminate_and_remove_primary().unwrap();
        println!("Primary node {} terminated and removed", old_primary_id);
        println!(
            "Shard state after primary terminated and removed: {:?}",
            shard
        );
        assert_eq!(primary_node_id, old_primary_id);
        assert!(!shard.is_stabilised());

        assert!(shard.primary.is_none());
        assert_eq!(shard.instances.len(), inst_count - 1);
        assert!(!old_primary.borrow().is_running());

        // Wait for the shard to auto failover
        println!("Waiting for fail-over to take place... (this can take up to 30 seconds)");
        shard.wait_for_shard_to_stabilise().unwrap();
        println!("{:?}", shard);
        assert!(shard.is_stabilised());

        // Restart the terminated primary and re-add it to the shard
        println!("Restarting old primary...");
        assert!(old_primary.borrow_mut().run().is_ok());
        shard.add_terminated_primary(old_primary);
        println!("Waiting for old primary to switch role and join the shard...");
        shard.wait_for_shard_to_stabilise().unwrap();
        println!("{:?}", shard);
        println!("Success!");
    }

    #[test]
    #[serial_test::serial]
    #[ntest_timeout::timeout(300_000)] // 5 minutes
    fn test_cluster_moved() {
        let cluster = match Cluster::new(3, 2, "my-cluster") {
            Ok(cluster) => cluster,
            Err(e) => {
                panic!("Could not create cluster. {}", e);
            }
        };
        println!("{:#?}", cluster);

        assert_eq!(cluster.shards.len(), 3);
        for shard in &cluster.shards {
            assert!(shard.is_stabilised());
        }

        // slots: 0-5460
        let mut shard_1_conn = cluster.connection_to_shard(0).unwrap();

        // slots: 5461-10921
        let mut shard_2_conn = cluster.connection_to_shard(1).unwrap();

        // slots: 10922-16383
        let mut shard_3_conn = cluster.connection_to_shard(2).unwrap();

        let shard_3_primary_node = cluster.shard_primary(2).unwrap();
        let shard_3_primary_node_address = shard_3_primary_node.borrow().address();

        // put "a" (slot: 15495, which is located in "shard-3") in the wrong shard
        let res = shard_1_conn.set::<&str, &str, redis::Value>("a", "b");
        // We expect "MOVED" with the address of the "shard3" primary address
        assert!(res.is_err_and(|e| e.kind() == redis::ErrorKind::Moved
            && e.detail().unwrap().contains(&shard_3_primary_node_address)));

        // We expect "MOVED" with the address of the "shard3" primary address
        let res = shard_2_conn.set::<&str, &str, redis::Value>("a", "b");
        assert!(res.is_err_and(|e| e.kind() == redis::ErrorKind::Moved
            && e.detail().unwrap().contains(&shard_3_primary_node_address)));

        // We expect "OK"
        let res = shard_3_conn.set::<&str, &str, redis::Value>("a", "b");
        assert_eq!(res.unwrap(), redis::Value::Okay);

        let res = execute_command(&mut shard_1_conn, "cluster nodes");
        match res {
            redis::Value::Array(arr) => {
                assert_eq!(arr.len(), 6);
                for line in arr {
                    match line {
                        redis::Value::BulkString(line) => {
                            println!("{}", String::from_utf8_lossy(&line));
                        }
                        _ => {
                            panic!("Expected string");
                        }
                    }
                }
            }
            _ => {
                panic!("Expected array of size 9");
            }
        }
    }

    fn execute_command(connection: &mut redis::Connection, command: &str) -> redis::Value {
        let mut command = command.to_string();
        command.push_str("\r\n");
        let _ = connection.send_packed_command(command.as_bytes()).unwrap();
        connection.recv_response().unwrap()
    }

    fn get_node_id(connection: &mut redis::Connection) -> String {
        let res = execute_command(connection, "cluster myid");
        let redis::Value::BulkString(node_id) = res else {
            panic!("Expected for a node-id");
        };
        String::from_utf8_lossy(&node_id).to_string()
    }

    #[test]
    #[serial_test::serial]
    #[ntest_timeout::timeout(300_000)] // 5 minutes
    fn test_slot_migration() {
        let cluster = match Cluster::new(2, 2, "test_slot_migration") {
            Ok(cluster) => cluster,
            Err(e) => {
                panic!("Could not create cluster. {}", e);
            }
        };
        println!("{:#?}", cluster);

        assert_eq!(cluster.shards.len(), 2);
        for shard in &cluster.shards {
            assert!(shard.is_stabilised());
        }

        let mut shard_1_conn = cluster.connection_to_shard(0).unwrap();
        let mut shard_2_conn = cluster.connection_to_shard(1).unwrap();

        let target_node_id = get_node_id(&mut shard_1_conn);
        let source_node_id = get_node_id(&mut shard_2_conn);

        // put "a" (slot: 15495) - this one should succeed
        let res = shard_2_conn.set::<&str, &str, redis::Value>("a", "b");
        assert_eq!(res.unwrap(), redis::Value::Okay);

        // this one should fail with a MOVED error
        let res = shard_1_conn.set::<&str, &str, redis::Value>("a", "b");
        assert!(res.is_err_and(|e| e.kind() == redis::ErrorKind::Moved));

        println!(
            "Sending slot 15495 from: {} -> {}",
            source_node_id, target_node_id
        );

        let res = execute_command(
            &mut shard_2_conn,
            format!("slot sendto {} 15495", target_node_id).as_str(),
        );
        assert_eq!(res, redis::Value::Okay);

        // We now expect "MOVED" with the address of the "shard1" primary address
        let res = shard_2_conn.set::<&str, &str, redis::Value>("a", "b");
        println!("set response: {:?}", res);
        let shard_1_primary_node_address = cluster.shard_primary(0).unwrap().borrow().address();
        assert!(res.is_err_and(|e| e.kind() == redis::ErrorKind::Moved
            && e.detail().unwrap().contains(&shard_1_primary_node_address)));

        // This time, it should succeed
        let res = shard_1_conn.set::<&str, &str, redis::Value>("a", "c");
        assert_eq!(res.unwrap(), redis::Value::Okay);

        let res = shard_1_conn.get::<&str, redis::Value>("a");
        assert_eq!(res.unwrap(), redis::Value::BulkString([b'c'].to_vec()));
    }
}
