use crate::{replication::ServerRole, CommandLineArgs, SableError};
use std::cell::RefCell;
use std::process::Command;
use std::rc::Rc;
use std::str::FromStr;

#[cfg(debug_assertions)]
const TARGET_CONFIG: &str = "debug";

#[cfg(not(debug_assertions))]
const TARGET_CONFIG: &str = "release";

#[derive(Default, Debug)]
pub struct Instance {
    /// Command line arguments
    pub args: CommandLineArgs,
    /// Handle to SableDB process
    hproc: Option<std::process::Child>,
    /// SableDB working directory
    working_dir: String,
}

impl std::fmt::Display for Instance {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let node_id = self.node_id();
        let primary_node_id = self.primary_node_id();
        let role = match self.role() {
            Ok(role) => format!("{}", role),
            Err(_) => String::from("Unknown"),
        };

        write!(f, "Role: {}, NodeID: {}", role, node_id)?;
        if let Ok(ServerRole::Replica) = self.role() {
            write!(f, ", Primary NodeID: {}", primary_node_id)?
        }
        Ok(())
    }
}

impl Instance {
    pub fn info_property(&self, name: &str) -> Result<String, SableError> {
        let mut conn = self.connect()?;
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

    /// Connect to the instance
    ///
    /// Returns the connection
    pub fn connect(&self) -> Result<redis::Connection, SableError> {
        let connect_string = format!("redis://{}", self.address());
        let client = redis::Client::open(connect_string.as_str())?;
        let conn = client.get_connection()?;
        Ok(conn)
    }

    pub fn address(&self) -> String {
        self.args.public_address.as_deref().unwrap().to_string()
    }

    pub fn private_address(&self) -> String {
        self.args.private_address.as_deref().unwrap().to_string()
    }

    /// Connect to SableDB with retries and timeout
    ///
    /// Returns the connection
    pub fn connect_with_timeout(&self) -> Result<redis::Connection, SableError> {
        let connect_string = format!("redis://{}", self.address());
        let client = redis::Client::open(connect_string.as_str())?;
        let mut retries = 10usize;
        while retries > 0 {
            if let Ok(conn) = client.get_connection() {
                return Ok(conn);
            };
            std::thread::sleep(std::time::Duration::from_millis(250));
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

type InstanceRefCell = Rc<RefCell<Instance>>;
#[allow(dead_code)]
#[derive(Default, Debug)]
/// Represents a collection of `Instances` that forms a a "Shard"
/// In a Shard we have:
/// - 1 cluster database
/// - 1 primary instance
/// - N replicas
pub struct Shard {
    cluster_db_instance: InstanceRefCell,
    instances: Vec<InstanceRefCell>,
    primary: Option<InstanceRefCell>,
    replicas: Option<Vec<InstanceRefCell>>,
}

impl std::fmt::Display for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "Shard {{")?;
        writeln!(f, "  Is stabilised: {}", self.is_stabilised())?;
        writeln!(f, "  Instance count: {}", self.instances.len())?;
        for inst in &self.instances {
            writeln!(f, "  {}", inst.borrow())?;
        }
        writeln!(f, "}}")
    }
}

impl Shard {
    pub fn with_instances(
        cluster_db_instance: InstanceRefCell,
        instances: Vec<InstanceRefCell>,
    ) -> Self {
        Shard {
            cluster_db_instance,
            instances,
            primary: None,
            replicas: None,
        }
    }

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

    pub fn cluster_db_instance(&self) -> Rc<RefCell<Instance>> {
        self.instances.first().unwrap().clone()
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
                let mut conn = inst.borrow_mut().connect()?;
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
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(250));
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

impl Drop for Shard {
    fn drop(&mut self) {
        let keep_dir = if let Ok(val) = std::env::var("SABLEDB_KEEP_GARBAGE") {
            val.eq("1")
        } else {
            false
        };

        let mut all_instances = self.instances.clone();
        all_instances.push(self.cluster_db_instance.clone());

        // Terminate the primary, replications and the cluster database instance
        all_instances.iter().for_each(|inst| {
            inst.borrow_mut().terminate();
            if !keep_dir {
                let d = &inst.borrow().working_dir;
                let _ = std::fs::remove_dir_all(d);
            }
        });
    }
}

fn create_sabledb_args(cluster_address: Option<String>) -> (String, CommandLineArgs) {
    let public_port = format!(
        "{}",
        portpicker::pick_unused_port().expect("No free ports!")
    );
    let private_port = format!(
        "{}",
        portpicker::pick_unused_port().expect("No free ports!")
    );
    let mut db_dir = std::env::temp_dir();
    db_dir.push("sabledb_tests");
    db_dir.push("instances");
    db_dir.push(format!("instance.{}", public_port));
    db_dir.push(format!("db.{}", public_port));

    let public_address = format!("127.0.0.1:{}", public_port);
    let private_address = format!("127.0.0.1:{}", private_port);

    let mut args = CommandLineArgs::default()
        .with_workers(2)
        .with_log_level("info")
        .with_db_path(db_dir.to_str().unwrap())
        .with_public_address(public_address.as_str())
        .with_private_address(private_address.as_str())
        .with_log_dir("logs");

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

#[allow(dead_code)]
/// Start a shard of count instances. `count` must be greater than `2` (1 primary 1 replica)
/// The shard will consist of:
/// - `count - 1` replicas
/// - 1 primary
/// - 1 cluster database instance
pub fn start_shard(instance_count: usize) -> Result<Shard, SableError> {
    if instance_count < 2 {
        return Err(SableError::InvalidArgument(
            "shard instance count must be greater or equal to 2".into(),
        ));
    }

    let (wd, cluster_db_args) = create_sabledb_args(None);
    let mut instances = Vec::<InstanceRefCell>::new();

    let cluster_inst = Instance::default()
        .with_args(cluster_db_args)
        .with_working_dir(&wd)
        .build()?;

    // We use the public address
    let cluster_address = cluster_inst.address();

    // Make sure that the host is reachable
    let _conn = cluster_inst.connect_with_timeout()?;
    drop(_conn);

    for _ in 0..instance_count {
        let (wd, args) = create_sabledb_args(Some(cluster_address.clone()));
        let inst = Instance::default()
            .with_args(args)
            .with_working_dir(&wd)
            .build()?;

        // Make sure that the host is reachable
        let _conn = inst.connect_with_timeout()?;
        drop(_conn);

        instances.push(Rc::new(RefCell::new(inst)));
    }
    let mut shard = Shard::with_instances(Rc::new(RefCell::new(cluster_inst)), instances);

    // Assign roles to each instance
    shard.create_replication_group()?;

    // Wait for the instances to connect each other
    shard.wait_for_shard_to_stabilise()?;
    Ok(shard)
}

/// Start shard and execute function `test_func`
pub fn start_and_test<F>(count: usize, mut test_func: F) -> Result<(), SableError>
where
    F: FnMut(Shard) -> Result<(), SableError>,
{
    // start the shard and run the test code
    let shard = start_shard(count)?;
    test_func(shard)
}

#[cfg(test)]
mod test {
    use super::*;
    use redis::Commands;
    use std::collections::HashSet;

    #[test]
    #[serial_test::serial]
    fn test_shard_args_are_unique() {
        let args_vec = create_sabledb_args(None).1.to_vec();
        let args: HashSet<&String> = args_vec.iter().collect();

        println!("{:?}", args_vec);
        assert_eq!(args_vec.len(), args.len());
    }

    #[test]
    #[serial_test::serial]
    fn test_start_shard() {
        // Start shard of 1 primary 2 replicas
        let shard = start_shard(3).unwrap();

        let all_instances = shard.instances();
        assert_eq!(all_instances.len(), 3);

        // At this point, we should have a primary instance
        let primary = shard.primary().unwrap();

        let mut conn = primary.borrow().connect().unwrap();
        let res: redis::Value = conn.set("hello", "world").unwrap();
        assert_eq!(res, redis::Value::Okay);

        let primary_node_id = primary.borrow().node_id();
        println!("Primary node ID: {}", primary_node_id);
        let replicas = shard.replicas().unwrap();
        for replica in &replicas {
            // refresh the instance information
            let mut conn = replica.borrow().connect().unwrap();

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
    fn test_restart() {
        let shard = start_shard(2).unwrap();
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
    fn test_auto_failover() {
        // start a shard consisting of 1 primary, 2 replicas and 1 cluster database
        let inst_count = 3usize;
        let mut shard = start_shard(inst_count).unwrap();
        println!("Initial state: {}", shard);
        let primary_node_id = shard.primary().unwrap().borrow().node_id();
        assert!(!primary_node_id.is_empty());

        let replicas = shard.replicas().unwrap();
        for replica in &replicas {
            let replica_id = replica.borrow().node_id();
            let replica_primary_id = replica.borrow().primary_node_id();

            assert_eq!(replica_primary_id, primary_node_id);
            assert!(!replica_id.is_empty());
        }

        // terminate the primary node
        let old_primary_id = shard.primary().unwrap().borrow().node_id();
        let old_primary = shard.terminate_and_remove_primary().unwrap();
        println!("Primary node {} terminated and removed", old_primary_id);
        println!(
            "Shard state after primary terminated and removed: {}",
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
        println!("{}", shard);
        assert!(shard.is_stabilised());

        // Restart the terminated primary and re-add it to the shard
        println!("Restarting old primary...");
        assert!(old_primary.borrow_mut().run().is_ok());
        shard.add_terminated_primary(old_primary);
        println!("Waiting for old primary to switch role and join the shard...");
        shard.wait_for_shard_to_stabilise().unwrap();
        println!("{}", shard);
        println!("Success!");
    }
}
