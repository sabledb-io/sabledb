use crate::{CommandLineArgs, SableError};
use std::cell::RefCell;
use std::process::Command;
use std::rc::Rc;

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

impl Instance {
    pub fn with_working_dir(mut self, working_dir: &str) -> Self {
        self.working_dir = working_dir.into();
        self
    }

    pub fn with_args(mut self, args: CommandLineArgs) -> Self {
        self.args = args;
        self
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
            println!("Terminating SableDB: {}", hproc.id());
            let _ = hproc.kill();
            std::thread::sleep(std::time::Duration::from_secs(1));
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
}

#[derive(Default, Debug)]
/// Represents a collection of `Instances` that forms a a "Shard"
/// In a Shard we have:
/// - 1 cluster database
/// - 1 primary instance
/// - N replicas
pub struct Shard {
    instances: Vec<Rc<RefCell<Instance>>>,
}

impl Shard {
    pub fn with_instances(instances: Vec<Rc<RefCell<Instance>>>) -> Self {
        Shard { instances }
    }

    pub fn primary(&self) -> Rc<RefCell<Instance>> {
        self.instances.get(1).unwrap().clone()
    }

    pub fn replicas(&self) -> Vec<Rc<RefCell<Instance>>> {
        self.instances[2..].to_vec()
    }

    pub fn cluster_db_instance(&self) -> Rc<RefCell<Instance>> {
        self.instances.first().unwrap().clone()
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        let keep_dir = if let Ok(val) = std::env::var("SABLEDB_KEEP_GARBAGE") {
            val.eq("1")
        } else {
            false
        };

        self.instances.iter().for_each(|inst| {
            inst.borrow_mut().terminate();
            if !keep_dir {
                let d = &inst.borrow().working_dir;
                println!("Deleting directory: {}", d);
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

    println!("Running process: {}", rootdir.display());
    println!("Working directory: {}", working_dir);

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
    let mut instances = Vec::<Rc<RefCell<Instance>>>::new();

    let cluster_inst = Instance::default()
        .with_args(cluster_db_args)
        .with_working_dir(&wd)
        .build()?;

    let cluster_address = cluster_inst.private_address();

    // Make sure that the host is reachable
    let _conn = cluster_inst.connect_with_timeout()?;
    instances.push(Rc::new(RefCell::new(cluster_inst)));

    for _ in 0..instance_count {
        let (wd, args) = create_sabledb_args(Some(cluster_address.clone()));
        let inst = Instance::default()
            .with_args(args)
            .with_working_dir(&wd)
            .build()?;

        // Make sure that the host is reachable
        let _conn = inst.connect_with_timeout()?;
        instances.push(Rc::new(RefCell::new(inst)));
    }
    Ok(Shard::with_instances(instances))
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
        let shard = start_shard(3).unwrap();
        let mut conn = shard.primary().borrow().connect().unwrap();

        let res: redis::Value = conn.set("hello", "world").unwrap();
        assert_eq!(res, redis::Value::Okay);
        assert_eq!(shard.instances.len(), 4);
        println!("{:?}", shard);
        std::thread::sleep(std::time::Duration::from_secs(5));
    }

    #[test]
    #[serial_test::serial]
    fn test_restart() {
        let shard = start_shard(3).unwrap();
        assert!(shard.primary().borrow().is_running());
        println!(
            "Server started with PID: {}",
            shard.primary().borrow().pid().unwrap()
        );
        shard.primary().borrow_mut().terminate();
        assert!(!shard.primary().borrow().is_running());
        assert!(shard.primary().borrow_mut().run().is_ok());

        println!(
            "Server started with PID: {}",
            shard.primary().borrow().pid().unwrap()
        );
    }
}
