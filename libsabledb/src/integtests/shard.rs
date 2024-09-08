use crate::{CommandLineArgs, SableError};
use std::process::Command;

#[cfg(debug_assertions)]
const TARGET_CONFIG: &str = "debug";

#[cfg(not(debug_assertions))]
const TARGET_CONFIG: &str = "release";

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct Instance {
    pub address: String,
    pub private_address: String,
    is_primary: bool,
    hproc: Option<std::process::Child>,
    working_dir: String,
}

#[derive(Debug, Default)]
pub struct Shard {
    instances: Vec<Instance>,
}

impl Shard {
    pub fn with_instances(instances: Vec<Instance>) -> Self {
        Shard { instances }
    }

    pub fn primary(&self) -> &Instance {
        &self.instances.get(1).unwrap()
    }

    pub fn cluster_db_instance(&self) -> &Instance {
        &self.instances.get(0).unwrap()
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        let keep_dir = if let Ok(val) = std::env::var("SABLEDB_KEEP_GARBAGE") {
            val.eq("1")
        } else {
            false
        };

        for inst in &mut self.instances {
            if let Some(hproc) = &mut inst.hproc {
                let _ = hproc.kill();
                if !keep_dir && !inst.working_dir.is_empty() {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    println!("Deleting directory: {}", &inst.working_dir);
                    let _ = std::fs::remove_dir_all(&inst.working_dir);
                } else {
                    println!("SableDB directory {} is kept", &inst.working_dir);
                }
            }
        }
    }
}

impl Instance {
    pub fn with_process(mut self, hproc: std::process::Child) -> Self {
        self.hproc = Some(hproc);
        self
    }

    pub fn with_working_dir(mut self, working_dir: &String) -> Self {
        self.working_dir = working_dir.clone();
        self
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
    let _ = std::fs::remove_dir_all(&working_dir);

    // Ensure that the folder exists
    let _ = std::fs::create_dir_all(&working_dir);

    let shard_args_as_vec = args.to_vec();
    let shard_args: Vec<&str> = shard_args_as_vec.iter().map(|s| s.as_str()).collect();
    let proc = Command::new(rootdir.to_string_lossy().to_string())
        .args(&shard_args)
        .current_dir(working_dir)
        .spawn()?;
    Ok(proc)
}

#[allow(dead_code)]
pub fn run_shard(count: usize) -> Result<Shard, SableError> {
    let (wd, cluster_db_args) = create_sabledb_args(None);
    let mut instances: Vec<Instance> = Vec::<Instance>::new();
    let inst = run_instance(&wd, &cluster_db_args)?;
    instances.push(Instance::default().with_process(inst).with_working_dir(&wd));

    for _ in 0..count {
        let (wd, args) = create_sabledb_args(cluster_db_args.private_address.clone());
        instances.push(
            Instance::default()
                .with_process(run_instance(&wd, &args)?)
                .with_working_dir(&wd),
        );
    }
    Ok(Shard::with_instances(instances))
}

pub fn start_and_test<F>(count: usize, mut test_func: F) -> Result<(), SableError>
where
    F: FnMut(Shard) -> Result<(), SableError>,
{
    // start the shard and run the test code
    let shard = run_shard(count)?;
    test_func(shard)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_shard_args_are_unique() {
        let args_vec = create_sabledb_args(None).1.to_vec();
        let args: HashSet<&String> = args_vec.iter().collect();

        println!("{:?}", args_vec);
        assert_eq!(args_vec.len(), args.len());
    }

    #[test]
    fn test_start_shard() {
        let shard = run_shard(3).unwrap();
        assert_eq!(shard.instances.len(), 4);
        println!("{:?}", shard);
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}
