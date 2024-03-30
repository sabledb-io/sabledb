use crate::{SableError, ServerState, StorageAdapter, Worker, WorkerContext};
#[cfg(not(target_os = "macos"))]
use affinity::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Return the number of cores available
pub fn cores_count() -> usize {
    #[cfg(not(target_os = "macos"))]
    return get_core_num();
    #[cfg(target_os = "macos")]
    return std::thread::available_parallelism().unwrap().get();
}

#[derive(Default)]
#[allow(dead_code)]
pub struct WorkerManager {
    server_state: Arc<ServerState>,
    workers: Vec<WorkerContext>,
    current_worker: AtomicUsize,
}

impl WorkerManager {
    /// Create new worker manager with `count` workers
    pub fn new(
        count: usize,
        store: StorageAdapter,
        server_state: Arc<ServerState>,
    ) -> Result<Self, SableError> {
        let mut workers = Vec::<WorkerContext>::with_capacity(count);
        for _ in 0..count {
            let worker_context = Worker::run(server_state.clone(), store.clone())?;
            workers.push(worker_context);
        }

        Ok(WorkerManager {
            server_state,
            workers,
            current_worker: AtomicUsize::new(0),
        })
    }

    /// Return a worker to handle the new connection
    pub fn pick(&self) -> &WorkerContext {
        // pick the worker to use (simple round robin)
        let worker = &self.workers[self.current_worker.load(Ordering::Relaxed)];
        self.current_worker.fetch_add(1, Ordering::Relaxed);
        if self.current_worker.load(Ordering::Relaxed) >= self.workers.len() {
            self.current_worker.store(0, Ordering::Relaxed);
        }
        worker
    }

    /// return the best workers count to use
    /// `count` - number of workers requested by the user
    /// if count is > 0, use it, otherwise, determine the
    /// workers count based on the cores count
    pub fn default_workers_count(requested_count: usize) -> usize {
        // determine the number of workers
        if requested_count > 0 {
            requested_count
        } else {
            // By default 1/2 of the threads of the machine
            let threads_count = cores_count().saturating_div(2);
            if threads_count > 0 {
                threads_count
            } else {
                1
            }
        }
    }
}
