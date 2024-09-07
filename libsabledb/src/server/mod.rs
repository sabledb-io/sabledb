mod client;
mod client_state;
mod cron_thread;
mod error_codes;
mod node_state;
#[allow(clippy::module_inception)]
mod server;
mod server_options;
mod telemetry;
mod watchers;
mod worker;
mod worker_manager;

pub type WorkerHandle = tokio::runtime::Handle;

pub use client::*;
pub use client_state::*;
pub use cron_thread::*;
pub use error_codes::*;
pub use node_state::*;
pub use server::*;
pub use server_options::*;
pub use telemetry::*;
pub use watchers::*;
pub use worker::*;
pub use worker_manager::*;

use std::sync::{Arc, RwLock};
pub type ServerOptionsRefMut = Arc<RwLock<ServerOptions>>;
