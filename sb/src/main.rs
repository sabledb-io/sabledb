mod bench_utils;
mod redis_client;
mod response_validators;
mod sb_options;
mod stats;
mod tests;

use clap::Parser;

use num_format::{Locale, ToFormattedString};
use sb_options::Options;

/// Thread main function
async fn thread_main(opts: Options) -> Result<(), Box<dyn std::error::Error>> {
    let count = opts.thread_clients();
    let local = tokio::task::LocalSet::new();

    for _ in 0..count {
        // span task per connection
        let opts_clone = opts.clone();
        local.spawn_local(async move {
            if let Err(e) = client_main(opts_clone).await {
                tracing::error!("{:?} client error. {:?}", std::thread::current().id(), e);
            }
        });
    }

    // wait for the tasks to complete
    local.await;

    // remove this thread from the pool
    stats::decr_threads_running();
    Ok(())
}

/// Client main function
async fn client_main(mut opts: Options) -> Result<(), Box<dyn std::error::Error>> {
    const LIST_KEY_RANGE: usize = 1000;
    let mut tx = crate::redis_client::SBClient::connect(
        opts.host.clone(),
        opts.port as u16,
        opts.tls,
        opts.pipeline,
    )
    .await?;
    match opts.test.as_str() {
        "set" => tests::run_set(&mut tx, opts).await?,
        "get" => tests::run_get(&mut tx, opts).await?,
        "ping" => tests::run_ping(&mut tx, opts).await?,
        "incr" => tests::run_incr(&mut tx, opts).await?,
        "rpush" => {
            opts.key_range = LIST_KEY_RANGE;
            tests::run_push(&mut tx, true, opts).await?;
        }
        "rpop" => {
            opts.key_range = LIST_KEY_RANGE;
            tests::run_pop(&mut tx, true, opts).await?;
        }
        "lpush" => {
            opts.key_range = LIST_KEY_RANGE;
            tests::run_push(&mut tx, false, opts).await?;
        }
        "lpop" => {
            opts.key_range = LIST_KEY_RANGE;
            tests::run_pop(&mut tx, false, opts).await?;
        }
        "hset" => {
            opts.key_range = LIST_KEY_RANGE;
            tests::run_hset(&mut tx, opts).await?;
        }
        _ => {
            panic!("don't know how to run test: `{}`", opts.test);
        }
    }
    drop(tx);
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = Options::parse();
    args.finalise();

    // prepare log formatter
    let debug_level = args.log_level.unwrap_or(tracing::Level::INFO);
    tracing_subscriber::fmt::fmt()
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_max_level(debug_level)
        .init();

    // panic! should go to the log
    std::panic::set_hook(Box::new(|e| {
        let errmsg = format!("{}", e);
        let lines = errmsg.split('\n');
        for line in lines.into_iter() {
            tracing::error!("{}", line);
        }
    }));

    let mut handles = Vec::new();
    tracing::debug!("Total requests: {}", args.num_requests);
    tracing::debug!("Test: {}", args.test.to_uppercase());
    tracing::debug!("Threads: {}", args.threads);
    tracing::debug!("Requests per connection: {}", args.client_requests());
    tracing::debug!("Connections: {}", args.connections);
    tracing::debug!("Conn per thread: {}", args.thread_clients());
    tracing::debug!("Key space: {}", args.key_range);
    tracing::debug!("Key size: {}", args.key_size);
    tracing::debug!("Data size: {}", args.data_size);

    let total_requests = args.thread_clients() * args.client_requests() * args.threads;
    stats::set_tests_requests_count(total_requests);
    stats::finalise_progress_setup(total_requests as u64);

    // Launch the threads. In turn, each thread will launch a N clients each running
    // within a dedicated tokio's task
    for _ in 0..args.threads {
        let args_clone = args.clone();
        handles.push(
            std::thread::Builder::new()
                .name("Worker".to_string())
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .thread_name("Worker")
                        .build()
                        .unwrap_or_else(|e| {
                            panic!("failed to create tokio runtime. {:?}", e);
                        });

                    rt.block_on(async move {
                        thread_main(args_clone).await.unwrap();
                    });
                })?,
        );
        stats::incr_threads_running();
    }

    // wait for all threads to join
    let sw = libsabledb::stopwatch::StopWatch::default();
    for h in handles {
        let _ = h.join();
    }

    stats::finish_progress();

    // calculate the RPS
    let millis = (sw.elapsed_micros()? / 1000) as f64; // duration in MS
    let total_requests = stats::requests_processed() as f64;
    let hits = stats::total_hits() as f64;

    let requests_per_ms = total_requests / millis;
    let requests_per_secs = (requests_per_ms * 1000.0) as usize;
    //let requests_per_ms: usize = requests_per_ms as usize;
    println!(
        "    RPS: {} / {}ms => {} requests / seconds",
        total_requests,
        millis / 1000.0,
        requests_per_secs.to_formatted_string(&Locale::en)
    );
    println!("    Hit rate: {}%", hits / total_requests * 100.0);
    stats::print_latency();
    Ok(())
}
