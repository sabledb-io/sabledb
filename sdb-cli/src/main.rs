use redis::Value;
mod sdb_cli_options;
use clap::Parser;
use sdb_cli_options::Options;

use bytes::BytesMut;

#[allow(unused_imports)]
use redis::Commands;
use rustyline::{Config, Editor};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::fmt()
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Options::parse();
    let connection_string = if args.tls {
        format!("rediss://{}:{}/#insecure", args.host, args.port)
    } else {
        format!("redis://{}:{}/#insecure", args.host, args.port)
    };

    tracing::info!("Connecting to {}..", connection_string);
    let client = redis::Client::open(connection_string.as_str())?;
    let mut conn = client.get_connection()?;
    let config = Config::builder().auto_add_history(true).build();
    let history =
        rustyline::sqlite_history::SQLiteHistory::open(config, ".sdb-cli-history.sqlite3")?;
    let mut rl: Editor<(), _> = Editor::with_history(config, history)?;
    loop {
        let line = rl.readline("> ")?;
        let mut line = BytesMut::from(line.as_str());
        line.extend_from_slice(b"\r\n");
        conn.send_packed_command(line.as_ref())?;
        match conn.recv_response() {
            Err(e) => println!("{:#?}", e),
            Ok(response) => {
                print_response_pretty(&response, 0);
            }
        }
    }
}

fn print_response_pretty(value: &Value, indent: usize) {
    match value {
        Value::Nil => {
            if indent == 0 {
                print_with_indent("NIL", indent);
            } else {
                print_with_indent("NIL,", indent);
            }
        }
        Value::Int(val) => {
            if indent == 0 {
                print_with_indent(val, indent);
            } else {
                print_with_indent(format!("{},", val), indent);
            }
        }
        Value::Data(ref val) => {
            let s = String::from_utf8_lossy(val);
            if indent == 0 {
                print_with_indent(format!(r#""{}"#, s), indent);
            } else {
                print_with_indent(format!(r#""{}","#, s), indent);
            }
        }
        Value::Bulk(ref values) => {
            if values.is_empty() {
                print_with_indent(r#"[],"#, indent);
            } else {
                print_with_indent("[", indent);
                for val in values.iter() {
                    print_response_pretty(val, indent + 1);
                }
                print_with_indent("],", indent);
            }
        }
        Value::Okay => {
            if indent == 0 {
                print_with_indent("OK", indent);
            } else {
                print_with_indent(r#""OK","#, indent);
            }
        }
        Value::Status(ref s) => {
            if indent == 0 {
                print_with_indent(s, indent);
            } else {
                print_with_indent(format!(r#""{}","#, s), indent);
            }
        }
    }
}

fn print_with_indent(val: impl std::fmt::Display, depth: usize) {
    let indent = (0..depth).map(|_| " ").collect::<String>();
    println!("{}{}", indent, val);
}
