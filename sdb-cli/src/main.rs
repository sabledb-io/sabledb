use redis::Value;
mod sdb_cli_options;
use bytes::BytesMut;
use clap::Parser;
use sdb_cli_options::Options;
use std::borrow::Cow::{self, Borrowed, Owned};
use std::fs;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

#[allow(unused_imports)]
use redis::Commands;
use rustyline::completion::FilenameCompleter;
use rustyline::highlight::{Highlighter, MatchingBracketHighlighter};
use rustyline::validate::MatchingBracketValidator;
use rustyline::{Completer, Helper, Hinter, Validator};
use rustyline::{Config, Editor};

lazy_static::lazy_static! {
    static ref INDENT_NEEDED: AtomicBool = AtomicBool::new(true);
}

const ESC: &str = "\x1b";
const RESET: &str = "\x1b[0m";
const GREY: &str = "[38;5;70m";

#[derive(Helper, Completer, Hinter, Validator)]
struct MyHelper {
    #[rustyline(Completer)]
    completer: FilenameCompleter,
    highlighter: MatchingBracketHighlighter,
    #[rustyline(Validator)]
    validator: MatchingBracketValidator,
    #[rustyline(Hinter)]
    hinter: (),
    colored_prompt: String,
}

impl Highlighter for MyHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        if default {
            Borrowed(&self.colored_prompt)
        } else {
            Borrowed(prompt)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Owned(ESC.to_owned() + GREY + hint + RESET)
    }

    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        self.highlighter.highlight(line, pos)
    }

    fn highlight_char(&self, line: &str, pos: usize) -> bool {
        self.highlighter.highlight_char(line, pos)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::fmt()
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Options::parse();

    let (connection_string, colored_prompt, base_prompt) = if args.tls {
        let base_prompt = format!("\u{1F5DD} {}:{} $ ", args.host, args.port);
        (
            format!("rediss://{}:{}/#insecure", args.host, args.port),
            format!("{ESC}{GREY}{}{RESET}", &base_prompt),
            base_prompt,
        )
    } else {
        let base_prompt = format!("{}:{} $ ", args.host, args.port);
        (
            format!("redis://{}:{}", args.host, args.port),
            format!("{ESC}{GREY}{}{RESET}", &base_prompt),
            base_prompt,
        )
    };

    tracing::debug!("Connecting to {}..", connection_string);
    let client = redis::Client::open(connection_string.as_str())?;
    let mut conn = client.get_connection()?;

    if let Some(input_file) = args.file {
        tracing::debug!("Loading file: {input_file}");
        // read the file's content. Each line in the file is considered as a command
        let file_content = fs::read_to_string(input_file)?;
        let commands: Vec<&str> = file_content.split('\n').collect();
        let commands: Vec<String> = commands
            .iter()
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().into())
            .collect();
        tracing::debug!("Running commands: {:#?}", commands);
        batch_execute(&mut conn, commands)?;
    } else if !args.parameters.is_empty() {
        // construct a command from the parameters and execute it
        let command = vec![args.parameters.join(" ")];
        batch_execute(&mut conn, command)?;
    } else {
        interactive_loop(colored_prompt, base_prompt, conn)?;
    }
    Ok(())
}

/// Run `sdb` in an interactive mode
fn interactive_loop(
    colored_prompt: String,
    base_prompt: String,
    mut conn: redis::Connection,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::builder().auto_add_history(true).build();
    let history =
        rustyline::sqlite_history::SQLiteHistory::open(config, ".sdb-cli-history.sqlite3")?;
    let mut rl: Editor<MyHelper, _> = Editor::with_history(config, history)?;

    let helper = MyHelper {
        completer: FilenameCompleter::new(),
        highlighter: MatchingBracketHighlighter::new(),
        hinter: (),
        colored_prompt: "".to_owned(),
        validator: MatchingBracketValidator::new(),
    };

    rl.set_helper(Some(helper));
    loop {
        rl.helper_mut().expect("No helper").colored_prompt.clone_from(&colored_prompt);
        let Ok(line) = rl.readline(&base_prompt) else {
            break;
        };
        batch_execute(&mut conn, vec![line])?;
    }
    Ok(())
}

/// Execute list of redis commands
fn batch_execute(
    conn: &mut redis::Connection,
    commands: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    for command in &commands {
        let command = command.trim();
        let mut line = BytesMut::from(command);
        line.extend_from_slice(b"\r\n");
        conn.send_packed_command(line.as_ref())?;
        match conn.recv_response() {
            Err(e) => tracing::error!("{:#?}", e),
            Ok(response) => {
                INDENT_NEEDED.store(true, Ordering::Relaxed);
                print_response_pretty(&response, 0, None);
            }
        }
    }
    Ok(())
}

fn print_response_pretty(value: &Value, indent: usize, seq: Option<usize>) {
    match value {
        Value::Nil => {
            print_sequence(seq);
            println_value("(nil)");
        }
        Value::Int(val) => {
            print_sequence(seq);
            println_value(format!("(integer) {}", val));
        }
        Value::Data(ref val) => {
            let s = String::from_utf8_lossy(val);
            print_sequence(seq);
            println_value(format!(r#"{}"#, s));
        }
        Value::Bulk(ref values) => {
            if values.is_empty() {
                print_indent(indent);
                print_sequence(seq);
                println_value("(empty array)");
            } else {
                print_sequence(seq);
                let mut new_seq = 1usize;
                for val in values.iter() {
                    print_indent(indent);
                    print_response_pretty(val, indent + 4, Some(new_seq));
                    new_seq += 1;
                }
            }
        }
        Value::Okay => {
            print_sequence(seq);
            println_value("OK");
        }
        Value::Status(ref s) => {
            print_sequence(seq);
            println_value(format!(r#""{}""#, s));
        }
    }
}

fn print_indent(indent: usize) {
    if !INDENT_NEEDED.load(Ordering::Relaxed) {
        return;
    }
    let indent = (0..indent).map(|_| " ").collect::<String>();
    print!("{}", indent);
    INDENT_NEEDED.store(false, Ordering::Relaxed);
}

fn print_sequence(seq: Option<usize>) {
    if let Some(seq) = seq {
        print!("{: >2}) ", seq);
    }
}

fn println_value(val: impl std::fmt::Display) {
    println!("{}", val);
    INDENT_NEEDED.store(true, Ordering::Relaxed);
}
