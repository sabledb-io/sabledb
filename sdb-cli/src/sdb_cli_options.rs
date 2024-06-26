use clap::Parser;

#[derive(Parser, Debug, Clone)]
pub struct Options {
    /// Host address
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Host port
    #[arg(short, long, default_value = "6379")]
    pub port: usize,

    /// use TLS
    #[arg(long, default_value = "false")]
    pub tls: bool,

    /// Read commands from file
    #[arg(long, short)]
    pub file: Option<String>,

    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub parameters: Vec<String>,
}
