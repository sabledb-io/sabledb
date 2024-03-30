pub struct ErrorStrings {}

#[allow(dead_code)]
impl ErrorStrings {
    pub const VALUE_NOT_AN_INT_OR_OUT_OF_RANGE: &'static str =
        "ERR value is not an integer or out of range";
    pub const VALUE_NOT_VALID_FLOAT: &'static str = "ERR value is not a valid float";
    pub const LCS_FAILED_TO_READ_EXTRA_ARG: &'static str =
        "failed to read extra argument for command 'lcs'";
    pub const LCS_LEN_AND_IDX: &'static str =
        "If you want both the length and indexes, please just use IDX";
    pub const LCS_UNSUPPORTED_ARGS: &'static str = "ERR unsupported arguments for command 'lcs'";
    pub const SYNTAX_ERROR: &'static str = "ERR syntax error";
    pub const WRONGTYPE: &'static str =
        "WRONGTYPE Operation against a key holding the wrong kind of value";
    pub const OUT_OF_BOUNDS: &'static str = "ERR requested argument is out of bounds";
    pub const INDEX_OUT_OF_BOUNDS: &'static str = "index out of range";
    pub const LIST_RANK_INVALID: &'static str
        = "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list";
    pub const COUNT_CANT_BE_NEGATIVE: &'static str = "ERR COUNT can't be negative";
    pub const MAXLNE_CANT_BE_NEGATIVE: &'static str = "ERR MAXLEN can't be negative";
    pub const WRITE_CMD_AGAINST_REPLICA: &'static str =
        "READONLY You can't write against a read only replica.";
    pub const INVALID_PRIMARY_PORT: &'static str = "ERR Invalid master port";
}
