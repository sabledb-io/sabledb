pub struct Strings {}

#[allow(dead_code)]
impl Strings {
    // Error codes

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
    pub const SERVER_CLOSED_CONNECTION: &'static str = "ERR: server closed the connection";
    pub const QUEUED: &'static str = "QUEUED";
    pub const EXEC_WITHOUT_MULTI: &'static str = "ERR EXEC without MULTI";
    pub const DISCARD_WITHOUT_MULTI: &'static str = "ERR DISCARD without MULTI";
    pub const WATCH_INSIDE_MULTI: &'static str = "ERR WATCH inside MULTI is not allowed";
    pub const ZERR_MIN_MAX_NOT_FLOAT: &'static str = "ERR min or max is not a float";
    pub const ZERR_VALUE_MUST_BE_POSITIVE: &'static str =
        "ERR value is out of range, must be positive";
    pub const ZERR_TIMEOUT_NOT_FLOAT: &'static str = "ERR timeout is not a float or out of range";
    pub const ERR_NUMKEYS: &'static str = "ERR Number of keys can't be greater than number of args";
    pub const NO_SUCH_KEY: &'static str = "ERR no such key";
    pub const ERR_NUM_KEYS_MUST_BE_GT_ZERO: &'static str = "ERR numkeys should be greater than 0";
    pub const ERR_EAGAIN: &'static str = "EAGAIN resource is not available, try again later";
    pub const ERR_DEADLOCK: &'static str = "DEADLOCK lock is already owned by the calling client";
    pub const ERR_NOT_OWNER: &'static str = "ERR resource is not owned by the calling client";

    // General strings
    pub const POISONED_MUTEX: &'static str = "poisoned mutex";
}
