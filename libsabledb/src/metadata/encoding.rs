pub struct Encoding {}

/// Encoding represents the first byte (u8) used for every data type (and some values)
/// that we store in the database
impl Encoding {
    // All primary data types are encoded using `0u8` as their
    // first byte.
    pub const KEY_STRING: u8 = 0u8;
    pub const KEY_LIST: u8 = 0u8;
    pub const KEY_HASH: u8 = 0u8;

    // Encoding for values, each data type is encoded with its own unique value
    // again, the first byte
    pub const VALUE_STRING: u8 = 0u8;
    pub const VALUE_LIST: u8 = 1u8;
    pub const VALUE_HASH: u8 = 2u8;
    pub const VALUE_ZSET: u8 = 3u8;

    // Secondary data type keys encoding
    pub const KEY_LIST_ITEM: u8 = 1u8;
    pub const KEY_HASH_ITEM: u8 = 2u8;
    pub const KEY_ZSET_SCORE_ITEM: u8 = 3u8;
    pub const KEY_ZSET_MEMBER_ITEM: u8 = 4u8;
}
