pub trait FromRaw {
    fn from_u8(v: u8) -> Option<ValueType>
    where
        Self: Sized;
}

#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum ValueType {
    Str = 0,
    List = 1,
    Hash = 2,
    Zset = 3,
}

impl Default for ValueType {
    fn default() -> Self {
        Self::Str
    }
}

impl FromRaw for ValueType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Str),
            1 => Some(Self::List),
            2 => Some(Self::Hash),
            3 => Some(Self::Zset),
            _ => None,
        }
    }
}

pub struct Encoding {}

/// Encoding represents the first byte (u8) used for every data type (and some values)
/// that we store in the database
impl Encoding {
    // All primary data types are encoded using `0u8` as their
    // first byte.
    pub const KEY_STRING: u8 = 0u8;
    pub const KEY_LIST: u8 = 0u8;
    pub const KEY_HASH: u8 = 0u8;
    pub const KEY_ZSET: u8 = 0u8;

    // Secondary data type keys encoding
    pub const KEY_LIST_ITEM: u8 = 1u8;
    pub const KEY_HASH_ITEM: u8 = 2u8;
    pub const KEY_ZSET_SCORE_ITEM: u8 = 3u8;
    pub const KEY_ZSET_MEMBER_ITEM: u8 = 4u8;
}
