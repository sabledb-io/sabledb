pub trait FromRaw {
    type Item;
    fn from_u8(v: u8) -> Option<Self::Item>
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
    // Update this when adding new types
    Last = 4,
}

impl Default for ValueType {
    fn default() -> Self {
        Self::Str
    }
}

impl FromRaw for ValueType {
    type Item = ValueType;
    fn from_u8(v: u8) -> Option<Self::Item> {
        match v {
            0 => Some(Self::Str),
            1 => Some(Self::List),
            2 => Some(Self::Hash),
            3 => Some(Self::Zset),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum KeyType {
    /// Used internally to track all complex records
    Bookkeeping = 0,
    PrimaryKey = 1,
    ListItem = 2,
    HashItem = 3,
    ZsetMemberItem = 4,
    ZsetScoreItem = 5,
}

impl Default for KeyType {
    fn default() -> Self {
        Self::PrimaryKey
    }
}

impl FromRaw for KeyType {
    type Item = KeyType;
    fn from_u8(v: u8) -> Option<Self::Item> {
        match v {
            0 => Some(Self::Bookkeeping),
            1 => Some(Self::PrimaryKey),
            2 => Some(Self::ListItem),
            3 => Some(Self::HashItem),
            4 => Some(Self::ZsetMemberItem),
            5 => Some(Self::ZsetScoreItem),
            _ => None,
        }
    }
}
