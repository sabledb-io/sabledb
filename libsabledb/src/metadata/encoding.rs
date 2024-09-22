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
    Set = 4,
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
            4 => Some(Self::Set),
            _ => None,
        }
    }
}

impl std::str::FromStr for ValueType {
    type Err = crate::SableError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "string" => Ok(Self::Str),
            "list" => Ok(Self::List),
            "hash" => Ok(Self::Hash),
            "set" => Ok(Self::Set),
            "zset" => Ok(Self::Zset),
            _ => Err(crate::SableError::InvalidArgument(format!(
                "Could not convert '{}' into ValueType",
                s
            ))),
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
    SetItem = 6,
}

impl Default for KeyType {
    fn default() -> Self {
        Self::PrimaryKey
    }
}

impl crate::FromU8Reader for KeyType {
    type Item = KeyType;
    fn from_reader(reader: &mut crate::U8ArrayReader) -> Option<Self::Item> {
        Self::from_u8(reader.read_u8()?)
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
            6 => Some(Self::SetItem),
            _ => None,
        }
    }
}
