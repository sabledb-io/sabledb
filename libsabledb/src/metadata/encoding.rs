use std::str::FromStr;

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
    Lock = 5,
}

impl Default for ValueType {
    fn default() -> Self {
        Self::Str
    }
}

impl crate::FromU8Reader for ValueType {
    type Item = ValueType;
    fn from_reader(reader: &mut crate::U8ArrayReader) -> Option<Self::Item> {
        Self::from_u8(reader.read_u8()?)
    }
}

impl crate::ToU8Writer for ValueType {
    fn to_writer(&self, builder: &mut crate::U8ArrayBuilder) {
        (*self as u8).to_writer(builder)
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
            5 => Some(Self::Lock),
            _ => None,
        }
    }
}

impl FromStr for ValueType {
    type Err = crate::SableError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "string" => Ok(Self::Str),
            "list" => Ok(Self::List),
            "hash" => Ok(Self::Hash),
            "set" => Ok(Self::Set),
            "zset" => Ok(Self::Zset),
            "lock" => Ok(Self::Lock),
            _ => Err(crate::SableError::InvalidArgument(format!(
                "Could not convert '{}' into ValueType",
                s
            ))),
        }
    }
}

impl From<&bytes::BytesMut> for ValueType {
    fn from(b: &bytes::BytesMut) -> Self {
        let s = String::from_utf8_lossy(b).to_string();
        ValueType::from_str(&s).unwrap_or_default()
    }
}

#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq, Copy, enum_iterator::Sequence)]
pub enum KeyType {
    /// Used internally to track all complex records
    Bookkeeping = 0,
    PrimaryKey = 1,
    ListItem = 2,
    HashItem = 3,
    ZsetMemberItem = 4,
    ZsetScoreItem = 5,
    SetItem = 6,
    Metadata = 7,
    /// Lock data type
    Lock = 8,
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

impl crate::ToU8Writer for KeyType {
    fn to_writer(&self, builder: &mut crate::U8ArrayBuilder) {
        (*self as u8).to_writer(builder)
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
            7 => Some(Self::Metadata),
            8 => Some(Self::Lock),
            _ => None,
        }
    }
}
