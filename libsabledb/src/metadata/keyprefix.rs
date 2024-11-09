use crate::{
    metadata::KeyType,
    utils::{U8ArrayBuilder, U8ArrayReader},
    FromU8Reader, ToU8Writer,
};

#[derive(Clone, Default, Debug, PartialEq, Eq)]
/// All *data* keys (e.g. hash, hash-field, list, list-item etc) in the database are starting with this prefix
/// With this prefix, it is possible to construct an iterator per slot or per db
pub struct KeyPrefix {
    /// The key type. For primary key, this will always be `0`
    /// This field must come first
    key_type: KeyType,
    /// Database ID
    db_id: u16,
    /// Keep the slot number as part of the key encoding
    /// with `<key-type><key_slot>` we can discover all keys belonged
    /// to a given slot by using a prefix iterator
    key_slot: u16,
}

impl KeyPrefix {
    pub fn set_key_type(&mut self, key_type: KeyType) {
        self.key_type = key_type;
    }
    pub fn key_type(&self) -> &KeyType {
        &self.key_type
    }
    pub fn set_db_id(&mut self, db_id: u16) {
        self.db_id = db_id;
    }
    pub fn db_id(&self) -> u16 {
        self.db_id
    }
    pub fn set_key_slot(&mut self, key_slot: u16) {
        self.key_slot = key_slot;
    }
    pub fn key_slot(&self) -> u16 {
        self.key_slot
    }
}

impl ToU8Writer for KeyPrefix {
    fn to_writer(&self, builder: &mut U8ArrayBuilder) {
        builder.write_key_type(self.key_type);
        self.db_id.to_writer(builder);
        self.key_slot.to_writer(builder);
    }
}

impl FromU8Reader for KeyPrefix {
    type Item = KeyPrefix;
    fn from_reader(reader: &mut U8ArrayReader) -> Option<Self::Item> {
        Some(KeyPrefix {
            key_type: KeyType::from_reader(reader)?,
            db_id: u16::from_reader(reader)?,
            key_slot: u16::from_reader(reader)?,
        })
    }
}
