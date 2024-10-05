use crate::{
    metadata::{Bookkeeping, ListValueMetadata, ValueType},
    storage::DbWriteCache,
    CommonValueMetadata, FromBytes, FromU8Reader, PrimaryKeyMetadata, SableError, StorageAdapter,
    ToBytes, ToU8Writer, U8ArrayBuilder, U8ArrayReader,
};
use bytes::BytesMut;

pub struct ListItemKey {
    pub list_id: u64,
    pub item_id: u64,
}

impl ListItemKey {
    pub fn new(list_id: u64, item_id: u64) -> Self {
        ListItemKey { list_id, item_id }
    }
}

#[derive(Default)]
pub struct ListItemValue {
    pub left: u64,
    pub right: u64,
    pub value: BytesMut,
}

impl ListItemValue {
    pub fn with_value(value: BytesMut) -> Self {
        ListItemValue {
            left: 0,
            right: 0,
            value,
        }
    }
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct List {
    pub key: PrimaryKeyMetadata,
    pub md: ListValueMetadata,
}

impl List {
    pub fn len(&self) -> usize {
        self.md.len().try_into().unwrap_or(usize::MAX)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn id(&self) -> u64 {
        self.md.id()
    }

    /// Return a the list key as bytes
    pub fn encode_key(&self) -> BytesMut {
        self.key.to_bytes()
    }

    /// Get the list value as bytes
    pub fn encode_value(&self) -> BytesMut {
        let mut buffer = BytesMut::with_capacity(ListValueMetadata::SIZE);
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        self.md.to_bytes(&mut builder);
        buffer
    }

    pub fn set_head(&mut self, item: &ListItem) {
        self.md.set_head(item.id());
    }

    pub fn set_tail(&mut self, item: &ListItem) {
        self.md.set_tail(item.id());
    }

    pub fn set_len(&mut self, newlen: u64) {
        self.md.set_len(newlen);
    }

    pub fn head(&self) -> u64 {
        self.md.head()
    }

    pub fn tail(&self) -> u64 {
        self.md.tail()
    }

    pub fn incr_len_by(&mut self, n: u64) {
        self.md.set_len(self.md.len().saturating_add(n))
    }
}

#[allow(dead_code)]
pub struct ListItem {
    pub key: ListItemKey,
    pub value: ListItemValue,
}

#[allow(dead_code)]
impl ListItem {
    pub fn new(key: ListItemKey, value: ListItemValue) -> Self {
        ListItem { key, value }
    }

    pub fn id(&self) -> u64 {
        self.key.item_id
    }

    pub fn encode_key(&self) -> BytesMut {
        self.key.to_bytes()
    }

    pub fn encode_value(&self) -> BytesMut {
        self.value.to_bytes()
    }

    /// Place this item between `left_item` and `right_item` (both can be optional)
    pub fn insert_between(
        &mut self,
        left_item: Option<&mut ListItem>,
        right_item: Option<&mut ListItem>,
    ) {
        if let Some(left_item) = left_item {
            self.value.right = left_item.value.right;
            left_item.value.right = self.id();
            self.value.left = left_item.id();
        }

        if let Some(right_item) = right_item {
            right_item.value.left = self.id();
            self.value.right = right_item.id();
        }
    }

    /// Make this item the first item.
    ///
    /// Arguments:
    ///
    /// `cur_first_item` the list currently first item
    pub fn insert_first(&mut self, cur_first_item: &mut ListItem) {
        self.insert_between(None, Some(cur_first_item));
    }

    /// Make this item to be the last item of the list
    ///
    /// Arguments:
    ///
    /// `cur_last_item` the list currently last item
    pub fn insert_last(&mut self, cur_last_item: &mut ListItem) {
        self.insert_between(Some(cur_last_item), None);
    }

    pub fn left(&self) -> u64 {
        self.value.left
    }

    pub fn right(&self) -> u64 {
        self.value.right
    }
}

impl ToU8Writer for ListItemValue {
    fn to_writer(&self, builder: &mut U8ArrayBuilder) {
        self.left.to_writer(builder);
        self.right.to_writer(builder);
        builder.write_bytes(&self.value);
    }
}

impl ToBytes for ListItemValue {
    fn to_bytes(&self) -> BytesMut {
        let mut buffer = BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        self.to_writer(&mut builder);
        buffer
    }
}

impl FromU8Reader for ListItemValue {
    type Item = ListItemValue;
    fn from_reader(reader: &mut U8ArrayReader) -> Option<Self::Item> {
        Some(ListItemValue {
            left: u64::from_reader(reader)?,
            right: u64::from_reader(reader)?,
            value: reader.remaining()?,
        })
    }
}

impl FromBytes for ListItemValue {
    type Item = ListItemValue;
    fn from_bytes(bytes: &[u8]) -> Option<Self::Item> {
        let mut reader = U8ArrayReader::with_buffer(bytes);
        Self::from_reader(&mut reader)
    }
}

impl ToU8Writer for ListItemKey {
    fn to_writer(&self, builder: &mut U8ArrayBuilder) {
        builder.write_key_type(crate::KeyType::ListItem);
        self.list_id.to_writer(builder);
        self.item_id.to_writer(builder);
    }
}

impl ToBytes for ListItemKey {
    fn to_bytes(&self) -> BytesMut {
        let mut buffer = BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        self.to_writer(&mut builder);
        buffer
    }
}

// Internal enumerator
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub enum GetListMetadataResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// A match was found
    Some(List),
    /// No entry exist
    NotFound,
}

#[derive(PartialEq, Eq, Debug)]
pub enum ListAppendResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Returns the list length
    Ok(usize),
}

#[derive(PartialEq, Eq, Debug)]
pub enum ListInsertResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Insert was successful
    Ok(usize),
}

#[derive(PartialEq, Eq, Debug)]
pub enum ListLenResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Returns the list length
    Ok(usize),
    /// List does not exist
    NotFound,
}

#[allow(dead_code)]
#[derive(PartialEq, Eq, Debug)]
enum InsertPosition {
    /// Append item
    Last,
    /// Prepend item
    First,
    /// Insert item after pivot
    After(BytesMut),
    /// Insert item before the pivot
    Before(BytesMut),
}

/// List DB wrapper. This class is specialized in reading/writing LIST
/// (commands from the `LPUSH`, `LLEN` etc family)
///
/// Locking strategy: this class does not lock anything and relies on the caller
/// to obtain the locks if needed
pub struct ListDb<'a> {
    store: &'a StorageAdapter,
    db_id: u16,
    cache: Box<DbWriteCache<'a>>,
}

#[allow(dead_code)]
impl<'a> ListDb<'a> {
    pub fn with_storage(store: &'a StorageAdapter, db_id: u16) -> Self {
        let cache = Box::new(DbWriteCache::with_storage(store));
        ListDb {
            store,
            db_id,
            cache,
        }
    }

    /// Append items to the end of the list
    pub fn append(
        &mut self,
        user_key: &BytesMut,
        values: &[&BytesMut],
    ) -> Result<ListAppendResult, SableError> {
        for value in values {
            self.insert(user_key, value, InsertPosition::Last)?;
        }
        Ok(ListAppendResult::Ok(0))
    }

    /// Return the list length
    pub fn len(&self, user_key: &BytesMut) -> Result<ListLenResult, SableError> {
        Ok(match self.list_metadata(user_key)? {
            GetListMetadataResult::Some(list) => ListLenResult::Ok(list.len()),
            GetListMetadataResult::WrongType => ListLenResult::WrongType,
            GetListMetadataResult::NotFound => ListLenResult::NotFound,
        })
    }

    /// Apply changes to disk
    pub fn commit(&mut self) -> Result<(), SableError> {
        self.cache.flush()
    }

    // Private helpers

    /// Insert item to the list at a given position
    fn insert(
        &mut self,
        user_key: &BytesMut,
        value: &BytesMut,
        pos: InsertPosition,
    ) -> Result<ListInsertResult, SableError> {
        let mut list = match self.list_metadata(user_key)? {
            GetListMetadataResult::WrongType => return Ok(ListInsertResult::WrongType),
            GetListMetadataResult::NotFound => self.new_list(user_key)?,
            GetListMetadataResult::Some(list) => list,
        };

        let new_item_key = ListItemKey::new(list.id(), self.store.generate_id());
        let new_item_value = ListItemValue::with_value(value.clone());
        let mut new_item = ListItem::new(new_item_key, new_item_value);

        self.put_list_item(&new_item)?;

        match pos {
            InsertPosition::Last => {
                self.push_back(&mut list, &mut new_item)?;
                Ok(ListInsertResult::Ok(list.len()))
            }
            InsertPosition::First => {
                self.push_front(&mut list, &mut new_item)?;
                Ok(ListInsertResult::Ok(list.len()))
            }
            InsertPosition::Before(_pivot) => Ok(ListInsertResult::WrongType),
            InsertPosition::After(_pivot) => Ok(ListInsertResult::WrongType),
        }
    }

    /// Push item at the start of the list
    fn push_front(&mut self, list: &mut List, new_item: &mut ListItem) -> Result<(), SableError> {
        if list.is_empty() {
            list.set_len(1);
            list.set_head(new_item);
            list.set_tail(new_item);
        } else {
            let old_head_id = list.head();
            let mut old_head_item = self
                .get_list_item_by_id(list, old_head_id)?
                .ok_or(SableError::NotFound)?;
            list.incr_len_by(1);
            // we are replacing the "head" of the list
            list.set_head(new_item);
            new_item.insert_first(&mut old_head_item);
            self.put_list_item(new_item)?;
            self.put_list_item(&old_head_item)?;
        }
        self.put_list_metadata(list)?;
        Ok(())
    }

    /// Push item at the back of the list
    fn push_back(&mut self, list: &mut List, new_item: &mut ListItem) -> Result<(), SableError> {
        if list.is_empty() {
            list.set_len(1);
            list.set_head(new_item);
            list.set_tail(new_item);
        } else {
            let old_head_id = list.tail();
            let mut old_tail_item = self
                .get_list_item_by_id(list, old_head_id)?
                .ok_or(SableError::NotFound)?;
            list.incr_len_by(1);
            new_item.insert_last(&mut old_tail_item);
            // we are replacing the "tail" of the list
            list.set_tail(new_item);
            self.put_list_item(new_item)?;
            self.put_list_item(&old_tail_item)?;
        }
        self.put_list_metadata(list)?;
        Ok(())
    }

    /// Load and cache the list metadata
    fn list_metadata(&self, user_key: &BytesMut) -> Result<GetListMetadataResult, SableError> {
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        let Some(value) = self.cache.get(&encoded_key)? else {
            return Ok(GetListMetadataResult::NotFound);
        };

        match self.try_decode_list_value_metadata(&value)? {
            None => Ok(GetListMetadataResult::WrongType),
            Some(md) => {
                let key = PrimaryKeyMetadata::new(user_key, self.db_id);
                Ok(GetListMetadataResult::Some(List { key, md }))
            }
        }
    }

    /// Create a new list in the database
    fn new_list(&mut self, user_key: &BytesMut) -> Result<List, SableError> {
        let md = ListValueMetadata::builder()
            .with_list_id(self.store.generate_id())
            .build();
        let key = PrimaryKeyMetadata::new(user_key, self.db_id);
        let list = List { key, md };
        self.put_list_metadata(&list)?;

        // Add a bookkeeping record
        let bookkeeping_record = Bookkeeping::new(self.db_id)
            .with_uid(list.id())
            .with_value_type(ValueType::List)
            .to_bytes();
        self.cache.put(&bookkeeping_record, user_key.clone())?;
        Ok(list)
    }

    /// Given raw bytes (read from the db) return whether it represents a `ListValueMetadata`
    fn try_decode_list_value_metadata(
        &self,
        value: &BytesMut,
    ) -> Result<Option<ListValueMetadata>, SableError> {
        let mut reader = U8ArrayReader::with_buffer(value);
        let common_md = CommonValueMetadata::from_bytes(&mut reader)?;
        if !common_md.is_set() {
            return Ok(None);
        }

        reader.rewind();
        let md = ListValueMetadata::from_bytes(&mut reader)?;
        Ok(Some(md))
    }

    /// Put a list entry in the database
    fn put_list_metadata(&mut self, list: &List) -> Result<(), SableError> {
        self.cache.put(&list.encode_key(), list.encode_value())?;
        Ok(())
    }

    fn put_list_item(&mut self, item: &ListItem) -> Result<(), SableError> {
        self.cache.put(&item.encode_key(), item.encode_value())?;
        Ok(())
    }

    fn get_list_item_by_id(
        &mut self,
        list: &List,
        item_id: u64,
    ) -> Result<Option<ListItem>, SableError> {
        let key = ListItemKey::new(list.id(), item_id);
        let Some(raw_value) = self.cache.get(&key.to_bytes())? else {
            return Ok(None);
        };
        let value = ListItemValue::from_bytes(&raw_value).ok_or(SableError::SerialisationError)?;
        Ok(Some(ListItem::new(key, value)))
    }

    fn iterate<F>(&mut self, list: &List, callback: F) -> Result<(), SableError>
    where
        F: FnMut(&ListItem, usize) -> Result<bool, SableError>,
    {
        self.do_iterate(list, false, callback)
    }

    fn reverse_iterate<F>(&mut self, list: &List, callback: F) -> Result<(), SableError>
    where
        F: FnMut(&ListItem, usize) -> Result<bool, SableError>,
    {
        self.do_iterate(list, true, callback)
    }

    /// Iterate over the list
    ///
    /// Arguments:
    ///
    /// - `list` the list on which we iterate
    /// - `reverse` iteration direction: head -> tail ("false"), or tail -> head
    /// - `callback` a callback which is called for every item on the list
    ///
    /// Callback arguments:
    ///
    /// - ListItem - the current item
    /// - usize the current item index (always starting from position)
    ///
    /// Callback return value:
    ///
    /// Return `true` to continue the iteration, `false` otherwise
    fn do_iterate<F>(
        &mut self,
        list: &List,
        reverse: bool,
        mut callback: F,
    ) -> Result<(), SableError>
    where
        F: FnMut(&ListItem, usize) -> Result<bool, SableError>,
    {
        if list.is_empty() {
            return Ok(());
        }

        let mut current_item_id = if reverse { list.tail() } else { list.head() };
        let mut counter = 0usize;
        while let Some(item) = self.get_list_item_by_id(list, current_item_id)? {
            // Determine the current item index
            let index = if reverse {
                // Going backward: index = list.length() - (counter + 1)
                list.len().saturating_sub(counter.saturating_add(1))
            } else {
                counter
            };

            if !callback(&item, index)? {
                break;
            }
            current_item_id = if reverse { item.left() } else { item.right() };
            counter = counter.saturating_add(1);
        }
        Ok(())
    }
}
