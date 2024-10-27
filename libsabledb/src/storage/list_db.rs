use crate::{
    metadata::{Bookkeeping, ListValueMetadata, ValueType},
    storage::DbWriteCache,
    CommonValueMetadata, FromBytes, FromU8Reader, PrimaryKeyMetadata, SableError, StorageAdapter,
    ToBytes, ToU8Writer, U8ArrayBuilder, U8ArrayReader,
};
use bytes::BytesMut;
use std::cmp::Ordering;

bitflags::bitflags! {
pub struct ListFlags: u32  {
    /// None: Actions are performed from the right side of the list
    const None = 0;
    /// For convenience, same as `None`
    const FromRight = 0;
    /// Before performing the operation, the list must exist
    const ListMustExist = 1 << 0;
    /// Perform the operation from the left side of the list (Lpush, Lpop)
    /// if not set, perform from right side
    const FromLeft = 1 << 1;
    /// Insert element after pivot element
    const InsertAfter = 1<< 2;
    /// Insert element before pivot element
    const InsertBefore = 1<< 3;
}
}

#[derive(Clone)]
struct ListItemKey {
    pub list_id: u64,
    pub item_id: u64,
}

impl ListItemKey {
    pub fn new(list_id: u64, item_id: u64) -> Self {
        ListItemKey { list_id, item_id }
    }
}

#[derive(Default, Clone)]
struct ListItemValue {
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
struct List {
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

    pub fn set_head(&mut self, item: Option<&ListItem>) {
        let id = if let Some(item) = item { item.id() } else { 0 };
        self.md.set_head(id);
    }

    pub fn set_tail(&mut self, item: Option<&ListItem>) {
        let id = if let Some(item) = item { item.id() } else { 0 };
        self.md.set_tail(id);
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

    pub fn decr_len_by(&mut self, n: u64) {
        self.md.set_len(self.md.len().saturating_sub(n))
    }

    pub fn fix_index(&self, index: isize) -> isize {
        if index < 0 {
            self.len()
                .try_into()
                .unwrap_or(isize::MAX)
                .saturating_add(index)
        } else {
            index
        }
    }

    #[allow(dead_code)]
    pub fn slot(&self) -> u16 {
        self.key.slot()
    }

    #[allow(dead_code)]
    pub fn database_id(&self) -> u16 {
        self.key.database_id()
    }
}

#[derive(Clone)]
struct ListItem {
    pub key: ListItemKey,
    pub value: ListItemValue,
}

impl std::fmt::Display for ListItem {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{{ {} | Id: {}, Value: '{}' | {} }}",
            self.value.left,
            self.id(),
            String::from_utf8_lossy(self.user_value()),
            self.value.right
        )
    }
}

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
        match (left_item, right_item) {
            (Some(left_item), Some(right_item)) => {
                self.set_right(right_item.id());
                self.set_left(left_item.id());
                left_item.set_right(self.id());
                right_item.set_left(self.id());
            }
            (None, Some(right_item)) => {
                self.set_right(right_item.id());
                right_item.set_left(self.id());
            }
            (Some(left_item), None) => {
                self.set_left(left_item.id());
                left_item.set_right(self.id());
            }
            (None, None) => {}
        }
    }

    pub fn user_value(&self) -> &BytesMut {
        &self.value.value
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

    pub fn set_left(&mut self, left: u64) {
        self.value.left = left;
    }

    pub fn set_right(&mut self, right: u64) {
        self.value.right = right;
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

//
// Public enumerators
//

#[derive(PartialEq, Eq, Debug)]
pub enum ListAppendResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Returns the list length
    Some(usize),
    /// List does not exist
    ListNotFound,
}

#[derive(PartialEq, Eq, Debug)]
pub enum ListPopResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Returns the items removed
    Some(Vec<BytesMut>),
}

#[derive(PartialEq, Eq, Debug)]
pub enum ListRemoveResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Returns the number of items removed
    Some(usize),
}

#[derive(PartialEq, Eq, Debug)]
pub enum ListLenResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Returns the list length
    Some(usize),
    /// List does not exist
    NotFound,
}

#[derive(PartialEq, Eq, Debug)]
pub enum ListRangeResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Returns the requested range
    Some(Vec<BytesMut>),
}

#[derive(PartialEq, Eq, Debug)]
pub enum ListInsertResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// The reference pivot element was not found in the list
    PivotNotFound,
    /// The list does not exist
    ListNotFound,
    /// Insert was successful
    Some(usize),
}

#[derive(PartialEq, Eq, Debug)]
pub enum ListItemAt {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Return the item at the given index
    Some(BytesMut),
    /// Index is out of range
    OutOfRange,
    /// The list does not exist
    ListNotFound,
}

#[derive(PartialEq, Eq, Debug)]
pub enum ListIndexOfResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Return the list of indexes found
    Some(Vec<usize>),
    /// No match was found
    None,
    /// Invalid rank provided
    InvalidRank,
}

// Private enumerators

#[derive(PartialEq, Eq, Debug)]
enum ListInsertInternalResult {
    /// The reference pivot element was not found in the list
    PivotNotFound,
    /// Insert was successful
    Some(usize),
}

// Internal enumerator
#[derive(Debug, PartialEq, Eq)]
enum GetListMetadataResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// A match was found
    Some(List),
    /// No entry exist
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

    /// Append items to the end or start of the list
    pub fn push(
        &mut self,
        user_key: &BytesMut,
        values: &[&BytesMut],
        flags: ListFlags,
    ) -> Result<ListAppendResult, SableError> {
        let mut list = match self.list_metadata(user_key)? {
            GetListMetadataResult::WrongType => return Ok(ListAppendResult::WrongType),
            GetListMetadataResult::NotFound => {
                if flags.contains(ListFlags::ListMustExist) {
                    return Ok(ListAppendResult::ListNotFound);
                }
                self.new_list(user_key)?
            }
            GetListMetadataResult::Some(list) => list,
        };

        for value in values {
            self.insert_internal(
                &mut list,
                value,
                if flags.contains(ListFlags::FromLeft) {
                    InsertPosition::First
                } else {
                    InsertPosition::Last
                },
            )?;
        }
        Ok(ListAppendResult::Some(list.len()))
    }

    /// Remove `count` elements from the head or tail of the list and return them.
    /// If the list is empty after this call, the list itself is also deleted
    pub fn pop(
        &mut self,
        user_key: &BytesMut,
        count: usize,
        flags: ListFlags,
    ) -> Result<ListPopResult, SableError> {
        let mut list = match self.list_metadata(user_key)? {
            GetListMetadataResult::WrongType => return Ok(ListPopResult::WrongType),
            GetListMetadataResult::NotFound => return Ok(ListPopResult::Some(Vec::default())),
            GetListMetadataResult::Some(list) => list,
        };

        let mut result = Vec::<BytesMut>::with_capacity(count);
        for _ in 0..count {
            let item = if flags.contains(ListFlags::FromLeft) {
                if let Some(value) = self.pop_front(&mut list)? {
                    value
                } else {
                    break;
                }
            } else if let Some(value) = self.pop_back(&mut list)? {
                value
            } else {
                break;
            };
            result.push(item);
        }

        if list.is_empty() {
            self.delete_list(&list)?;
        } else {
            self.put_list_metadata(&list)?;
        }
        Ok(ListPopResult::Some(result))
    }

    /// Return the list length
    pub fn len(&self, user_key: &BytesMut) -> Result<ListLenResult, SableError> {
        Ok(match self.list_metadata(user_key)? {
            GetListMetadataResult::Some(list) => ListLenResult::Some(list.len()),
            GetListMetadataResult::WrongType => ListLenResult::WrongType,
            GetListMetadataResult::NotFound => ListLenResult::NotFound,
        })
    }

    /// Apply changes to disk
    pub fn commit(&mut self) -> Result<(), SableError> {
        self.cache.flush()
    }

    /// Discard changes
    pub fn discard(&mut self) -> Result<(), SableError> {
        self.cache.clear();
        Ok(())
    }

    /// Return list item starting from `start` -> `end`.
    /// If `start` > `end` this
    pub fn range(
        &self,
        user_key: &BytesMut,
        start: isize,
        end: isize,
    ) -> Result<ListRangeResult, SableError> {
        let list = match self.list_metadata(user_key)? {
            GetListMetadataResult::WrongType => return Ok(ListRangeResult::WrongType),
            GetListMetadataResult::NotFound => return Ok(ListRangeResult::Some(Vec::default())),
            GetListMetadataResult::Some(list) => list,
        };

        // convert indices
        let start = list.fix_index(start);
        let end = list.fix_index(end);

        if start > end || end < 0 {
            // empty array
            Ok(ListRangeResult::Some(Vec::default()))
        } else {
            let start = if start < 0 { 0 } else { start };

            // At this point, both indices are positive, so go ahead and convert them into usize
            let start = start.try_into().unwrap_or(0usize);
            let end = end.try_into().unwrap_or(0usize);
            let mut result = Vec::<BytesMut>::new();
            self.iterate(&list, |item, index| {
                if index >= start && index <= end {
                    result.push(item.user_value().clone());
                    Ok(index != end)
                } else {
                    Ok(true)
                }
            })?;
            Ok(ListRangeResult::Some(result))
        }
    }

    /// Insert `value` after `pivot` item
    pub fn insert_after(
        &mut self,
        user_key: &BytesMut,
        value: &BytesMut,
        pivot: &BytesMut,
    ) -> Result<ListInsertResult, SableError> {
        let mut list = match self.list_metadata(user_key)? {
            GetListMetadataResult::WrongType => return Ok(ListInsertResult::WrongType),
            GetListMetadataResult::NotFound => return Ok(ListInsertResult::ListNotFound), // List not found
            GetListMetadataResult::Some(list) => list,
        };

        Ok(
            match self.insert_internal(&mut list, value, InsertPosition::After(pivot.clone()))? {
                ListInsertInternalResult::PivotNotFound => ListInsertResult::PivotNotFound,
                ListInsertInternalResult::Some(newlen) => ListInsertResult::Some(newlen),
            },
        )
    }

    /// Insert `value` before `pivot` item
    pub fn insert_before(
        &mut self,
        user_key: &BytesMut,
        value: &BytesMut,
        pivot: &BytesMut,
    ) -> Result<ListInsertResult, SableError> {
        let mut list = match self.list_metadata(user_key)? {
            GetListMetadataResult::WrongType => return Ok(ListInsertResult::WrongType),
            GetListMetadataResult::NotFound => return Ok(ListInsertResult::ListNotFound), // List not found
            GetListMetadataResult::Some(list) => list,
        };

        Ok(
            match self.insert_internal(&mut list, value, InsertPosition::Before(pivot.clone()))? {
                ListInsertInternalResult::PivotNotFound => ListInsertResult::PivotNotFound,
                ListInsertInternalResult::Some(newlen) => ListInsertResult::Some(newlen),
            },
        )
    }

    /// Return the item at the given index
    pub fn item_at(&self, user_key: &BytesMut, index: isize) -> Result<ListItemAt, SableError> {
        let list = match self.list_metadata(user_key)? {
            GetListMetadataResult::WrongType => return Ok(ListItemAt::WrongType),
            GetListMetadataResult::NotFound => return Ok(ListItemAt::ListNotFound),
            GetListMetadataResult::Some(list) => list,
        };

        let index = list.fix_index(index).try_into().unwrap_or(usize::MAX);
        Ok(if index >= list.len() {
            ListItemAt::OutOfRange
        } else {
            // iterate
            let mut value = BytesMut::default();
            self.iterate(&list, |item, item_index| {
                if item_index == index {
                    value = item.user_value().clone();
                    Ok(false)
                } else {
                    Ok(true)
                }
            })?;
            ListItemAt::Some(value)
        })
    }
    /// Removes the first `count` occurrences of elements equal to `element` from the list stored at `user_key`.
    /// The `count` argument influences the operation in the following ways:
    ///
    /// - `count > 0`: Remove elements equal to `element` moving from head to tail.
    /// - `count < 0`: Remove elements equal to `element` moving from tail to head.
    /// - `count = 0`: Remove all elements equal to `element`.
    pub fn remove_items(
        &mut self,
        user_key: &BytesMut,
        count: isize,
        value: &BytesMut,
    ) -> Result<ListRemoveResult, SableError> {
        let mut list = match self.list_metadata(user_key)? {
            GetListMetadataResult::WrongType => return Ok(ListRemoveResult::WrongType),
            GetListMetadataResult::NotFound => return Ok(ListRemoveResult::Some(0)),
            GetListMetadataResult::Some(list) => list,
        };

        let mut items_to_remove = Vec::<u64>::default();
        match count.cmp(&0) {
            Ordering::Equal => {
                self.iterate(&list, |item, _index| {
                    if item.user_value().eq(value) {
                        items_to_remove.push(item.id());
                    }
                    Ok(true)
                })?;
            }
            Ordering::Less => {
                // Remove all items matching element from tail -> head
                let count: usize = count.abs().try_into().unwrap_or(usize::MAX);
                self.reverse_iterate(&list, |item, _index| {
                    if item.user_value().eq(value) {
                        items_to_remove.push(item.id());
                    }
                    Ok(items_to_remove.len() < count)
                })?;
            }
            Ordering::Greater => {
                // Remove all items matching element head -> tail
                let count: usize = count.abs().try_into().unwrap_or(usize::MAX);
                self.iterate(&list, |item, _index| {
                    if item.user_value().eq(value) {
                        items_to_remove.push(item.id());
                    }
                    Ok(items_to_remove.len() < count)
                })?;
            }
        }

        for item_id in &items_to_remove {
            self.remove_internal(&mut list, *item_id)?;
        }
        Ok(ListRemoveResult::Some(items_to_remove.len()))
    }

    /// Return the index(s) of `value` inside list `user_key`. If `rank` is provided, return the index of the `rank`'s
    /// item, i.e. if `rank = Some(2)` return the position of the 2nd item, if `rank` is negative, return the `rank`'s
    /// item from the end of the list. If `count` is provided, return the position of the `count` items. If `maxlen` is
    /// provided, ensure that only `maxlen` items are compared
    pub fn index_of(
        &self,
        user_key: &BytesMut,
        value: &BytesMut,
        rank: Option<isize>,
        count: Option<usize>,
        maxlen: Option<usize>,
    ) -> Result<ListIndexOfResult, SableError> {
        let list = match self.list_metadata(user_key)? {
            GetListMetadataResult::WrongType => return Ok(ListIndexOfResult::WrongType),
            GetListMetadataResult::NotFound => return Ok(ListIndexOfResult::None),
            GetListMetadataResult::Some(list) => list,
        };

        // fix the rank
        let (reverse, rank) = if let Some(rank) = rank {
            match rank.cmp(&0) {
                Ordering::Equal => return Ok(ListIndexOfResult::InvalidRank),
                Ordering::Less => (true, rank.unsigned_abs()),
                Ordering::Greater => (false, rank.unsigned_abs()),
            }
        } else {
            (false, 1usize)
        };

        // Adjust the number of items to find
        let count = match count {
            Some(0) => usize::MAX, // 0 means - all items
            Some(count) => count,
            None => 1usize,
        };

        // Adjust the number of comparisons to perform
        let maxlen = match maxlen {
            Some(0) | None => usize::MAX, // 0 or not provided maxlen means: scan the entire list
            Some(maxlen) => maxlen,
        };

        let mut current_rank = 1usize;
        let mut indexes: Vec<usize> = Vec::default();
        let mut loop_counter = 0usize;

        self.iterate_internal(&list, reverse, |item, index| {
            loop_counter = loop_counter.saturating_add(1);
            if item.user_value().eq(value) {
                if current_rank >= rank {
                    indexes.push(index);
                }
                current_rank = current_rank.saturating_add(1);
            }
            Ok(loop_counter < maxlen && indexes.len() < count)
        })?;

        Ok(if indexes.is_empty() {
            ListIndexOfResult::None
        } else {
            ListIndexOfResult::Some(indexes)
        })
    }

    // ===-----------------------------
    // Private helpers
    // ===-----------------------------

    /// Insert item to the list at a given position
    fn insert_internal(
        &mut self,
        list: &mut List,
        value: &BytesMut,
        pos: InsertPosition,
    ) -> Result<ListInsertInternalResult, SableError> {
        let new_item_key = ListItemKey::new(list.id(), self.store.generate_id());
        let new_item_value = ListItemValue::with_value(value.clone());
        let mut new_item = ListItem::new(new_item_key, new_item_value);

        self.put_list_item(&new_item)?;

        match pos {
            InsertPosition::Last => {
                self.push_back(list, &mut new_item)?;
                Ok(ListInsertInternalResult::Some(list.len()))
            }
            InsertPosition::First => {
                self.push_front(list, &mut new_item)?;
                Ok(ListInsertInternalResult::Some(list.len()))
            }
            InsertPosition::Before(pivot) => {
                let Some(mut pivot) = self.get_list_item_by_value(list, &pivot)? else {
                    return Ok(ListInsertInternalResult::PivotNotFound);
                };
                self.insert_before_item(list, &mut pivot, &mut new_item)?;
                Ok(ListInsertInternalResult::Some(list.len()))
            }
            InsertPosition::After(pivot) => {
                let Some(mut pivot) = self.get_list_item_by_value(list, &pivot)? else {
                    return Ok(ListInsertInternalResult::PivotNotFound);
                };
                self.insert_after_item(list, &mut pivot, &mut new_item)?;
                Ok(ListInsertInternalResult::Some(list.len()))
            }
        }
    }

    /// Remove `item` from `list`
    fn remove_internal(&mut self, list: &mut List, item_id: u64) -> Result<(), SableError> {
        let Some(item) = self.get_list_item_by_id(list, item_id)? else {
            return Ok(());
        };
        self.cache.delete(&item.encode_key())?;
        list.decr_len_by(1);

        // update the remaining items
        let left_item = self.get_list_item_by_id(list, item.left())?;
        let right_item = self.get_list_item_by_id(list, item.right())?;

        match (left_item, right_item) {
            (Some(mut left), Some(mut right)) => {
                left.set_right(right.id());
                right.set_left(left.id());
                self.cache.put(&left.encode_key(), left.encode_value())?;
                self.cache.put(&right.encode_key(), right.encode_value())?;
            }
            (Some(mut left), None) => {
                // Removing last item
                left.set_right(0);
                // update the list new tail
                list.set_tail(Some(&left));
                self.cache.put(&left.encode_key(), left.encode_value())?;
            }
            (None, Some(mut right)) => {
                // Removing the list's head
                right.set_left(0);
                list.set_head(Some(&right));
                self.cache.put(&right.encode_key(), right.encode_value())?;
            }
            (None, None) => {
                // the removed item was the last item
                list.set_head(None);
                list.set_tail(None);
            }
        }

        if list.is_empty() {
            self.cache.delete(&list.encode_key())?;
        } else {
            self.cache.put(&list.encode_key(), list.encode_value())?;
        }
        Ok(())
    }

    /// Push item at the start of the list
    fn push_front(&mut self, list: &mut List, new_item: &mut ListItem) -> Result<(), SableError> {
        if list.is_empty() {
            list.set_len(1);
            list.set_head(Some(new_item));
            list.set_tail(Some(new_item));
        } else {
            let old_head_id = list.head();
            let mut old_head_item = self
                .get_list_item_by_id(list, old_head_id)?
                .ok_or(SableError::NotFound)?;
            list.incr_len_by(1);
            // we are replacing the "head" of the list
            list.set_head(Some(new_item));
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
            list.set_head(Some(new_item));
            list.set_tail(Some(new_item));
        } else {
            let old_head_id = list.tail();
            let mut old_tail_item = self
                .get_list_item_by_id(list, old_head_id)?
                .ok_or(SableError::NotFound)?;
            list.incr_len_by(1);
            new_item.insert_last(&mut old_tail_item);
            // we are replacing the "tail" of the list
            list.set_tail(Some(new_item));
            self.put_list_item(new_item)?;
            self.put_list_item(&old_tail_item)?;
        }
        self.put_list_metadata(list)?;
        Ok(())
    }

    fn pop_front(&mut self, list: &mut List) -> Result<Option<BytesMut>, SableError> {
        let Some(item) = self.get_list_item_by_id(list, list.head())? else {
            return Ok(None);
        };

        let right_item = item.right();
        let value = item.user_value().clone();

        // delete the item
        self.cache.delete(&item.encode_key())?;
        self.cache.put(&list.encode_key(), list.encode_value())?;
        if let Some(mut next_item) = self.get_list_item_by_id(list, right_item)? {
            // update the next_item's "prev" to 0 (i.e. this item is now the new "head")
            next_item.set_left(0);
            // update the new head of the list
            list.set_head(Some(&next_item));
            self.cache
                .put(&next_item.encode_key(), next_item.encode_value())?;
        } else {
            // the list is now empty
            list.set_head(None);
            list.set_tail(None);
        }

        // update the list length
        list.decr_len_by(1);
        self.cache.put(&list.encode_key(), list.encode_value())?;
        Ok(Some(value))
    }

    fn pop_back(&mut self, list: &mut List) -> Result<Option<BytesMut>, SableError> {
        let Some(item) = self.get_list_item_by_id(list, list.tail())? else {
            return Ok(None);
        };

        let left_item = item.left();
        let value = item.user_value().clone();

        // delete the item
        self.cache.delete(&item.encode_key())?;
        // Check if the deleted item had items to its left
        if let Some(mut left_item) = self.get_list_item_by_id(list, left_item)? {
            // update the next item to None
            left_item.set_right(0);
            // left_item is the new tail
            list.set_tail(Some(&left_item));
            self.cache
                .put(&left_item.encode_key(), left_item.encode_value())?;
        } else {
            // The deleted item was the last item
            list.set_head(None);
            list.set_tail(None);
        }
        list.decr_len_by(1);
        self.cache.put(&list.encode_key(), list.encode_value())?;
        Ok(Some(value))
    }

    /// Insert `new_item` after the `pivot` item
    fn insert_after_item(
        &mut self,
        list: &mut List,
        pivot: &mut ListItem,
        new_item: &mut ListItem,
    ) -> Result<(), SableError> {
        if let Some(mut right_item) = self.get_list_item_by_id(list, pivot.right())? {
            // pivot is pointing to a valid item in the list
            new_item.insert_between(Some(pivot), Some(&mut right_item));
            list.incr_len_by(1);
            self.put_list_item(new_item)?;
            self.put_list_item(pivot)?;
            self.put_list_item(&right_item)?;
            self.put_list_metadata(list)?;
        } else {
            // pivot is the last item
            self.push_back(list, new_item)?;
        }
        Ok(())
    }

    /// Insert `new_item` before the `pivot` item
    fn insert_before_item(
        &mut self,
        list: &mut List,
        pivot: &mut ListItem,
        new_item: &mut ListItem,
    ) -> Result<(), SableError> {
        if let Some(mut left_item) = self.get_list_item_by_id(list, pivot.left())? {
            // pivot is pointing to a valid item in the list
            new_item.insert_between(Some(&mut left_item), Some(pivot));
            list.incr_len_by(1);
            self.put_list_item(new_item)?;
            self.put_list_item(pivot)?;
            self.put_list_item(&left_item)?;
            self.put_list_metadata(list)?;
        } else {
            // pivot is the first item
            self.push_front(list, new_item)?;
        }
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
        if !common_md.is_list() {
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
        &self,
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

    fn delete_list(&mut self, list: &List) -> Result<(), SableError> {
        self.cache.delete(&list.encode_key())
    }

    /// Iterate over the list, head -> tail and find the first item
    /// the matches `value`
    fn get_list_item_by_value(
        &self,
        list: &List,
        value: &BytesMut,
    ) -> Result<Option<ListItem>, SableError> {
        // Search for the first item with the given value
        let mut matched_item: Option<ListItem> = None;
        self.iterate(list, |item, _index| {
            if item.user_value().eq(value) {
                matched_item = Some(item.clone());
                return Ok(false);
            }
            Ok(true)
        })?;
        Ok(matched_item)
    }

    fn iterate<F>(&self, list: &List, callback: F) -> Result<(), SableError>
    where
        F: FnMut(ListItem, usize) -> Result<bool, SableError>,
    {
        self.iterate_internal(list, false, callback)
    }

    fn reverse_iterate<F>(&self, list: &List, callback: F) -> Result<(), SableError>
    where
        F: FnMut(ListItem, usize) -> Result<bool, SableError>,
    {
        self.iterate_internal(list, true, callback)
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
    fn iterate_internal<F>(
        &self,
        list: &List,
        reverse: bool,
        mut callback: F,
    ) -> Result<(), SableError>
    where
        F: FnMut(ListItem, usize) -> Result<bool, SableError>,
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

            current_item_id = if reverse { item.left() } else { item.right() };
            if !callback(item, index)? {
                break;
            }
            counter = counter.saturating_add(1);
        }
        Ok(())
    }
}

//  _    _ _   _ _____ _______      _______ ______  _____ _______ _____ _   _  _____
// | |  | | \ | |_   _|__   __|    |__   __|  ____|/ ____|__   __|_   _| \ | |/ ____|
// | |  | |  \| | | |    | |    _     | |  | |__  | (___    | |    | | |  \| | |  __|
// | |  | | . ` | | |    | |   / \    | |  |  __|  \___ \   | |    | | | . ` | | |_ |
// | |__| | |\  |_| |_   | |   \_/    | |  | |____ ____) |  | |   _| |_| |\  | |__| |
//  \____/|_| \_|_____|  |_|          |_|  |______|_____/   |_|  |_____|_| \_|\_____|
//
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_left_push() {
        let (_deleter, db) = crate::tests::open_store();
        let mut db = ListDb::with_storage(&db, 0);

        let mut values = Vec::<BytesMut>::new();
        for i in 0..10 {
            let key = format!("key_{}", i);
            values.push(BytesMut::from(key.as_bytes()));
        }

        let list_name = BytesMut::from("mylist");
        let values: Vec<&BytesMut> = values.iter().collect();
        let res = db.push(&list_name, &values, ListFlags::FromLeft).unwrap();
        assert_eq!(res, ListAppendResult::Some(10));

        db.commit().unwrap();

        let ListRangeResult::Some(mut items) = db.range(&list_name, 0, -1).unwrap() else {
            panic!("Expected ListRangeResult::Ok");
        };

        // items needs to be reversed (we added them at the HEAD of the list)
        items.reverse();
        println!("{:?}", items);
        assert_eq!(items.len(), values.len());
        assert_eq!(items, values);
    }

    #[test]
    fn test_right_push() {
        let (_deleter, db) = crate::tests::open_store();
        let mut db = ListDb::with_storage(&db, 0);

        let mut values = Vec::<BytesMut>::new();
        for i in 0..10 {
            let key = format!("key_{}", i);
            values.push(BytesMut::from(key.as_bytes()));
        }

        let list_name = BytesMut::from("mylist");
        let values: Vec<&BytesMut> = values.iter().collect();
        let res = db.push(&list_name, &values, ListFlags::FromRight).unwrap();
        assert_eq!(res, ListAppendResult::Some(10));

        db.commit().unwrap();

        let ListRangeResult::Some(items) = db.range(&list_name, 0, -1).unwrap() else {
            panic!("Expected ListRangeResult::Ok");
        };

        println!("{:?}", items);
        assert_eq!(items.len(), values.len());
        assert_eq!(items, values);
    }

    #[test]
    fn test_pop_item_with_list_of_len_1() {
        let (_deleter, db) = crate::tests::open_store();
        let mut db = ListDb::with_storage(&db, 0);
        let list_name = BytesMut::from("mylist");
        let value = BytesMut::from("value");
        let res = db
            .push(&list_name, &vec![&value], ListFlags::FromRight)
            .unwrap();
        assert_eq!(res, ListAppendResult::Some(1));
        db.commit().unwrap();

        let res = db.pop(&list_name, 10, ListFlags::FromLeft).unwrap();
        assert_eq!(res, ListPopResult::Some(vec![value.clone()]));
        // Removing all items should have delete the list
        let res = db.len(&list_name).unwrap();
        assert_eq!(res, ListLenResult::NotFound);

        db.discard().unwrap();

        let res = db.pop(&list_name, 10, ListFlags::FromRight).unwrap();
        assert_eq!(res, ListPopResult::Some(vec![value.clone()]));
        // Removing all items should have delete the list
        let res = db.len(&list_name).unwrap();
        assert_eq!(res, ListLenResult::NotFound);
        db.commit().unwrap();
    }

    #[test]
    fn test_pop_items_from_left() {
        let (_deleter, db) = crate::tests::open_store();
        let mut db = ListDb::with_storage(&db, 0);
        let list_name = BytesMut::from("mylist");
        populate_list(&mut db, &list_name, 10);

        let _ = db.pop(&list_name, 2, ListFlags::FromLeft).unwrap();
        db.commit().unwrap();

        assert_eq!(db.len(&list_name).unwrap(), ListLenResult::Some(8));
        let ListRangeResult::Some(items) = db.range(&list_name, 0, -1).unwrap() else {
            panic!("Expected ListRangeResult::Ok");
        };

        // items needs to be reversed (we added them at the HEAD of the list)
        println!("{:?}", items);
        assert_eq!(items.len(), 8);
        // after the removal, items are: [key_2,...,key_9]
        assert_eq!(items.first().unwrap(), &BytesMut::from("key_2"));
        assert_eq!(items.last().unwrap(), &BytesMut::from("key_9"));
    }

    #[test]
    fn test_pop_items_from_right() {
        let (_deleter, db) = crate::tests::open_store();
        let mut db = ListDb::with_storage(&db, 0);
        let list_name = BytesMut::from("mylist");

        populate_list(&mut db, &list_name, 10);

        let _ = db.pop(&list_name, 2, ListFlags::FromRight).unwrap();
        db.commit().unwrap();

        assert_eq!(db.len(&list_name).unwrap(), ListLenResult::Some(8));
        let ListRangeResult::Some(items) = db.range(&list_name, 0, -1).unwrap() else {
            panic!("Expected ListRangeResult::Ok");
        };

        // items needs to be reversed (we added them at the HEAD of the list)
        println!("{:?}", items);
        assert_eq!(items.len(), 8);
        // after the removal, items are: [key_0,...,key_7]
        assert_eq!(items.first().unwrap(), &BytesMut::from("key_0"));
        assert_eq!(items.last().unwrap(), &BytesMut::from("key_7"));
    }

    use test_case::test_case;

    #[test_case(true; "pop from right")]
    #[test_case(false; "pop from left")]
    fn test_pop_1_item_from_list_of_size_2(from_right: bool) {
        let (_deleter, db) = crate::tests::open_store();
        let mut db = ListDb::with_storage(&db, 2);
        let list_name = BytesMut::from("mylist");

        populate_list(&mut db, &list_name, 2);

        let _ = db
            .pop(
                &list_name,
                1,
                if from_right {
                    ListFlags::FromRight
                } else {
                    ListFlags::FromLeft
                },
            )
            .unwrap();
        db.commit().unwrap();

        assert_eq!(db.len(&list_name).unwrap(), ListLenResult::Some(1));
        let ListRangeResult::Some(items) = db.range(&list_name, 0, -1).unwrap() else {
            panic!("Expected ListRangeResult::Ok");
        };

        // items needs to be reversed (we added them at the HEAD of the list)
        println!("{:?}", items);
        assert_eq!(items.len(), 1);

        let last_item = if from_right {
            BytesMut::from("key_0")
        } else {
            BytesMut::from("key_1")
        };
        assert_eq!(items.first().unwrap(), &last_item);
        assert_eq!(items.last().unwrap(), &last_item);

        let GetListMetadataResult::Some(list) = db.list_metadata(&list_name).unwrap() else {
            panic!("Expected GetListMetadataResult::Some");
        };
        assert_eq!(list.head(), list.tail());
        assert!(list.head() != 0);
    }

    #[test_case(vec!["a", "b", "b", "a"], "b", -1, vec!["a", "b", "a"];"removing 'b' item from tail")]
    #[test_case(vec!["a", "b", "b", "a"], "b", 1, vec!["a", "b", "a"];"removing 'b' item from head")]
    #[test_case(vec!["a", "b", "b", "a"], "a", 1, vec!["b", "b", "a"];"removing 'a' item from head")]
    #[test_case(vec!["a", "b", "b", "a"], "a", -1, vec!["a", "b", "b"];"removing 'a' item from tail")]
    #[test_case(vec!["a", "b", "b", "a"], "a", 10, vec!["b", "b"];"removing all 'a' from head")]
    #[test_case(vec!["a", "b", "b", "a"], "a", -10, vec!["b", "b"];"removing all 'a' from tail")]
    fn test_remove_items(initial_list: Vec<&str>, value: &str, count: isize, expected: Vec<&str>) {
        let (_deleter, db) = crate::tests::open_store();
        let mut db = ListDb::with_storage(&db, 0);
        let list_name = BytesMut::from("mylist");
        let values: Vec<BytesMut> = initial_list.iter().map(|s| BytesMut::from(*s)).collect();
        let values: Vec<&BytesMut> = values.iter().collect();

        let expected: Vec<BytesMut> = expected.iter().map(|s| BytesMut::from(*s)).collect();

        db.push(&list_name, &values, ListFlags::FromRight).unwrap();
        db.commit().unwrap();
        assert_eq!(db.len(&list_name).unwrap(), ListLenResult::Some(4));

        // Remove 1 item from last equal to "2"
        let remove_me = BytesMut::from(value);
        let ListRemoveResult::Some(_) = db.remove_items(&list_name, count, &remove_me).unwrap()
        else {
            panic!("Expected ListRemoveResult::Some");
        };

        let ListRangeResult::Some(items) = db.range(&list_name, 0, -1).unwrap() else {
            panic!("Expected ListRangeResult::Ok");
        };
        println!("{:?}", items);
        assert_eq!(items, expected);
    }

    #[test_case(vec!["a", "b", "c", "d"], "a", vec!["new_item", "a", "b", "c", "d"]; "insert before first item")]
    #[test_case(vec!["a", "b", "c", "d"], "b", vec!["a", "new_item", "b", "c", "d"]; "insert in the middle 1")]
    #[test_case(vec!["a", "b", "c", "d"], "c", vec!["a", "b", "new_item", "c", "d"]; "insert in the middle 2")]
    #[test_case(vec!["a", "b", "c", "d"], "d", vec!["a", "b", "c", "new_item", "d"]; "insert before last item")]
    fn test_insert_before(initial_list: Vec<&str>, pivot: &str, expected: Vec<&str>) {
        let (_deleter, db) = crate::tests::open_store();
        let mut db = ListDb::with_storage(&db, 0);
        let list_name = BytesMut::from("mylist");
        let values: Vec<BytesMut> = initial_list.iter().map(|s| BytesMut::from(*s)).collect();
        let values: Vec<&BytesMut> = values.iter().collect();
        let pivot = BytesMut::from(pivot);
        let expected: Vec<BytesMut> = expected.iter().map(|s| BytesMut::from(*s)).collect();

        db.push(&list_name, &values, ListFlags::FromRight).unwrap();
        db.commit().unwrap();
        assert_eq!(db.len(&list_name).unwrap(), ListLenResult::Some(4));

        // insert new item "new_item" before pivot
        let insert_me = BytesMut::from("new_item");
        let ListInsertResult::Some(new_len) =
            db.insert_before(&list_name, &insert_me, &pivot).unwrap()
        else {
            panic!("Expected ListRemoveResult::Some");
        };

        assert_eq!(new_len, expected.len());
        let ListRangeResult::Some(items) = db.range(&list_name, 0, -1).unwrap() else {
            panic!("Expected ListRangeResult::Ok");
        };
        println!("{:?}", items);
        assert_eq!(items, expected);
    }

    #[test_case(vec!["a", "b", "c", "d"], "a", vec!["a", "new_item", "b", "c", "d"]; "insert after first item")]
    #[test_case(vec!["a", "b", "c", "d"], "b", vec!["a", "b", "new_item", "c", "d"]; "insert after second item")]
    #[test_case(vec!["a", "b", "c", "d"], "c", vec!["a", "b", "c", "new_item", "d"]; "insert after third item")]
    #[test_case(vec!["a", "b", "c", "d"], "d", vec!["a", "b", "c", "d", "new_item"]; "insert after last item")]
    fn test_insert_after(initial_list: Vec<&str>, pivot: &str, expected: Vec<&str>) {
        let (_deleter, db) = crate::tests::open_store();
        let mut db = ListDb::with_storage(&db, 0);
        let list_name = BytesMut::from("mylist");
        let values: Vec<BytesMut> = initial_list.iter().map(|s| BytesMut::from(*s)).collect();
        let values: Vec<&BytesMut> = values.iter().collect();
        let pivot = BytesMut::from(pivot);
        let expected: Vec<BytesMut> = expected.iter().map(|s| BytesMut::from(*s)).collect();

        db.push(&list_name, &values, ListFlags::FromRight).unwrap();
        db.commit().unwrap();
        assert_eq!(db.len(&list_name).unwrap(), ListLenResult::Some(4));

        // insert new item "new_item" before pivot
        let insert_me = BytesMut::from("new_item");
        let ListInsertResult::Some(new_len) =
            db.insert_after(&list_name, &insert_me, &pivot).unwrap()
        else {
            panic!("Expected ListRemoveResult::Some");
        };

        assert_eq!(new_len, expected.len());
        let ListRangeResult::Some(items) = db.range(&list_name, 0, -1).unwrap() else {
            panic!("Expected ListRangeResult::Ok");
        };
        println!("{:?}", items);
        assert_eq!(items, expected);
    }

    #[test_case("a b c d 1 2 3 4 3 3 3", "a", None, None, None, ListIndexOfResult::Some(vec![0]); "common case")]
    #[test_case("a b c d 1 2 3 4 3 3 3", "3", None, None, None, ListIndexOfResult::Some(vec![6]); "find in the middle")]
    #[test_case("a b c d 1 2 3 4 3 3 3", "3", Some(0), Some(2), None, ListIndexOfResult::Some(vec![8, 9, 10]); "find last three items")]
    #[test_case("a b c d 1 2 3 4 3 3 3", "1", None, Some(2), None, ListIndexOfResult::None; "out of range rank")]
    #[test_case("a b c d 1 2 3 4 3 3 3", "1", None, Some(0), None, ListIndexOfResult::InvalidRank; "zero rank")]
    #[test_case("a b c d 1 2 3 4 3 3 3", "3", Some(0), Some(2), Some(0), ListIndexOfResult::Some(vec![8, 9, 10]); "find last three items with maxlen 0")]
    #[test_case("a b c d 1 2 3 4 3 3 3", "3", Some(0), Some(2), Some(9), ListIndexOfResult::Some(vec![8]); "find last three items with maxlen 9")]
    fn test_index_of(
        initial_list: &str,
        value: &str,
        count: Option<usize>,
        rank: Option<isize>,
        maxlen: Option<usize>,
        expected: ListIndexOfResult,
    ) {
        let (_deleter, db) = crate::tests::open_store();
        let mut db = ListDb::with_storage(&db, 0);
        let list_name = BytesMut::from("mylist");
        let values: Vec<BytesMut> = initial_list.split(' ').map(|s| BytesMut::from(s)).collect();
        let values: Vec<&BytesMut> = values.iter().collect();
        let value = BytesMut::from(value);

        db.push(&list_name, &values, ListFlags::FromRight).unwrap();
        db.commit().unwrap();
        assert_eq!(
            db.len(&list_name).unwrap(),
            ListLenResult::Some(values.len())
        );
        assert_eq!(
            db.index_of(&list_name, &value, rank, count, maxlen)
                .unwrap(),
            expected
        );
    }

    fn populate_list(db: &mut ListDb, name: &BytesMut, count: usize) {
        let mut values = Vec::<BytesMut>::new();
        for i in 0..count {
            let key = format!("key_{}", i);
            values.push(BytesMut::from(key.as_bytes()));
        }

        let values: Vec<&BytesMut> = values.iter().collect();
        let res = db.push(name, &values, ListFlags::FromRight).unwrap();
        assert_eq!(res, ListAppendResult::Some(count));
    }
}
