use crate::{
    commands::ErrorStrings,
    iter_next_or_prev, list_md_or_null_string, list_or_size_0,
    metadata::PrimaryKeyMetadata,
    metadata::{CommonValueMetadata, ListValueMetadata},
    storage::PutFlags,
    BatchUpdate, BytesMutUtils, RespBuilderV2, SableError, Storable, StorageAdapter, StorageCache,
    U8ArrayBuilder, U8ArrayReader,
};

use bytes::BytesMut;
use std::cell::RefCell;
use std::rc::Rc;

pub struct List<'a> {
    store: &'a StorageAdapter,
    db_id: u16,
}

enum GetListMetadataResult {
    Some(ListValueMetadata),
    WrongType,
    None,
}

#[derive(PartialEq, Eq)]
pub enum MoveResult {
    Some(Rc<ListItem>),
    WrongType,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiPopResult {
    /// The list from which we popped + items popped
    Some((BytesMut, Vec<Rc<ListItem>>)),
    WrongType,
    None,
}

#[derive(PartialEq, Eq)]
enum IterResult {
    Some(Rc<RefCell<ListItem>>),
    WrongType,
    None,
}
#[derive(Debug, Clone)]
enum InsertResult {
    NotFound,
    Some(ListItem),
}

enum PosResult {
    Some(Vec<usize>),
    WrongType,
    InvalidRank,
    None,
}

#[derive(Clone, Debug)]
pub enum BlockingCommandResult {
    WouldBlock,
    Ok,
}

#[derive(Clone, Debug)]
pub enum BlockingPopInternalResult {
    /// Client should block
    WouldBlock,
    /// Return the name of the list from which we found the items + the items themselves
    Some((BytesMut, Vec<Rc<ListItem>>)),
    /// Invalid input arguments
    InvalidArguments,
}

bitflags::bitflags! {
pub struct ListFlags: u32  {
    /// None: Actions are performed from the right side of the list
    const None = 0;
    /// For convinience, same as `None`
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

type ListItemCache = StorageCache<ListItem>;

impl<'a> List<'a> {
    pub fn with_storage(store: &'a StorageAdapter, db_id: u16) -> Self {
        List { store, db_id }
    }

    /// Return the list size
    pub fn len(
        &self,
        list_name: &BytesMut,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let list = list_or_size_0!(&self, list_name, response_buffer, builder);
        builder.number_u64(response_buffer, list.len());
        Ok(())
    }

    /// pop `count` items from the first non empty list
    /// If this function could not find a list with items,
    /// return `BlockingCommandResult::WouldBlock` to the caller
    pub fn blocking_pop(
        &self,
        lists: &[&BytesMut],
        count: usize,
        response_buffer: &mut BytesMut,
        flags: ListFlags,
    ) -> Result<BlockingCommandResult, SableError> {
        let builder = RespBuilderV2::default();
        match self.blocking_pop_internal(lists, count, flags)? {
            BlockingPopInternalResult::WouldBlock => Ok(BlockingCommandResult::WouldBlock),
            BlockingPopInternalResult::InvalidArguments => {
                builder.error_string(response_buffer, ErrorStrings::SYNTAX_ERROR);
                Ok(BlockingCommandResult::Ok)
            }
            BlockingPopInternalResult::Some((list_name, values)) => {
                let Some(value) = values.first() else {
                    return Err(SableError::InvalidArgument(
                        "Expected at least one element in the array".to_string(),
                    ));
                };

                builder.add_array_len(response_buffer, 2);
                builder.add_bulk_string(response_buffer, &list_name);
                builder.add_bulk_string(response_buffer, &value.user_data);
                Ok(BlockingCommandResult::Ok)
            }
        }
    }

    /// pop `count` items from the first non empty list
    /// If this function could not find a list with items,
    /// return `BlockingCommandResult::WouldBlock` to the caller
    pub fn blocking_pop_internal(
        &self,
        lists: &[&BytesMut],
        count: usize,
        flags: ListFlags,
    ) -> Result<BlockingPopInternalResult, SableError> {
        if count == 0 {
            return Ok(BlockingPopInternalResult::InvalidArguments);
        }

        for list_name in lists {
            let GetListMetadataResult::Some(mut list_md) =
                self.get_list_metadata_with_name(list_name)?
            else {
                continue;
            };

            if list_md.is_empty() {
                continue;
            }

            // md is a list that is not empty
            let mut cache = ListItemCache::new();
            let Some(v) = self.pop_internal(&mut list_md, &mut cache, count, &flags)? else {
                continue;
            };

            if v.is_empty() {
                continue;
            }

            // Create a batch update from the cache
            let mut updates = BatchUpdate::default();
            cache.as_batch_update(&mut updates);

            // delete or update source list if needed
            if list_md.is_empty() {
                self.delete_list_metadata_internal(list_name, &mut updates)?;
            } else {
                self.put_list_metadata_internal(&list_md, list_name, &mut updates)?;
            }
            self.store.apply_batch(&updates)?;

            // Build RESP response
            return Ok(BlockingPopInternalResult::Some(((*list_name).clone(), v)));
        }

        Ok(BlockingPopInternalResult::WouldBlock)
    }

    /// Insert `element` after or before `pivot` element
    pub fn linsert(
        &self,
        list_name: &BytesMut,
        element: &BytesMut,
        pivot: &BytesMut,
        response_buffer: &mut BytesMut,
        flags: ListFlags,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let mut list_md = match self.get_list_metadata_with_name(list_name)? {
            GetListMetadataResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                return Ok(());
            }
            GetListMetadataResult::None => {
                // return 0 when the key doesn't exist.
                builder.number_usize(response_buffer, 0);
                return Ok(());
            }
            GetListMetadataResult::Some(list) => list,
        };

        // find the pivot
        let Some(pivot) = self.find_by_value(&list_md, pivot, false)? else {
            // return -1 when the pivot wasn't found.
            builder.number_i64(response_buffer, -1);
            return Ok(());
        };

        let (left_id, right_id) = if flags.intersects(ListFlags::InsertAfter) {
            (Some(pivot.borrow().id()), pivot.borrow().next())
        } else {
            // InsertBefore
            (pivot.borrow().prev(), Some(pivot.borrow().id()))
        };

        let mut cache = ListItemCache::new();
        let _ = self.insert_internal(&mut list_md, element, left_id, right_id, &mut cache)?;

        // update the storage
        let mut updates = BatchUpdate::default();
        cache.as_batch_update(&mut updates);
        self.put_list_metadata_internal(&list_md, list_name, &mut updates)?;
        self.store.apply_batch(&updates)?;

        // return the list length after a successful insert operation.
        builder.number_u64(response_buffer, list_md.len());
        Ok(())
    }

    /// First `count` elements from the non empty list from the `list_names`
    pub fn multi_pop(
        &self,
        list_names: &[&BytesMut],
        count: usize,
        flags: ListFlags,
    ) -> Result<MultiPopResult, SableError> {
        let _builder = RespBuilderV2::default();
        // identical behavior to `blocking_pop_internal`
        // we just need to translate the return value
        match self.blocking_pop_internal(list_names, count, flags)? {
            BlockingPopInternalResult::WouldBlock | BlockingPopInternalResult::InvalidArguments => {
                Ok(MultiPopResult::None)
            }
            BlockingPopInternalResult::Some((list_name, values)) => {
                Ok(MultiPopResult::Some((list_name, values)))
            }
        }
    }

    pub fn pop(
        &self,
        list_name: &BytesMut,
        count: usize,
        response_buffer: &mut BytesMut,
        flags: ListFlags,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let mut list = list_md_or_null_string!(&self, list_name, response_buffer, builder);
        let mut output_arr = Vec::<ListItem>::with_capacity(count);
        let mut cache = ListItemCache::new();
        for _ in 0..count {
            let item_to_remove = if flags.intersects(ListFlags::FromLeft) {
                list.head()
            } else {
                list.tail()
            };

            if let Some(removed_item) =
                self.remove_internal(item_to_remove, &mut list, &mut cache)?
            {
                output_arr.push(removed_item);
            }
        }

        let mut updates = BatchUpdate::default();
        cache.as_batch_update(&mut updates);

        if list.is_empty() {
            self.delete_list_metadata_internal(list_name, &mut updates)?;
        } else {
            self.put_list_metadata_internal(&list, list_name, &mut updates)?;
        }

        self.store.apply_batch(&updates)?;

        match output_arr.len() {
            0 => builder.null_string(response_buffer),
            1 => builder.bulk_string(response_buffer, &output_arr.first().unwrap().user_data),
            _ => {
                // expected array of bulk strings
                response_buffer.clear();
                builder.add_array_len(response_buffer, output_arr.len());
                for item in output_arr.iter() {
                    builder.add_bulk_string(response_buffer, &item.user_data);
                }
            }
        }
        Ok(())
    }

    fn pop_internal(
        &self,
        list: &mut ListValueMetadata,
        cache: &mut ListItemCache,
        mut count: usize,
        flags: &ListFlags,
    ) -> Result<Option<Vec<Rc<ListItem>>>, SableError> {
        count = std::cmp::min(count, list.len() as usize);
        if count == 0 {
            return Ok(None);
        }

        let mut items_popped = Vec::<Rc<ListItem>>::with_capacity(count);
        while count > 0 {
            let item_to_remove = if flags.intersects(ListFlags::FromLeft) {
                list.head()
            } else {
                list.tail()
            };

            let Some(removed_item) = self.remove_internal(item_to_remove, list, cache)? else {
                break;
            };
            items_popped.push(Rc::new(removed_item));
            count = count.saturating_sub(1);
        }

        Ok(Some(items_popped))
    }

    /// Returns the element at index `index` in the list stored at key. The `index` is zero-based
    /// so `0` means the first element, `1` the second element and so on.
    /// Negative indices can be used to designate elements starting at the tail of the list.
    /// Here, `-1` means the last element, `-2` means the penultimate and so forth.
    /// When the value at key is not a list, an error is returned
    pub fn index(
        &self,
        list_name: &BytesMut,
        index: i32,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let item = match self.find_by_index(list_name, index)? {
            IterResult::None => {
                builder.null_string(response_buffer);
                return Ok(());
            }
            IterResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                return Ok(());
            }
            IterResult::Some(item) => item,
        };
        builder.bulk_string(response_buffer, &item.borrow().user_data);
        Ok(())
    }

    /// Update element at a given position with `user_value`
    pub fn set(
        &self,
        list_name: &BytesMut,
        index: i32,
        user_value: BytesMut,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let item = match self.find_by_index(list_name, index)? {
            IterResult::None => {
                builder.error_string(response_buffer, ErrorStrings::INDEX_OUT_OF_BOUNDS);
                return Ok(());
            }
            IterResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                return Ok(());
            }
            IterResult::Some(item) => item,
        };
        item.borrow_mut().user_data = user_value;
        item.borrow().save(self.store, PutFlags::Override)?;
        builder.ok(response_buffer);
        Ok(())
    }

    /// Remove `count` elements from the list that matches `element`.
    /// If `count` is lower than `0`, remove from tail
    /// If the list is empty, delete it
    pub fn remove(
        &self,
        list_name: &BytesMut,
        element: Option<&BytesMut>,
        count: i32,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let mut list_md = match self.get_list_metadata_with_name(list_name)? {
            GetListMetadataResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                return Ok(());
            }
            GetListMetadataResult::None => {
                builder.ok(response_buffer);
                return Ok(());
            }
            GetListMetadataResult::Some(list) => list,
        };

        let backward = count < 0;

        let mut items_removed = 0u32;
        let mut cache = ListItemCache::new();

        // change the index to absolute value
        let count = count.abs().try_into().unwrap_or(u32::MAX);

        // start iterating
        let mut cur_item_opt = iter_next_or_prev!(self, list_md, None, backward);
        while let IterResult::Some(cur_item) = cur_item_opt {
            // Delete the item if it matches the element's value or if `None` was provided
            let should_delete = match element {
                Some(element_value) => cur_item.borrow().user_data.eq(element_value),
                None => true,
            };

            if should_delete {
                // delete the item
                self.remove_internal(cur_item.borrow().id(), &mut list_md, &mut cache)?;
                items_removed = items_removed.saturating_add(1);

                // limit the removed items if count is greater than 0
                if count.gt(&0) && items_removed.ge(&count) {
                    break;
                }
            }
            cur_item_opt = iter_next_or_prev!(self, list_md, Some(cur_item), backward);
        }

        // Apply changes to the disk
        let mut updates = BatchUpdate::default();
        cache.as_batch_update(&mut updates);

        if list_md.is_empty() {
            self.delete_list_metadata_internal(list_name, &mut updates)?;
        } else {
            self.put_list_metadata_internal(&list_md, list_name, &mut updates)?;
        }

        self.store.apply_batch(&updates)?;
        builder.number::<u32>(response_buffer, items_removed, false);
        Ok(())
    }

    /// Return array of positions for all elements matching `user_value`
    /// If `rank` is provided, return matching starting from `nth` match
    /// If `count` is provided, return up to `count` matches. If `rank` and `count` are both
    /// provided, return up to `count` matches starting from the `nth` match
    /// if `maxlen` is provided, limit the number of comparions to `maxlex`
    /// If rank is provided as negative number, scan backward
    pub fn lpos(
        &self,
        list_name: &BytesMut,
        user_value: &BytesMut,
        rank: Option<i32>,
        count: Option<usize>,
        maxlen: Option<usize>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        match (count, self.pos(list_name, user_value, rank, count, maxlen)?) {
            (_, PosResult::InvalidRank) => {
                builder.error_string(response_buffer, ErrorStrings::LIST_RANK_INVALID);
            }
            (Some(_), PosResult::Some(value)) => {
                // count provided, we return array
                response_buffer.clear();
                if value.is_empty() {
                    builder.null_string(response_buffer);
                } else {
                    builder.add_array_len(response_buffer, value.len());
                    for val in value {
                        builder.add_number::<usize>(response_buffer, val, false);
                    }
                }
            }
            (None, PosResult::Some(value)) => {
                // count was not provided, return number response or null string
                if let Some(v) = value.first() {
                    builder.number_usize(response_buffer, *v);
                } else {
                    builder.null_string(response_buffer);
                }
            }
            (_, PosResult::WrongType) => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
            }
            (_, PosResult::None) => {
                builder.null_string(response_buffer);
            }
        }
        Ok(())
    }

    pub fn lrange(
        &self,
        list_name: &BytesMut,
        start: i32,
        end: i32,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let list = match self.get_list_metadata_with_name(list_name)? {
            GetListMetadataResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                return Ok(());
            }
            GetListMetadataResult::None => {
                builder.ok(response_buffer);
                return Ok(());
            }
            GetListMetadataResult::Some(list) => list,
        };
        // translate indices
        let (start, end) = self.convert_trim_indices(start, end, list.len() as i32);

        #[derive(PartialEq, Eq, Debug)]
        enum State {
            Skip = 0,
            Include = 1,
        }

        let mut cur_idx = 0u32;
        let mut cur_item_opt: Option<Rc<RefCell<ListItem>>> = None;
        let mut values = Vec::<Rc<RefCell<ListItem>>>::new();
        loop {
            // determine the loop state
            let state = if cur_idx < start || cur_idx > end {
                State::Skip
            } else {
                State::Include
            };

            let cur_item = match self.next(&list, cur_item_opt.clone())? {
                IterResult::WrongType => {
                    builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                    return Ok(());
                }
                IterResult::None => {
                    break;
                }
                IterResult::Some(list_item) => list_item,
            };

            if state == State::Include {
                values.push(cur_item.clone());
            }

            cur_item_opt = Some(cur_item);
            cur_idx = cur_idx.saturating_add(1);
        }

        builder.add_array_len(response_buffer, values.len());
        for item in values.iter() {
            builder.add_bulk_string(response_buffer, &item.borrow().user_data);
        }
        Ok(())
    }

    pub fn ltrim(
        &self,
        list_name: &BytesMut,
        start: i32,
        end: i32,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let mut cache = ListItemCache::new();
        let mut list = match self.get_list_metadata_with_name(list_name)? {
            GetListMetadataResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                return Ok(());
            }
            GetListMetadataResult::None => {
                builder.ok(response_buffer);
                return Ok(());
            }
            GetListMetadataResult::Some(list) => list,
        };

        // translate indices
        let (start, end) = self.convert_trim_indices(start, end, list.len() as i32);

        #[derive(PartialEq, Eq, Debug)]
        enum State {
            Delete = 0,
            Keep = 1,
        }

        let mut cur_idx = 0u32;
        let mut cur_item_opt: Option<Rc<RefCell<ListItem>>> = None;
        loop {
            // determine the loop state
            let state = if cur_idx < start || cur_idx > end {
                State::Delete
            } else {
                State::Keep
            };

            let cur_item = match self.next(&list, cur_item_opt.clone())? {
                IterResult::WrongType => {
                    builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                    return Ok(());
                }
                IterResult::None => {
                    break;
                }
                IterResult::Some(list_item) => list_item,
            };

            if state == State::Delete {
                self.remove_internal(cur_item.borrow().id(), &mut list, &mut cache)?;
            }
            cur_item_opt = Some(cur_item);
            cur_idx = cur_idx.saturating_add(1);
        }

        // Apply changes to the disk
        let mut updates = BatchUpdate::default();
        cache.as_batch_update(&mut updates);

        if list.is_empty() {
            self.delete_list_metadata_internal(list_name, &mut updates)?;
        } else {
            self.put_list_metadata_internal(&list, list_name, &mut updates)?;
        }
        self.store.apply_batch(&updates)?;
        builder.ok(response_buffer);
        Ok(())
    }

    /// Convert range `start,end` into positive indices
    fn convert_trim_indices(&self, start_i32: i32, end_i32: i32, llen: i32) -> (u32, u32) {
        /* convert negative indexes */
        let mut start = if start_i32 < 0 {
            llen.saturating_add(start_i32)
        } else {
            start_i32
        };

        let end = if end_i32 < 0 {
            llen.saturating_add(end_i32)
        } else {
            end_i32
        };

        if start < 0 {
            start = 0;
        }

        if start > end || start >= llen {
            // delete everything
            (u32::MAX, u32::MAX)
        } else {
            (start as u32, end as u32)
        }
    }

    /// Move item from `src` -> `target`
    pub async fn move_item(
        &self,
        src_list_name: &BytesMut,
        target_list_name: &BytesMut,
        src_flags: ListFlags,
        target_flags: ListFlags,
    ) -> Result<MoveResult, SableError> {
        // source list must exist
        let mut src_list_md = match self.get_list_metadata_with_name(src_list_name)? {
            GetListMetadataResult::WrongType => return Ok(MoveResult::WrongType),
            GetListMetadataResult::None => return Ok(MoveResult::None),
            GetListMetadataResult::Some(list) => list,
        };

        let mut target_list_md = match self.get_list_metadata_with_name(target_list_name)? {
            GetListMetadataResult::WrongType => return Ok(MoveResult::WrongType),
            GetListMetadataResult::None => self.new_list_internal(),
            GetListMetadataResult::Some(list) => list,
        };

        let mut cache = ListItemCache::new();
        let Some(v) = self.pop_internal(&mut src_list_md, &mut cache, 1, &src_flags)? else {
            return Ok(MoveResult::None);
        };

        let Some(popped_item) = v.first() else {
            // No item to move
            return Ok(MoveResult::None);
        };

        self.push_internal(
            &mut target_list_md,
            &popped_item.user_data,
            &mut cache,
            &target_flags,
        )?;

        // Create a batch update from the cache
        let mut updates = BatchUpdate::default();
        cache.as_batch_update(&mut updates);

        // delete or update source list if needed
        if src_list_md.is_empty() {
            self.delete_list_metadata_internal(src_list_name, &mut updates)?;
        } else {
            self.put_list_metadata_internal(&src_list_md, src_list_name, &mut updates)?;
        }

        // update target list
        self.put_list_metadata_internal(&target_list_md, target_list_name, &mut updates)?;
        self.store.apply_batch(&updates)?;

        // the clone below is cheap (done on an Rc)
        Ok(MoveResult::Some(popped_item.clone()))
    }

    /// Find an item, by value in the list
    fn find_by_value(
        &self,
        list: &ListValueMetadata,
        value: &BytesMut,
        backward: bool,
    ) -> Result<Option<Rc<RefCell<ListItem>>>, SableError> {
        let mut cur_item_opt = if backward {
            self.prev(list, None)?
        } else {
            self.next(list, None)?
        };
        while let IterResult::Some(cur_item) = cur_item_opt {
            if cur_item.borrow().user_data.eq(value) {
                return Ok(Some(cur_item));
            }

            // next item
            cur_item_opt = if backward {
                self.prev(list, Some(cur_item))?
            } else {
                self.next(list, Some(cur_item))?
            };
        }
        Ok(None)
    }

    /// Return array of positions for all elements matching `user_value`
    /// If `rank` is provided, return matching starting from `nth` match
    /// If `count` is provided, return up to `count` matches. If `rank` and `count` are both
    /// provided, return up to `count` matches starting from the `nth` match
    /// if `maxlen` is provided, limit the number of comparions to `maxlex`
    /// If rank is provided as negative number, scan backward
    fn pos(
        &self,
        list_name: &BytesMut,
        user_value: &BytesMut,
        rank: Option<i32>,
        count: Option<usize>,
        maxlen: Option<usize>,
    ) -> Result<PosResult, SableError> {
        // get the list metadata and ensure it exists and its of type list
        let list = match self.get_list_metadata_with_name(list_name)? {
            GetListMetadataResult::WrongType => return Ok(PosResult::WrongType),
            GetListMetadataResult::None => return Ok(PosResult::None),
            GetListMetadataResult::Some(list) => list,
        };

        let max_match_count = match count {
            Some(0) => usize::MAX,
            Some(count) => count,
            None => 1,
        };

        let mut max_iteration = match maxlen {
            Some(0) | None => usize::MAX,
            Some(n) => n,
        };

        let (mut matches_to_skip, backward) = match rank {
            Some(rank) if rank < 0 => {
                (
                    // negative indices are 1 based
                    // so:
                    // -1 means: start matches from the first match from the end of the list
                    // -2 means: start matches from the second match from the end of the list
                    rank.abs()
                        .try_into()
                        .unwrap_or(usize::MAX)
                        .saturating_sub(1),
                    true,
                )
            }
            Some(0) => return Ok(PosResult::InvalidRank),
            Some(rank) => ((rank as usize).saturating_sub(1), false),
            None => (0usize, false),
        };

        let (mut cur_item_opt, mut cur_idx) = if backward {
            (self.prev(&list, None)?, list.len() as usize - 1)
        } else {
            (self.next(&list, None)?, 0usize)
        };

        let mut result = Vec::<usize>::new();
        while let IterResult::Some(cur_item) = cur_item_opt {
            if cur_item.borrow().user_data.eq(user_value) {
                if matches_to_skip == 0 {
                    // collect this result
                    result.push(cur_idx);
                    // did we reach the COUNT limit?
                    if result.len() == max_match_count {
                        break;
                    }
                } else {
                    // skip this match
                    matches_to_skip = matches_to_skip.saturating_sub(1);
                }
            }

            // did we exhaust our comparison limit? ("MATCHLEN")
            max_iteration = max_iteration.saturating_sub(1);
            if max_iteration == 0 {
                break;
            }

            // next item
            cur_item_opt = if backward {
                cur_idx = cur_idx.saturating_sub(1);
                self.prev(&list, Some(cur_item))?
            } else {
                cur_idx = cur_idx.saturating_add(1);
                self.next(&list, Some(cur_item))?
            };
        }
        Ok(PosResult::Some(result))
    }

    /// Find item by position
    fn find_by_index(&self, list_name: &BytesMut, index: i32) -> Result<IterResult, SableError> {
        let backward = index < 0;
        let pos: usize = if index < 0 {
            index
                .abs()
                .try_into()
                .unwrap_or(usize::MAX)
                .saturating_sub(1)
        } else {
            index as usize
        };

        let list_md = match self.get_list_metadata_with_name(list_name)? {
            GetListMetadataResult::WrongType => {
                return Ok(IterResult::WrongType);
            }
            GetListMetadataResult::None => {
                return Ok(IterResult::None);
            }
            GetListMetadataResult::Some(list) => list,
        };

        if pos as u64 >= list_md.len() {
            return Ok(IterResult::None);
        }

        let mut next_item: Option<Rc<RefCell<ListItem>>> = None;
        for _ in 0..pos + 1 {
            let item = if backward {
                let IterResult::Some(item) = self.prev(&list_md, next_item)? else {
                    return Ok(IterResult::None);
                };
                item
            } else {
                let IterResult::Some(item) = self.next(&list_md, next_item)? else {
                    return Ok(IterResult::None);
                };
                item
            };
            next_item = Some(item);
        }

        if let Some(next_item) = next_item {
            Ok(IterResult::Some(next_item))
        } else {
            Ok(IterResult::None)
        }
    }

    /// Iterate, starting from `curr` item and return the next or previous item
    /// depending on `forward` & `curr` values:
    /// - If `curr` is `None` and `forward` is `true` -> return the list head
    /// - If `curr` is `Some(item)` and `forward` is `true` -> return the item pointed by `curr.next`
    /// - If `curr` is `None` and `forward` is `false` -> return the list tail
    /// - If `curr` is `Some(item)` and `forward` is `false` -> return the item pointed by `curr.prev`
    fn iterate(
        &self,
        list: &ListValueMetadata,
        curr: Option<Rc<RefCell<ListItem>>>,
        forward: bool,
    ) -> Result<IterResult, SableError> {
        let item_id = if let Some(curr) = curr {
            if forward {
                curr.borrow().next
            } else {
                curr.borrow().prev
            }
        } else if forward {
            list.head()
        } else {
            list.tail()
        };

        if item_id == 0 {
            return Ok(IterResult::None);
        }

        let mut item = ListItem::new(list.id(), item_id);
        if !item.load(self.store)? {
            // we have a pointer to a non existing item in the database
            return Ok(IterResult::None);
        }

        Ok(IterResult::Some(Rc::new(RefCell::new(item))))
    }

    /// see `iterate` method
    fn next(
        &self,
        list: &ListValueMetadata,
        curr: Option<Rc<RefCell<ListItem>>>,
    ) -> Result<IterResult, SableError> {
        self.iterate(list, curr, true)
    }

    /// see `iterate` method
    fn prev(
        &self,
        list: &ListValueMetadata,
        curr: Option<Rc<RefCell<ListItem>>>,
    ) -> Result<IterResult, SableError> {
        self.iterate(list, curr, false)
    }

    fn push_internal(
        &self,
        list: &mut ListValueMetadata,
        element: &BytesMut,
        cache: &mut ListItemCache,
        flags: &ListFlags,
    ) -> Result<(), SableError> {
        // do all the manipulation in memory and apply it to the disk
        // as a single batch operation
        if flags.intersects(ListFlags::FromLeft) {
            let left_item = if !list.is_empty() {
                Some(list.head())
            } else {
                None
            };
            self.insert_internal(list, element, None, left_item, cache)?;
        } else {
            let right_item = if !list.is_empty() {
                Some(list.tail())
            } else {
                None
            };
            self.insert_internal(list, element, right_item, None, cache)?;
        }
        Ok(())
    }

    pub fn push(
        &self,
        list_name: &BytesMut,
        elements: &[&BytesMut],
        response_buffer: &mut BytesMut,
        flags: ListFlags,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let mut list = match self.get_list_metadata_with_name(list_name)? {
            GetListMetadataResult::None => {
                if flags.intersects(ListFlags::ListMustExist) {
                    builder.number_usize(response_buffer, 0);
                    return Ok(());
                } else {
                    self.new_list_internal()
                }
            }
            GetListMetadataResult::Some(list) => list,
            GetListMetadataResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                return Ok(());
            }
        };

        // do all the manipulation in memory and apply it to the disk
        // as a single batch operation
        let mut cache = ListItemCache::new();
        for element in elements {
            self.push_internal(&mut list, element, &mut cache, &flags)?;
        }

        // Create a batch update from the cache
        let mut updates = BatchUpdate::default();
        cache.as_batch_update(&mut updates);

        // add the list to the batch update
        self.put_list_metadata_internal(&list, list_name, &mut updates)?;

        // and apply it
        self.store.apply_batch(&updates)?;
        builder.number_u64(response_buffer, list.len());
        Ok(())
    }

    // Remove `item_id` from the list
    fn remove_internal(
        &self,
        item_id: u64,
        list: &mut ListValueMetadata,
        cache: &mut ListItemCache,
    ) -> Result<Option<ListItem>, SableError> {
        let item_key = ListItem::create_key(list.id(), item_id);
        let Some(to_be_removed) = cache.get(&item_key, self.store)? else {
            return Ok(None);
        };

        if to_be_removed.has_previous() {
            let left_item_key = ListItem::create_key(list.id(), to_be_removed.prev);
            let Some(mut left_item) = cache.get(&left_item_key, self.store)? else {
                return Ok(None);
            };
            left_item.set_next(to_be_removed.next);
            if !left_item.has_next() {
                list.set_tail(left_item.id());
            }
            cache.put(left_item);
        }

        if to_be_removed.has_next() {
            let right_item_key = ListItem::create_key(list.id(), to_be_removed.next);
            let Some(mut right_item) = cache.get(&right_item_key, self.store)? else {
                return Ok(None);
            };

            right_item.set_previous(to_be_removed.prev);
            if !right_item.has_previous() {
                list.set_head(right_item.id());
            }
            cache.put(right_item);
        }

        list.set_len(list.len().saturating_sub(1));

        // delete the item
        cache.delete(&to_be_removed);
        Ok(Some(to_be_removed))
    }

    /// Create a new `ListItem` and insert it between `left_id` and `right_id`
    /// If `left_id` is `None`, the new `ListItem` becomes the first item in the list
    /// If `right_id` is `None`, the new `ListItem` becomes the last item in the list
    fn insert_internal(
        &self,
        list: &mut ListValueMetadata,
        element: &BytesMut,
        left_id: Option<u64>,
        right_id: Option<u64>,
        cache: &mut ListItemCache,
    ) -> Result<InsertResult, SableError> {
        let mut new_item = ListItem::new(list.id(), self.store.generate_id());
        new_item.user_data = element.clone();

        match (left_id, right_id) {
            (Some(left_id), Some(right_id)) => {
                // load the left item from the database
                let left_key = ListItem::create_key(list.id(), left_id);
                let right_key = ListItem::create_key(list.id(), right_id);
                let Some(mut left_item) = cache.get(&left_key, self.store)? else {
                    return Ok(InsertResult::NotFound);
                };

                let Some(mut right_item) = cache.get(&right_key, self.store)? else {
                    return Ok(InsertResult::NotFound);
                };
                new_item.insert_between(Some(&mut left_item), Some(&mut right_item))?;
                // update the cache
                cache.put(left_item);
                cache.put(right_item);
            }
            (Some(left_id), None) => {
                // new list
                let left_key = ListItem::create_key(list.id(), left_id);
                let Some(mut left_item) = cache.get(&left_key, self.store)? else {
                    return Ok(InsertResult::NotFound);
                };
                new_item.insert_between(Some(&mut left_item), None)?;
                // the new item is now the new tail
                list.set_tail(new_item.id());

                // update the cache
                cache.put(left_item);
            }
            (None, Some(right_id)) => {
                let right_key = ListItem::create_key(list.id(), right_id);
                let Some(mut right_item) = cache.get(&right_key, self.store)? else {
                    return Ok(InsertResult::NotFound);
                };
                new_item.insert_between(None, Some(&mut right_item))?;

                // the new item is now the new head
                list.set_head(new_item.id());

                // update the cache
                cache.put(right_item);
            }
            (None, None) => {
                // list is empty (probably a new list)
                list.set_head(new_item.id());
                list.set_tail(new_item.id());
            }
        }

        // update the list length and store it
        list.set_len(list.len().saturating_add(1));

        // store the new element
        cache.put(new_item.clone());
        Ok(InsertResult::Some(new_item))
    }

    /// ---
    /// List database API
    /// ---

    fn get_list_metadata_with_name(
        &self,
        list_name: &BytesMut,
    ) -> Result<GetListMetadataResult, SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(list_name, self.db_id);
        if let Some(mut value) = self.store.get(&internal_key)? {
            let mut reader = U8ArrayReader::with_buffer(&value);
            let common_md = CommonValueMetadata::from_bytes(&mut reader)?;

            if !common_md.is_list() {
                return Ok(GetListMetadataResult::WrongType);
            }

            let mut reader = U8ArrayReader::with_buffer(&value);
            let md = ListValueMetadata::from_bytes(&mut reader)?;
            if md.expiration().is_expired()? {
                self.store.delete(&internal_key)?;
                Ok(GetListMetadataResult::None)
            } else {
                let _ = value.split_to(ListValueMetadata::SIZE);
                Ok(GetListMetadataResult::Some(md))
            }
        } else {
            Ok(GetListMetadataResult::None)
        }
    }

    /// Delete `ListValueMetadata` from the database
    fn delete_list_metadata_internal(
        &self,
        list_name: &BytesMut,
        updates: &mut BatchUpdate,
    ) -> Result<(), SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(list_name, self.db_id);
        updates.delete(internal_key);
        Ok(())
    }

    /// Put `ListValueMetadata` into the database
    fn put_list_metadata_internal(
        &self,
        metadata: &ListValueMetadata,
        list_name: &BytesMut,
        updates: &mut BatchUpdate,
    ) -> Result<(), SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(list_name, self.db_id);
        let mut buf = BytesMut::with_capacity(ListValueMetadata::SIZE);
        let mut builder = U8ArrayBuilder::with_buffer(&mut buf);
        metadata.to_bytes(&mut builder);
        updates.put(internal_key, buf);
        Ok(())
    }

    /// Create new `ListValueMetadata` for `user_key` store and reutrn it to the caller
    fn new_list_internal(&self) -> ListValueMetadata {
        let mut md = ListValueMetadata::new();
        let list_id = self.store.generate_id();
        md.set_id(list_id);
        md
    }
}

/// the order of the fields matters here
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListItem {
    pub key_type: u8,
    pub list_id: u64,
    pub item_id: u64,
    pub prev: u64,
    pub next: u64,
    pub user_data: BytesMut,
}

#[allow(dead_code)]
impl ListItem {
    // SIZE contain only the serialisable items
    pub const SIZE: usize = 4 * std::mem::size_of::<u64>() + std::mem::size_of::<u8>();
    pub const KEY_LIST_ITEM: u8 = 1u8;

    pub fn id(&self) -> u64 {
        self.item_id
    }

    pub fn prev(&self) -> Option<u64> {
        if self.has_previous() {
            Some(self.prev)
        } else {
            None
        }
    }

    pub fn next(&self) -> Option<u64> {
        if self.has_next() {
            Some(self.next)
        } else {
            None
        }
    }

    /// Serialise this `ListItem` by creating two `BytesMut` objects:
    /// One for the `key` and the other for the `value`
    pub fn serialise(&self) -> (BytesMut, BytesMut) {
        (self.serialised_key(), self.serialised_value())
    }

    /// Save the current list item into the database
    pub fn save(&self, store: &StorageAdapter, put_flags: PutFlags) -> Result<(), SableError> {
        let (key, val) = self.serialise();
        store.put(&key, &val, put_flags)
    }

    /// Load list item from the database based on its parent list ID and its own ID
    /// Note: `self.list_id` and `self.item_id` must be set before calling this
    /// function
    pub fn load(&mut self, store: &StorageAdapter) -> Result<bool, SableError> {
        // as the key, we use:
        // `key_type`, `list_id` and `item_id`
        // in as the value:
        // `left`, `right` and `user_data`
        let mut key = BytesMut::with_capacity(
            std::mem::size_of_val(&self.key_type)
                + std::mem::size_of_val(&self.list_id)
                + std::mem::size_of_val(&self.item_id),
        );

        key.extend_from_slice(&BytesMutUtils::from_u8(&self.key_type));
        key.extend_from_slice(&BytesMutUtils::from_u64(&self.list_id));
        key.extend_from_slice(&BytesMutUtils::from_u64(&self.item_id));

        let Some(mut value) = store.get(&key)? else {
            return Ok(false); // not found
        };

        // decode the value
        self.construct_from(&mut key, &mut value)?;
        Ok(true)
    }

    /// Delete the `ListItem` from the database
    pub fn delete(&self, store: &StorageAdapter) -> Result<(), SableError> {
        // as the key, we use:
        // `key_type`, `list_id` and `item_id`
        // in as the value:
        // `left`, `right` and `user_data`
        let mut key = BytesMut::with_capacity(
            std::mem::size_of_val(&self.key_type)
                + std::mem::size_of_val(&self.list_id)
                + std::mem::size_of_val(&self.item_id),
        );

        key.extend_from_slice(&BytesMutUtils::from_u8(&self.key_type));
        key.extend_from_slice(&BytesMutUtils::from_u64(&self.list_id));
        key.extend_from_slice(&BytesMutUtils::from_u64(&self.item_id));
        store.delete(&key)
    }

    /// insert this list item between `item_left` and `item_right`
    pub fn insert_between(
        &mut self,
        item_left: Option<&mut ListItem>,
        item_right: Option<&mut ListItem>,
    ) -> Result<(), SableError> {
        match (item_left, item_right) {
            (None, None) => {}
            (Some(item_left), None) => {
                // last item
                if item_left.list_id != self.list_id {
                    return Err(SableError::InvalidArgument(
                        "items must be part of the same list".to_string(),
                    ));
                }
                item_left.next = self.item_id;
                self.prev = item_left.item_id;
                self.next = 0;
            }
            (None, Some(item_right)) => {
                // first item
                if item_right.list_id != self.list_id {
                    return Err(SableError::InvalidArgument(
                        "items must be part of the same list".to_string(),
                    ));
                }
                self.prev = 0;
                self.next = item_right.item_id;
                item_right.prev = self.item_id;
            }
            (Some(item_left), Some(item_right)) => {
                // item_left and item_right must be "connected"
                if item_left.list_id != item_right.list_id {
                    return Err(SableError::InvalidArgument(
                        "left and right items are not connected".to_string(),
                    ));
                }

                if item_left.list_id != self.list_id || item_right.list_id != self.list_id {
                    return Err(SableError::InvalidArgument(
                        "items must be part of the same list".to_string(),
                    ));
                }

                self.next = item_right.item_id;
                self.prev = item_left.item_id;
                item_left.next = self.item_id;
                item_right.prev = self.item_id;
            }
        }
        Ok(())
    }

    pub fn has_previous(&self) -> bool {
        self.prev != 0
    }

    pub fn has_next(&self) -> bool {
        self.next != 0
    }

    /// is this element the first element of the list?
    pub fn is_first(&self) -> bool {
        self.prev == 0
    }

    /// is this element the last element of the list?
    pub fn is_last(&self) -> bool {
        self.next == 0
    }

    /// Set the key type
    fn set_type(&mut self, key_type: u8) {
        self.key_type = key_type;
    }

    fn set_list_id(&mut self, list_id: u64) {
        self.list_id = list_id;
    }

    fn set_id(&mut self, item_id: u64) {
        self.item_id = item_id;
    }

    fn set_previous(&mut self, item_id: u64) {
        self.prev = item_id;
    }

    fn set_next(&mut self, item_id: u64) {
        self.next = item_id;
    }

    /// Build key from `list_id` and `item_id`
    pub fn create_key(list_id: u64, item_id: u64) -> BytesMut {
        let mut key = BytesMut::with_capacity(
            std::mem::size_of::<u8>() + std::mem::size_of::<u64>() + std::mem::size_of::<u64>(),
        );

        key.extend_from_slice(&BytesMutUtils::from_u8(&ListItem::KEY_LIST_ITEM));
        key.extend_from_slice(&BytesMutUtils::from_u64(&list_id));
        key.extend_from_slice(&BytesMutUtils::from_u64(&item_id));
        key
    }

    /// Create list item with a given id and list-id
    pub fn new(list_id: u64, item_id: u64) -> ListItem {
        let mut list_item = ListItem {
            key_type: ListItem::KEY_LIST_ITEM,
            list_id: 0,
            item_id: 0,
            prev: 0,
            next: 0,
            user_data: BytesMut::new(),
        };
        list_item.set_list_id(list_id);
        list_item.set_id(item_id);
        list_item
    }

    /// Load list item from the store
    pub fn from_storage(
        list_id: u64,
        item_id: u64,
        store: &StorageAdapter,
    ) -> Result<Option<ListItem>, SableError> {
        let mut list_item = ListItem::new(list_id, item_id);
        if !list_item.load(store)? {
            Ok(None)
        } else {
            Ok(Some(list_item))
        }
    }
}

impl Storable for ListItem {
    /// Return a serialised key for this `ListItem`
    fn serialised_key(&self) -> BytesMut {
        Self::create_key(self.list_id, self.item_id)
    }

    /// Return a serialised value for this `ListItem`
    fn serialised_value(&self) -> BytesMut {
        let mut val = BytesMut::with_capacity(
            std::mem::size_of_val(&self.prev)
                + std::mem::size_of_val(&self.next)
                + self.user_data.len(),
        );

        val.extend_from_slice(&BytesMutUtils::from_u64(&self.prev));
        val.extend_from_slice(&BytesMutUtils::from_u64(&self.next));
        val.extend_from_slice(&self.user_data);

        val
    }

    /// Given a serialised value, unpack it into the proper properties
    fn construct_from(
        &mut self,
        key: &mut BytesMut,
        value: &mut BytesMut,
    ) -> Result<(), SableError> {
        // decode the key
        self.key_type = BytesMutUtils::to_u8(key);
        let _ = key.split_to(std::mem::size_of_val(&self.key_type));

        self.list_id = BytesMutUtils::to_u64(key);
        let _ = key.split_to(std::mem::size_of_val(&self.list_id));

        self.item_id = BytesMutUtils::to_u64(key);
        let _ = key.split_to(std::mem::size_of_val(&self.item_id));

        // decode the value
        self.prev = BytesMutUtils::to_u64(value);
        let _ = value.split_to(std::mem::size_of_val(&self.prev));

        self.next = BytesMutUtils::to_u64(value);
        let _ = value.split_to(std::mem::size_of_val(&self.prev));

        self.user_data = value.clone();
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
    use crate::StorageOpenParams;
    use std::fs;
    use std::path::PathBuf;
    use test_case::test_case;

    /// Helper macro for building database for tests
    macro_rules! prepare_db {
        ($dbpath:expr) => {{
            let _ = std::fs::create_dir_all("tests");
            let db_path = PathBuf::from($dbpath);
            let _ = fs::remove_dir_all(db_path.clone());
            let open_params = StorageOpenParams::default()
                .set_compression(false)
                .set_cache_size(64)
                .set_path(&db_path)
                .set_wal_disabled(true);
            crate::storage_rocksdb!(open_params)
        }};
    }

    #[test]
    fn test_serialise_list_item() -> Result<(), SableError> {
        let store = prepare_db!("tests/test_serialise_list_item.db");

        let mut src = ListItem::new(1975, 42);
        src.prev = 10;
        src.next = 11;
        src.user_data = BytesMut::from("hello world");
        src.save(&store, PutFlags::Override)?;

        let mut target = ListItem::new(1975, 42);
        assert_eq!(target.load(&store)?, true);
        assert_eq!(target.list_id, src.list_id);
        assert_eq!(target.prev, src.prev);
        assert_eq!(target.next, src.next);
        assert_eq!(target.user_data, src.user_data);
        Ok(())
    }

    #[test]
    fn test_list_item_insert() -> Result<(), SableError> {
        let mut a = ListItem::new(1, 1);
        let mut b = ListItem::new(1, 2);
        let mut c = ListItem::new(1, 3);

        b.insert_between(Some(&mut a), None)?;
        c.insert_between(Some(&mut a), Some(&mut b))?;
        assert_eq!(a.prev, 0);
        assert_eq!(a.next, c.item_id);
        assert_eq!(b.prev, c.item_id);
        assert_eq!(b.next, 0);
        assert_eq!(c.prev, 1);
        assert_eq!(c.next, 2);
        assert!(a.is_first());
        assert!(b.is_last());
        Ok(())
    }

    #[test]
    fn test_push_pop() -> Result<(), SableError> {
        let store = prepare_db!("tests/test_push_pop.db");
        let list_name = BytesMut::from("my list");
        let list = List::with_storage(&store, 0);
        let elements = [
            &BytesMut::from("hello"),
            &BytesMut::from("world"),
            &BytesMut::from("foo"),
        ];
        let mut response_buffer = BytesMut::new();
        // loop to reuse the same list
        for _ in 0..5 {
            list.push(
                &list_name,
                &elements,
                &mut response_buffer,
                ListFlags::FromLeft,
            )?;

            list.len(&list_name, &mut response_buffer)?;
            // our list should have 3 elements
            assert_eq!(
                BytesMutUtils::to_string(&response_buffer).as_str(),
                ":3\r\n"
            );

            list.pop(&list_name, 1, &mut response_buffer, ListFlags::FromLeft)?;
            assert_eq!(
                BytesMutUtils::to_string(&response_buffer).as_str(),
                "$3\r\nfoo\r\n"
            );
            list.pop(&list_name, 1, &mut response_buffer, ListFlags::FromLeft)?;
            assert_eq!(
                BytesMutUtils::to_string(&response_buffer).as_str(),
                "$5\r\nworld\r\n"
            );

            list.pop(&list_name, 1, &mut response_buffer, ListFlags::FromLeft)?;
            assert_eq!(
                BytesMutUtils::to_string(&response_buffer).as_str(),
                "$5\r\nhello\r\n"
            );

            list.pop(&list_name, 1, &mut response_buffer, ListFlags::FromLeft)?;
            assert_eq!(
                BytesMutUtils::to_string(&response_buffer).as_str(),
                "$-1\r\n"
            );
        }
        Ok(())
    }

    #[test]
    fn test_list_iterate_forward() -> Result<(), SableError> {
        let store = prepare_db!("tests/test_iterate_list_forward.db");
        let list_name = BytesMut::from("my list");
        let list = List::with_storage(&store, 0);
        let elements = [
            &BytesMut::from("hello"),
            &BytesMut::from("world"),
            &BytesMut::from("foo"),
            &BytesMut::from("bar"),
            &BytesMut::from("baz"),
        ];
        let mut response_buffer = BytesMut::new();
        // append items to the end of the list
        list.push(&list_name, &elements, &mut response_buffer, ListFlags::None)?;
        list.len(&list_name, &mut response_buffer)?;
        assert_eq!(
            BytesMutUtils::to_string(&response_buffer).as_str(),
            ":5\r\n"
        );

        let list_md = match list.get_list_metadata_with_name(&list_name)? {
            GetListMetadataResult::WrongType | GetListMetadataResult::None => {
                return Err(SableError::NotFound)
            }
            GetListMetadataResult::Some(list_md) => list_md,
        };

        let mut prev_item: Option<Rc<RefCell<ListItem>>> = None;
        for i in 0..elements.len() {
            let IterResult::Some(item) = list.next(&list_md, prev_item.clone())? else {
                panic!("failed to fetch first item from list");
            };
            assert_eq!(&item.borrow().user_data, elements.get(i).unwrap());
            prev_item = Some(item);
        }
        Ok(())
    }

    #[test]
    fn test_list_iterate_backward() -> Result<(), SableError> {
        let store = prepare_db!("tests/test_iterate_list_backward.db");
        let list_name = BytesMut::from("my list");
        let list = List::with_storage(&store, 0);
        let elements = [
            &BytesMut::from("hello"),
            &BytesMut::from("world"),
            &BytesMut::from("foo"),
            &BytesMut::from("bar"),
            &BytesMut::from("baz"),
        ];
        let mut response_buffer = BytesMut::new();
        // append items to the end of the list
        list.push(&list_name, &elements, &mut response_buffer, ListFlags::None)?;
        list.len(&list_name, &mut response_buffer)?;
        assert_eq!(
            BytesMutUtils::to_string(&response_buffer).as_str(),
            ":5\r\n"
        );

        let list_md = match list.get_list_metadata_with_name(&list_name)? {
            GetListMetadataResult::WrongType | GetListMetadataResult::None => {
                return Err(SableError::NotFound)
            }
            GetListMetadataResult::Some(list_md) => list_md,
        };

        let mut prev_item: Option<Rc<RefCell<ListItem>>> = None;
        for i in (0..elements.len()).rev() {
            let IterResult::Some(item) = list.prev(&list_md, prev_item.clone())? else {
                panic!("failed to fetch first item from list");
            };
            assert_eq!(&item.borrow().user_data, elements.get(i).unwrap());
            prev_item = Some(item);
        }
        Ok(())
    }

    #[test_case(0, Some("hello"); "index_0")]
    #[test_case(1, Some("world"); "index_1")]
    #[test_case(2, Some("foo"); "index_2")]
    #[test_case(3, Some("bar"); "index_3")]
    #[test_case(4, Some("baz"); "index_4")]
    #[test_case(-1, Some("baz"); "index_1_neg")]
    #[test_case(-2, Some("bar"); "index_2_neg")]
    #[test_case(-5, Some("hello"); "index_5_neg")]
    #[test_case(-6, None; "index_neg_out_of_bounds")]
    #[test_case(5, None; "index_out_of_bounds")]
    fn test_list_index(index: i32, element: Option<&'static str>) -> Result<(), SableError> {
        let store = prepare_db!(&format!("tests/test_list_index{}.db", index));
        let list_name = BytesMut::from("my list");
        let list = List::with_storage(&store, 0);
        let elements = [
            &BytesMut::from("hello"),
            &BytesMut::from("world"),
            &BytesMut::from("foo"),
            &BytesMut::from("bar"),
            &BytesMut::from("baz"),
        ];
        let mut response_buffer = BytesMut::new();
        // append items to the end of the list
        list.push(&list_name, &elements, &mut response_buffer, ListFlags::None)?;

        // get elemenet at position 0
        let builder = RespBuilderV2::default();
        let mut expected_response = BytesMut::new();
        list.index(&list_name, index, &mut response_buffer)?;
        if let Some(element) = element {
            builder.bulk_string(&mut expected_response, &BytesMut::from(element));
        } else {
            builder.null_string(&mut expected_response);
        }
        assert_eq!(response_buffer, expected_response);
        Ok(())
    }

    #[test_case(2, 2, 1, false; "start and end are equal")]
    #[test_case(3, 3, 1, false; "start and end are equal #2")]
    #[test_case(0, -1, 5, false; "end is negative index")]
    #[test_case(10, 2, 0, true; "start is gt end")]
    #[test_case(-5, 3, 4, false; "start is negative end is in range")]
    #[test_case(-4, 3, 3, false; "start is negative end is in range #2")]
    #[test_case(-5, -10, 0, true; "start is greater than end #2")]
    #[test_case(1, 2, 2, false; "happy path")]
    fn test_list_trim(
        start: i32,
        end: i32,
        expected_len: u64,
        list_deleted: bool,
    ) -> Result<(), SableError> {
        let store = prepare_db!(&format!("tests/test_list_trim_{}_{}.db", start, end));
        let list_name = BytesMut::from("my list");
        let list = List::with_storage(&store, 0);
        let elements = [
            &BytesMut::from("hello"),
            &BytesMut::from("world"),
            &BytesMut::from("foo"),
            &BytesMut::from("bar"),
            &BytesMut::from("baz"),
        ];
        let mut response_buffer = BytesMut::new();
        // append items to the end of the list
        list.push(&list_name, &elements, &mut response_buffer, ListFlags::None)?;

        // trim the list
        list.ltrim(&list_name, start, end, &mut response_buffer)?;

        let list_md = match list.get_list_metadata_with_name(&list_name)? {
            GetListMetadataResult::WrongType | GetListMetadataResult::None => {
                if list_deleted {
                    return Ok(());
                } else {
                    return Err(SableError::NotFound);
                }
            }
            GetListMetadataResult::Some(list_md) => list_md,
        };

        assert_eq!(list_md.len(), expected_len);
        Ok(())
    }
}
