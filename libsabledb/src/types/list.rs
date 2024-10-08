#[allow(unused_imports)]
use crate::{
    commands::Strings,
    iter_next_or_prev, list_md_or_null_string, list_or_size_0,
    metadata::{
        Bookkeeping, CommonValueMetadata, FromRaw, KeyType, ListValueMetadata, PrimaryKeyMetadata,
        ValueType,
    },
    storage::PutFlags,
    storage::{DbWriteCache, ListFlags},
    BatchUpdate, BytesMutUtils, RespBuilderV2, SableError, StorageAdapter, U8ArrayBuilder,
    U8ArrayReader,
};

use bytes::BytesMut;
use std::cell::RefCell;
use std::rc::Rc;

pub struct List<'a> {
    store: &'a StorageAdapter,
    cache: Box<DbWriteCache<'a>>,
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
    ListNotFound,
    None,
}
#[derive(Debug, Clone)]
enum InsertResult {
    NotFound,
    Some(()),
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

impl<'a> List<'a> {
    pub fn with_storage(store: &'a StorageAdapter, db_id: u16) -> Self {
        List {
            store,
            db_id,
            cache: Box::new(DbWriteCache::with_storage(store)),
        }
    }

    /// Return the list size
    pub fn len(
        &mut self,
        list_name: &BytesMut,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let list = list_or_size_0!(self, list_name, response_buffer, builder);
        builder.number_u64(response_buffer, list.len());
        Ok(())
    }

    /// pop `count` items from the first non empty list
    /// If this function could not find a list with items,
    /// return `BlockingCommandResult::WouldBlock` to the caller
    pub fn blocking_pop(
        &mut self,
        lists: &[&BytesMut],
        count: usize,
        response_buffer: &mut BytesMut,
        flags: ListFlags,
    ) -> Result<BlockingCommandResult, SableError> {
        let builder = RespBuilderV2::default();
        match self.blocking_pop_internal(lists, count, flags)? {
            BlockingPopInternalResult::WouldBlock => Ok(BlockingCommandResult::WouldBlock),
            BlockingPopInternalResult::InvalidArguments => {
                builder.error_string(response_buffer, Strings::SYNTAX_ERROR);
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

    /// Insert `element` after or before `pivot` element
    pub fn linsert(
        &mut self,
        list_name: &BytesMut,
        element: &BytesMut,
        pivot: &BytesMut,
        response_buffer: &mut BytesMut,
        flags: ListFlags,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let mut list_md = match self.get_list_metadata_with_name(list_name)? {
            GetListMetadataResult::WrongType => {
                builder.error_string(response_buffer, Strings::WRONGTYPE);
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

        let _ = self.insert_internal(&mut list_md, element, left_id, right_id)?;

        // update the storage
        self.put_list_metadata_internal(&list_md, list_name)?;
        self.flush_cache()?;

        // return the list length after a successful insert operation.
        builder.number_u64(response_buffer, list_md.len());
        Ok(())
    }

    /// First `count` elements from the non empty list from the `list_names`
    pub fn multi_pop(
        &mut self,
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
        &mut self,
        list_name: &BytesMut,
        count: usize,
        response_buffer: &mut BytesMut,
        flags: ListFlags,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let mut list = list_md_or_null_string!(self, list_name, response_buffer, builder);
        let mut output_arr = Vec::<ListItem>::with_capacity(count);
        for _ in 0..count {
            let item_to_remove = if flags.intersects(ListFlags::FromLeft) {
                list.head()
            } else {
                list.tail()
            };

            if let Some(removed_item) = self.remove_internal(item_to_remove, &mut list)? {
                output_arr.push(removed_item);
            }
        }

        if list.is_empty() {
            self.delete_list_metadata_internal(&list, list_name)?;
        } else {
            self.put_list_metadata_internal(&list, list_name)?;
        }

        self.flush_cache()?;

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

    /// Returns the element at index `index` in the list stored at key. The `index` is zero-based
    /// so `0` means the first element, `1` the second element and so on.
    /// Negative indices can be used to designate elements starting at the tail of the list.
    /// Here, `-1` means the last element, `-2` means the penultimate and so forth.
    /// When the value at key is not a list, an error is returned
    pub fn index(
        &mut self,
        list_name: &BytesMut,
        index: i32,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let item = match self.find_by_index(list_name, index)? {
            IterResult::None | IterResult::ListNotFound => {
                builder.null_string(response_buffer);
                return Ok(());
            }
            IterResult::WrongType => {
                builder.error_string(response_buffer, Strings::WRONGTYPE);
                return Ok(());
            }
            IterResult::Some(item) => item,
        };
        builder.bulk_string(response_buffer, &item.borrow().user_data);
        Ok(())
    }

    /// Update element at a given position with `user_value`
    pub fn set(
        &mut self,
        list_name: &BytesMut,
        index: i32,
        user_value: BytesMut,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let item = match self.find_by_index(list_name, index)? {
            IterResult::None => {
                builder.error_string(response_buffer, Strings::INDEX_OUT_OF_BOUNDS);
                return Ok(());
            }
            IterResult::WrongType => {
                builder.error_string(response_buffer, Strings::WRONGTYPE);
                return Ok(());
            }
            IterResult::ListNotFound => {
                builder.error_string(response_buffer, "ERR no such key");
                return Ok(());
            }
            IterResult::Some(item) => item,
        };
        item.borrow_mut().user_data = user_value;
        item.borrow_mut().save(&mut self.cache)?;
        self.flush_cache()?;
        builder.ok(response_buffer);
        Ok(())
    }

    /// Remove `count` elements from the list that matches `element`.
    /// If `count` is lower than `0`, remove from tail
    /// If the list is empty, delete it
    pub fn remove(
        &mut self,
        list_name: &BytesMut,
        element: Option<&BytesMut>,
        count: i32,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let mut list_md = match self.get_list_metadata_with_name(list_name)? {
            GetListMetadataResult::WrongType => {
                builder.error_string(response_buffer, Strings::WRONGTYPE);
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
                self.remove_internal(cur_item.borrow().id(), &mut list_md)?;
                items_removed = items_removed.saturating_add(1);

                // limit the removed items if count is greater than 0
                if count.gt(&0) && items_removed.ge(&count) {
                    break;
                }
            }
            cur_item_opt = iter_next_or_prev!(self, list_md, Some(cur_item), backward);
        }

        if list_md.is_empty() {
            self.delete_list_metadata_internal(&list_md, list_name)?;
        } else {
            self.put_list_metadata_internal(&list_md, list_name)?;
        }

        self.flush_cache()?;
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
        &mut self,
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
                builder.error_string(response_buffer, Strings::LIST_RANK_INVALID);
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
                builder.error_string(response_buffer, Strings::WRONGTYPE);
            }
            (_, PosResult::None) => {
                builder.null_string(response_buffer);
            }
        }
        Ok(())
    }

    pub fn lrange(
        &mut self,
        list_name: &BytesMut,
        start: i32,
        end: i32,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let list = match self.get_list_metadata_with_name(list_name)? {
            GetListMetadataResult::WrongType => {
                builder.error_string(response_buffer, Strings::WRONGTYPE);
                return Ok(());
            }
            GetListMetadataResult::None => {
                builder.null_array(response_buffer);
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
                    builder.error_string(response_buffer, Strings::WRONGTYPE);
                    return Ok(());
                }
                IterResult::None | IterResult::ListNotFound /* shouldn't happen as we check this before*/=> {
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
        &mut self,
        list_name: &BytesMut,
        start: i32,
        end: i32,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let mut list = match self.get_list_metadata_with_name(list_name)? {
            GetListMetadataResult::WrongType => {
                builder.error_string(response_buffer, Strings::WRONGTYPE);
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
                    builder.error_string(response_buffer, Strings::WRONGTYPE);
                    return Ok(());
                }
                IterResult::None | IterResult::ListNotFound => {
                    break;
                }
                IterResult::Some(list_item) => list_item,
            };

            if state == State::Delete {
                self.remove_internal(cur_item.borrow().id(), &mut list)?;
            }
            cur_item_opt = Some(cur_item);
            cur_idx = cur_idx.saturating_add(1);
        }

        if list.is_empty() {
            self.delete_list_metadata_internal(&list, list_name)?;
        } else {
            self.put_list_metadata_internal(&list, list_name)?;
        }
        self.flush_cache()?;
        builder.ok(response_buffer);
        Ok(())
    }

    pub fn push(
        &mut self,
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
                    self.new_list_internal(list_name)?
                }
            }
            GetListMetadataResult::Some(list) => list,
            GetListMetadataResult::WrongType => {
                builder.error_string(response_buffer, Strings::WRONGTYPE);
                return Ok(());
            }
        };

        for element in elements {
            self.push_internal(&mut list, element, &flags)?;
        }

        // add the list to the batch update
        self.put_list_metadata_internal(&list, list_name)?;

        // and apply it
        self.flush_cache()?;
        builder.number_u64(response_buffer, list.len());
        Ok(())
    }

    /// pop `count` items from the first non empty list
    /// If this function could not find a list with items,
    /// return `BlockingCommandResult::WouldBlock` to the caller
    fn blocking_pop_internal(
        &mut self,
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
            let Some(v) = self.pop_internal(&mut list_md, count, &flags)? else {
                continue;
            };

            if v.is_empty() {
                continue;
            }

            // delete or update source list if needed
            if list_md.is_empty() {
                self.delete_list_metadata_internal(&list_md, list_name)?;
            } else {
                self.put_list_metadata_internal(&list_md, list_name)?;
            }
            self.flush_cache()?;

            // Build RESP response
            return Ok(BlockingPopInternalResult::Some(((*list_name).clone(), v)));
        }

        Ok(BlockingPopInternalResult::WouldBlock)
    }

    fn pop_internal(
        &mut self,
        list: &mut ListValueMetadata,
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

            let Some(removed_item) = self.remove_internal(item_to_remove, list)? else {
                break;
            };
            items_popped.push(Rc::new(removed_item));
            count = count.saturating_sub(1);
        }

        Ok(Some(items_popped))
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
        &mut self,
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
            GetListMetadataResult::None => self.new_list_internal(target_list_name)?,
            GetListMetadataResult::Some(list) => list,
        };

        let Some(v) = self.pop_internal(&mut src_list_md, 1, &src_flags)? else {
            return Ok(MoveResult::None);
        };

        let Some(popped_item) = v.first() else {
            // No item to move
            return Ok(MoveResult::None);
        };

        self.push_internal(&mut target_list_md, &popped_item.user_data, &target_flags)?;

        // delete or update source list if needed
        if src_list_md.is_empty() {
            self.delete_list_metadata_internal(&src_list_md, src_list_name)?;
        } else {
            self.put_list_metadata_internal(&src_list_md, src_list_name)?;
        }

        // update target list
        self.put_list_metadata_internal(&target_list_md, target_list_name)?;
        self.flush_cache()?;

        // the clone below is cheap (done on an Rc)
        Ok(MoveResult::Some(popped_item.clone()))
    }

    ///--------------------------------------------
    /// Privae methods
    ///--------------------------------------------

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
        &mut self,
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
    fn find_by_index(
        &mut self,
        list_name: &BytesMut,
        index: i32,
    ) -> Result<IterResult, SableError> {
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
                return Ok(IterResult::ListNotFound);
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
        if !item.load(&self.cache)? {
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
        &mut self,
        list: &mut ListValueMetadata,
        element: &BytesMut,
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
            self.insert_internal(list, element, None, left_item)?;
        } else {
            let right_item = if !list.is_empty() {
                Some(list.tail())
            } else {
                None
            };
            self.insert_internal(list, element, right_item, None)?;
        }
        Ok(())
    }

    // Remove `item_id` from the list
    fn remove_internal(
        &mut self,
        item_id: u64,
        list: &mut ListValueMetadata,
    ) -> Result<Option<ListItem>, SableError> {
        let Some(mut to_be_removed) = self.get_list_item_by_key(list.id(), item_id)? else {
            return Ok(None);
        };

        if to_be_removed.has_previous() {
            let Some(mut left_item) = self.get_list_item_by_key(list.id(), to_be_removed.prev)?
            else {
                return Ok(None);
            };
            left_item.set_next(to_be_removed.next);
            if !left_item.has_next() {
                list.set_tail(left_item.id());
            }
            left_item.save(&mut self.cache)?;
        }

        if to_be_removed.has_next() {
            let Some(mut right_item) = self.get_list_item_by_key(list.id(), to_be_removed.next)?
            else {
                return Ok(None);
            };

            right_item.set_previous(to_be_removed.prev);
            if !right_item.has_previous() {
                list.set_head(right_item.id());
            }
            right_item.save(&mut self.cache)?;
        }

        list.set_len(list.len().saturating_sub(1));

        // delete the item
        to_be_removed.delete(&mut self.cache)?;
        Ok(Some(to_be_removed))
    }

    fn get_list_item_by_key(
        &mut self,
        list_id: u64,
        item_id: u64,
    ) -> Result<Option<ListItem>, SableError> {
        ListItem::from_storage(list_id, item_id, &self.cache)
    }

    /// Create a new `ListItem` and insert it between `left_id` and `right_id`
    /// If `left_id` is `None`, the new `ListItem` becomes the first item in the list
    /// If `right_id` is `None`, the new `ListItem` becomes the last item in the list
    fn insert_internal(
        &mut self,
        list: &mut ListValueMetadata,
        element: &BytesMut,
        left_id: Option<u64>,
        right_id: Option<u64>,
    ) -> Result<InsertResult, SableError> {
        let mut new_item = ListItem::new(list.id(), self.store.generate_id());
        new_item.user_data = element.clone();

        match (left_id, right_id) {
            (Some(left_id), Some(right_id)) => {
                // load the left item from the database
                let Some(mut left_item) = self.get_list_item_by_key(list.id(), left_id)? else {
                    return Ok(InsertResult::NotFound);
                };

                let Some(mut right_item) = self.get_list_item_by_key(list.id(), right_id)? else {
                    return Ok(InsertResult::NotFound);
                };
                new_item.insert_between(Some(&mut left_item), Some(&mut right_item))?;
                left_item.save(&mut self.cache)?;
                right_item.save(&mut self.cache)?;
            }
            (Some(left_id), None) => {
                // new list
                let Some(mut left_item) = self.get_list_item_by_key(list.id(), left_id)? else {
                    return Ok(InsertResult::NotFound);
                };
                new_item.insert_between(Some(&mut left_item), None)?;

                // the new item is now the new tail
                list.set_tail(new_item.id());

                // update the cache
                left_item.save(&mut self.cache)?;
            }
            (None, Some(right_id)) => {
                let Some(mut right_item) = self.get_list_item_by_key(list.id(), right_id)? else {
                    return Ok(InsertResult::NotFound);
                };
                new_item.insert_between(None, Some(&mut right_item))?;

                // the new item is now the new head
                list.set_head(new_item.id());

                // update the cache
                right_item.save(&mut self.cache)?;
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
        new_item.save(&mut self.cache)?;
        Ok(InsertResult::Some(()))
    }

    /// ---
    /// List database API
    /// ---

    fn get_list_metadata_with_name(
        &mut self,
        list_name: &BytesMut,
    ) -> Result<GetListMetadataResult, SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(list_name, self.db_id);
        if let Some(mut value) = self.cache.get(&internal_key)? {
            let mut reader = U8ArrayReader::with_buffer(&value);
            let common_md = CommonValueMetadata::from_bytes(&mut reader)?;

            if !common_md.is_list() {
                return Ok(GetListMetadataResult::WrongType);
            }

            let mut reader = U8ArrayReader::with_buffer(&value);
            let md = ListValueMetadata::from_bytes(&mut reader)?;
            if md.expiration().is_expired()? {
                self.cache.delete(&internal_key)?;
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
        &mut self,
        metadata: &ListValueMetadata,
        list_name: &BytesMut,
    ) -> Result<(), SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(list_name, self.db_id);
        self.cache.delete(&internal_key)?;

        // delete the bookkeeping record for this list
        let bookkeeping_record_key = Bookkeeping::new(self.db_id)
            .with_uid(metadata.id())
            .with_value_type(ValueType::List)
            .to_bytes();
        self.cache.delete(&bookkeeping_record_key)?;
        Ok(())
    }

    /// Put `ListValueMetadata` into the database
    fn put_list_metadata_internal(
        &mut self,
        metadata: &ListValueMetadata,
        list_name: &BytesMut,
    ) -> Result<(), SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(list_name, self.db_id);
        let mut buf = BytesMut::with_capacity(ListValueMetadata::SIZE);
        let mut builder = U8ArrayBuilder::with_buffer(&mut buf);
        metadata.to_bytes(&mut builder);
        self.cache.put(&internal_key, buf)?;
        Ok(())
    }

    /// Create new `ListValueMetadata` for `list_name` and return it to the caller
    fn new_list_internal(&mut self, list_name: &BytesMut) -> Result<ListValueMetadata, SableError> {
        let mut md = ListValueMetadata::new();
        let list_id = self.store.generate_id();
        md.set_id(list_id);

        // create a new bookkeeping record for this list
        let bookkeeping_record_key = Bookkeeping::new(self.db_id)
            .with_uid(list_id)
            .with_value_type(ValueType::List)
            .to_bytes();
        self.cache.put(&bookkeeping_record_key, list_name.clone())?;
        Ok(md)
    }

    fn flush_cache(&mut self) -> Result<(), SableError> {
        self.cache.flush()
    }
}

/// the order of the fields matters here
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListItem {
    pub key_type: KeyType,
    pub list_id: u64,
    pub item_id: u64,
    pub prev: u64,
    pub next: u64,
    pub user_data: BytesMut,
}

#[allow(dead_code)]
impl ListItem {
    // SIZE contain only the serialisable items
    pub const SIZE: usize = 4 * std::mem::size_of::<u64>() + std::mem::size_of::<KeyType>();

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
    pub fn save(&mut self, store: &mut DbWriteCache) -> Result<(), SableError> {
        let (key, val) = self.serialise();
        store.put(&key, val)
    }

    /// Load list item from the database based on its parent list ID and its own ID
    /// Note: `self.list_id` and `self.item_id` must be set before calling this
    /// function
    pub fn load(&mut self, store: &DbWriteCache) -> Result<bool, SableError> {
        // as the key, we use:
        // `key_type`, `list_id` and `item_id`
        // in as the value:
        // `left`, `right` and `user_data`
        let mut key = BytesMut::with_capacity(
            std::mem::size_of_val(&self.key_type)
                + std::mem::size_of_val(&self.list_id)
                + std::mem::size_of_val(&self.item_id),
        );

        let mut builder = U8ArrayBuilder::with_buffer(&mut key);
        builder.write_u8(self.key_type as u8);
        builder.write_u64(self.list_id);
        builder.write_u64(self.item_id);

        let Some(value) = store.get(&key)? else {
            return Ok(false); // not found
        };

        // decode the value
        self.construct_from(&key, value)?;
        Ok(true)
    }

    /// Delete the `ListItem` from the database
    pub fn delete(&mut self, store: &mut DbWriteCache) -> Result<(), SableError> {
        // as the key, we use:
        // `key_type`, `list_id` and `item_id`
        // in as the value:
        // `left`, `right` and `user_data`
        let mut key = BytesMut::with_capacity(
            std::mem::size_of_val(&self.key_type)
                + std::mem::size_of_val(&self.list_id)
                + std::mem::size_of_val(&self.item_id),
        );

        let mut builder = U8ArrayBuilder::with_buffer(&mut key);
        builder.write_u8(self.key_type as u8);
        builder.write_u64(self.list_id);
        builder.write_u64(self.item_id);
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

        let mut builder = U8ArrayBuilder::with_buffer(&mut key);
        builder.write_u8(KeyType::ListItem as u8);
        builder.write_u64(list_id);
        builder.write_u64(item_id);
        key
    }

    /// Create list item with a given id and list-id
    pub fn new(list_id: u64, item_id: u64) -> ListItem {
        let mut list_item = ListItem {
            key_type: KeyType::ListItem,
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
        store: &DbWriteCache,
    ) -> Result<Option<ListItem>, SableError> {
        let mut list_item = ListItem::new(list_id, item_id);
        if !list_item.load(store)? {
            Ok(None)
        } else {
            Ok(Some(list_item))
        }
    }

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

        let mut builder = U8ArrayBuilder::with_buffer(&mut val);
        builder.write_u64(self.prev);
        builder.write_u64(self.next);
        builder.write_bytes(&self.user_data);
        val
    }

    /// Given a serialised value, unpack it into the proper properties
    fn construct_from(&mut self, key: &BytesMut, mut value: BytesMut) -> Result<(), SableError> {
        // decode the key
        let mut reader = U8ArrayReader::with_buffer(key);
        let kind = reader.read_u8().ok_or(SableError::SerialisationError)?;
        self.key_type = KeyType::from_u8(kind).ok_or(SableError::SerialisationError)?;
        self.list_id = reader.read_u64().ok_or(SableError::SerialisationError)?;
        self.item_id = reader.read_u64().ok_or(SableError::SerialisationError)?;

        // these 2 are coming from the value
        let mut reader = U8ArrayReader::with_buffer(&value);
        self.prev = reader.read_u64().ok_or(SableError::SerialisationError)?;
        self.next = reader.read_u64().ok_or(SableError::SerialisationError)?;
        self.user_data = value.split_off(reader.consumed());
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
    use test_case::test_case;

    #[test]
    fn test_serialise_list_item() -> Result<(), SableError> {
        let (_guard, store) = crate::tests::open_store();
        let mut src = ListItem::new(1975, 42);
        src.prev = 10;
        src.next = 11;

        let mut cache = DbWriteCache::with_storage(&store);
        src.user_data = BytesMut::from("hello world");
        src.save(&mut cache)?;

        let mut target = ListItem::new(1975, 42);
        assert_eq!(target.load(&cache)?, true);
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
        let (_guard, store) = crate::tests::open_store();
        let list_name = BytesMut::from("my list");
        let mut list = List::with_storage(&store, 0);
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

            let list_id = match list.get_list_metadata_with_name(&list_name).unwrap() {
                GetListMetadataResult::Some(md) => md.id(),
                _ => {
                    panic!("Expected to find the list MD in the database");
                }
            };

            // Check that the book-keeping record is found
            let bookkeeping_record = Bookkeeping::new(0)
                .with_uid(list_id)
                .with_value_type(ValueType::List)
                .to_bytes();

            {
                let value = store.get(&bookkeeping_record)?.unwrap();
                assert_eq!(value, list_name);
            }

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

            // Check that the book-keeping record no longer exists
            assert!(store.get(&bookkeeping_record)?.is_none());

            // the list no longer exist
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
        let (_guard, store) = crate::tests::open_store();
        let list_name = BytesMut::from("my list");
        let mut list = List::with_storage(&store, 0);
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
        let (_guard, store) = crate::tests::open_store();
        let list_name = BytesMut::from("my list");
        let mut list = List::with_storage(&store, 0);
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
        let (_guard, store) = crate::tests::open_store();
        let list_name = BytesMut::from("my list");
        let mut list = List::with_storage(&store, 0);
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
        let (_guard, store) = crate::tests::open_store();
        let list_name = BytesMut::from("my list");
        let mut list = List::with_storage(&store, 0);
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
