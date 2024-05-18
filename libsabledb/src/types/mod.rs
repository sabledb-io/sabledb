mod list;

#[macro_export]
macro_rules! list_or_size_0 {
    ($list:expr, $list_name:expr, $response_buffer:expr, $builder:expr) => {{
        match $list.get_list_metadata_with_name($list_name)? {
            GetListMetadataResult::WrongType => {
                $builder.error_string($response_buffer, Strings::WRONGTYPE);
                return Ok(());
            }
            GetListMetadataResult::None => {
                $builder.number_usize($response_buffer, 0);
                return Ok(());
            }
            GetListMetadataResult::Some(list) => list,
        }
    }};
}

#[macro_export]
macro_rules! list_md_or_null_string {
    ($list:expr, $list_name:expr, $response_buffer:expr, $builder:expr) => {{
        match $list.get_list_metadata_with_name($list_name)? {
            GetListMetadataResult::WrongType => {
                $builder.error_string($response_buffer, Strings::WRONGTYPE);
                return Ok(());
            }
            GetListMetadataResult::None => {
                $builder.null_string($response_buffer);
                return Ok(());
            }
            GetListMetadataResult::Some(list) => list,
        }
    }};
}

#[macro_export]
macro_rules! list_md_with_name_or {
    ($list:expr, $name:expr, $return_value:expr) => {{
        match $list.get_list_metadata_with_name($name)? {
            GetListMetadataResult::WrongType | GetListMetadataResult::None => {
                return $return_value;
            }
            GetListMetadataResult::Some(list) => list,
        }
    }};
}

#[macro_export]
macro_rules! iter_next_or_prev {
    ($list:expr, $list_md:expr, $cur_item:expr, $backward:expr) => {{
        if $backward {
            $list.prev(&$list_md, $cur_item)?
        } else {
            $list.next(&$list_md, $cur_item)?
        }
    }};
}

#[allow(unused_imports)]
pub use list::{BlockingCommandResult, List, ListFlags, MoveResult, MultiPopResult};
