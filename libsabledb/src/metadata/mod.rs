/// Possible value types
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[allow(dead_code)]
#[repr(usize)]
pub enum ValueType {
    #[default]
    Str,
    List,
    Hash,
}

mod encoding;
mod expiration;
mod hash_metadata;
mod list_metadata;
mod primary_key_metadata;
mod string_value_metadata;
mod value_metadata;
mod zset_metadata;

pub use encoding::Encoding;
pub use expiration::Expiration;

pub use hash_metadata::{HashFieldKey, HashValueMetadata};
pub use zset_metadata::*;

#[allow(unused_imports)]
pub use list_metadata::ListValueMetadata;
pub use primary_key_metadata::PrimaryKeyMetadata;
pub use string_value_metadata::StringValueMetadata;
pub use value_metadata::{CommonValueMetadata, ValueTypeIs};
