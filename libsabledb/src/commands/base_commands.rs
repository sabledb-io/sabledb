pub struct BaseCommands {}

#[allow(dead_code)]
impl BaseCommands {
    /// Redis uses weird indexing: inclusive, both ways and it supports negative indexes
    pub fn fix_range_indexes(
        range_len: usize,
        mut start: i64,
        mut end: i64,
    ) -> Option<(usize, usize)> {
        let strlen = range_len as i64;
        if strlen == 0 {
            return None;
        }
        if start < 0 && end < 0 && start > end {
            return None;
        }
        if start < 0 {
            start += strlen;
        }

        if end < 0 {
            end += strlen;
        }
        if start < 0 {
            start = 0;
        }
        if end < 0 {
            end = 0;
        }

        if end >= strlen {
            end = strlen - 1;
        }

        // Precondition: end >= 0 && end < strlen, so the only condition where
        // nothing can be returned is: start > end.
        if start > end || strlen == 0 {
            None
        } else {
            Some((start as usize, (end + 1) as usize))
        }
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
    use bytes::BytesMut;
    use test_case::test_case;

    #[test_case("mystring", -1, -1, Some((7, 8)); "start and end are negative -1")]
    #[test_case("mystring", -2, -2, Some((6, 7)); "start and end are negative -2")]
    #[test_case("mystring", 0, 100000, Some((0, 8)); "end out of bound")]
    #[test_case("mystring", -1, -2, None; "start greater than end")]
    #[test_case("", 0, 0, None; "empty input str")]
    #[test_case("mystring", 10, 20, None; "start and end out of bounds")]
    pub fn test_convert_negative_index(
        input_str: &'static str,
        start: i64,
        end: i64,
        expected_indexes: Option<(usize, usize)>,
    ) {
        let buffer = BytesMut::from(input_str.as_bytes());
        assert_eq!(
            BaseCommands::fix_range_indexes(buffer.len(), start, end),
            expected_indexes
        );
    }
}
