pub mod request_parser;
pub mod resp_builder_v2;
pub mod resp_response_parser_v2;
pub mod shard_locker;
pub mod stopwatch;
pub mod ticker;
pub use crate::{
    metadata::KeyType,
    server::{ParserError, SableError},
};
pub use request_parser::*;
pub use resp_builder_v2::RespBuilderV2;
pub use resp_response_parser_v2::RedisObject;
pub use shard_locker::*;
pub use stopwatch::*;

use bytes::BytesMut;
use rand::prelude::*;
use std::collections::VecDeque;
use std::str::FromStr;

pub struct StringUtils {}
pub struct BytesMutUtils {}
pub struct TimeUtils {}

#[derive(Copy, Clone, PartialEq, Eq)]
enum InlineState {
    Normal,
    DoubleQuotes,
    SingleQuotes,
    Escape,
}

const UNCLOSED_DOUBLE_QUOTES: &str = "unclosed double quotes";
const UNCLOSED_SINGLE_QUOTES: &str = "unclosed single quotes";
const TRAILING_ESCAPE_CHAR: &str = "trailing escape character";

impl StringUtils {
    /// Find `what` in `buffer`
    pub fn find_subsequence(buffer: &[u8], what: &[u8]) -> Option<usize> {
        buffer.windows(what.len()).position(|window| window == what)
    }

    /// Split `buffer` by whitespace
    pub fn split(buffer: &mut BytesMut) -> Result<Vec<BytesMut>, ParserError> {
        let mut word = BytesMut::with_capacity(1024);
        let mut words = Vec::<BytesMut>::new();
        let mut state = InlineState::Normal;
        let mut prev_state = InlineState::Normal;

        for ch in buffer.iter() {
            match state {
                InlineState::Escape => match ch {
                    b'n' => {
                        word.extend([b'\n']);
                        state = prev_state;
                    }
                    b'r' => {
                        word.extend([b'\r']);
                        state = prev_state;
                    }
                    b't' => {
                        word.extend([b'\t']);
                        state = prev_state;
                    }
                    _ => {
                        word.extend([*ch]);
                        state = prev_state;
                    }
                },
                InlineState::Normal => match ch {
                    b'"' => {
                        if !word.is_empty() {
                            words.push(word.clone());
                            word.clear();
                        }
                        state = InlineState::DoubleQuotes;
                    }
                    b'\'' => {
                        if !word.is_empty() {
                            words.push(word.clone());
                            word.clear();
                        }
                        state = InlineState::SingleQuotes;
                    }
                    b' ' | b'\t' => {
                        if !word.is_empty() {
                            words.push(word.clone());
                            word.clear();
                        }
                    }
                    b'\\' => {
                        prev_state = InlineState::Normal;
                        state = InlineState::Escape;
                    }
                    _ => {
                        word.extend([ch]);
                    }
                },
                InlineState::DoubleQuotes => match ch {
                    b'"' => {
                        if !word.is_empty() {
                            words.push(word.clone());
                            word.clear();
                        }
                        state = InlineState::Normal;
                    }
                    b'\\' => {
                        prev_state = InlineState::DoubleQuotes;
                        state = InlineState::Escape;
                    }
                    _ => {
                        word.extend([ch]);
                    }
                },
                InlineState::SingleQuotes => match ch {
                    b'\'' => {
                        if !word.is_empty() {
                            words.push(word.clone());
                            word.clear();
                        }
                        state = InlineState::Normal;
                    }
                    b'\\' => {
                        prev_state = InlineState::SingleQuotes;
                        state = InlineState::Escape;
                    }
                    _ => {
                        word.extend([ch]);
                    }
                },
            }
        }

        match state {
            InlineState::DoubleQuotes => {
                // parsing ended with broken string or bad escaping
                Err(ParserError::InvalidInput(
                    UNCLOSED_DOUBLE_QUOTES.to_string(),
                ))
            }
            InlineState::Normal => {
                // add the remainder
                if !word.is_empty() {
                    words.push(word.clone());
                    word.clear();
                }
                Ok(words)
            }
            InlineState::SingleQuotes => Err(ParserError::InvalidInput(
                UNCLOSED_SINGLE_QUOTES.to_string(),
            )),
            InlineState::Escape => Err(ParserError::InvalidInput(TRAILING_ESCAPE_CHAR.to_string())),
        }
    }

    /// Convert `s` into `usize`
    pub fn parse_str_to_number<F: FromStr>(s: &str) -> Result<F, SableError> {
        let Ok(num) = FromStr::from_str(s) else {
            return Err(SableError::InvalidArgument(format!(
                "failed to parse string `{}` to number",
                s
            )));
        };
        Ok(num)
    }
}

impl TimeUtils {
    /// Return milliseconds elapsed since EPOCH
    pub fn epoch_ms() -> Result<u64, SableError> {
        let Ok(timestamp_ms) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        else {
            return Err(SableError::OtherError(
                "failed to retrieve std::time::UNIX_EPOCH".to_string(),
            ));
        };
        Ok(timestamp_ms.as_millis().try_into().unwrap_or(u64::MAX))
    }

    /// Return microseconds elapsed since EPOCH
    pub fn epoch_micros() -> Result<u64, SableError> {
        let Ok(timestamp_ms) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        else {
            return Err(SableError::OtherError(
                "failed to retrieve std::time::UNIX_EPOCH".to_string(),
            ));
        };
        Ok(timestamp_ms.as_micros().try_into().unwrap_or(u64::MAX))
    }

    /// Return seconds elapsed since EPOCH
    pub fn epoch_seconds() -> Result<u64, SableError> {
        let Ok(timestamp_ms) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        else {
            return Err(SableError::OtherError(
                "failed to retrieve std::time::UNIX_EPOCH".to_string(),
            ));
        };
        Ok(timestamp_ms.as_secs())
    }
}

#[allow(dead_code)]
impl BytesMutUtils {
    /// Convert `value` into `F`
    pub fn parse<F: FromStr>(value: &BytesMut) -> Option<F> {
        let value_as_number = String::from_utf8_lossy(&value[..]);
        let Ok(num) = F::from_str(&value_as_number) else {
            return None;
        };
        Some(num)
    }

    pub fn to_string(value: &[u8]) -> String {
        String::from_utf8_lossy(value).to_string()
    }

    pub fn from_string(value: &str) -> BytesMut {
        BytesMut::from(value)
    }

    pub fn from<N: std::fmt::Display>(value: &N) -> BytesMut {
        let as_str = format!("{}", value);
        BytesMut::from(as_str.as_str())
    }

    // conversion functions
    pub fn from_u64(num: &u64) -> BytesMut {
        let arr = num.to_be_bytes();
        BytesMut::from(&arr[..])
    }

    pub fn from_u8(ch: &u8) -> BytesMut {
        let arr = ch.to_be_bytes();
        BytesMut::from(&arr[..])
    }

    pub fn from_u16(short: &u16) -> BytesMut {
        let arr = short.to_be_bytes();
        BytesMut::from(&arr[..])
    }

    pub fn from_usize(size: &usize) -> BytesMut {
        let arr = size.to_be_bytes();
        BytesMut::from(&arr[..])
    }

    pub fn to_usize(bytes: &BytesMut) -> usize {
        let mut arr = [0u8; std::mem::size_of::<usize>()];
        arr.copy_from_slice(&bytes[0..std::mem::size_of::<usize>()]);
        usize::from_be_bytes(arr)
    }

    pub fn to_u64(bytes: &BytesMut) -> u64 {
        let mut arr = [0u8; std::mem::size_of::<u64>()];
        arr.copy_from_slice(&bytes[0..std::mem::size_of::<u64>()]);
        u64::from_be_bytes(arr)
    }

    pub fn to_u32(bytes: &BytesMut) -> u32 {
        let mut arr = [0u8; std::mem::size_of::<u32>()];
        arr.copy_from_slice(&bytes[0..std::mem::size_of::<u32>()]);
        u32::from_be_bytes(arr)
    }

    pub fn to_u8(bytes: &BytesMut) -> u8 {
        let mut arr = [0u8; std::mem::size_of::<u8>()];
        arr.copy_from_slice(&bytes[0..std::mem::size_of::<u8>()]);
        u8::from_be_bytes(arr)
    }

    pub fn to_u16(bytes: &BytesMut) -> u16 {
        let mut arr = [0u8; std::mem::size_of::<u16>()];
        arr.copy_from_slice(&bytes[0..std::mem::size_of::<u16>()]);
        u16::from_be_bytes(arr)
    }

    pub fn to_f64(bytes: &BytesMut) -> f64 {
        let mut arr = [0u8; std::mem::size_of::<f64>()];
        arr.copy_from_slice(&bytes[0..std::mem::size_of::<f64>()]);
        f64::from_be_bytes(arr)
    }

    pub fn from_f64(num: f64) -> BytesMut {
        let arr = num.to_be_bytes();
        BytesMut::from(&arr[..])
    }

    /// Given two sequences, return the longest subsequence present in both of them
    /// and the indices in each sequence
    pub fn lcs(seq1: &BytesMut, seq2: &BytesMut) -> (BytesMut, Vec<(usize, usize)>) {
        let m = seq1.len();
        let n = seq2.len();
        let mut table = vec![vec![0; n + 1]; m + 1];

        if seq1.is_empty() || seq2.is_empty() {
            return (BytesMut::new(), vec![]);
        }

        // Following steps build table[m+1][n+1] in bottom up
        // fashion. Note that table[i][j] contains length of LCS of
        // X[0..i-1] and Y[0..j-1]
        // the length of the LCS is at the bottom right cell of the table
        for i in 0..m + 1 {
            for j in 0..n + 1 {
                if i == 0 || j == 0 {
                    table[i][j] = 0;
                } else if seq1[i - 1] == seq2[j - 1] {
                    table[i][j] = table[i - 1][j - 1] + 1;
                } else {
                    table[i][j] = std::cmp::max(table[i - 1][j], table[i][j - 1]);
                }
            }
        }

        let mut indices = Vec::<(usize, usize)>::new();
        let mut i = m;
        let mut j = n;

        let mut lcs_str = BytesMut::with_capacity(table[m - 1][n - 1]);

        // Traverse the table
        while i > 0 && j > 0 {
            // If current character in X and Y is the same, then
            // the current character is part of the LCS
            if seq1[i - 1] == seq2[j - 1] {
                indices.insert(0, (i - 1, j - 1));
                let byte = seq1[i - 1];
                lcs_str.extend([byte]);
                // reduce values of i, j
                i -= 1;
                j -= 1;
            }
            // If not the same, then find the larger of two and
            // go in the direction of the larger value
            else if table[i - 1][j] > table[i][j - 1] {
                i -= 1;
            } else {
                j -= 1;
            }
        }

        lcs_str.reverse();
        (lcs_str, indices)
    }
}

pub struct U8ArrayReader<'a> {
    buffer: &'a [u8],
    consumed: usize,
}

impl<'a> U8ArrayReader<'a> {
    const U8_SIZE: usize = std::mem::size_of::<u8>();
    const USIZE_SIZE: usize = std::mem::size_of::<usize>();
    const U64_SIZE: usize = std::mem::size_of::<u64>();
    const F64_SIZE: usize = std::mem::size_of::<f64>();
    const U16_SIZE: usize = std::mem::size_of::<u16>();

    /// Return the number of bytes consumed so far
    pub fn consumed(&self) -> usize {
        self.consumed
    }

    pub fn with_buffer(buffer: &'a [u8]) -> Self {
        U8ArrayReader {
            buffer,
            consumed: 0,
        }
    }

    pub fn read_u8(&mut self) -> Option<u8> {
        if self.buffer.len().saturating_sub(self.consumed) < U8ArrayReader::U8_SIZE {
            return None;
        }

        let mut arr = [0u8; U8ArrayReader::U8_SIZE];
        arr.copy_from_slice(&self.buffer[self.consumed..self.consumed + U8ArrayReader::U8_SIZE]);
        self.consumed = self.consumed.saturating_add(U8ArrayReader::U8_SIZE);
        Some(u8::from_be_bytes(arr))
    }

    pub fn read_u16(&mut self) -> Option<u16> {
        if self.buffer.len().saturating_sub(self.consumed) < U8ArrayReader::U16_SIZE {
            return None;
        }

        let mut arr = [0u8; U8ArrayReader::U16_SIZE];
        arr.copy_from_slice(&self.buffer[self.consumed..self.consumed + U8ArrayReader::U16_SIZE]);
        self.consumed = self.consumed.saturating_add(U8ArrayReader::U16_SIZE);
        Some(u16::from_be_bytes(arr))
    }

    pub fn read_usize(&mut self) -> Option<usize> {
        if self.buffer.len().saturating_sub(self.consumed) < U8ArrayReader::USIZE_SIZE {
            return None;
        }

        let mut arr = [0u8; U8ArrayReader::USIZE_SIZE];
        arr.copy_from_slice(&self.buffer[self.consumed..self.consumed + U8ArrayReader::USIZE_SIZE]);
        self.consumed = self.consumed.saturating_add(U8ArrayReader::USIZE_SIZE);
        Some(usize::from_be_bytes(arr))
    }

    pub fn read_bytes(&mut self, len: usize) -> Option<BytesMut> {
        if self.buffer.len().saturating_sub(self.consumed) < len {
            return None;
        }

        let mut arr = BytesMut::with_capacity(len);
        arr.extend_from_slice(&self.buffer[self.consumed..self.consumed + len]);
        self.consumed = self.consumed.saturating_add(len);
        Some(arr)
    }

    pub fn read_u64(&mut self) -> Option<u64> {
        if self.buffer.len().saturating_sub(self.consumed) < U8ArrayReader::U64_SIZE {
            return None;
        }

        let mut arr = [0u8; U8ArrayReader::U64_SIZE];
        arr.copy_from_slice(&self.buffer[self.consumed..self.consumed + U8ArrayReader::U64_SIZE]);
        self.consumed = self.consumed.saturating_add(U8ArrayReader::U64_SIZE);
        Some(u64::from_be_bytes(arr))
    }

    pub fn read_f64(&mut self) -> Option<f64> {
        if self.buffer.len().saturating_sub(self.consumed) < U8ArrayReader::F64_SIZE {
            return None;
        }

        let mut arr = [0u8; U8ArrayReader::F64_SIZE];
        arr.copy_from_slice(&self.buffer[self.consumed..self.consumed + U8ArrayReader::F64_SIZE]);
        self.consumed = self.consumed.saturating_add(U8ArrayReader::F64_SIZE);
        Some(f64::from_be_bytes(arr))
    }

    /// Rewind the reader back to the beginning
    pub fn rewind(&mut self) {
        self.consumed = 0;
    }
}

pub struct U8ArrayBuilder<'a> {
    buffer: &'a mut BytesMut,
}

impl<'a> U8ArrayBuilder<'a> {
    pub fn with_buffer(buffer: &'a mut BytesMut) -> Self {
        U8ArrayBuilder { buffer }
    }

    pub fn write_u8(&mut self, val: u8) {
        self.buffer.extend_from_slice(&u8::to_be_bytes(val));
    }

    pub fn write_key_type(&mut self, val: KeyType) {
        self.buffer.extend_from_slice(&u8::to_be_bytes(val as u8));
    }

    pub fn write_u16(&mut self, val: u16) {
        self.buffer.extend_from_slice(&u16::to_be_bytes(val));
    }

    pub fn write_u64(&mut self, val: u64) {
        self.buffer.extend_from_slice(&u64::to_be_bytes(val));
    }

    pub fn write_f64(&mut self, val: f64) {
        self.buffer.extend_from_slice(&f64::to_be_bytes(val));
    }

    pub fn write_usize(&mut self, val: usize) {
        self.buffer.extend_from_slice(&usize::to_be_bytes(val));
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
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
mod test {
    use super::*;
    use test_case::test_case;

    #[test_case(r#"set key "value with space""#, 3, "value with space" ; "with double quotes")]
    #[test_case(r#"set key 'value with space'"#, 3, "value with space" ; "with single quotes")]
    #[test_case(r#"set key 'value "with space'"#, 3,"value \"with space" ; "double quotes inside single quotes")]
    #[test_case(r#"set key "value 'with space""#, 3,"value 'with space" ; "single quotes inside double quotes")]
    #[test_case(r#"set key "value \"with space""#, 3,"value \"with space" ; "escape double quote")]
    #[test_case(r#"set key 'value \'with space'"#, 3,"value 'with space" ; "escape single quote")]
    #[test_case(r#"set key "value with space" extra"#, 4, "extra" ; "param after")]
    fn test_split_with_spaces(input_string: &str, count: usize, third_string: &str) {
        let mut input = BytesMut::from(input_string);
        let res = StringUtils::split(&mut input);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert_eq!(res.len(), count);
        assert_eq!(res.get(0).unwrap(), "set");
        assert_eq!(res.get(1).unwrap(), "key");
        assert_eq!(res.get(count - 1).unwrap(), third_string);
    }

    #[test_case(r#"set key value '"#, UNCLOSED_SINGLE_QUOTES ; "broken single quote")]
    #[test_case(r#"set key value ""#, UNCLOSED_DOUBLE_QUOTES ; "broken double quote")]
    #[test_case(r#"set key "value \"#, TRAILING_ESCAPE_CHAR; "broken escape")]
    fn test_split_with_broke_string(input_string: &str, message: &str) {
        let mut input = BytesMut::from(input_string);
        let res = StringUtils::split(&mut input);
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err(),
            ParserError::InvalidInput(message.to_string())
        );
    }
    #[test_case("AGGTAB", "GXTXAYB", "GTAB" ; "b greater than b")]
    #[test_case("", "GXTXAYB", "" ; "a is empty string")]
    #[test_case("GXTXAYB", "", "" ; "b is empty string")]
    #[test_case("", "", "" ; "both empty")]
    fn test_lcs(a: &str, b: &str, lcs: &str) -> Result<(), SableError> {
        let bma1 = BytesMut::from(a);
        let bma2 = BytesMut::from(b);
        let (lcs_return_value, v) = BytesMutUtils::lcs(&bma1, &bma2);
        assert_eq!(v.len(), lcs.len());

        let mut result_str1 = BytesMut::new();
        let mut result_str2 = BytesMut::new();
        for (idx1, idx2) in v.iter() {
            let byte1 = bma1[*idx1];
            let byte2 = bma2[*idx2];
            result_str1.extend([byte1]);
            result_str2.extend([byte2]);
        }

        assert_eq!(result_str1, result_str2);
        assert_eq!(lcs, String::from_utf8_lossy(&result_str2));
        assert_eq!(lcs, lcs_return_value);
        Ok(())
    }

    #[test]
    fn test_conversion() -> Result<(), SableError> {
        {
            let val = 11u8;
            let b = BytesMutUtils::from_u8(&val);
            let from_b = BytesMutUtils::to_u8(&b);

            println!("{:?}", b.to_vec());
            assert_eq!(val, from_b);
        }
        {
            let val = 11usize;
            let b = BytesMutUtils::from_usize(&val);
            let from_b = BytesMutUtils::to_usize(&b);

            println!("{:?}", b.to_vec());
            assert_eq!(val, from_b);
        }
        {
            let val = 11u64;
            let b = BytesMutUtils::from_u64(&val);
            let from_b = BytesMutUtils::to_u64(&b);

            println!("{:?}", b.to_vec());
            assert_eq!(val, from_b);
        }
        {
            let val = 1024u64;
            let b = BytesMutUtils::from_u64(&val);
            let from_b = BytesMutUtils::to_u64(&b);

            println!("{:?}", b.to_vec());
            assert_eq!(val, from_b);
        }
        Ok(())
    }
}

/// Number of slots
pub const SLOT_SIZE: u16 = 16384;

/// Provide an API for calculating the slot from a given user key
pub fn calculate_slot(key: &BytesMut) -> u16 {
    crc16::State::<crc16::XMODEM>::calculate(key) % SLOT_SIZE
}

pub enum CurrentTimeResolution {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

/// Return the current timestamp since UNIX EPOCH - in seconds
pub fn current_time(res: CurrentTimeResolution) -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("SystemTime::now");
    match res {
        CurrentTimeResolution::Nanoseconds => now.as_nanos().try_into().unwrap_or(u64::MAX),
        CurrentTimeResolution::Microseconds => now.as_micros().try_into().unwrap_or(u64::MAX),
        CurrentTimeResolution::Milliseconds => now.as_millis().try_into().unwrap_or(u64::MAX),
        CurrentTimeResolution::Seconds => now.as_secs(),
    }
}

/// Given list of values `options`, return up to `count` values.
/// The output is sorted.
pub fn choose_multiple_values(
    count: usize,
    options: &Vec<usize>,
    allow_dups: bool,
) -> Result<VecDeque<usize>, SableError> {
    let mut rng = rand::thread_rng();
    let mut chosen = Vec::<usize>::new();
    if allow_dups {
        for _ in 0..count {
            chosen.push(*options.choose(&mut rng).unwrap_or(&0));
        }
    } else {
        let mut unique_values = options.clone();
        unique_values.sort();
        unique_values.dedup();
        loop {
            if unique_values.is_empty() {
                break;
            }

            if chosen.len() == count {
                break;
            }

            let pos = rng.gen_range(0..unique_values.len());
            let Some(val) = unique_values.get(pos) else {
                return Err(SableError::OtherError(format!(
                    "Internal error: failed to read from vector (len: {}, pos: {})",
                    unique_values.len(),
                    pos
                )));
            };
            chosen.push(*val);
            unique_values.remove(pos);
        }
    }

    chosen.sort();
    let chosen: VecDeque<usize> = chosen.iter().copied().collect();
    Ok(chosen)
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
    fn test_rng_selection() {
        let options = vec![1, 2, 2, 2, 3, 4, 5, 6, 7, 7, 7];
        let selections = choose_multiple_values(8, &options, false).unwrap();
        assert_eq!(selections.len(), 7);

        let selections = choose_multiple_values(8, &options, true).unwrap();
        assert_eq!(selections.len(), 8);
    }
}
