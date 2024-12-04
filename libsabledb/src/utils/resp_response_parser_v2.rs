use crate::{BytesMutUtils, SableError, StringUtils};
use bytes::BytesMut;

#[derive(PartialEq, Eq, Debug)]
pub enum ResponseParseResult {
    NeedMoreData,
    /// How many bytes consumed to form the ValkeyObject + the object
    Ok((usize, ValkeyObject)),
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum ValkeyObject {
    Status(BytesMut),
    Error(BytesMut),
    Str(BytesMut),
    Array(Vec<ValkeyObject>),
    NullArray,
    NullString,
    Integer(u64),
}

impl ValkeyObject {
    pub fn integer(&self) -> Result<u64, SableError> {
        match self {
            Self::Integer(num) => Ok(*num),
            other => Err(SableError::OtherError(
                format!("Expected Integer. Found: {:?}", other)
                    .as_str()
                    .into(),
            )),
        }
    }

    pub fn status(&self) -> Result<BytesMut, SableError> {
        match self {
            Self::Status(s) => Ok(s.clone()),
            other => Err(SableError::OtherError(
                format!("Expected Status. Found: {:?}", other)
                    .as_str()
                    .into(),
            )),
        }
    }

    pub fn string(&self) -> Result<BytesMut, SableError> {
        match self {
            Self::Str(s) => Ok(s.clone()),
            other => Err(SableError::OtherError(
                format!("Expected Str. Found: {:?}", other).as_str().into(),
            )),
        }
    }

    pub fn error_string(&self) -> Result<BytesMut, SableError> {
        match self {
            Self::Error(s) => Ok(s.clone()),
            other => Err(SableError::OtherError(
                format!("Expected Err. Found: {:?}", other).as_str().into(),
            )),
        }
    }

    pub fn array(&self) -> Result<Vec<ValkeyObject>, SableError> {
        match self {
            Self::Array(arr) => Ok(arr.clone()),
            other => Err(SableError::OtherError(
                format!("Expected Array. Found: {:?}", other)
                    .as_str()
                    .into(),
            )),
        }
    }

    pub fn is_null_string(&self) -> bool {
        matches!(self, Self::NullString)
    }
}

#[derive(Default)]
pub struct RespResponseParserV2 {}

impl RespResponseParserV2 {
    pub fn parse_response(buffer: &[u8]) -> Result<ResponseParseResult, SableError> {
        if buffer.is_empty() {
            return Ok(ResponseParseResult::NeedMoreData);
        }

        // buffer contains something
        match buffer[0] {
            b'+' => {
                // consume the prefix
                let mut consume = 1usize;
                let buffer = &buffer[1..];
                let Some(crlf_pos) = StringUtils::find_subsequence(buffer, b"\r\n") else {
                    return Ok(ResponseParseResult::NeedMoreData);
                };
                consume = consume.saturating_add(crlf_pos + 2);
                Ok(ResponseParseResult::Ok((
                    consume,
                    ValkeyObject::Status(BytesMut::from(&buffer[..crlf_pos])),
                )))
            }
            b'-' => {
                // consume the prefix
                let mut consume = 1usize;
                let buffer = &buffer[1..];
                let Some(crlf_pos) = StringUtils::find_subsequence(buffer, b"\r\n") else {
                    return Ok(ResponseParseResult::NeedMoreData);
                };
                consume = consume.saturating_add(crlf_pos + 2);
                Ok(ResponseParseResult::Ok((
                    consume,
                    ValkeyObject::Error(BytesMut::from(&buffer[..crlf_pos])),
                )))
            }
            b':' => {
                // consume the prefix
                let mut consume = 1usize;
                let buffer = &buffer[1..];
                let Some(crlf_pos) = StringUtils::find_subsequence(buffer, b"\r\n") else {
                    return Ok(ResponseParseResult::NeedMoreData);
                };
                consume = consume.saturating_add(crlf_pos + 2);
                let num = BytesMut::from(&buffer[..crlf_pos]);
                let Some(num) = BytesMutUtils::parse::<u64>(&num) else {
                    return Err(SableError::OtherError(format!(
                        "failed to parse number: `{:?}`",
                        num
                    )));
                };

                Ok(ResponseParseResult::Ok((
                    consume,
                    ValkeyObject::Integer(num),
                )))
            }
            b'$' => {
                // consume the prefix
                let mut consume = 1usize;
                let buffer = &buffer[1..];
                // read the length
                let Some(crlf_pos) = StringUtils::find_subsequence(buffer, b"\r\n") else {
                    return Ok(ResponseParseResult::NeedMoreData);
                };
                consume = consume.saturating_add(crlf_pos + 2);
                let num = BytesMut::from(&buffer[..crlf_pos]);
                let Some(strlen) = BytesMutUtils::parse::<i32>(&num) else {
                    return Err(SableError::OtherError(format!(
                        "failed to parse number: `{:?}`",
                        num
                    )));
                };

                if strlen <= 0 {
                    // Null or empty string
                    Ok(ResponseParseResult::Ok((consume, ValkeyObject::NullString)))
                } else {
                    let strlen = strlen as usize;
                    // read the string content
                    let buffer = &buffer[crlf_pos + 2..];
                    if buffer.len() < strlen + 2
                    /* the terminator \r\n */
                    {
                        return Ok(ResponseParseResult::NeedMoreData);
                    }

                    let str_content = BytesMut::from(&buffer[..strlen]);
                    consume = consume.saturating_add(strlen + 2);
                    Ok(ResponseParseResult::Ok((
                        consume,
                        ValkeyObject::Str(str_content),
                    )))
                }
            }
            b'*' => {
                // consume the prefix
                let mut consume = 1usize;
                let buffer = &buffer[1..];
                // read & parse the array length
                let Some(crlf_pos) = StringUtils::find_subsequence(buffer, b"\r\n") else {
                    return Ok(ResponseParseResult::NeedMoreData);
                };
                let num = BytesMut::from(&buffer[..crlf_pos]);
                let Some(arrlen) = BytesMutUtils::parse::<i32>(&num) else {
                    return Err(SableError::OtherError(format!(
                        "failed to parse number: `{:?}`",
                        num
                    )));
                };
                // skip the array length
                consume = consume.saturating_add(crlf_pos + 2);

                // Null array?
                if arrlen < 0 {
                    return Ok(ResponseParseResult::Ok((consume, ValkeyObject::NullArray)));
                }

                let mut objects = Vec::<ValkeyObject>::with_capacity(arrlen as usize);
                let mut buffer = &buffer[crlf_pos + 2..];

                // start reading elements. At this point `buffer` points to the first element
                for _ in 0..arrlen {
                    let result = Self::parse_response(buffer)?;
                    match result {
                        ResponseParseResult::Ok((bytes_read, obj)) => {
                            consume = consume.saturating_add(bytes_read);
                            buffer = &buffer[bytes_read..];
                            objects.push(obj);
                        }
                        ResponseParseResult::NeedMoreData => {
                            return Ok(ResponseParseResult::NeedMoreData)
                        }
                    }
                }
                Ok(ResponseParseResult::Ok((
                    consume,
                    ValkeyObject::Array(objects),
                )))
            }
            _ => Err(SableError::OtherError(format!(
                "unexpected token found `{}`",
                buffer[0]
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;
    #[test_case(b"*4\r\n$5\r\nvalue\r\n$5\r\nvalue\r\n$-1\r\n$5\r\nvalue\r\n",
        ValkeyObject::Array(vec![
            ValkeyObject::Str(BytesMut::from("value")),
            ValkeyObject::Str(BytesMut::from("value")),
            ValkeyObject::NullString,
            ValkeyObject::Str(BytesMut::from("value"))
        ])
    ; "parse array with 4 elements")]
    #[test_case(b"$11\r\nhello world\r\n",
        ValkeyObject::Str(BytesMut::from("hello world"))
    ; "parse simple string")]
    #[test_case(b"+OK\r\n",
        ValkeyObject::Status(BytesMut::from("OK"))
    ; "parse status OK")]
    #[test_case(b"-ERR bad thing happened\r\n",
        ValkeyObject::Error(BytesMut::from("ERR bad thing happened"))
    ; "parse err message")]
    #[test_case(b":42\r\n", ValkeyObject::Integer(42); "parse integer")]
    fn test_happy_response_parser(
        buffer: &[u8],
        expected_response: ValkeyObject,
    ) -> Result<(), SableError> {
        let response = RespResponseParserV2::parse_response(buffer)?;
        let ResponseParseResult::Ok((consumed, obj)) = response else {
            assert!(false, "Expected ParseResult::Ok");
            return Err(SableError::OtherError(
                "Expected ParseResult::Ok".to_string(),
            ));
        };
        assert_eq!(buffer.len(), consumed);
        assert_eq!(expected_response, obj);
        Ok(())
    }

    #[test_case(b"$11\r\nhello world\r\n$5\r\n",
        ValkeyObject::Str(BytesMut::from("hello world")), 18
    ; "parse simple string with excessive data")]
    #[test_case(b"*4\r\n$5\r\nvalue\r\n$5\r\nvalue\r\n$-1\r\n$5\r\nvalue\r\n$5\r\n",
        ValkeyObject::Array(vec![
            ValkeyObject::Str(BytesMut::from("value")),
            ValkeyObject::Str(BytesMut::from("value")),
            ValkeyObject::NullString,
            ValkeyObject::Str(BytesMut::from("value"))
        ]), 42
    ; "parse array with 4 elements and excessive data")]
    fn test_buffer_too_long(
        buffer: &[u8],
        expected_response: ValkeyObject,
        expected_consumed: usize,
    ) -> Result<(), SableError> {
        let response = RespResponseParserV2::parse_response(buffer)?;
        let ResponseParseResult::Ok((consumed, obj)) = response else {
            assert!(false, "Expected ParseResult::Ok");
            return Err(SableError::OtherError(
                "Expected ParseResult::Ok".to_string(),
            ));
        };
        assert_eq!(expected_consumed, consumed);
        assert_eq!(expected_response, obj);
        Ok(())
    }
}
