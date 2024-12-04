use crate::{ParserError, SableError, StringUtils, ValkeyCommand};
use bytes::BytesMut;
use std::rc::Rc;

#[derive(Default)]
pub struct RequestParser {
    /// Parsed arguments in the input buffer in the form
    /// of: (`start_pos`, `token_len`)
    args: Vec<(usize, usize)>,
    /// The current string start pos
    cur_string_start_pos: usize,
    /// The current string end position
    curpos: usize,
    /// The current state
    state: ParserState,
    /// Number of items we expect
    expected_items: usize,
}

#[derive(Default, Debug)]
pub struct ParseResult {
    pub command: Rc<ValkeyCommand>,
    pub bytes_consumed: usize,
}

#[derive(Default)]
enum ParserState {
    #[default]
    Initial,
    /// Parsing an inline string
    InlineString,
    /// Parsing array of bulk strings
    BulkString,
}

impl RequestParser {
    /// Read length. Return the `length` + number of bytes consumed from buffer
    /// in order to parse the length
    fn read_len(&self, buffer: &[u8]) -> Result<(usize, usize), SableError> {
        let Some(pos) = StringUtils::find_subsequence(buffer, b"\r\n") else {
            return Err(SableError::Parser(ParserError::NeedMoreData));
        };

        if pos > 20 {
            // we allow max of 20 chars of string length
            return Err(SableError::Parser(ParserError::BufferTooBig));
        }

        let len_as_str = String::from_utf8_lossy(&buffer[0..pos]);
        let Ok(length) = len_as_str.parse::<usize>() else {
            return Err(SableError::Parser(ParserError::ProtocolError(
                "failed to parse string to usize".to_string(),
            )));
        };

        if length > 512 << 20 {
            // string input is larger than 512MB
            return Err(SableError::Parser(ParserError::BufferTooBig));
        }

        Ok((length, len_as_str.len().saturating_add(2)))
    }

    /// Reset the parser state
    pub fn reset(&mut self) {
        self.curpos = 0;
        self.cur_string_start_pos = 0;
        self.state = ParserState::Initial;
        self.args.clear();
    }

    /// Parse `buffer` and read the request.
    /// A request can be:
    /// - Inline string
    /// - Array of bulk strings
    pub fn parse(&mut self, buffer: &[u8]) -> Result<ParseResult, SableError> {
        // sanity check
        if buffer.is_empty() {
            return Err(SableError::Parser(ParserError::NeedMoreData));
        }

        // buffer limit is set to 512MB
        if buffer.len() > (512 << 20) {
            return Err(SableError::Parser(ParserError::BufferTooBig));
        }

        match self.state {
            ParserState::Initial => match buffer[0] {
                b'*' => {
                    let (array_len, bytes_to_skip) = self.read_len(&buffer[1..])?;
                    let Some(curpos) = self.curpos.checked_add(1 + bytes_to_skip) else {
                        return Err(SableError::Parser(ParserError::Overflow));
                    };
                    self.curpos = curpos;
                    self.expected_items = array_len;
                    self.state = ParserState::BulkString;
                    self.parse_array_of_bulk_strings(buffer)
                }
                _ => {
                    // inline string
                    self.state = ParserState::InlineString;
                    self.curpos = 0;
                    self.parse_inline_string(buffer)
                }
            },
            ParserState::InlineString => {
                // skip what we already parsed
                self.parse_inline_string(buffer)
            }
            ParserState::BulkString => self.parse_array_of_bulk_strings(buffer),
        }
    }

    /// Read until the first `\r\n` that we find
    fn parse_inline_string(&mut self, buffer: &[u8]) -> Result<ParseResult, SableError> {
        if self.curpos >= buffer.len() {
            return Err(SableError::Parser(ParserError::ProtocolError(
                "parser in invalid state: current position exceeds or equal to the buffer len"
                    .to_string(),
            )));
        }

        let curbuf = &buffer[self.curpos..];
        let Some(pos) = StringUtils::find_subsequence(curbuf, b"\r\n") else {
            if buffer.ends_with(b"\r") {
                // we managed to find the `\r` but not the `\n`
                self.curpos = buffer.len() - 1;
            } else {
                self.curpos = buffer.len();
            }
            return Err(SableError::Parser(ParserError::NeedMoreData));
        };

        let mut command_string = BytesMut::with_capacity(self.curpos + pos);
        command_string.extend_from_slice(&buffer[..self.curpos + pos]);
        let bytes_consumed = self.curpos.saturating_add(pos).saturating_add(2);

        // reset the parser
        self.reset();

        Ok(ParseResult {
            command: Rc::new(ValkeyCommand::new(StringUtils::split(
                &mut command_string,
            )?)?),
            // its OK to add `+2` here, since we managed to find `\r\n` above
            bytes_consumed,
        })
    }

    fn parse_array_of_bulk_strings(&mut self, buffer: &[u8]) -> Result<ParseResult, SableError> {
        let mut curpos = self.curpos;
        while self.expected_items > 0 {
            if curpos >= buffer.len() {
                return Err(SableError::Parser(ParserError::NeedMoreData));
            }

            // locate the `$`
            if buffer[curpos] != b'$' {
                return Err(SableError::Parser(ParserError::ProtocolError(format!(
                    "bulk string must start with '$'. Found {}",
                    buffer[curpos] as char
                ))));
            }
            // skip the $
            curpos = curpos.saturating_add(1);

            // read the length
            let (str_len, bytes_to_skip) = self.read_len(&buffer[curpos..])?;
            curpos = curpos.saturating_add(bytes_to_skip);

            // keep the current string position
            let string_start_pos = curpos;

            // check that we have enough buffer
            if str_len.saturating_add(2) > buffer.len().saturating_sub(curpos) {
                return Err(SableError::Parser(ParserError::NeedMoreData));
            }

            curpos = curpos.saturating_add(str_len).saturating_add(2);

            self.args.push((string_start_pos, str_len));
            self.curpos = curpos;
            self.expected_items = self.expected_items.saturating_sub(1);
        }

        let mut args = Vec::<BytesMut>::with_capacity(self.args.len());
        for (str_pos, str_len) in self.args.clone().into_iter() {
            let s = &buffer[str_pos..str_pos + str_len];
            args.push(BytesMut::from(s));
        }

        let bytes_consumed = self.curpos;

        // reset the parser
        self.reset();

        Ok(ParseResult {
            command: Rc::new(ValkeyCommand::new(args)?),
            bytes_consumed,
        })
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
    use crate::ParserError;
    use bytes::BytesMut;

    #[test]
    fn test_incomplete_inline_string() {
        let mut request_parser = RequestParser::default();
        let mut buffer = BytesMut::from("hello");
        let mut result = request_parser.parse(&buffer);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .eq_parser_error(&ParserError::NeedMoreData));

        // add another part, but not enough to complete a message
        buffer.extend_from_slice(b"world\r");
        result = request_parser.parse(&buffer);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .eq_parser_error(&ParserError::NeedMoreData));

        // add the extra bytes
        buffer.extend_from_slice(b"\nanotherstring");
        let result = request_parser.parse(&buffer);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.bytes_consumed, 12); // "helloworld\r\n"
        assert_eq!(result.command.arg(0).unwrap(), "helloworld");

        let _ = buffer.split_to(result.bytes_consumed);
        let result = request_parser.parse(&buffer);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .eq_parser_error(&ParserError::NeedMoreData));

        // complete the second message
        buffer.extend_from_slice(b"\r\n");
        let result = request_parser.parse(&buffer);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.bytes_consumed, 15); // "anotherstring\r\n"
        assert_eq!(result.command.arg(0).unwrap(), "anotherstring");
    }

    #[test]
    fn test_string_with_more_than_one_inline_message() {
        let mut request_parser = RequestParser::default();
        let mut buffer = BytesMut::from("SET KEY VALUE\r\nINFO\r\n");
        let result = request_parser.parse(&buffer);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.bytes_consumed, 15); // "SET KEY VALUE\r\n"
        assert_eq!(result.command.arg_count(), 3);
        assert_eq!(result.command.arg(0).unwrap(), "SET");
        assert_eq!(result.command.arg(1).unwrap(), "KEY");
        assert_eq!(result.command.arg(2).unwrap(), "VALUE");

        let _ = buffer.split_to(result.bytes_consumed);
        let result = request_parser.parse(&buffer);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.bytes_consumed, 6); // "INFO\r\n"
        assert_eq!(result.command.arg(0).unwrap(), "INFO");
    }

    #[test]
    fn test_empty_inline_message() {
        let mut request_parser = RequestParser::default();
        let buffer = BytesMut::new();
        let result = request_parser.parse(&buffer);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .eq_parser_error(&ParserError::NeedMoreData));
    }

    #[test]
    fn test_inline_message_happy_path() {
        let mut request_parser = RequestParser::default();
        let buffer = BytesMut::from("SET KEY VALUE\r\n");
        let result = request_parser.parse(&buffer);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.bytes_consumed, buffer.len());
        assert_eq!(result.command.arg_count(), 3);
        assert_eq!(result.command.arg(0).unwrap(), "SET");
        assert_eq!(result.command.arg(1).unwrap(), "KEY");
        assert_eq!(result.command.arg(2).unwrap(), "VALUE");
    }

    #[test]
    fn test_inline_message_with_quotes() {
        let mut request_parser = RequestParser::default();
        let buffer = BytesMut::from("SET KEY \"VALUE W SPACE\"\r\n");
        let result = request_parser.parse(&buffer);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.bytes_consumed, buffer.len());
        assert_eq!(result.command.arg_count(), 3);
        assert_eq!(result.command.arg(0).unwrap(), "SET");
        assert_eq!(result.command.arg(1).unwrap(), "KEY");
        assert_eq!(result.command.arg(2).unwrap(), "VALUE W SPACE");
    }

    #[test]
    fn test_happy_path_array_strings() {
        let mut request_parser = RequestParser::default();
        let buffer = BytesMut::from("*3\r\n$3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n");
        let result = request_parser.parse(&buffer);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.bytes_consumed, buffer.len());
        assert_eq!(result.command.arg_count(), 3);
        assert_eq!(result.command.arg(0).unwrap(), "SET");
        assert_eq!(result.command.arg(1).unwrap(), "KEY");
        assert_eq!(result.command.arg(2).unwrap(), "VALUE");
    }

    #[test]
    fn test_incomplete_array_strings() {
        let mut request_parser = RequestParser::default();
        let buffer = BytesMut::from("*4\r\n$3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n");
        let result = request_parser.parse(&buffer);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .eq_parser_error(&ParserError::NeedMoreData));
    }

    #[test]
    fn test_read_incomplete_array_strings_in_chunks() {
        let mut request_parser = RequestParser::default();
        let mut buffer = BytesMut::from("*4\r\n$3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n");
        let result = request_parser.parse(&buffer);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .eq_parser_error(&ParserError::NeedMoreData));

        buffer.extend_from_slice(b"$6\r\nVALUE2\r\ninelinestring\r\n");
        let result = request_parser.parse(&buffer);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert_eq!(result.bytes_consumed, 45);
        assert_eq!(result.command.arg_count(), 4);
        assert_eq!(result.command.arg(0).unwrap(), "SET");
        assert_eq!(result.command.arg(1).unwrap(), "KEY");
        assert_eq!(result.command.arg(2).unwrap(), "VALUE");
        assert_eq!(result.command.arg(3).unwrap(), "VALUE2");

        // remove the buffer read
        let _ = buffer.split_to(result.bytes_consumed);

        let result = request_parser.parse(&buffer);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.bytes_consumed, 15);
        assert_eq!(result.command.arg_count(), 1);
        assert_eq!(result.command.arg(0).unwrap(), "inelinestring");
    }

    #[test]
    fn test_read_incomplete_array_strings_in_chunks_cut_in_middle_of_string() {
        let mut request_parser = RequestParser::default();
        let mut buffer = BytesMut::from("*4\r\n$3\r\nSET\r\n$3\r\nKEY\r\n$5\r");
        let result = request_parser.parse(&buffer);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .eq_parser_error(&ParserError::NeedMoreData));

        buffer.extend_from_slice(b"\nVALUE\r\n$6\r\nVALUE2\r\n");
        let result = request_parser.parse(&buffer);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert_eq!(result.bytes_consumed, 45);
        assert_eq!(result.command.arg_count(), 4);
        assert_eq!(result.command.arg(0).unwrap(), "SET");
        assert_eq!(result.command.arg(1).unwrap(), "KEY");
        assert_eq!(result.command.arg(2).unwrap(), "VALUE");
        assert_eq!(result.command.arg(3).unwrap(), "VALUE2");

        // remove the buffer read
        let _ = buffer.split_to(result.bytes_consumed);
    }
}
