use bytes::BytesMut;

const CRLF: &str = "\r\n";
const DOLLAR: &str = "$";
const CRLF_LEN: usize = 2;
const DOLLAR_LEN: usize = 1;

//const PLUS: &str = "+";
const ERR: &str = "-";
const STATUS: &str = "+";
const OK: &str = "+OK\r\n";
const EMPTY_STRING: &str = "$0\r\n\r\n";
const NULL_STRING: &str = "$-1\r\n";
const EMPTY_ARRAY: &str = "*0\r\n";
const NULL_ARRAY: &str = "*-1\r\n";
const PONG: &str = "+PONG\r\n";

#[derive(Default, Clone)]
pub struct RespBuilderV2 {}

#[allow(dead_code)]
impl RespBuilderV2 {
    fn append_str(&self, buffer: &mut BytesMut, s: &str) {
        buffer.extend_from_slice(s.as_bytes());
    }

    fn append_bytes(&self, buffer: &mut BytesMut, bytes: &[u8]) {
        buffer.extend_from_slice(bytes);
    }

    fn add_bulk_string_internal(&self, buffer: &mut BytesMut, content: &[u8]) {
        let str_len = format!("{}", content.len());
        // extend the buffer as needed
        buffer.reserve(DOLLAR_LEN + str_len.len() + content.len() + (2 * CRLF_LEN));
        self.append_str(buffer, DOLLAR);
        buffer.extend_from_slice(str_len.as_bytes());
        self.append_str(buffer, CRLF);
        self.append_bytes(buffer, content);
        self.append_str(buffer, CRLF);
    }

    fn add_null_string_internal(&self, buffer: &mut BytesMut) {
        self.append_str(buffer, NULL_STRING);
    }

    /// Clears the buffer and create a bulk string RESP response
    pub fn bulk_string(&self, buffer: &mut BytesMut, content: &[u8]) {
        buffer.clear();
        self.add_bulk_string_internal(buffer, content);
    }

    /// Clears the buffer and create an `OK` RESP response
    pub fn ok(&self, buffer: &mut BytesMut) {
        buffer.clear();
        self.append_str(buffer, OK);
    }

    /// Clears the buffer and create a null string RESP response
    pub fn null_string(&self, buffer: &mut BytesMut) {
        buffer.clear();
        self.add_null_string_internal(buffer);
    }

    /// Clears the buffer and create a null array RESP response
    pub fn null_array(&self, buffer: &mut BytesMut) {
        buffer.clear();
        self.add_null_array(buffer);
    }

    /// Clears the buffer and create an empty string RESP response
    pub fn empty_string(&self, buffer: &mut BytesMut) {
        buffer.clear();
        self.append_str(buffer, EMPTY_STRING);
    }

    /// Clears the buffer and create an error string RESP response
    pub fn error_string(&self, buffer: &mut BytesMut, msg: &str) {
        buffer.clear();
        self.append_str(buffer, ERR);
        self.append_str(buffer, msg);
        self.append_str(buffer, CRLF);
    }

    /// Clears the buffer and create a status string message
    pub fn status_string(&self, buffer: &mut BytesMut, msg: &str) {
        buffer.clear();
        self.append_str(buffer, STATUS);
        self.append_str(buffer, msg);
        self.append_str(buffer, CRLF);
    }

    /// Clears the buffer and create a empty RESP response
    pub fn empty_array(&self, buffer: &mut BytesMut) {
        buffer.clear();
        self.append_str(buffer, EMPTY_ARRAY);
    }

    /// Clears the buffer and create a RESP `PONG` response
    pub fn pong(&self, buffer: &mut BytesMut) {
        buffer.clear();
        self.append_str(buffer, PONG);
    }

    /// Clears the buffer and create a RESP number response
    pub fn number_u64(&self, buffer: &mut BytesMut, num: u64) {
        buffer.clear();
        let str_len = format!(":{}\r\n", num);
        buffer.extend_from_slice(str_len.as_bytes());
    }

    /// Clears the buffer and create a RESP number response
    pub fn number_usize(&self, buffer: &mut BytesMut, num: usize) {
        buffer.clear();
        let str_len = format!(":{}\r\n", num);
        buffer.extend_from_slice(str_len.as_bytes());
    }

    /// Clears the buffer and create a RESP number response
    pub fn number_i64(&self, buffer: &mut BytesMut, num: i64) {
        buffer.clear();
        let str_len = format!(":{}\r\n", num);
        buffer.extend_from_slice(str_len.as_bytes());
    }

    /// Clears the buffer and create a RESP number response
    pub fn number<NumberT: std::fmt::Display>(
        &self,
        buffer: &mut BytesMut,
        num: NumberT,
        is_float: bool,
    ) {
        buffer.clear();
        let str_len = if is_float {
            format!(",{}\r\n", num)
        } else {
            format!(":{}\r\n", num)
        };
        buffer.extend_from_slice(str_len.as_bytes());
    }

    /// Append array len to the buffer
    /// NOTE: this function does not clear the buffer
    pub fn add_array_len(&self, buffer: &mut BytesMut, num: usize) {
        let s = format!("*{}\r\n", num);
        buffer.extend_from_slice(s.as_bytes());
    }

    /// Append bulk string to the buffer.
    /// NOTE: this function does not clear the buffer
    pub fn add_bulk_string(&self, buffer: &mut BytesMut, content: &[u8]) {
        self.add_bulk_string_internal(buffer, content);
    }

    /// Convert vector of strings into Resp array of strings
    /// and append it to the buffer
    /// NOTE: this function does not clear the buffer
    pub fn add_strings(&self, buffer: &mut BytesMut, strings: &[&str]) {
        self.add_array_len(buffer, strings.len());
        for s in strings {
            self.add_bulk_string(buffer, s.as_bytes());
        }
    }

    /// Append `resp` into the current buffer
    pub fn add_resp_string(&self, buffer: &mut BytesMut, resp: &[u8]) {
        buffer.extend_from_slice(resp);
    }

    /// Append number
    /// NOTE: this function does not clear the buffer
    pub fn add_number<NumberT: std::fmt::Display>(
        &self,
        buffer: &mut BytesMut,
        num: NumberT,
        is_float: bool,
    ) {
        let str_len = if is_float {
            format!(",{}\r\n", num)
        } else {
            format!(":{}\r\n", num)
        };
        buffer.extend_from_slice(str_len.as_bytes());
    }

    /// Append null string to the buffer
    /// NOTE: this function does not clear the buffer
    pub fn add_null_string(&self, buffer: &mut BytesMut) {
        self.add_null_string_internal(buffer);
    }

    /// Append an empty array
    /// NOTE: this function does not clear the buffer
    pub fn add_empty_array(&self, buffer: &mut BytesMut) {
        buffer.extend_from_slice(EMPTY_ARRAY.as_bytes());
    }

    /// Append an empty array
    /// NOTE: this function does not clear the buffer
    pub fn add_null_array(&self, buffer: &mut BytesMut) {
        buffer.extend_from_slice(NULL_ARRAY.as_bytes());
    }
}
