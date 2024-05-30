use bytes::{Bytes, BytesMut};
use std::string::FromUtf8Error;
use thiserror::Error;

use crate::message::Message;

// Only RESP3 is implement for now, null values for bulk strings and arrays are not handled

#[derive(Error, Debug, PartialEq)]
pub enum ParseError {
    #[error(transparent)]
    InvalidStringContent(#[from] FromUtf8Error),
    #[error("not a valid string `{0:?}`")]
    InvalidString(Bytes),
    #[error("not a valid message `{0:?}`")]
    InvalidSizeContent(Vec<u8>),
    #[error("unknown message type `{0}`")]
    UnknownMessage(char),
    #[error("no data was passed")]
    NoData,
}

type Result<T> = std::result::Result<T, ParseError>;

type ParsedData = (Message, BytesMut);

pub fn parse_data(mut data: BytesMut) -> Result<Vec<Message>> {
    let mut result = vec![];

    while data.len() > 0 {
        match parse(data) {
            Ok((message, rest)) => {
                result.push(message);
                data = rest;
            }
            Err(err) => return Err(err),
        }
    }

    Ok(result)
}

fn parse(mut data: BytesMut) -> Result<ParsedData> {
    if data.len() == 0 {
        return Err(ParseError::NoData);
    }

    let type_spec = data.split_to(1);
    match type_spec[0] {
        b'+' => parse_simple_string(data),
        b'$' => parse_bulk_string(data),
        b':' => parse_integer(data),
        b'*' => parse_array(data),
        rest => Err(ParseError::UnknownMessage(rest as char)),
    }
}

fn parse_simple_string(mut data: BytesMut) -> Result<ParsedData> {
    match find_linebreak(&data) {
        Some(pos) => {
            let rest = data.split_off(pos + 2);
            let result = String::from_utf8(data[..pos].to_vec())?;
            return Ok((Message::SimpleString(result), rest));
        }
        None => Err(ParseError::InvalidString(data.freeze())),
    }
}

fn parse_bulk_string(mut data: BytesMut) -> Result<ParsedData> {
    if data.is_empty() {
        return Err(ParseError::NoData);
    }

    match data[0] {
        b'-' => {
            if &data[1..4] != b"1\r\n" {
                Err(ParseError::InvalidSizeContent(data.to_vec()))
            } else {
                Ok((Message::NullBulkString, data.split_off(4)))
            }
        }
        _ => match read_number(data) {
            Ok((size, mut data)) => {
                const REDIS_MAGIC: &[u8; 5] = b"REDIS";
                if size > 5
                    // the rdb file has no ending linebreak
                    && (data.len() == size || &data[size + 1..size + 2] != b"\r\n")
                    && &data[..5] == REDIS_MAGIC
                {
                    return Ok((
                        Message::RdbFile(data[..size].to_vec()),
                        data.split_off(size),
                    ));
                }

                let bulk_string = String::from_utf8(data[..size].to_vec())?;
                Ok((Message::BulkString(bulk_string), data.split_off(size + 2)))
            }
            Err(err) => Err(err),
        },
    }
}

fn parse_integer(mut data: BytesMut) -> Result<ParsedData> {
    let negative = if data[0] == b'-' {
        data = data.split_off(1);
        true
    } else if data[0] == b'+' {
        data = data.split_off(1);
        false
    } else {
        false
    };

    match read_number(data) {
        Ok((num, rest)) => {
            if negative {
                Ok((Message::Integer(-(num as i64)), rest))
            } else {
                Ok((Message::Integer(num as i64), rest))
            }
        }
        Err(err) => Err(err),
    }
}

fn parse_array(data: BytesMut) -> Result<ParsedData> {
    match read_number(data) {
        Ok((array_len, mut data)) => {
            let mut result = vec![];
            for _ in 0..array_len {
                match parse(data) {
                    Ok((message, rest_data)) => {
                        result.push(message);
                        data = rest_data;
                    }
                    err => return err,
                }
            }
            Ok((Message::Array(result), data))
        }
        Err(err) => Err(err),
    }
}

fn read_number(mut data: BytesMut) -> Result<(usize, BytesMut)> {
    match find_linebreak(&data[..]) {
        Some(pos) => match convert_to_number(&data[..pos]) {
            Ok(size) => Ok((size, data.split_off(pos + 2))),
            Err(err) => Err(err),
        },
        None => Err(ParseError::InvalidSizeContent(data.to_vec())),
    }
}

fn convert_to_number(data: &[u8]) -> Result<usize> {
    let mut result = 0;
    for c in data {
        if *c < 48 || *c > 57 {
            return Err(ParseError::InvalidSizeContent(data.to_vec()));
        }

        result = result * 10 + (c - b'0') as usize;
    }

    Ok(result)
}

// returns the position of \r in \r\n
fn find_linebreak(data: &[u8]) -> Option<usize> {
    let mut i = 0;

    while i < data.len() - 1 {
        if data[i] == b'\r' && data[i + 1] == b'\n' {
            return Some(i);
        }

        i += 1;
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const RDB_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

    fn str_to_bytes(input: &str) -> BytesMut {
        BytesMut::from(input)
    }

    fn assert_simple_string(input: &str, expected: &str) {
        assert_simple_string_with_rest(input, expected, "");
    }

    fn assert_simple_string_with_rest(input: &str, expected: &str, rest: &str) {
        let data = str_to_bytes(input);
        let rest = str_to_bytes(rest);
        assert_eq!(
            parse_simple_string(data),
            Ok((Message::SimpleString(expected.to_string()), rest))
        );
    }

    fn assert_bulk_string(input: &str, expected: &str) {
        assert_bulk_string_with_rest(input, expected, "");
    }

    fn assert_bulk_string_with_rest(input: &str, expected: &str, rest: &str) {
        let data = str_to_bytes(input);
        let rest = str_to_bytes(rest);
        assert_eq!(
            parse_bulk_string(data),
            Ok((Message::BulkString(expected.to_string()), rest))
        );
    }

    fn assert_array(input: &str, expected: Vec<Message>, rest: &str) {
        let data = str_to_bytes(input);
        let rest = str_to_bytes(rest);
        assert_eq!(parse_array(data), Ok((Message::Array(expected), rest)));
    }

    #[test]
    fn test_read_to_linebreak_no_linebreak() {
        let data = str_to_bytes("Hello");
        let pos = find_linebreak(&data);
        assert_eq!(None, pos);
    }

    #[test]
    fn test_read_to_linebreak_contains_linebreak() {
        let data = str_to_bytes("Hello\r\n");
        let pos = find_linebreak(&data);
        assert_eq!(Some(5), pos);
    }

    #[test]
    fn test_convert_to_number() {
        let result = convert_to_number(&[51, 52]);
        assert_eq!(Ok(34), result);
    }

    #[test]
    fn test_correct_string() {
        assert_simple_string("Hello\r\n", "Hello");
    }

    #[test]
    fn test_simple_string_with_rest() {
        assert_simple_string_with_rest("Hello\r\nAndSomethingElse", "Hello", "AndSomethingElse")
    }

    #[test]
    fn test_simple_string_missing_ending() {
        let data = str_to_bytes("NoCarriageReturnNewline");
        assert_eq!(
            parse_simple_string(data.clone()),
            Err(ParseError::InvalidString(data.freeze()))
        );
    }

    #[test]
    fn test_bulk_string() {
        assert_bulk_string("12\r\nHello\r\nThere\r\n", "Hello\r\nThere");
    }

    #[test]
    fn test_bulk_string_with_rest() {
        assert_bulk_string_with_rest(
            "12\r\nHello\r\nThere\r\n->AndSomethingElse",
            "Hello\r\nThere",
            "->AndSomethingElse",
        );
    }

    #[test]
    fn test_empty_bulk_string() {
        assert_bulk_string("0\r\n\r\n", "");
    }

    #[test]
    fn test_null_bulk_string() {
        let data = str_to_bytes("-1\r\n");
        assert_eq!(
            parse_bulk_string(data),
            Ok((Message::NullBulkString, BytesMut::new()))
        );
    }

    #[test]
    fn test_integer() {
        let data = str_to_bytes("-1939\r\n");
        assert_eq!(
            parse_integer(data),
            Ok((Message::Integer(-1939), BytesMut::new()))
        );

        let data = str_to_bytes("1939\r\naab");
        assert_eq!(
            parse_integer(data),
            Ok((Message::Integer(1939), BytesMut::from("aab")))
        );

        let data = str_to_bytes("+234\r\n");
        assert_eq!(
            parse_integer(data),
            Ok((Message::Integer(234), BytesMut::new()))
        );
    }

    #[test]
    fn test_array_with_two_strings() {
        assert_array(
            "2\r\n+thestr\r\n+theother\r\n",
            vec![
                Message::SimpleString("thestr".to_string()),
                Message::SimpleString("theother".to_string()),
            ],
            "",
        );
    }

    #[test]
    fn test_array_with_nested_array() {
        assert_array(
            "2\r\n$3\r\nstr\r\n*1\r\n+theother\r\nSomething",
            vec![
                Message::BulkString("str".to_string()),
                Message::Array(vec![Message::SimpleString("theother".to_string())]),
            ],
            "Something",
        );
    }

    #[test]
    fn test_parse() {
        let data = str_to_bytes("+simple\r\n");
        assert_eq!(
            parse(data),
            Ok((Message::SimpleString("simple".to_string()), BytesMut::new()))
        );
    }

    #[test]
    fn test_parse_data_multiple_messages() {
        let data = str_to_bytes("*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n");
        assert_eq!(
            parse_data(data).unwrap(),
            vec![
                Message::Array(vec![
                    Message::BulkString("SET".to_string()),
                    Message::BulkString("bar".to_string()),
                    Message::BulkString("456".to_string()),
                ]),
                Message::Array(vec![
                    Message::BulkString("SET".to_string()),
                    Message::BulkString("baz".to_string()),
                    Message::BulkString("789".to_string()),
                ]),
            ]
        );
    }

    #[test]
    fn test_parse_data_rdb_file() {
        let rdb = Message::rdb_file_from_hex(RDB_HEX);
        let data = rdb.to_data();

        assert_eq!(parse_data(BytesMut::from(&data[..])).unwrap()[0], rdb);
    }

    #[test]
    fn test_parse_data_rdb_file_and_message() {
        let rdb = Message::rdb_file_from_hex(RDB_HEX);
        let mut data = rdb.to_data();
        let message2 = Message::SimpleString("theother".to_string());
        data.extend_from_slice(&message2.to_data());

        assert_eq!(
            parse_data(BytesMut::from(&data[..])).unwrap(),
            vec![rdb, message2]
        );
    }
}
