use core::fmt;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Message {
    //    Null,
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    Integer(i64),
    Array(Vec<Message>),
    RdbFile(Vec<u8>),
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Message::Null => write!(f, "null"),
            Self::SimpleString(the_str) => write!(f, "simple string `{}`", the_str),
            Self::BulkString(the_str) => write!(f, "bulk string `{}`", the_str),
            Self::NullBulkString => write!(f, "null bulk string"),
            Self::Integer(the_int) => write!(f, "integer `{}`", the_int),
            Self::Array(vec) => {
                if vec.len() == 0 {
                    write!(f, "array with zero items")
                } else {
                    write!(f, "array with `{}` items, first: `{}`", vec.len(), vec[0])
                }
            }
            Self::RdbFile(content) => write!(f, "rdb file, len {}", content.len()),
        }
    }
}

fn add_cr_nl(data: &mut Vec<u8>) {
    data.push(b'\r');
    data.push(b'\n');
}

fn add_len(len: usize, data: &mut Vec<u8>) {
    let len = len.to_string();
    data.extend_from_slice(len.as_bytes());
    add_cr_nl(data);
}

impl Message {
    pub fn to_data(&self) -> Vec<u8> {
        match self {
            // Message::Null => {
            //     vec![b'_', b'\r', b'\n']
            // }
            Self::SimpleString(the_str) => {
                let mut data = vec![b'+'];
                data.extend_from_slice(the_str.as_bytes());
                add_cr_nl(&mut data);
                data
            }
            Self::BulkString(the_str) => {
                let mut data = vec![b'$'];
                add_len(the_str.len(), &mut data);
                data.extend_from_slice(the_str.as_bytes());
                add_cr_nl(&mut data);
                data
            }
            Self::NullBulkString => b"$-1\r\n".to_vec(),
            Self::Integer(the_int) => {
                let mut data = vec![b':'];
                data.extend(the_int.to_string().as_bytes());
                add_cr_nl(&mut data);
                data
            }
            Self::Array(arr) => {
                let mut data = vec![b'*'];
                add_len(arr.len(), &mut data);
                for item in arr {
                    data.extend(item.to_data());
                }
                data
            }
            Self::RdbFile(content) => {
                let mut data = vec![b'$'];
                add_len(content.len(), &mut data);
                data.extend(content);
                data
            }
        }
    }

    pub fn rdb_file_from_hex(hex_string: &str) -> Message {
        assert!(hex_string.len() % 2 == 0, "hex string length must be even");

        let bytes = (0..hex_string.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex_string[i..i + 2], 16).expect("hex_string is invalid"))
            .collect::<Vec<_>>();
        Message::RdbFile(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_vec(input: &str) -> Vec<u8> {
        input.into()
    }

    #[test]
    fn test_simple_string() {
        let m = Message::SimpleString("hello".to_string());
        let expected = create_vec("+hello\r\n");

        assert_eq!(expected, m.to_data());
    }

    #[test]
    fn test_bulk_string() {
        let m = Message::BulkString("hell\no".to_string());
        let expected = create_vec("$6\r\nhell\no\r\n");

        assert_eq!(expected, m.to_data());
    }

    #[test]
    fn test_array() {
        let m = Message::Array(vec![
            Message::SimpleString("hello".to_string()),
            Message::SimpleString("trello".to_string()),
        ]);
        let expected = create_vec("*2\r\n+hello\r\n+trello\r\n");

        assert_eq!(expected, m.to_data());
    }

    #[test]
    fn test_integer() {
        let m = Message::Integer(-293);
        let expected = create_vec(":-293\r\n");

        assert_eq!(expected, m.to_data());
    }
}
