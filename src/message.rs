use core::fmt;

#[derive(Clone, Debug, PartialEq)]
pub enum Message{
    SimpleString(String),
    BulkString(String),
    Array(Vec<Message>),
}

impl fmt::Display for Message{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Message::SimpleString(the_str) => write!(f, "simple string `{}`", the_str),
            Message::BulkString(the_str) => write!(f, "bulk string `{}`", the_str),
            Message::Array(vec) => {
                if vec.len() == 0 {
                    write!(f, "array with zero items")
                } else {
                    write!(f, "array with `{}` items, first: `{}`", vec.len(), vec[0])
                }
            }
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
            Message::SimpleString(the_str) => {
                let mut data = vec![b'+'];
                data.extend_from_slice(the_str.as_bytes());
                add_cr_nl(&mut data);
                data
            }
            Message::BulkString(the_str) => {
                let mut data = vec![b'$'];
                add_len(the_str.len(), &mut data);
                data.extend_from_slice(the_str.as_bytes());
                add_cr_nl(&mut data);
                data
            }
            Message::Array(arr) => {
                let mut data = vec![b'*'];
                add_len(arr.len(), &mut data);
                for item in arr {
                    data.extend(item.to_data());
                }
                data
            },
        }
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
}

