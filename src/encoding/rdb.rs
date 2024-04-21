use std::time::SystemTime;

use eyre::eyre;
use tracing::{instrument, Level};

use crate::value::RedisValue;

#[instrument(level = Level::DEBUG, skip(input), err)]
pub fn read_rdb_file<'a>(
    input: &'a [u8],
) -> eyre::Result<(
    Vec<(String, String)>,
    impl Iterator<Item = (String, RedisValue, Option<SystemTime>)> + 'a,
)> {
    let (mut input, aux_data) =
        parse::header(input).map_err(|_| eyre!("failed reading header of rdb file"))?;
    tracing::info!(?aux_data, ?input, "Read header of RDB file");

    let items_iter = std::iter::from_fn(move || {
        let Ok((rest, entry)) = parse::data_entry(input) else {
            return None;
        };
        input = rest;
        entry
    });
    Ok((aux_data, items_iter))
}

mod parse {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use nom::{
        branch::alt,
        bytes::complete::{tag, take},
        combinator::opt,
        error,
        error::ErrorKind,
        multi::{count, many0},
        number::complete::{be_u64, le_u128, le_u32, le_u64, le_u8},
        sequence::pair,
        IResult,
    };

    use crate::value::{RedisValue, Stream, StreamId, ValueType};

    pub fn metadata(input: &[u8]) -> IResult<&[u8], (Option<SystemTime>, ValueType)> {
        let (input, ts) = opt(alt((expiry_seconds, expiry_milliseconds)))(input)?;
        let (input, tag) = value_type(input)?;

        Ok((input, (ts, tag)))
    }

    pub fn data_entry(
        input: &[u8],
    ) -> IResult<&[u8], Option<(String, RedisValue, Option<SystemTime>)>> {
        let (input, _) = opt(sector_start)(input)?;

        if let (_, Some(_)) = opt(database_end)(input)? {
            return Ok((&[], None));
        }

        let (input, entry) = sector_entry(input)?;
        Ok((input, Some(entry)))
    }

    fn sector_entry(input: &[u8]) -> IResult<&[u8], (String, RedisValue, Option<SystemTime>)> {
        let (input, (expiration, value_type)) = metadata(input)?;
        let (input, key) = string_data(input)?;
        let (input, value) = value(input, value_type)?;

        Ok((input, (key, value, expiration)))
    }

    fn value(input: &[u8], ty: ValueType) -> IResult<&[u8], RedisValue> {
        match ty {
            ValueType::String => {
                let (input, v) = string_data(input)?;
                Ok((input, RedisValue::String(v)))
            }
            ValueType::Stream => {
                let (input, v) = stream_data(input)?;
                Ok((input, RedisValue::Stream(v)))
            }
        }
    }

    fn database_end(input: &[u8]) -> IResult<&[u8], u64> {
        let (input, _) = tag(&[0xFF])(input)?;
        let (input, checksum) = be_u64(input)?;
        Ok((input, checksum))
    }

    fn sector_start(input: &[u8]) -> IResult<&[u8], usize> {
        let (input, _) = tag(&[0xFE])(input)?;
        let (input, size) = length_encoding(input)?;
        let (input, _) = opt(sector_resize)(input)?;
        Ok((input, size))
    }

    fn sector_resize(input: &[u8]) -> IResult<&[u8], (usize, usize)> {
        let (input, _) = tag(&[0xFB])(input)?;
        let (input, hash_table_size) = length_encoding(input)?;
        let (input, expire_table_size) = length_encoding(input)?;
        Ok((input, (hash_table_size, expire_table_size)))
    }

    pub fn header(input: &[u8]) -> IResult<&[u8], Vec<(String, String)>> {
        let (input, _) = tag(b"REDIS")(input)?;
        let (input, version) = take(4usize)(input)?;
        let version = std::str::from_utf8(version).expect("to be utf8");

        let (input, mut aux) = many0(aux_field)(input)?;

        aux.push(("version".to_owned(), version.to_owned()));
        Ok((input, aux))
    }

    fn aux_field(input: &[u8]) -> IResult<&[u8], (String, String)> {
        let (input, _) = tag(&[0xFA])(input)?;
        let (input, key) = string_data(input)?;
        let (input, value) = string_data(input)?;
        Ok((input, (key, value)))
    }

    pub fn string_data(input: &[u8]) -> IResult<&[u8], String> {
        let (input, len) = length_encoding(input)?;
        let (input, bytes) = take(len)(input)?;
        let v = match std::str::from_utf8(bytes) {
            Ok(v) => v.to_owned(),
            Err(_) if len == 1 => bytes[0].to_string(),
            Err(_) if len == 2 => i16::from_le_bytes([bytes[0], bytes[1]]).to_string(),
            Err(_) if len == 4 => {
                i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]).to_string()
            }
            Err(_) => String::from_utf8_lossy(bytes).into_owned(),
        };

        Ok((input, v))
    }

    #[allow(unused_variables)]
    pub fn stream_data(input: &[u8]) -> IResult<&[u8], Stream> {
        let (input, listpacks) = length_encoding(input)?;
        let (input, stream_listpacks) = count(pair(string_data, string_data), listpacks)(input)?;

        let (input, items) = length_encoding(input)?;
        let (input, last_entry) = stream_id(input)?;

        let (input, first_entry) = stream_id(input)?;
        let (input, maximal_deleted) = stream_id(input)?;
        let (input, entries_added) = length_encoding(input)?;

        let (input, cgroups) = length_encoding(input)?;
        let (input, cgroup_entries) = count(cgroup, cgroups)(input)?;
        Ok((input, Stream::new())) // todo: implement proper decoding
    }

    fn stream_id(input: &[u8]) -> IResult<&[u8], StreamId> {
        let (input, (ts, c)) = pair(length_encoding, length_encoding)(input)?;
        Ok((input, StreamId::from((ts as u64, c as u64))))
    }

    #[allow(unused_variables)]
    fn cgroup(input: &[u8]) -> IResult<&[u8], ()> {
        let (input, name) = string_data(input)?;
        let (input, last_entry) = stream_id(input)?;
        let (input, entries_read) = length_encoding(input)?;

        let (input, pending) = length_encoding(input)?;
        fn group_pending_entries(input: &[u8]) -> IResult<&[u8], ()> {
            let (input, eid) = le_u128(input)?;
            let (input, delivery_time) = le_u64(input)?;
            let (input, delivery_count) = length_encoding(input)?;
            Ok((input, ()))
        }

        let (input, pending_entries) = count(group_pending_entries, pending)(input)?;

        let (input, consumers) = length_encoding(input)?;
        fn consumer_data(input: &[u8]) -> IResult<&[u8], ()> {
            let (input, name) = string_data(input)?;
            let (input, seen_time) = le_u64(input)?;
            let (input, active_time) = le_u64(input)?;
            let (input, pending) = length_encoding(input)?;
            let (input, eids) = count(le_u128, pending)(input)?;
            Ok((input, ()))
        }
        let (input, consumer_entries) = count(consumer_data, consumers)(input)?;
        Ok((input, ()))
    }

    fn length_encoding(input: &[u8]) -> IResult<&[u8], usize> {
        let (rest, &[first]) = take(1usize)(input)? else {
            unreachable!()
        };

        return match first {
            0b11000000.. => {
                let special_encoding = first & !0b11_000_000;
                let len = match special_encoding {
                    0 => 1usize,
                    1 => 2usize,
                    2 => 4usize,
                    3 => unimplemented!("lzf encoding"),
                    _ => return Err(nom::Err::Error(error::Error::new(input, ErrorKind::Digit))),
                };
                Ok((rest, len))
            }
            0b10000000.. => {
                let (input, len) = le_u32(rest)?;
                Ok((input, len as usize))
            }
            0b01000000.. => {
                let low = (first & !0b01_000_000) as usize;
                let (input, high) = le_u8(rest)?;
                let len = (high as usize) << 8 + low;
                Ok((input, len))
            }
            _ => {
                return Ok((rest, first as usize));
            }
        };
    }

    fn expiry_seconds(input: &[u8]) -> IResult<&[u8], SystemTime> {
        let (input, _) = tag(&[0xFD])(input)?;
        let (input, ts) = le_u32(input)?;
        let t = UNIX_EPOCH
            .checked_add(Duration::from_secs(ts as u64))
            .expect("to be valid instant");
        Ok((input, t))
    }

    fn expiry_milliseconds(input: &[u8]) -> IResult<&[u8], SystemTime> {
        let (input, _) = tag(&[0xFC])(input)?;
        let (input, ts) = le_u64(input)?;
        let t = UNIX_EPOCH
            .checked_add(Duration::from_millis(ts))
            .expect("to be valid instant");
        Ok((input, t))
    }

    fn value_type(input: &[u8]) -> IResult<&[u8], ValueType> {
        let (rest, &[tag]) = take(1usize)(input)? else {
            unreachable!()
        };

        let tag = match tag {
            0 => ValueType::String,
            19 | 21 => ValueType::Stream,
            _ => return Err(nom::Err::Error(error::Error::new(input, ErrorKind::Char))),
        };
        Ok((rest, tag))
    }
}

mod encode {
    use bytes::BufMut;

    #[allow(dead_code)]
    fn length_encode(output: &mut impl BufMut, len: usize) {
        const MAX: usize = 2 << 32 - 1;
        match len {
            0..=63 => {
                output.put_u8(len as _);
            }
            64..=16383 => {
                let [a, b, ..] = len.to_le_bytes();
                let a = a | 0b01000000;
                output.put_slice(&[a, b]);
            }
            16384..=MAX => {
                output.put_u8(0b10000000);
                output.put_u32_le(len as u32);
            }
            _ => unimplemented!(),
        }
    }
}

pub static EMPTY: &'static [u8] = &[
    0x52u8, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69,
    0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64,
    0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65,
    0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2,
    0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00,
    0xff, 0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
];
