#![allow(dead_code, unused_imports, unreachable_code, unused_variables)]

use std::marker::PhantomData;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use nom::AsBytes;
use serde::{
    de,
    de::{DeserializeSeed, EnumAccess, SeqAccess, VariantAccess, Visitor},
};

use super::Error;

pub struct Deserializer<'de> {
    input: &'de [u8],
}

impl<'de> Deserializer<'de> {
    pub fn left(&self) -> usize {
        self.input.len()
    }
    pub fn is_empty(&self) -> bool {
        self.input.is_empty()
    }
    pub fn new(input: &'de [u8]) -> Self {
        Self { input }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(0).cloned()
    }

    fn consume(&mut self) -> u8 {
        self.input.get_u8()
    }

    fn sep(&mut self) -> Result<(), Error> {
        let (rest, _) = parse::separator(self.input)?;
        self.input = rest;
        Ok(())
    }

    fn get_integer(&mut self) -> Result<i64, Error> {
        let (rest, number) = parse::number(self.input)?;
        self.input = rest;
        self.sep()?;
        Ok(number)
    }

    fn get_bytes(&mut self) -> Result<Vec<u8>, Error> {
        let (rest, bytes) = parse::bytes(self.input)?;
        self.input = rest;
        self.sep()?;

        Ok(bytes.to_owned())
    }

    fn get_any_string(&mut self) -> Result<String, Error> {
        let (rest, string) = parse::any_string(self.input)?;
        self.input = rest;
        self.sep()?;
        Ok(string)
    }
}

mod parse {
    use nom::{
        branch::alt,
        bytes::complete::{tag, take, take_until, take_while},
        character::is_digit,
        error,
        error::ErrorKind,
        IResult,
    };

    pub fn number(input: &[u8]) -> IResult<&[u8], i64> {
        let (rest, u) = take_while(is_digit)(input)?;
        let u = std::str::from_utf8(u).expect("valid utf8");
        let u: i64 = u
            .parse()
            .map_err(|_| nom::Err::Error(error::Error::new(input, ErrorKind::Digit)))?;
        Ok((rest, u))
    }

    pub fn any_string(input: &[u8]) -> IResult<&[u8], String> {
        alt((simple_string, byte_string))(input)
    }

    pub fn bytes(input: &[u8]) -> IResult<&[u8], &[u8]> {
        let (input, _) = tag("$")(input)?;
        let (input, len) = number(input)?;
        if len == -1 {
            return Ok((input, &[]));
        }
        let (input, _) = separator(input)?;
        let (input, b) = take(len as usize)(input)?;
        Ok((input, b))
    }

    pub fn byte_string(input: &[u8]) -> IResult<&[u8], String> {
        let (input, bytes) = bytes(input)?;

        if bytes.len() == 0 {
            return Ok((input, String::new()));
        }

        let s = std::str::from_utf8(bytes).expect("valid utf8");
        Ok((input, s.to_owned()))
    }

    pub fn simple_string(input: &[u8]) -> IResult<&[u8], String> {
        let (input, _) = tag("+")(input)?;
        let (input, b) = take_until("\r\n")(input)?;
        let s = std::str::from_utf8(b).expect("valid utf8");

        Ok((input, s.to_owned()))
    }

    pub fn separator(input: &[u8]) -> IResult<&[u8], ()> {
        let (input, _) = tag("\r\n")(input)?;
        Ok((input, ()))
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let v = self.get_bytes()?;

        visitor.visit_byte_buf(v)
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_string(self.get_any_string()?)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_byte_buf(self.get_bytes()?)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.peek() == Some(b'*') {
            let _ = self.consume();
            let length = self.get_integer()? as _;
            let value = visitor.visit_seq(Array(length, self))?;

            Ok(value)
        } else {
            Err(Error::ExpectedArray)
        }
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_enum(Enum(&mut *self))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_string(self.get_any_string()?.to_lowercase())
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }
}

struct Array<'de, 'a>(usize, &'a mut Deserializer<'de>);

impl<'de, 'a> SeqAccess<'de> for Array<'de, 'a> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.0 == 0 {
            return Ok(None);
        }
        self.0 -= 1;

        let v = seed.deserialize(&mut *self.1)?;

        Ok(Some(v))
    }
}

struct Enum<'de, 'a>(&'a mut Deserializer<'de>);

impl<'de, 'a> EnumAccess<'de> for Enum<'de, 'a> {
    type Error = Error;
    type Variant = Variant<'de, 'a>;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let val = seed.deserialize(&mut *self.0)?;
        Ok((val, Variant(&mut *self.0)))
    }
}

struct Variant<'de, 'a>(&'a mut Deserializer<'de>);

impl<'de, 'a> VariantAccess<'de> for Variant<'de, 'a> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self.0)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }
}
