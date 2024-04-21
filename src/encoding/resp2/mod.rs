mod de;
mod ser;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub use crate::encoding::Error;

pub fn from_bytes<T>(input: &[u8]) -> Result<(T, usize), Error>
where
    T: for<'de> Deserialize<'de>,
{
    let mut deserializer = de::Deserializer::new(input);
    let t = T::deserialize(&mut deserializer)?;

    Ok((t, input.len() - deserializer.left()))
}

pub fn to_bytes<T>(value: &T) -> Result<Bytes, Error>
where
    T: Serialize,
{
    let mut serializer = ser::Serializer::default();
    value.serialize(&mut serializer)?;
    Ok(serializer.into_output())
}
