use std::{ops::Bound, time::Duration};

use crate::{
    commands::stream::parameters::{StreamRangeEnd, StreamRangeStart, StreamReadStart},
    engine::SharedEngine,
    error::RedisError,
    flag,
    request::{Arg, ArgParse, Extension, Request},
    response::{IntoResponse, Resp2},
    value::{StreamId, StreamRange},
};

mod parameters {
    use std::{ops::Bound, str::FromStr};

    use derive_more::{Display, From, Into};
    use eyre::eyre;

    use crate::{
        error::RedisError,
        value::{StreamId, StreamRange},
    };

    #[derive(Debug, Clone, Copy, From, Into, Display)]
    #[display(fmt = "{}", _0)]
    pub struct StreamRangeStart(pub StreamId);

    impl FromStr for StreamRangeStart {
        type Err = RedisError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            if s == "-" {
                return Ok(Self(StreamId::MIN));
            }

            if s.contains("-") {
                return Ok(Self(s.parse()?));
            }
            let ts = s.parse().map_err(|_| eyre!("Could not parse stream id"))?;

            Ok(Self(StreamId::from((ts, 0))))
        }
    }

    #[derive(Debug, Clone, Copy, From, Into, Display)]
    #[display(fmt = "{}", _0)]
    pub struct StreamRangeEnd(pub StreamId);

    impl FromStr for StreamRangeEnd {
        type Err = RedisError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            if s == "+" {
                return Ok(Self(StreamId::MAX));
            }

            if s.contains("-") {
                return Ok(Self(s.parse()?));
            }
            let ts = s.parse().map_err(|_| eyre!("Could not parse stream id"))?;

            Ok(Self(StreamId::from((ts, u64::MAX))))
        }
    }

    impl Into<StreamRange> for (StreamRangeStart, StreamRangeEnd) {
        fn into(self) -> StreamRange {
            StreamRange::from((Bound::Included(self.0 .0), Bound::Included(self.1 .0)))
        }
    }

    #[derive(Debug, Clone, Copy, From, Into, Display)]
    #[display(fmt = "{}", _0)]
    pub struct StreamReadStart(pub StreamId);

    impl StreamReadStart {
        pub fn into_bound(self) -> Bound<StreamId> {
            if self.0 == StreamId::MAX {
                Bound::Included(self.0)
            } else {
                Bound::Excluded(self.0)
            }
        }
    }

    impl FromStr for StreamReadStart {
        type Err = RedisError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            if s == "$" {
                return Ok(Self(StreamId::MAX));
            }

            if s.contains("-") {
                return Ok(Self(s.parse()?));
            }
            let ts = s.parse().map_err(|_| eyre!("Could not parse stream id"))?;

            Ok(Self(StreamId::from((ts, u64::MIN))))
        }
    }
}

pub async fn xadd(
    Extension(engine): Extension<SharedEngine>,
    Arg(stream): Arg<1>,
    ArgParse(id): ArgParse<StreamId, 2>,
    request: Request,
) -> Result<impl IntoResponse, RedisError> {
    if request.args.len() % 2 != 0 {
        return Err(RedisError::UnknownCommand);
    }

    let data = request.args.into_iter().skip(2).collect::<Vec<_>>();

    let id = engine.append(&stream, id, data)?;

    Ok(id.to_string())
}

pub async fn xrange(
    Extension(engine): Extension<SharedEngine>,
    Arg(stream): Arg<1>,
    ArgParse(start): ArgParse<StreamRangeStart, 2>,
    ArgParse(end): ArgParse<StreamRangeEnd, 3>,
) -> Result<impl IntoResponse, RedisError> {
    let data = engine.range(&stream, (start, end).into(), usize::MAX)?;

    Ok(Resp2(data))
}

flag!(Count, usize, "count");
flag!(Block, usize, "block");

pub async fn xread(
    Extension(engine): Extension<SharedEngine>,
    count: Option<Count>,
    block: Option<Block>,
    request: Request,
) -> Result<impl IntoResponse, RedisError> {
    let count = count.unwrap_or(Count(usize::MAX)).0;

    let mut output = vec![];
    let Some(streams_pos) = request
        .args
        .iter()
        .position(|it| it.eq_ignore_ascii_case("streams"))
    else {
        return Err(RedisError::UnknownCommand);
    };

    let args = &request.args[streams_pos + 1..];

    if args.len() % 2 != 0 {
        return Err(RedisError::UnknownCommand);
    }

    let (keys, ids) = args.split_at(args.len() / 2);
    for (key, id) in keys.iter().zip(ids) {
        let start: StreamReadStart = id.parse()?;
        if start.0 == StreamId::MAX {
            continue;
        }

        let values = engine.range(
            key,
            StreamRange(start.into_bound(), Bound::Unbounded),
            count,
        )?;
        if values.is_empty() {
            continue;
        }
        output.push((key.to_owned(), values));
    }

    match block {
        Some(Block(timeout)) if output.is_empty() => {
            let timeout = if timeout == 0 {
                u64::MAX
            } else {
                timeout as u64
            };
            let Ok(_) =
                tokio::time::timeout(Duration::from_millis(timeout), engine.wait().for_keys(keys))
                    .await
            else {
                return Ok(Resp2(None));
            };

            for (key, id) in keys.iter().zip(ids) {
                let start: StreamReadStart = id.parse()?;
                let values = engine.range(
                    key,
                    StreamRange(start.into_bound(), Bound::Unbounded),
                    count,
                )?;
                if values.is_empty() {
                    continue;
                }
                output.push((key.to_owned(), values));
            }
        }
        _ => {}
    }

    Ok(Resp2(Some(output)))
}
