#[macro_export]
macro_rules! flag {
    ($name: tt, $flag: literal) => {
        pub struct $name(pub String);

        #[async_trait::async_trait]
        impl $crate::request::FromRequest for $name {
            async fn from_request(
                request: crate::request::Request,
            ) -> Result<Self, $crate::error::RedisError> {
                if let Some(pos) = request.args.iter().position(|it| it == $flag) {
                    if let Some(value) = request.args.get(pos + 1) {
                        return Ok($name(value.clone()));
                    }
                }

                Err($crate::error::RedisError::Smth)
            }
        }
    };
    ($name: tt, $ty: ty, $flag: literal) => {
        pub struct $name(pub $ty);

        #[async_trait::async_trait]
        impl $crate::request::FromRequest for $name {
            async fn from_request(
                request: crate::request::Request,
            ) -> Result<Self, $crate::error::RedisError> {
                if let Some(pos) = request.args.iter().position(|it| it == $flag) {
                    if let Some(value) = request.args.get(pos + 1) {
                        return Ok($name(value.parse()?));
                    }
                }

                Err($crate::error::RedisError::Smth)
            }
        }
    };
}
