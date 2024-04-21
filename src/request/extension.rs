use std::task::{Context, Poll};

use async_trait::async_trait;
use tower::{Layer, Service};

use crate::{
    error::RedisError,
    request::{FromRequest, Request},
};

#[derive(Clone)]
pub struct Extension<T>(pub T);

#[async_trait]
impl<T: Clone + Send + Sync + 'static> FromRequest for Extension<T> {
    async fn from_request(request: Request) -> Result<Self, RedisError> {
        match request.extensions().get::<T>() {
            Some(v) => Ok(Extension(v.clone())),
            _ => panic!("Extension {} is not installed", std::any::type_name::<T>()),
        }
    }
}

impl<S, T> Layer<S> for Extension<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Service = AddExtension<S, T>;

    fn layer(&self, inner: S) -> Self::Service {
        AddExtension {
            inner,
            value: self.0.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct AddExtension<S, T> {
    pub(crate) inner: S,
    pub(crate) value: T,
}

impl<S, T> Service<Request> for AddExtension<S, T>
where
    S: Service<Request>,
    T: Clone + Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request) -> Self::Future {
        req.extensions_mut().insert(self.value.clone());
        self.inner.call(req)
    }
}
