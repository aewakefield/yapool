/// Manager of the objects in the pool. Responsible for the lifecycle of the objects in the pool.
/// The only menthod that is required is create.
pub trait Manager: Send + Sync + 'static {
    /// The type of the objects in the pool.
    type Object: Send;
    /// The type of the errors the manager can return.
    type Error: Send;

    /// Create a new object to be used in the pool.
    fn create(&self) -> impl Future<Output = Result<Self::Object, Self::Error>> + Send;

    /// Destroy an object when it is removed from the pool. If not implemented
    /// the object will just be dropped when removed from the pool.
    fn destroy(
        &self,
        _object: Self::Object,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }

    /// Reset an object when it is returned to the pool.
    fn reset(
        &self,
        _object: &mut Self::Object,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }

    /// Check if an object is still alive before being acquired from the pool.
    fn check_liveness(
        &self,
        _object: &mut Self::Object,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send {
        std::future::ready(Ok(true))
    }
}
