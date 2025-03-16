# Yet Another Pool

A pool implementation for [Tokio](https://tokio.rs).

## Examples

```rust
// Object that needs to be pooled.
struct MyObject;

// Define a manager to oversee the lifecycle of the objects.
#[derive(Debug)]
struct MyManager;
impl yapool::Manager for MyManager {
    type Object = MyObject;
    type Error = &'static str;

    // Called to ceate new objects.
    async fn create(&self) -> Result<Self::Object, Self::Error> {
        Ok(MyObject)
    }
}

#[tokio::main]
async fn main() {
    // Dive in and create a pool.
    let manager = MyManager;
    let config = yapool::PoolConfig::default();
    let pool = yapool::Pool::new(manager, config).await.unwrap();

    // Pull an object from the pool.
    let object = pool.acquire().await.unwrap();

    // Objects are thrown back into the pool when dropped.
    drop(object);

    // Swimming is finished for the day, the pool is closed. Objects will be destroyed.
    pool.close().await.unwrap();
}
```
