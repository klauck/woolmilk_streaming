# Apache DataFusion

Apache DataFusion is a query engine written in `Rust` that uses Apache Arrow as its backend. DataFusion has built-in support for different kinds of files such as CSV, Parquet, and JSON.

To get started with DataFusion in Rust, follow these simple steps.

## Create `Cargo.toml`

Firstly, create a `Cargo.toml` file in the base directory with the following contents:

```toml
[package]
name = "streaming-example"
version = "0.1.0"
edition = "2021"

[dependencies]
datafusion = "43.0.0"
tokio = { version = "1.43.0", features = ["full"] }
```

## Create src/main.rs

Create a src directory and add a main.rs file inside it with the following content:

```rust
use datafusion::{
    prelude::SessionContext,
    datasource::parquet::ParquetReadOptions,
};
use tokio;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    ctx.register_parquet("data", "path/to/data.parquet", ParquetReadOptions::default()).await?;

    let df = ctx.sql("SELECT * FROM data").await?;

    df.show().await?;

    Ok(())
}
```

## Running the Example

Ensure that the path to your Parquet file `path/to/data.parquet` is correct.

```bash
cargo update
cargo run
```

## Continue Reading

[Nexmark Queries Over Datafusion](nexmark-queries-over-datafusion.md)
