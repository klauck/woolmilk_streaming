# Nexmark Queries Over Datafusion

Running nexmark queries over Datafusion is difficult because Apache DataFusion is built around SQL. To run `CQL` over DataFusion we need to convert `SQL` queries into `CQL`.

## Project Setup

### Cargo.toml

```toml
[package]
name = "streaming-example"
version = "0.1.0"
edition = "2021"

[dependencies]
datafusion = "43.0.0"
tokio = "1.43.0"
```

### src/main.rs

```rust
use datafusion::{
    prelude::{ParquetReadOptions, SessionConfig, SessionContext}
};
use tokio;
use std::env;

#[tokio::main]
async fn main() -> Result<(), std::io::Error>  {
    let path = env::current_dir()?;

    let auction_path = path.join("../data/auction.parquet").to_str().unwrap().to_string();
    let person_path = path.join("../data/person.parquet").to_str().unwrap().to_string();
    let bid_path = path.join("../data/bid.parquet").to_str().unwrap().to_string();

    let ctx = SessionContext::new();

    ctx.register_parquet("auction", auction_path, ParquetReadOptions::default()).await?;
    ctx.register_parquet("person", person_path, ParquetReadOptions::default()).await?;
    ctx.register_parquet("bid", bid_path, ParquetReadOptions::default()).await?;

    let df_q1 = nex_mark_q1(&ctx).await?;
    let df_q2 = nex_mark_q2(&ctx).await?;
    let df_q3 = nex_mark_q3(&ctx).await?;

    df_q1.show().await?;
    df_q2.show().await?;
    df_q3.show().await?;

    Ok(())
}
```

### Running Queries

```rust
async fn nex_mark_q3(ctx: &SessionContext) -> Result<DataFrame, std::io::Error> {
    let df = ctx
        .sql(
            r#"SELECT P.name, P.city, P.state, A.id
            FROM Auction A JOIN Person P ON A.seller = P.id
            WHERE (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA') AND A.category = 10;
            "#,
        )
        .await?;

    Ok(df)
}

async fn nex_mark_q2(ctx: &SessionContext) -> Result<DataFrame, std::io::Error> {
    let df = ctx
        .sql(
            r#"
            SELECT
                auction,
                price
            FROM
                Bid
            WHERE
                auction = 1007
                OR auction = 1020
                OR auction = 2001
                OR auction = 2019
                OR auction = 2087;
            "#,
        )
        .await?;

    Ok(df)
}

async fn nex_mark_q1(ctx: &SessionContext) -> Result<DataFrame, std::io::Error> {
    let df = ctx
        .sql(
            r#"
            SELECT
                auction,
                price,
                bidder,
                date_time
            FROM
                bid;
            "#,
        )
        .await?;

    Ok(df)
}
```
