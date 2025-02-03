use ballista::prelude::*;
use datafusion::{
    execution::SessionStateBuilder, prelude::{DataFrame, ParquetReadOptions, SessionConfig, SessionContext}
};
use tokio;
use std::env;

#[tokio::main]
async fn main() -> Result<(), std::io::Error>  {
    let path = env::current_dir()?;

    let auction_path = path.join("../data/auction.parquet").to_str().unwrap().to_string();
    let person_path = path.join("../data/person.parquet").to_str().unwrap().to_string();
    let bid_path = path.join("../data/bid.parquet").to_str().unwrap().to_string();

    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_ballista_job_name("Nex Mark Streaming Example");

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

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

async fn nex_mark_q5(ctx: &SessionContext) -> Result<DataFrame, std::io::Error> {
    let df = ctx
        .sql(
            r#"
            WITH WindowedBids AS (
                SELECT auction, COUNT(*) AS num
                FROM Bid
                WHERE date_time >= CURRENT_TIMESTAMP - INTERVAL '60 minutes'
                GROUP BY auction
            ),
            MaxBidCount AS (
                SELECT MAX(num) AS max_num
                FROM WindowedBids)
            SELECT auction
            FROM WindowedBids
            WHERE num = (SELECT max_num FROM MaxBidCount);
            "#,
        )
        .await?;

    Ok(df)
}

async fn nex_mark_q4(ctx: &SessionContext) -> Result<DataFrame, std::io::Error> {
    let df = ctx
        .sql(
            r#"SELECT 
                    C.id AS category_id,
                    AVG(Q.final) AS avg_final_price
                FROM 
                    Category C
                JOIN 
                    (
                        SELECT 
                            MAX(B.price) AS final, 
                            A.category AS category_id
                        FROM 
                            Auction A
                        JOIN 
                            Bid B 
                        ON 
                            A.id = B.auction
                        WHERE 
                            B.datetime < A.expires 
                            AND A.expires < CURRENT_TIMESTAMP
                        GROUP BY 
                            A.id, A.category
                    ) Q
                ON 
                    Q.category_id = C.id
                GROUP BY 
                    C.id;
            "#,
        )
        .await?;

    Ok(df)
}

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