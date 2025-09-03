use ballista::prelude::*;
use datafusion::{
    execution::SessionStateBuilder, logical_expr::LogicalPlan, prelude::{ParquetReadOptions, SessionConfig, SessionContext}
};
use substrait_test::{generate_substrait_plan_from_df, load_substrait_plan_from_file};
use tokio;
use std::env;
mod nexmark_queries;
mod substrait_test;

#[tokio::main]
async fn main() -> Result<(), std::io::Error>  {
    let path = env::current_dir()?;

    let auction_path = path.join("../data/auction.parquet").to_str().unwrap().to_string();
    let person_path = path.join("../data/person.parquet").to_str().unwrap().to_string();
    let bid_path = path.join("../data/bid.parquet").to_str().unwrap().to_string();
    let category_path = path.join("../data/category.parquet").to_str().unwrap().to_string();

    //if we want to use datafusion without ballista we can set this to true
    let use_plain_datafusion = true;
    let ctx: SessionContext;

    if use_plain_datafusion 
    {
        //use data fusion without ballista
        ctx = SessionContext::new();
    }
    else 
    {
        //use data fusion with ballista
        //make sure to start the ballista scheduler followed by the executor before, other wise this will fail
        let config = SessionConfig::new_with_ballista()
            .with_target_partitions(4)
            .with_ballista_job_name("Nex Mark Streaming Example");

        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();

        ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;
    }

    ctx.register_parquet("auction", auction_path, ParquetReadOptions::default()).await?;
    ctx.register_parquet("person", person_path, ParquetReadOptions::default()).await?;
    ctx.register_parquet("bid", bid_path, ParquetReadOptions::default()).await?;
    ctx.register_parquet("category", category_path, ParquetReadOptions::default()).await?;

    let df_q1 = nexmark_queries::nex_mark_q1(&ctx).await?;

    let out_file = "nx_1.json";
    generate_substrait_plan_from_df(&df_q1, &ctx, out_file).await?;
    load_substrait_plan_from_file(out_file, &ctx).await?;    

    let df_q2 = nexmark_queries::nex_mark_q2(&ctx).await?;
    let df_q3 = nexmark_queries::nex_mark_q3(&ctx).await?;
    let df_q4 = nexmark_queries::nex_mark_q4(&ctx).await?;
    let df_q5 = nexmark_queries::nex_mark_q5(&ctx).await?;

    df_q1.show().await?;
    df_q2.show().await?;
    df_q3.show().await?;
    df_q4.show().await?;
    df_q5.show().await?;

    Ok(())
}