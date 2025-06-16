use std::io::{self, Error, Write};
use arrow::array::RecordBatch;
use datafusion::prelude::*;
use serde_json::to_string_pretty;
use crate::nexmark::NexmarkDataGenerator;
use datafusion_substrait::{logical_plan::{consumer::from_substrait_plan, producer::to_substrait_plan}};

pub async fn run_nexmark_query_2() -> Result<(),  Error> {
    println!("Generating Nexmark data for query 2.");
    let data_generator = NexmarkDataGenerator::new(100000, 100000);
    let mut bids: Option<RecordBatch> = None;

    for (_, _, bid_batch, _) in data_generator.iter_batches() {
       bids = Some(bid_batch);
    }

    println!("Nexmark data generated with {} bids.", 
             bids.as_ref().map_or(0, |b| b.num_rows()));

    let ctx = SessionContext::new();

    // unwrap bids
    let bids = bids.ok_or_else(|| io::Error::new(io::ErrorKind::Other, "No bids data found"))?;

    ctx.register_batch("bid", bids)?;

    let df_q2_sql = run_query_2_sql(&ctx).await?;
    let df_q2_df_api = run_query_2_dataframe_api(&ctx).await?;
    let df_q2_substrait = run_query_2_substrait(&ctx, true).await?;

    df_q2_sql.show().await?;
    df_q2_df_api.show().await?;
    df_q2_substrait.show().await?;

    println!("Query 2 completed successfully.");

    Ok(())
}

async fn run_query_2_substrait(ctx: &SessionContext, save_and_load_from_json: bool) -> Result<DataFrame, Error> {
    let df = ctx
        .table("bid")
        .await?
        .filter(
            col("auction").eq(lit(1007))
                .or(col("auction").eq(lit(1020)))
                .or(col("auction").eq(lit(2001)))
                .or(col("auction").eq(lit(2019)))
                .or(col("auction").eq(lit(2087)))
        )?
        .select(vec![col("auction"), col("price")])?;
    
    let optimized_plan = df.clone().into_optimized_plan()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Plan optimization error: {}", e)))?;
    
    let substrait_plan = to_substrait_plan(&optimized_plan, &ctx.state())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Substrait conversion error: {}", e)))?;

    let restored_df = if save_and_load_from_json {
        let json = to_string_pretty(&substrait_plan)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("JSON serialization error: {}", e)))?;

        let mut file = std::fs::File::create("query2_substrait_plan.json")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("File creation error: {}", e)))?;
        
        file.write_all(json.as_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("File write error: {}", e)))?;
        
        println!("Substrait plan saved to query2_substrait_plan.json");
        
        let file = std::fs::File::open("query2_substrait_plan.json")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("File open error: {}", e)))?;
        
        let loaded_plan: datafusion_substrait::substrait::proto::Plan = serde_json::from_reader(file)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("JSON deserialization error: {}", e)))?;
        
        println!("Substrait plan loaded from query2_substrait_plan.json");
        
        let restored_plan = from_substrait_plan(&ctx.state(), &loaded_plan).await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Substrait restoration from JSON error: {}", e)))?;
        
        DataFrame::new(ctx.state(), restored_plan)
    } else {
        //direct restoration without saving to JSON
        let restored_plan = from_substrait_plan(&ctx.state(), &substrait_plan).await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Substrait restoration error: {}", e)))?;
        
        DataFrame::new(ctx.state(), restored_plan)
    };
    
    Ok(restored_df)
}

async fn run_query_2_dataframe_api(ctx: &SessionContext)->Result<DataFrame, Error> {
    let df = ctx
        .table("bid")
        .await?
        .filter(
            col("auction").eq(lit(1007))
                .or(col("auction").eq(lit(1020)))
                .or(col("auction").eq(lit(2001)))
                .or(col("auction").eq(lit(2019)))
                .or(col("auction").eq(lit(2087)))
        )?
        .select(vec![col("auction"), col("price")])?;

    Ok(df)
}

async fn run_query_2_sql(ctx: &SessionContext) -> Result<DataFrame, Error> {
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