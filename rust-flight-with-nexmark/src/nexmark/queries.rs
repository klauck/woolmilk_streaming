use std::{io::{self, Error, Write}, sync::Arc};
use arrow::array::RecordBatch;
use datafusion::{catalog::MemTable, logical_expr::LogicalPlanBuilder, prelude::*, datasource::DefaultTableSource};
use serde_json::to_string_pretty;
use crate::nexmark::{queries_physical_operator::run_query_2_physical_operators_lowest_level, NexmarkDataGenerator};
use datafusion_substrait::logical_plan::{consumer::from_substrait_plan, producer::to_substrait_plan};

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

    ctx.register_batch("bid", bids.clone())?;

    let df_q2_sql = run_query_2_sql(&ctx).await?;
    let df_q2_df_api = run_query_2_dataframe_api(&ctx).await?;
    let df_q2_substrait = run_query_2_substrait(&ctx, true).await?;
    let df_q2_low_level = run_query_2_low_level(&ctx, &bids).await?;

    // Compare only row counts
    let sql_count = df_q2_sql.count().await?;
    let df_api_count = df_q2_df_api.count().await?;
    let substrait_count = df_q2_substrait.count().await?;
    let low_level_count = df_q2_low_level.count().await?;
    let lowest_level = run_query_2_physical_operators_lowest_level(&bids)
        .await?;

    if sql_count == df_api_count && df_api_count == substrait_count && substrait_count == low_level_count && low_level_count == lowest_level.len() {
        println!("Row counts match across all query methods: {}", sql_count);
    } else {
        println!("Row counts no same!");
        return Err(io::Error::new(io::ErrorKind::Other, "Row counts do not match across different query methods"));
    }

    println!("Query 2 completed successfully.");

    Ok(())
}

async fn run_query_2_low_level(ctx: &SessionContext, bids: &RecordBatch) -> Result<DataFrame, Error> {
    let schema = bids.schema();
    let mem_table = MemTable::try_new(schema.clone(), vec![vec![bids.clone()]])
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("MemTable creation error: {}", e)))?;
    
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(mem_table)));
    
    // create table scan
    let table_scan = LogicalPlanBuilder::scan(
        "bid",
        table_source,
        None
    )
    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Table scan error: {}", e)))?;
    
    //build filter expression
    let auction_col = col("auction");
    let filter_expr = auction_col.clone().eq(lit(1007))
        .or(auction_col.clone().eq(lit(1020)))
        .or(auction_col.clone().eq(lit(2001)))
        .or(auction_col.clone().eq(lit(2019)))
        .or(auction_col.eq(lit(2087)));
    
    let filtered_plan = table_scan
        .filter(filter_expr)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Filter error: {}", e)))?;
    
    let projection_exprs = vec![
        col("auction"),
        col("price")
    ];
    
    let final_plan = filtered_plan
        .project(projection_exprs)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Projection error: {}", e)))?
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Plan build error: {}", e)))?;
    
    let df = DataFrame::new(ctx.state(), final_plan);
    
    Ok(df)
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
        
        let file = std::fs::File::open("query2_substrait_plan.json")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("File open error: {}", e)))?;
        
        let loaded_plan: datafusion_substrait::substrait::proto::Plan = serde_json::from_reader(file)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("JSON deserialization error: {}", e)))?;
        
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