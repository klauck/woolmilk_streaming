use std::{collections::HashMap, io::{self, Error}, sync::Arc};
use arrow::array::RecordBatch;
use dataframe::DataFrameWriteOptions;
use datafusion::{
    catalog::MemTable, physical_plan::
        ExecutionPlan
    , prelude::*,
};
use datafusion_substrait::physical_plan::producer::to_substrait_rel;
use serde_json;

pub async fn build_query_2_physical_plan(bids: &RecordBatch) -> Result<Arc<dyn ExecutionPlan>, Error> {
    let ctx = SessionContext::new();
    let schema = bids.schema();
    let mem_table = MemTable::try_new(schema, vec![vec![bids.clone()]])
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("MemTable creation error: {}", e)))?;
    
    ctx.register_table("temp_bids", Arc::new(mem_table))
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Table registration error: {}", e)))?;
    
    let temp_df = ctx.table("temp_bids").await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Table access error: {}", e)))?;
    
    let filtered_df = temp_df
        .filter(
            col("auction").eq(lit(1007))
                .or(col("auction").eq(lit(1020)))
                .or(col("auction").eq(lit(2001)))
                .or(col("auction").eq(lit(2019)))
                .or(col("auction").eq(lit(2087)))
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Filter error: {}", e)))?
        .select(vec![col("auction"), col("price")])
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Select error: {}", e)))?;

    filtered_df.write_parquet("query2_filtered_bids.parquet", DataFrameWriteOptions::new(), None).await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Parquet write error: {}", e)))?;

    let ctx = SessionContext::new();
    
    ctx.register_parquet("bids", "query2_filtered_bids.parquet", ParquetReadOptions::default()).await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Parquet registration error: {}", e)))?;
    
    let df = ctx.table("bids").await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Table access error: {}", e)))?;
    
    let physical_plan = df.create_physical_plan().await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Physical plan creation error: {}", e)))?;
    
    Ok(physical_plan)
}

pub fn serialize_physical_plan_to_json(
    physical_plan: Arc<dyn ExecutionPlan>
) -> Result<String, Error> {
    let mut extensions = (Vec::new(), HashMap::new());
    let substrait_rel = to_substrait_rel(physical_plan.as_ref(), &mut extensions)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Physical to Substrait error: {}", e)))?;
    
    let json_string = serde_json::to_string_pretty(&substrait_rel)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("JSON serialization error: {}", e)))?;
    
    Ok(json_string)
}

pub async fn save_physical_plan_to_json(bids: &RecordBatch) -> Result<(), Error> {
    let physical_plan = build_query_2_physical_plan(bids).await?;
    
    let json_string = serialize_physical_plan_to_json(physical_plan)?;
    
    let file_path = "query2_physical_plan.json";
    
    std::fs::write(file_path, json_string)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("File write error: {}", e)))?;
    
    println!("Query 2 physical plan saved to: {}", file_path);
    Ok(())
}