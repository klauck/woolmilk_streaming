use std::io::Write;

use datafusion::{prelude::{DataFrame, SessionContext}};
use datafusion_substrait::{logical_plan::{consumer::from_substrait_plan, producer::to_substrait_plan}, serializer::serialize, substrait::proto::Plan};
use serde_json::to_string_pretty;

pub async fn generate_substrait_plan_from_df(df: &DataFrame, ctx: &SessionContext, out_file: &str) -> Result<(), std::io::Error>{
    let optimized_plan = df.clone().into_optimized_plan()?;
    let plan = to_substrait_plan(&optimized_plan, &ctx)?;
    let json = to_string_pretty(&plan)?; 

    let mut file = std::fs::File::create(out_file)?;
    file.write_all(json.as_bytes())?;

    return Ok(());
}

pub async fn load_substrait_plan_from_file(path: &str, ctx: &SessionContext) -> Result<(), std::io::Error> {
    let file = std::fs::File::open(path)?;
    let plan: Plan = serde_json::from_reader(file)?;

    let substrait_plan = from_substrait_plan(ctx, &plan).await?;

    let df = DataFrame::new(ctx.state(), substrait_plan);

    // Execute the query and collect the results
    let results = df.collect().await?;

    for batch in results {
        println!("{:?}", batch);
    }

    return Ok(());
}

pub async fn serialize_plan_directly_to_file(query: String, ctx: &SessionContext) -> Result<(), std::io::Error> {
    serialize("select * from auction", ctx, "serialized.plan").await?;
    return Ok(());
}