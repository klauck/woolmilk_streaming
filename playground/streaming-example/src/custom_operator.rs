use datafusion::common::DataFusionError;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::logical_expr::{LogicalPlanBuilder, LogicalTableSource};
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
pub async fn test_custom_operator() -> Result<(), DataFusionError> {
    // create a logical table source
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    
    let table_source = LogicalTableSource::new(SchemaRef::new(schema));

    let projection = None;

    // create a LogicalPlanBuilder for a table scan
    let builder = LogicalPlanBuilder::scan("person", Arc::new(table_source), projection)?;

    //gt is greater than
    //lit is literal
    let plan = builder
        .filter(col("id").gt(lit(500)))? // WHERE id > 500
        .build()?;

    // print the plan
    println!("{}", plan.display_indent_schema());

    let ctx = SessionContext::new();
    let _physical_plan = ctx.state().create_physical_plan(&plan).await?;

    Ok(())
}