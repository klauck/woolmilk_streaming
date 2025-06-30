use core::fmt;
use std::{collections::HashMap, io::{self, Error}, sync::Arc}; //Arc for accessing shared data across threads
use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::{error::Result as DFResult, logical_expr::Operator, physical_expr::expressions::{BinaryExpr, Column as PhysicalColumn, Literal}, physical_plan::{
        filter::FilterExec, 
        memory::{LazyBatchGenerator, LazyMemoryExec}, 
        projection::ProjectionExec, 
        ExecutionPlan
    }, scalar::ScalarValue
};
use datafusion_physical_expr::PhysicalExpr;
use parking_lot::RwLock; //RwLock for multiple threads access

#[derive(Debug)]
// a single batch generator that returns a single RecordBatch
struct SingleBatchGen {
    batch: Option<RecordBatch>,
}

// fmt::Display used to implement a custom string representation for SingleBatchGen
// for user-friendly output, we need this because datafusion execution engine use it for debugging and logging
impl fmt::Display for SingleBatchGen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SingleBatchGen")
    }
}

// LazyBatchGenerator used to generate batches on demand
impl LazyBatchGenerator for SingleBatchGen {
    fn generate_next_batch(&mut self) -> DFResult<Option<RecordBatch>> {
        Ok(self.batch.take())          // first call returns Some(batch), afterwards None
    }
}

pub async fn run_query_2_physical_operators_lowest_level(bids: &RecordBatch) -> Result<Vec<HashMap<String, serde_json::Value>>, Error> {
    let schema = bids.schema();
    // Arc for sharing across threads, RwLock for mutable access
    type GenPtr = Arc<RwLock<dyn LazyBatchGenerator>>;

    //vector of generators of bids
    let generators: Vec<GenPtr> = vec![
        Arc::new(RwLock::new(SingleBatchGen {
            batch: Some(bids.clone()),
        }))
    ];

    // initiating memory lazy memory execution for schema and generators
    let memory_exec = Arc::new(
        LazyMemoryExec::try_new(schema, generators)
            .map_err(|e| io::Error::new(io::ErrorKind::Other,
                    format!("LazyMemoryExec creation error: {e}")))?,
    );

    let auction_col = Arc::new(PhysicalColumn::new("auction", 0)); // auction is at index 0
    
    let lit_1007 = Arc::new(Literal::new(ScalarValue::Int64(Some(1007))));
    let lit_1020 = Arc::new(Literal::new(ScalarValue::Int64(Some(1020))));
    let lit_2001 = Arc::new(Literal::new(ScalarValue::Int64(Some(2001))));
    let lit_2019 = Arc::new(Literal::new(ScalarValue::Int64(Some(2019))));
    let lit_2087 = Arc::new(Literal::new(ScalarValue::Int64(Some(2087))));
    let lit_0 = Arc::new(Literal::new(ScalarValue::Int64(Some(0))));

    let eq_1007 = Arc::new(BinaryExpr::new(auction_col.clone(), Operator::Eq, lit_1007));
    let eq_1020 = Arc::new(BinaryExpr::new(auction_col.clone(), Operator::Eq, lit_1020));
    let eq_2001 = Arc::new(BinaryExpr::new(auction_col.clone(), Operator::Eq, lit_2001));
    let eq_2019 = Arc::new(BinaryExpr::new(auction_col.clone(), Operator::Eq, lit_2019));
    let eq_2087 = Arc::new(BinaryExpr::new(auction_col.clone(), Operator::Eq, lit_2087));
    let eq_0 = Arc::new(BinaryExpr::new(auction_col.clone(), Operator::GtEq, lit_0));

    let or_1 = Arc::new(BinaryExpr::new(eq_1007, Operator::Or, eq_1020));
    let or_2 = Arc::new(BinaryExpr::new(or_1, Operator::Or, eq_2001));
    let or_3 = Arc::new(BinaryExpr::new(or_2, Operator::Or, eq_2019));
    let or_4 = Arc::new(BinaryExpr::new(or_3, Operator::Or, eq_0));
    
    //combining all conditions with OR
    let filter_expr = Arc::new(BinaryExpr::new(or_4, Operator::Or, eq_2087));

    // FilterExec evaluates a boolean predicate against all input batches to determine which rows to
    // include in the output.
    let filter_exec = Arc::new(FilterExec::try_new(filter_expr, memory_exec)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("FilterExec creation error: {}", e)))?);

    // ProjectionExec is used to select specific columns from the input data.
    let auction_col_proj = (Arc::new(PhysicalColumn::new("auction", 0)) as Arc<dyn PhysicalExpr>, "auction".to_string());
    let price_col_proj = (Arc::new(PhysicalColumn::new("price", 2)) as Arc<dyn PhysicalExpr>, "price".to_string());
    
    let projection_exprs = vec![auction_col_proj, price_col_proj];

    let projection_exec = Arc::new(ProjectionExec::try_new(projection_exprs, filter_exec)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("ProjectionExec creation error: {}", e)))?);

    let task_ctx = Arc::new(datafusion::execution::TaskContext::default());
    let stream = projection_exec.execute(0, task_ctx) // 0 is the partition index
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Execution error: {}", e)))?;

    let batches = datafusion::physical_plan::common::collect(stream).await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Collection error: {}", e)))?;

    // convert batches to Vec<HashMap<String, serde_json::Value>>
    let mut result = Vec::new();

    for batch in batches {
        let schema = batch.schema();
        let num_rows = batch.num_rows();
        
        for row_idx in 0..num_rows {
            let mut row_map = HashMap::new();
            
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let column = batch.column(col_idx);
                let value = extract_value_at_index(column.as_ref(), row_idx);
                row_map.insert(field.name().clone(), value);
            }
            
            result.push(row_map);
        }
    }
    
    Ok(result)
}

fn extract_value_at_index(array: &dyn Array, index: usize) -> serde_json::Value {
    if array.is_null(index) {
        return serde_json::Value::Null;
    }
    
    match array.data_type() {
        arrow::datatypes::DataType::Int64 => {
            let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            serde_json::Value::Number(serde_json::Number::from(int_array.value(index)))
        },
        arrow::datatypes::DataType::Utf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            serde_json::Value::String(string_array.value(index).to_string())
        },
        arrow::datatypes::DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            serde_json::Value::Number(serde_json::Number::from_f64(float_array.value(index)).unwrap())
        },
        _ => serde_json::Value::String(format!("Unsupported type: {:?}", array.data_type()))
    }
}