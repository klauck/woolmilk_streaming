use std::time::Instant;
use std::sync::Arc;
use arrow::array::RecordBatch;
use arrow_flight::{FlightData, utils::flight_data_to_batches, utils::batches_to_flight_data};
use tokio::sync::mpsc;
use tonic::Status;
use crate::nexmark::queries_physical_operator::run_query_2_physical_operators_lowest_level;
use crate::flights::stats::{ProcessorStats, Stats};
use crate::flights::processor::Sender;

#[derive(Debug, Clone)]
pub struct BatchData {
    pub data: Vec<FlightData>,
    pub batch_id: usize,
}

#[derive(Clone)]
pub struct Processor {
    label: String,
    sender: Sender,
    exit_stream_sender: Option<mpsc::Sender<FlightData>>,
    exit_request_handle: Option<Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>>,
    // Batch processing components
    batch_tx: Option<mpsc::Sender<BatchData>>,
    processing_handle: Option<Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>>,
    completion_rx: Option<Arc<tokio::sync::Mutex<Option<mpsc::Receiver<()>>>>>,
}

impl Processor {
    pub async fn new(label: String, exit_server_addr: String) -> Result<Self, Box<dyn std::error::Error>> {
        let sender = Sender::new(exit_server_addr, label.clone());
        
        let mut processor = Self { 
            label: label.clone(),
            sender,
            exit_stream_sender: None,
            exit_request_handle: None,
            batch_tx: None,
            processing_handle: None,
            completion_rx: None,
        };
        
        processor.setup_exit_connection().await?;
        
        processor.setup_batch_processing().await?;
        
        Ok(processor)
    }

    async fn setup_exit_connection(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.sender.exit_server_addr.is_empty() {
            match self.sender.setup_exit_connection().await {
                Ok((tx, handle)) => {
                    self.exit_stream_sender = Some(tx);
                    self.exit_request_handle = Some(Arc::new(tokio::sync::Mutex::new(Some(handle))));
                    println!("[EXIT] Connection established in processor");
                }
                Err(e) => {
                    println!("[EXIT] Failed to setup exit connection: {}", e);
                    return Err(e);
                }
            }
        } else {
            println!("[EXIT] No exit server address provided, skipping data transfer.");
        }
        Ok(())
    }

    async fn setup_batch_processing(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Create channel for batch processing
        let (batch_tx, batch_rx) = mpsc::channel::<BatchData>(100);
        let (completion_tx, completion_rx) = mpsc::channel::<()>(1);

        // Clone necessary components for the processing thread
        let processor_for_thread = Self {
            label: self.label.clone(),
            sender: self.sender.clone(),
            exit_stream_sender: self.exit_stream_sender.clone(),
            exit_request_handle: self.exit_request_handle.clone(),
            batch_tx: None,  // The processing thread doesn't need this
            processing_handle: None,  // The processing thread doesn't need this
            completion_rx: None,  // The processing thread doesn't need this
        };

        let label_clone = self.label.clone();

        // Start the background processing thread
        let processing_handle = tokio::spawn(async move {
            let mut stats = ProcessorStats::new(label_clone);
            Self::processing_thread(processor_for_thread, batch_rx, &mut stats, completion_tx).await;
        });

        // Store the components
        self.batch_tx = Some(batch_tx);
        self.processing_handle = Some(Arc::new(tokio::sync::Mutex::new(Some(processing_handle))));
        self.completion_rx = Some(Arc::new(tokio::sync::Mutex::new(Some(completion_rx))));

        println!("[PROCESSOR] Batch processing setup completed");
        Ok(())
    }

    /// Enqueue a batch for processing
    pub async fn enqueue_batch(&self, data: Vec<FlightData>, batch_id: usize) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(ref batch_tx) = self.batch_tx {
            let batch_data = BatchData { data, batch_id };
            batch_tx.send(batch_data).await
                .map_err(|e| format!("Failed to enqueue batch {}: {}", batch_id, e))?;
            println!("[PROCESSOR] Enqueued batch {} for processing", batch_id);
        } else {
            return Err("Batch processing not initialized".into());
        }
        Ok(())
    }

    /// Signal that no more batches will be sent and wait for processing to complete
    pub async fn finish_processing(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Drop the batch sender to signal completion
        self.batch_tx.take();
        
        // Wait for processing thread to complete
        if let Some(ref handle_mutex) = self.processing_handle {
            let mut handle_guard = handle_mutex.lock().await;
            if let Some(handle) = handle_guard.take() {
                let _ = handle.await?;
            }
        }

        // Wait for completion signal
        if let Some(ref completion_rx_mutex) = self.completion_rx {
            let mut completion_rx_guard = completion_rx_mutex.lock().await;
            if let Some(mut completion_rx) = completion_rx_guard.take() {
                let _ = completion_rx.recv().await;
            }
        }

        println!("[PROCESSOR] Finished processing all batches");
        Ok(())
    }

    /// Background processing thread
    async fn processing_thread(
        processor: Self,
        mut batch_rx: mpsc::Receiver<BatchData>,
        stats: &mut ProcessorStats,
        completion_tx: mpsc::Sender<()>,
    ) {
        println!("[PROCESSING_THREAD] Started processing thread");
        let mut processed_count = 0;

        while let Some(batch_data) = batch_rx.recv().await {
            let batch_start = Instant::now();
            processed_count += 1;

            // Process the batch
            match processor.process_batch(batch_data.data, batch_data.batch_id, stats).await {
                Ok(_) => {
                    let batch_elapsed = batch_start.elapsed().as_secs_f64();
                    println!(
                        "[PROCESSING_THREAD] Completed batch {} in {:.3} seconds",
                        batch_data.batch_id,
                        batch_elapsed
                    );
                }
                Err(e) => {
                    eprintln!(
                        "[PROCESSING_THREAD] Error processing batch {}: {}",
                        batch_data.batch_id,
                        e
                    );
                }
            }
        }

        println!("[PROCESSING_THREAD] Finished processing {} batches", processed_count);
        
        // Save stats to file
        if let Err(e) = stats.save_to_file() {
            eprintln!("Failed to save processor stats: {}", e);
        }
        
        // Signal completion
        let _ = completion_tx.send(()).await;
    }

    pub async fn send_schema(&self, schema_data: FlightData) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(ref sender) = self.exit_stream_sender {
            if let Err(_) = sender.send(schema_data).await {
                return Err("Failed to send schema to exit server".into());
            }
        }
        Ok(())
    }

    pub async fn process_batch(
        &self,
        current_data: Vec<FlightData>,
        batch_count: usize,
        stats: &mut ProcessorStats,
    ) -> Result<(), Status> {
        let batch_start = Instant::now();

        let batches = match flight_data_to_batches(&current_data) {
            Ok(batches) => batches,
            Err(e) => {
                println!("Failed to convert FlightData to batches for batch {}: {}", batch_count, e);
                return Ok(());
            }
        };

        let mut input_rows = 0;
        let mut output_rows = 0;
        let data_size = current_data.iter().map(|d| d.data_body.len() as u64).sum();
        
        if !batches.is_empty() {
            for batch in &batches {
                input_rows += batch.num_rows();
                let query_start = Instant::now();
                
                let processed_results = match run_query_2_physical_operators_lowest_level(batch).await {
                    Ok(results) => results,
                    Err(e) => {
                        println!("Query execution failed: {}", e);
                        continue;
                    }
                };

                let query_elapsed = query_start.elapsed().as_secs_f64();
                output_rows += processed_results.len();
                
                // Add query processing stats
                stats.add_query_processing(batch_count, input_rows, output_rows, query_elapsed);
                
                if output_rows > 0 {
                    let send_start = Instant::now();
                    let mut auction_values = Vec::new();
                    let mut price_values = Vec::new();
                    
                    for result in &processed_results {
                        if let Some(auction) = result.get("auction") {
                            if let Some(auction_num) = auction.as_i64() {
                                auction_values.push(Some(auction_num));
                            }
                        }
                        if let Some(price) = result.get("price") {
                            if let Some(price_num) = price.as_f64() {
                                price_values.push(Some(price_num));
                            }
                        }
                    }

                    let auction_array = arrow::array::Int64Array::from(auction_values);
                    let price_array = arrow::array::Float64Array::from(price_values);
                    
                    let schema = arrow::datatypes::Schema::new(vec![
                        arrow::datatypes::Field::new("auction", arrow::datatypes::DataType::Int64, true),
                        arrow::datatypes::Field::new("price", arrow::datatypes::DataType::Float64, true),
                    ]);
                    
                    let processed_batch = RecordBatch::try_new(
                        Arc::new(schema.clone()),
                        vec![Arc::new(auction_array), Arc::new(price_array)]
                    ).map_err(|e| Status::internal(format!("Failed to create processed batch: {}", e)))?;

                    let processed_schema = Arc::new(schema);
                    let flight_data_vec = batches_to_flight_data(&processed_schema, vec![processed_batch.clone()])
                        .map_err(|e| Status::internal(format!("Failed to convert to flight data: {}", e)))?;

                    // Calculate actual size of data being sent
                    let mut actual_send_size = 0u64;
                    for flight_data in &flight_data_vec {
                        actual_send_size += flight_data.data_body.len() as u64;
                    }

                    // Send processed data using internal sender
                    if let Some(ref sender) = self.exit_stream_sender {
                        for flight_data in flight_data_vec {
                            if let Err(_) = sender.send(flight_data).await {
                                println!("[PROCESSOR] Failed to send processed batch {} to exit server", batch_count);
                            }
                        }
                    }
                    
                    let send_elapsed = send_start.elapsed().as_secs_f64();
                    // Add send stats with actual data size
                    stats.add_send_batch(batch_count, send_elapsed, actual_send_size, output_rows);
                }
            }
        }
        
        let batch_elapsed = batch_start.elapsed().as_secs_f64();
        
        // Add batch stats
        stats.add_batch(batch_count, batch_elapsed, data_size, input_rows);
        
        println!(
            "[PROCESSOR] Processed batch {}: {} input rows -> {} output rows in {:.3} seconds",
            batch_count,
            input_rows,
            output_rows,
            batch_elapsed
        );

        Ok(())
    }

    pub async fn cleanup(&mut self) {
        // Finish batch processing 
        if let Err(e) = self.finish_processing().await {
            eprintln!("[PROCESSOR] Error finishing processing: {}", e);
        }

        // Wait for the exit request handle to complete
        if let Some(ref handle_mutex) = self.exit_request_handle {
            let mut handle_guard = handle_mutex.lock().await;
            if let Some(handle) = handle_guard.take() {
                let _ = handle.await;
            }
        }
        
        println!("[EXIT] Processor cleanup completed");
    }
}

impl Drop for Processor {
    fn drop(&mut self) {
        // Drop the batch sender to signal the background thread to finish
        // This is non-blocking and allows the background thread to complete naturally
        self.batch_tx.take();
        println!("[PROCESSOR] Signaled background processing to complete");
    }
}
