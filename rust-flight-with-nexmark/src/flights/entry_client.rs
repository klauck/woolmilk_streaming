use std::error::Error;
use std::time::Instant;
use std::sync::{Arc, Mutex};
use arrow::datatypes::Schema;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor, SchemaAsIpc};
use futures::stream::{self, BoxStream, StreamExt};
use async_stream::stream;
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::Request;
use crate::nexmark::{bid_schema, NexmarkDataGenerator};
use crate::flights::stats::{EntryStats, Stats};

#[derive(Debug, Clone)]
pub enum DataGenerationMode {
    PreGenerated,  // Generate all data before connecting
    RealTime,      // Generate data as streaming
}

pub struct EntryClient {
    server_addr: String,
    records_per_chunk: usize,
    no_records: usize,
    generation_mode: DataGenerationMode,
}

impl EntryClient {
    pub fn new(
        server_addr: impl Into<String>,
        records_per_chunk: usize,
        no_records: usize,
        generation_mode: DataGenerationMode,
    ) -> Self {
        Self {
            server_addr: server_addr.into(),
            records_per_chunk,
            no_records,
            generation_mode,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn Error>> {
        let start_time = Instant::now();
        
        let bids_descriptor = FlightDescriptor {
            r#type: DescriptorType::Path as i32,
            cmd: Default::default(),
            path: vec!["bids".to_string()],
        };

        let mode_str = match self.generation_mode {
            DataGenerationMode::PreGenerated => "PRE-GEN".to_string(),
            DataGenerationMode::RealTime => "REAL-TIME".to_string(),
        };

        // Use Arc<Mutex<>> to share stats between main thread and stream
        let stats = Arc::new(Mutex::new(EntryStats::new(mode_str.clone())));
        let stats_for_stream = stats.clone();

        let bid_schema: Schema = bid_schema();
        let data_generator = NexmarkDataGenerator::new(self.records_per_chunk, self.no_records);

        let (all_batches, is_pregenerated) = match self.generation_mode {
            DataGenerationMode::PreGenerated => {
                println!("Generating Nexmark data before connecting...");
                let mut all_bid_batches = Vec::new();
                
                for (_people_batch, _auction_batch, bid_batch, _category_batch) in data_generator.iter_batches() {
                    all_bid_batches.push(bid_batch);
                }
                
                println!("Generated {} batches", all_bid_batches.len());
                (Some(all_bid_batches), true)
            }
            DataGenerationMode::RealTime => {
                (None, false)
            }
        };

        // for pre-generated mode, start timing after data generation
        let streaming_start = if is_pregenerated {
            Instant::now()
        } else {
            start_time
        };

        println!("Entry client: connecting to Flight server at {}", self.server_addr);
        let mut client = FlightServiceClient::connect(format!("http://{}", self.server_addr)).await?;
        println!("Connected to Flight server.");

        // Shared counters using Arc<Mutex<>>
        let batch_count = Arc::new(Mutex::new(0));
        let total_rows = Arc::new(Mutex::new(0));
        let total_flight_data_size = Arc::new(Mutex::new(0u64));

        let batch_count_stream = batch_count.clone();
        let total_rows_stream = total_rows.clone();
        let total_flight_data_size_stream = total_flight_data_size.clone();

        let outbound: BoxStream<'static, FlightData> = Box::pin(stream! {
            // First, add schema
            let schema_data = SchemaAsIpc::new(&bid_schema, &IpcWriteOptions::default())
                .try_into()
                .unwrap();

            yield schema_data;

            if let Some(pregenerated_batches) = all_batches {
                // Process pre-generated batches
                for bid_batch in pregenerated_batches {
                    let batch_start_time = Instant::now();
                    
                    let current_batch_count = {
                        let mut count = batch_count_stream.lock().unwrap();
                        *count += 1;
                        *count
                    };
                    
                    {
                        let mut rows = total_rows_stream.lock().unwrap();
                        *rows += bid_batch.num_rows();
                    }
                    
                    let batch_stream = stream::iter(vec![Ok(bid_batch.clone())]);
                    let mut encoder = FlightDataEncoderBuilder::new().build(batch_stream);

                    let mut first = true;
                    let mut batch_data_size = 0u64;
                    let mut batch_flight_data_count = 0;
                    
                    while let Some(Ok(flight_data_chunk)) = encoder.next().await {
                        if first {
                            first = false;
                            continue; // Skip schema
                        }
                        let chunk_size = flight_data_chunk.data_body.len() as u64;
                        batch_data_size += chunk_size;
                        
                        {
                            let mut total_size = total_flight_data_size_stream.lock().unwrap();
                            *total_size += chunk_size;
                        }
                        
                        batch_flight_data_count += 1;
                        yield flight_data_chunk;
                    }

                    let batch_elapsed = batch_start_time.elapsed().as_secs_f64();
                    
                    // Add batch to stats
                    {
                        let mut stats_lock = stats_for_stream.lock().unwrap();
                        stats_lock.add_batch(current_batch_count, batch_elapsed, batch_data_size, bid_batch.num_rows());
                    }

                    let batch_mb = batch_data_size as f64 / 1_000_000.0;
                    println!(
                        "[PRE-GEN] Prepared logical batch {}: {} rows, {:.2} MB actual ({} FlightData chunks)",
                        current_batch_count,
                        bid_batch.num_rows(),
                        batch_mb,
                        batch_flight_data_count
                    );
                }
            } else {
                // Process real-time batches
                for (_, _, bid_batch, _) in data_generator.iter_batches() {
                    let batch_start_time = Instant::now();
                    
                    let current_batch_count = {
                        let mut count = batch_count_stream.lock().unwrap();
                        *count += 1;
                        *count
                    };
                    
                    {
                        let mut rows = total_rows_stream.lock().unwrap();
                        *rows += bid_batch.num_rows();
                    }
                    
                    let batch_stream = stream::iter(vec![Ok(bid_batch.clone())]);
                    let mut encoder = FlightDataEncoderBuilder::new().build(batch_stream);

                    let mut first = true;
                    let mut batch_data_size = 0u64;
                    let mut batch_flight_data_count = 0;
                    
                    while let Some(Ok(flight_data_chunk)) = encoder.next().await {
                        if first {
                            first = false;
                            continue; // Skip schema
                        }
                        let chunk_size = flight_data_chunk.data_body.len() as u64;
                        batch_data_size += chunk_size;
                        
                        {
                            let mut total_size = total_flight_data_size_stream.lock().unwrap();
                            *total_size += chunk_size;
                        }
                        
                        batch_flight_data_count += 1;
                        yield flight_data_chunk;
                    }

                    let batch_elapsed = batch_start_time.elapsed().as_secs_f64();
                    
                    // Add batch to stats
                    {
                        let mut stats_lock = stats_for_stream.lock().unwrap();
                        stats_lock.add_batch(current_batch_count, batch_elapsed, batch_data_size, bid_batch.num_rows());
                    }

                    let batch_mb = batch_data_size as f64 / 1_000_000.0;
                    println!(
                        "[REAL-TIME] Streaming logical batch {}: {} rows, {:.2} MB actual ({} FlightData chunks)",
                        current_batch_count,
                        bid_batch.num_rows(),
                        batch_mb,
                        batch_flight_data_count
                    );
                }
            }
        });

        let mut request = Request::new(outbound);
        let descriptor_bytes = bids_descriptor.encode_to_vec();
        let bin_val: MetadataValue<_> = MetadataValue::from_bytes(&descriptor_bytes);

        request
            .metadata_mut()
            .insert_bin("grpc-metadata-flight-descriptor-bin", bin_val);

        let response = client.do_put(request).await?;
        let mut put_results = response.into_inner();
        while let Some(_put_res) = put_results.next().await {}

        let elapsed = streaming_start.elapsed();
        
        // Get final values from shared variables
        let final_batch_count = *batch_count.lock().unwrap();
        let final_total_rows = *total_rows.lock().unwrap();
        let final_total_flight_data_size = *total_flight_data_size.lock().unwrap();
        
        let total_mb = final_total_flight_data_size as f64 / 1_000_000.0;
        let rate = total_mb / elapsed.as_secs_f64();

        // Set total completion time and save stats
        {
            let mut stats_lock = stats.lock().unwrap();
            stats_lock.set_total_completion_time(elapsed.as_secs_f64());
            
            // Save stats to file
            if let Err(e) = stats_lock.save_to_file() {
                eprintln!("Failed to save entry stats: {}", e);
            }
        }

        println!("\n[{}] Summary:", mode_str);
        println!("  Total logical batches: {}", final_batch_count);
        println!("  Total rows: {}", final_total_rows);
        println!("  Total data sent: {:.2} MB", total_mb);
        println!("  Time taken: {:.2} seconds", elapsed.as_secs_f64());
        println!("  Rate: {:.2} MB/s", rate);
        
        println!("[{}] Finished sending all batches", mode_str);
        Ok(())
    }
}
