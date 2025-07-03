use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Instant;
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
use serde_json::json;
use crate::flights::stats::{EntryStats, Stats};
use crate::nexmark::{bid_schema, NexmarkDataGenerator};

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
        let program_start = Instant::now();
        
        let bids_descriptor = FlightDescriptor {
            r#type: DescriptorType::Path as i32,
            cmd: Default::default(),
            path: vec!["bids".to_string()],
        };

        let mode_str = match self.generation_mode {
            DataGenerationMode::PreGenerated => "PRE-GEN".to_string(),
            DataGenerationMode::RealTime => "REAL-TIME".to_string(),
        };

        let stats = Arc::new(Mutex::new(EntryStats::new(mode_str.clone(), self.records_per_chunk)));
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
            program_start
        };

        println!("Entry client: connecting to Flight server at {}", self.server_addr);
        let mut client = FlightServiceClient::connect(format!("http://{}", self.server_addr)).await?;
        println!("Connected to Flight server.");
        
        let outbound: BoxStream<'static, FlightData> = Box::pin(stream! {
            // Send schema first
            let schema_data = SchemaAsIpc::new(&bid_schema, &IpcWriteOptions::default())
                .try_into()
                .unwrap();
            yield schema_data;

            if let Some(pregenerated_batches) = all_batches {
                // If we have pre-generated batches, send them
                for (batch_idx, bid_batch) in pregenerated_batches.into_iter().enumerate() {
                    let start = Instant::now();
                    
                    let batch_stream = stream::iter(vec![Ok(bid_batch.clone())]);
                    let mut encoder = FlightDataEncoderBuilder::new().build(batch_stream);

                    let mut first = true;
                    let mut total_flight_data_size = 0u64;
                    let row_count = bid_batch.num_rows();
                    
                    while let Some(Ok(flight_data_chunk)) = encoder.next().await {
                        if first {
                            first = false;
                            continue;
                        }
                        total_flight_data_size += flight_data_chunk.data_body.len() as u64;
                        yield flight_data_chunk;
                    }

                    let duration_secs = start.elapsed().as_secs_f64();
                    
                    {
                        let mut stats_lock = stats_for_stream.lock().unwrap();
                        stats_lock.add_batch(batch_idx + 1, row_count, total_flight_data_size, duration_secs);
                    }

                    let total_mb = total_flight_data_size as f64 / 1_000_000.0;

                    println!(
                        "[PRE-GEN] Sent batch {}: {} rows (~{:.1} MB actual) in {:.3} sec",
                        batch_idx + 1,
                        row_count,
                        total_mb,
                        duration_secs
                    );
                }
            } else {
                let mut batch_count = 0;
                
                for (_, _, bid_batch, _) in data_generator.iter_batches() {
                    batch_count += 1;
                    let start = Instant::now();
                    
                    //create a stream for the current batch
                    let batch_stream = stream::iter(vec![Ok(bid_batch.clone())]);
                    let mut encoder = FlightDataEncoderBuilder::new().build(batch_stream);

                    let mut first = true;
                    let mut total_flight_data_size = 0u64;
                    let row_count = bid_batch.num_rows();
                    
                    while let Some(Ok(flight_data_chunk)) = encoder.next().await {
                        // Skip the first chunk which is the schema
                        if first {
                            first = false;
                            continue;
                        }
                        total_flight_data_size += flight_data_chunk.data_body.len() as u64;
                        yield flight_data_chunk;
                    }

                    let duration_secs = start.elapsed().as_secs_f64();
                    
                    {
                        let mut stats_lock = stats_for_stream.lock().unwrap();
                        stats_lock.add_batch(batch_count, row_count, total_flight_data_size, duration_secs);
                    }

                    let total_mb = total_flight_data_size as f64 / 1_000_000.0;

                    println!(
                        "[REAL-TIME] Sent batch {}: {} rows (~{:.1} MB actual) in {:.3} sec",
                        batch_count,
                        row_count,
                        total_mb,
                        duration_secs
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

        let final_stats = {
            let stats_lock = stats.lock().unwrap();
            let streaming_duration = streaming_start.elapsed().as_secs_f64();
            let program_duration = program_start.elapsed().as_secs_f64();
            
            let mut additional_stats = serde_json::Map::new();
            if is_pregenerated {
                additional_stats.insert("total_time_seconds".to_string(), json!(streaming_duration));
            } else {
                additional_stats.insert("total_time_seconds".to_string(), json!(program_duration));
            }
            
            stats_lock.save_to_file(Some(additional_stats)).unwrap_or_else(|e| {
                eprintln!("Failed to save stats: {}", e);
                "failed".to_string()
            });
            
            stats_lock.to_json()
        };

        println!("[{}] Finished sending all batches", mode_str);
        println!("STATS_JSON: {}", final_stats);
        Ok(())
    }
}
