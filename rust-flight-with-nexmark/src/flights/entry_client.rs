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
        let bids_descriptor = FlightDescriptor {
            r#type: DescriptorType::Path as i32,
            cmd: Default::default(),
            path: vec!["bids".to_string()],
        };

        let batch_times: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(Vec::new()));
        let bid_schema: Schema = bid_schema();
        let batch_times_for_stream = batch_times.clone();

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
                    while let Some(Ok(flight_data_chunk)) = encoder.next().await {
                        // Skip the first chunk which is the schema
                        if first {
                            first = false;
                            continue;
                        }
                        yield flight_data_chunk;
                    }

                    let duration_secs = start.elapsed().as_secs_f64();
                    {
                        let mut lock = batch_times_for_stream.lock().unwrap();
                        lock.push(duration_secs);
                    }

                    let total_mb = bid_batch.get_array_memory_size() as f64 / 1_000_000.0;

                    println!(
                        "[PRE-GEN] Sent batch {}: {} rows (~{} MB) in {:.3} sec",
                        batch_idx + 1,
                        bid_batch.num_rows(),
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
                    while let Some(Ok(flight_data_chunk)) = encoder.next().await {
                        // Skip the first chunk which is the schema
                        if first {
                            first = false;
                            continue;
                        }
                        yield flight_data_chunk;
                    }

                    let duration_secs = start.elapsed().as_secs_f64();
                    {
                        let mut lock = batch_times_for_stream.lock().unwrap();
                        lock.push(duration_secs);
                    }

                    let total_mb = bid_batch.get_array_memory_size() as f64 / 1_000_000.0;

                    println!(
                        "[REAL-TIME] Sent batch {}: {} rows (~{} MB) in {:.3} sec",
                        batch_count,
                        bid_batch.num_rows(),
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

        let total_time: f64 = {
            let lock = batch_times.lock().unwrap();
            lock.iter().copied().sum()
        };

        let mode = if is_pregenerated { "PRE-GEN" } else { "REAL-TIME" };
        println!("[{}] Finished sending all batches in {:.3} sec total", mode, total_time);
        Ok(())
    }
}
