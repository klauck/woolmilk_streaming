use std::time::Instant;
use std::sync::{Arc, Mutex};
use arrow::array::RecordBatch;
use arrow_flight::{flight_descriptor::DescriptorType, utils::flight_data_to_batches, utils::batches_to_flight_data};
use async_stream::stream;
use futures::stream::{BoxStream, StreamExt};
use tokio::{sync::mpsc::Sender, task::JoinHandle};
use tonic::{Request, Response, Status, Streaming};
use arrow_flight::flight_service_client::FlightServiceClient;
use prost::Message;
use tonic::metadata::MetadataValue;
use serde_json::json;
use arrow_flight::{
    flight_service_server::FlightService, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use super::stats::{ProcessorStats, Stats};
use crate::nexmark::queries_physical_operator::run_query_2_physical_operators_lowest_level;

#[derive(Clone)]
pub struct ProcessorFlightServer {
    exit_server_addr: String,
    stats: Arc<Mutex<ProcessorStats>>,
}

impl ProcessorFlightServer {
    pub fn new(exit_server_addr: impl Into<String>) -> Self {
        let stats = Arc::new(Mutex::new(ProcessorStats::new(0)));
        Self { 
            exit_server_addr: exit_server_addr.into(),
            stats,
        }
    }
}

#[tonic::async_trait]
impl FlightService for ProcessorFlightServer {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let stream = request.into_inner();
        // to store all received FlightData messages
        let mut all_flight_data: Vec<FlightData> = Vec::new();

        println!("[Processor] Starting to receive FlightData stream...");

        let mut stream_pin = Box::pin(stream); // pin is used so that the memory location of the stream is fixed
        let mut message_count = 0; // total number of messages received
        let mut schema_data: Option<FlightData> = None; // store the first message which is expected to be the schema
        
        // create a connection to exit server at the start
        let mut exit_stream_sender: Option<Sender<FlightData>> = None;
        let mut exit_request_handle: Option<JoinHandle<()>> = None; // type of thread which we can use await to let it finish

        let transfer_to_exit = !self.exit_server_addr.is_empty();
        let mut exit_client: Option<FlightServiceClient<tonic::transport::Channel>> = None;

        if transfer_to_exit {
            let client = FlightServiceClient::connect(format!("http://{}", self.exit_server_addr))
                .await
                .map_err(|e| Status::internal(format!("Failed to connect to exit server: {}", e)))?;
            exit_client = Some(client);
            println!("[Exit] Connected to exit server at {}", self.exit_server_addr);
        } else {
            println!("[Exit] No exit server address provided, skipping data transfer.");
        }
        
        if let Some(ref mut client) = exit_client {
            // create a channel for sending data to exit server
            let (tx, mut rx) = tokio::sync::mpsc::channel::<FlightData>(100);
            
            let flight_descriptor = FlightDescriptor {
                r#type: DescriptorType::Path as i32,
                cmd: Default::default(),
                path: vec!["processed_bids_data".to_string()],
            };

            // create the stream from the receiver
            let transfer_stream = stream! {
                while let Some(data) = rx.recv().await {
                    yield data;
                }
            };

            let mut request = Request::new(Box::pin(transfer_stream));
            
            let descriptor_bytes = flight_descriptor.encode_to_vec();
            let bin_val: MetadataValue<_> = MetadataValue::from_bytes(&descriptor_bytes);
            request
                .metadata_mut()
                .insert_bin("grpc-metadata-flight-descriptor-bin", bin_val);

            // start the persistent do_put request in a background task
            let mut client_clone = client.clone();
            exit_request_handle = Some(tokio::spawn(async move {
                match client_clone.do_put(request).await {
                    Ok(response) => {
                        let mut put_results = response.into_inner();
                        while let Some(_put_res) = put_results.next().await {
                            
                        }
                        println!("[EXIT] Persistent connection closed successfully");
                    }
                    Err(e) => {
                        println!("[EXIT] Persistent connection error: {}", e);
                    }
                }
            }));

            exit_stream_sender = Some(tx);
        }

        let start_time = Instant::now();
        let mut batch_count = 0;
        
        while let Some(maybe_msg) = stream_pin.next().await {
            let data_msg = maybe_msg.map_err(|e| {
                Status::internal(format!("Error reading FlightData stream: {}", e))
            })?;
            
            message_count += 1;

            if message_count % 50 == 0 {
                println!("Received {} messages so far...", message_count);
            }

            // the first message is always the schema
            if message_count == 1 {
                schema_data = Some(data_msg.clone());
                
                // send schema to exit server
                if let Some(ref sender) = exit_stream_sender {
                    if let Err(_) = sender.send(data_msg.clone()).await {
                        println!("[EXIT] Failed to send schema to exit server");
                    }
                }
                
                all_flight_data.push(data_msg);
                continue;
            }

            // Only process data messages that have actual data
            if !data_msg.data_body.is_empty() {
                let batch_start_time = Instant::now(); // Total time starts here
                
                let receive_start = Instant::now();
                let mut current_data = vec![data_msg.clone()];
                if let Some(ref schema) = schema_data {
                    current_data.insert(0, schema.clone());
                }

                let batches = match flight_data_to_batches(&current_data) {
                    Ok(batches) => batches,
                    Err(e) => {
                        println!("Failed to convert FlightData to batches for message {}: {}", message_count, e);
                        all_flight_data.push(data_msg);
                        continue;
                    }
                };
                let receive_duration = receive_start.elapsed().as_secs_f64();

                let actual_input_bytes = data_msg.data_body.len() as u64;
                let total_batch_size: u64 = batches.iter().map(|b| b.get_array_memory_size() as u64).sum();
                
                if !batches.is_empty() {
                    batch_count += 1;
                    
                    for batch in &batches {

                        let query_start = Instant::now();
                        let processed_results = match run_query_2_physical_operators_lowest_level(batch).await {
                            Ok(results) => results,
                            Err(e) => {
                                println!("Query execution failed: {}", e);
                                continue;
                            }
                        };
                        let query_duration = query_start.elapsed().as_secs_f64();

                        let output_rows = processed_results.len();
                        
                        // Phase 3: Send time
                        let send_start = Instant::now();
                        let mut output_bytes = 0u64;
                        
                        if output_rows > 0 {
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

                            output_bytes = processed_batch.get_array_memory_size() as u64;

                            let processed_schema = Arc::new(schema);
                            let flight_data_vec = batches_to_flight_data(&processed_schema, vec![processed_batch.clone()])
                                .map_err(|e| Status::internal(format!("Failed to convert to flight data: {}", e)))?;

                            if let Some(ref sender) = exit_stream_sender {
                                for flight_data in flight_data_vec {
                                    if let Err(_) = sender.send(flight_data).await {
                                        println!("[EXIT] Failed to send processed batch {} to exit server", batch_count);
                                    }
                                }
                                if batch_count % 10 == 0 {
                                    println!("[EXIT] Successfully sent processed batch {} with {} rows to exit server", batch_count, output_rows);
                                }
                            }
                        }
                        let send_duration = send_start.elapsed().as_secs_f64();
                        
                        // Phase 4: Total time
                        let total_batch_duration = batch_start_time.elapsed().as_secs_f64();
                        
                        {
                            let mut stats_lock = self.stats.lock().unwrap();
                            // Use original FlightData sizes, not Arrow memory sizes
                            let per_batch_input_bytes = actual_input_bytes / batches.len() as u64;
                            
                            // For output, calculate the FlightData size that will be sent
                            let output_flight_data_size = if output_rows > 0 {
                                // Estimate: Arrow to FlightData compression ratio is similar
                                (output_bytes as f64 * (actual_input_bytes as f64 / total_batch_size as f64)) as u64
                            } else {
                                0
                            };
                            
                            stats_lock.add_receive_batch(batch_count, batch.num_rows(), per_batch_input_bytes, receive_duration);
                            stats_lock.add_send_batch(batch_count, output_rows, output_flight_data_size, send_duration);
                            stats_lock.add_processing_stats(batch_count, batch.num_rows(), output_rows, 
                                total_batch_duration, query_duration);
                        }
                    }
                }
            }

            all_flight_data.push(data_msg);
        }

        let elapsed_time = start_time.elapsed();

        // close the exit stream and wait for completion
        if let Some(sender) = exit_stream_sender {
            drop(sender); // closes the channel
        }
        
        if let Some(handle) = exit_request_handle {
            let _ = handle.await;
        }

        if all_flight_data.is_empty() {
            println!("do_put received no data");
            let output_stream: BoxStream<'static, Result<PutResult, Status>> =
                Box::pin(futures::stream::empty());
            return Ok(Response::new(output_stream));
        }

        let record_batches: Vec<RecordBatch> = flight_data_to_batches(&all_flight_data)
            .map_err(|e| {
                println!("ERROR during conversion: {}", e);
                Status::internal(format!(
                    "Failed to convert FlightData → RecordBatch: {}",
                    e
                ))
            })?;

        let mut total_rows: usize = 0;
        let mut total_bytes: usize = 0;
        
        for batch in record_batches.iter() {
            let rows = batch.num_rows();
            let bytes = batch.get_array_memory_size();
            total_rows += rows;
            total_bytes += bytes;
        }

        let total_mbs = total_bytes as f64 / 1_000_000.0;
        let elapsed_secs = elapsed_time.as_millis() as f64 / 1000.0;
        let rate = if elapsed_secs > 0.0 {
            total_mbs / elapsed_secs
        } else {
            0.0
        };

        let stats_lock = self.stats.lock().unwrap();
        let mut additional_stats = serde_json::Map::new();
        additional_stats.insert("total_time_seconds".to_string(), json!(elapsed_secs));
        additional_stats.insert("total_input_rows".to_string(), json!(total_rows));
        additional_stats.insert("total_input_bytes".to_string(), json!(total_bytes));
        additional_stats.insert("total_input_mb".to_string(), json!(total_mbs));
        additional_stats.insert("input_rate_mbps".to_string(), json!(rate));
        
        stats_lock.save_to_file(Some(additional_stats)).unwrap_or_else(|e| {
            eprintln!("Failed to save processor stats: {}", e);
            "failed".to_string()
        });

        println!(
            "Processed {} input batches with {} rows and {:.2} MB in {:.2?} seconds with input rate {:.2} MB/s",
            record_batches.len(), total_rows, total_mbs, elapsed_time, rate
        );

        let response_stream = stream! {
            yield Ok(PutResult { app_metadata: vec![].into() });
        };

        Ok(Response::new(Box::pin(response_stream)))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Implement handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }
}