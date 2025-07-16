use std::time::Instant;
use std::sync::Arc;
use arrow::array::RecordBatch;
use arrow_flight::{flight_descriptor::DescriptorType, utils::flight_data_to_batches, utils::batches_to_flight_data};
use async_stream::stream;
use futures::stream::{BoxStream, StreamExt};
use tokio::{sync::mpsc::Sender, task::JoinHandle};
use tonic::{Request, Response, Status, Streaming};
use arrow_flight::flight_service_client::FlightServiceClient;
use prost::Message;
use tonic::metadata::MetadataValue;
use arrow_flight::{
    flight_service_server::FlightService, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use crate::nexmark::queries_physical_operator::run_query_2_physical_operators_lowest_level;
use crate::flights::stats::{ProcessorStats, Stats};

#[derive(Clone)]
pub struct ProcessorFlightServer {
    exit_server_addr: String,
    label: String,
}

impl ProcessorFlightServer {
    pub fn new(exit_server_addr: impl Into<String>, label: impl Into<String>) -> Self {
        Self { 
            exit_server_addr: exit_server_addr.into(),
            label: label.into(),
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
        let mut all_flight_data: Vec<FlightData> = Vec::new();

        println!("[PROCESSOR] Starting to receive FlightData stream...");

        let mut stream_pin = Box::pin(stream);
        let mut message_count = 0;
        let mut total_data_size = 0u64;
        let mut batch_count = 0;
        let mut total_rows = 0;
        let mut schema_data: Option<FlightData> = None;

        let start_time = Instant::now();
        
        // Initialize stats with the server's label
        let mut stats = ProcessorStats::new(self.label.clone());
        
        // Setup exit server connection if needed
        let mut exit_stream_sender: Option<Sender<FlightData>> = None;
        let mut exit_request_handle: Option<JoinHandle<()>> = None;
        let transfer_to_exit = !self.exit_server_addr.is_empty();
        let mut exit_client: Option<FlightServiceClient<tonic::transport::Channel>> = None;

        if transfer_to_exit {
            let client = FlightServiceClient::connect(format!("http://{}", self.exit_server_addr))
                .await
                .map_err(|e| Status::internal(format!("Failed to connect to exit server: {}", e)))?;
            exit_client = Some(client);
            println!("[EXIT] Connected to exit server at {}", self.exit_server_addr);
        } else {
            println!("[EXIT] No exit server address provided, skipping data transfer.");
        }
        
        if let Some(ref mut client) = exit_client {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<FlightData>(100);
            
            let flight_descriptor = FlightDescriptor {
                r#type: DescriptorType::Path as i32,
                cmd: Default::default(),
                path: vec!["processed_bids_data".to_string()],
            };

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
        
        while let Some(maybe_msg) = stream_pin.next().await {
            let data_msg = maybe_msg.map_err(|e| {
                Status::internal(format!("Error reading FlightData stream: {}", e))
            })?;
            
            message_count += 1;

            // First message is schema
            if message_count == 1 {
                println!("[PROCESSOR] Received schema");
                schema_data = Some(data_msg.clone());
                
                // Send schema to exit server
                if let Some(ref sender) = exit_stream_sender {
                    if let Err(_) = sender.send(data_msg.clone()).await {
                        println!("[EXIT] Failed to send schema to exit server");
                    }
                }
                
                all_flight_data.push(data_msg);
                continue;
            }

            // Count data size for non-empty data messages
            if !data_msg.data_body.is_empty() {
                let batch_start = Instant::now();
                batch_count += 1;
                let data_size = data_msg.data_body.len() as u64;
                total_data_size += data_size;
                
                let data_mb = data_size as f64 / 1_000_000.0;

                // Process the data and send to exit server
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

                let mut input_rows = 0;
                let mut output_rows = 0;
                
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

                            if let Some(ref sender) = exit_stream_sender {
                                for flight_data in flight_data_vec {
                                    if let Err(_) = sender.send(flight_data).await {
                                        println!("[EXIT] Failed to send processed batch {} to exit server", batch_count);
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
                
                // Add batch and receive stats
                stats.add_batch(batch_count, batch_elapsed, data_size, input_rows);
                
                println!(
                    "[PROCESSOR] Processed batch {}: {:.2} MB in {:.3} seconds",
                    batch_count,
                    data_mb,
                    batch_elapsed
                );
            }

            all_flight_data.push(data_msg);
        }

        // Close the exit stream and wait for completion
        if let Some(sender) = exit_stream_sender {
            drop(sender);
        }
        
        if let Some(handle) = exit_request_handle {
            let _ = handle.await;
        }

        // Convert to record batches to count rows
        if all_flight_data.len() > 1 {
            let record_batches = flight_data_to_batches(&all_flight_data)
                .map_err(|e| Status::internal(format!("Failed to convert FlightData to batches: {}", e)))?;

            for batch in record_batches.iter() {
                total_rows += batch.num_rows();
            }
        }

        let elapsed = start_time.elapsed();
        let total_mb = total_data_size as f64 / 1_000_000.0;
        let rate = total_mb / elapsed.as_secs_f64();

        // Set total time and save stats
        stats.set_total_time(elapsed.as_secs_f64());
        
        // Save stats to file
        if let Err(e) = stats.save_to_file() {
            eprintln!("Failed to save processor stats: {}", e);
        }

        println!("\n[PROCESSOR] Summary:");
        println!("  Total batches: {}", batch_count);
        println!("  Total rows: {}", total_rows);
        println!("  Total data received: {:.2} MB", total_mb);
        println!("  Time taken: {:.2} seconds", elapsed.as_secs_f64());
        println!("  Rate: {:.2} MB/s", rate);

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