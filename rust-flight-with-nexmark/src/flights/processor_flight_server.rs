use std::time::Instant;
use arrow::array::RecordBatch;
use arrow_flight::{flight_descriptor::DescriptorType, utils::flight_data_to_batches};
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

#[derive(Clone)]
pub struct ProcessorFlightServer {
    exit_server_addr: String
}

impl ProcessorFlightServer {
    pub fn new(exit_server_addr: impl Into<String>) -> Self {
        Self { 
            exit_server_addr: exit_server_addr.into() 
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

        while let Some(maybe_msg) = stream_pin.next().await {
            let data_msg = maybe_msg.map_err(|e| {
                Status::internal(format!("Error reading FlightData stream: {}", e))
            })?;
            
            message_count += 1;
            println!("Received {} messages so far...", message_count);

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

            let mut current_data = vec![data_msg.clone()];
            if let Some(ref schema) = schema_data {
                current_data.insert(0, schema.clone());
            }

            if let Ok(batches) = flight_data_to_batches(&current_data) {
                if !batches.is_empty() {
                    // send to exit server
                    if let Some(ref sender) = exit_stream_sender {
                        if let Err(_) = sender.send(data_msg.clone()).await {
                            println!("[EXIT] Failed to send batch {} to exit server", message_count);
                        } else {
                            println!("[EXIT] Successfully sent batch {} to exit server", message_count);
                        }
                    }

                    for batch in &batches {
                        let rows = batch.num_rows();
                        let bytes = batch.get_array_memory_size();
                        let mbs = bytes as f64 / 1_000_000.0;
                        println!("Processed real-time batch: {} rows, {:.2} MB", rows, mbs);
                    }
                }
            }

            all_flight_data.push(data_msg);
        }

        // close the exit stream and wait for completion
        if let Some(sender) = exit_stream_sender {
            drop(sender); // closes the channel
        }
        
        if let Some(handle) = exit_request_handle {
            let _ = handle.await;
        }

        let elapsed_time = Instant::now().elapsed();

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
        
        for (_, batch) in record_batches.iter().enumerate() {
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

        println!(
            "Received {} batches with {} rows and {} total MBs in {:.2?} seconds with transfer rate {:.2} MB/s",
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