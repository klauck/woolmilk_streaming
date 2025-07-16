use std::time::Instant;
use arrow_flight::utils::flight_data_to_batches;
use async_stream::stream;
use futures::stream::{BoxStream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use arrow_flight::{
    flight_service_server::FlightService, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use crate::flights::processor::Processor;

#[derive(Clone)]
pub struct ProcessorFlightServerMultiThreaded {
    exit_server_addr: String,
    label: String,
}

impl ProcessorFlightServerMultiThreaded {
    pub fn new(exit_server_addr: impl Into<String>, label: impl Into<String>) -> Self {
        Self { 
            exit_server_addr: exit_server_addr.into(),
            label: label.into(),
        }
    }

    /// Initialize components (processor only - it handles sender internally)
    async fn initialize_components(&self) -> Result<Processor, Status> {
        match Processor::new(self.label.clone(), self.exit_server_addr.clone()).await {
            Ok(processor) => Ok(processor),
            Err(e) => Err(Status::internal(format!("Failed to initialize processor: {}", e)))
        }
    }

    /// Handle schema message (first message in the stream)
    async fn handle_schema_message(
        &self,
        data_msg: FlightData,
        processor: &Processor,
    ) -> Option<FlightData> {
        println!("[PROCESSOR] Received schema");
        
        // Send schema to exit server via processor
        if let Err(e) = processor.send_schema(data_msg.clone()).await {
            println!("[EXIT] Failed to send schema to exit server: {}", e);
        }
        
        Some(data_msg)
    }

    /// Prepare flight data for processing by adding schema
    fn prepare_data_for_processing(&self, data_msg: FlightData, schema_data: &Option<FlightData>) -> Vec<FlightData> {
        let mut current_data = vec![data_msg];
        if let Some(schema) = schema_data {
            current_data.insert(0, schema.clone());
        }
        current_data
    }

    /// Calculate statistics and convert data to batches for row counting
    fn calculate_stats(&self, current_data: &[FlightData], message_count: usize) -> Result<(usize, u64, f64), Status> {
        let batches = flight_data_to_batches(current_data)
            .map_err(|e| {
                println!("Failed to convert FlightData to batches for message {}: {}", message_count, e);
                Status::internal(format!("Failed to convert FlightData to batches: {}", e))
            })?;

        let mut input_rows = 0;
        for batch in &batches {
            input_rows += batch.num_rows();
        }

        let data_size = current_data.iter().map(|d| d.data_body.len() as u64).sum();
        let data_mb = data_size as f64 / 1_000_000.0;

        Ok((input_rows, data_size, data_mb))
    }
}

#[tonic::async_trait]
impl FlightService for ProcessorFlightServerMultiThreaded {
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
        
        println!("[PROCESSOR] Starting non-blocking processing pipeline...");
        
        let start_time = Instant::now();
        
        // Initialize components - processor now owns the queue and processing thread
        let processor = self.initialize_components().await?;

        // Process the data stream (receiving thread)
        let mut stream_pin = Box::pin(stream);
        let mut message_count = 0;
        let mut batch_count = 0;
        let mut schema_data: Option<FlightData> = None;

        while let Some(maybe_msg) = stream_pin.next().await {
            let data_msg = maybe_msg.map_err(|e| {
                Status::internal(format!("Error reading FlightData stream: {}", e))
            })?;
            
            message_count += 1;

            // Handle schema message (first message)
            if message_count == 1 {
                schema_data = self.handle_schema_message(data_msg, &processor).await;
                continue;
            }

            // Process data batches
            if !data_msg.data_body.is_empty() {
                batch_count += 1;

                // Prepare data for processing
                let current_data = self.prepare_data_for_processing(data_msg, &schema_data);

                // Calculate statistics for logging
                let (input_rows, _data_size, data_mb) = self.calculate_stats(&current_data, message_count)?;

                // Enqueue batch for processing (non-blocking)
                if let Err(e) = processor.enqueue_batch(current_data, batch_count).await {
                    eprintln!("[RECEIVER] Failed to enqueue batch {}: {}", batch_count, e);
                    continue;
                }

                // Log receipt (immediate)
                println!(
                    "[RECEIVER] Received batch {}: {:.2} MB with {} rows - enqueued for processing",
                    batch_count,
                    data_mb,
                    input_rows
                );
            }
        }

        // Signal that no more batches will be sent by dropping the processor
        // This will cause the batch sender to be dropped, signaling the background thread
        // The background thread will continue processing and save stats when done
        drop(processor);

        let elapsed = start_time.elapsed();
        println!("\n[PROCESSOR] Entry node completed in {:.2} seconds (processing continues in background)", elapsed.as_secs_f64());

        let response_stream = stream! {
            yield Ok(PutResult { app_metadata: vec![].into() });
        };

        Ok(Response::new(Box::pin(response_stream)))
    }
    //region
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
    //endregion
}