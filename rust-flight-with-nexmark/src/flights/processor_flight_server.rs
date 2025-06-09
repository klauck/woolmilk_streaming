use std::time::Instant;
use arrow::array::RecordBatch;
use arrow_flight::{utils::flight_data_to_batches};
use futures::stream::{BoxStream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::{
    flight_service_server::FlightService, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};

#[derive(Clone)]
pub struct ProcessorFlightServer {
    exit_server_addr: String
}

// Define constructor in a separate impl block
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
        let mut all_flight_data: Vec<FlightData> = Vec::new();

        println!("Starting to receive FlightData stream...");

        let mut stream_pin = Box::pin(stream);
        let mut message_count = 0;

        let start_time = Instant::now();
        
        while let Some(maybe_msg) = stream_pin.next().await {
            let data_msg = maybe_msg.map_err(|e| {
                Status::internal(format!("Error reading FlightData stream: {}", e))
            })?;
            
            message_count += 1;

            println!("Received {} messages so far...", message_count);

            all_flight_data.push(data_msg);
        }

        let elapsed_time = start_time.elapsed();

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
        
        for (i, batch) in record_batches.iter().enumerate() {
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

        let output_stream: BoxStream<'static, Result<PutResult, Status>> =
            Box::pin(futures::stream::empty());

        Ok(Response::new(output_stream))
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