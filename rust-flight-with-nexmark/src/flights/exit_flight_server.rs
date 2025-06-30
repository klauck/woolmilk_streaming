use std::time::Instant;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use futures::{stream::BoxStream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use serde_json::json;

use arrow_flight::{
    flight_service_server::FlightService, utils::flight_data_to_batches, Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket
};
use super::stats::{ExitStats, Stats};

#[derive(Clone)]
pub struct ExitFlightServer {
    stats: Arc<Mutex<ExitStats>>,
}

impl ExitFlightServer {
    pub fn new() -> Self {
        let stats = Arc::new(Mutex::new(ExitStats::new(0)));
        Self { stats }
    }
}

#[tonic::async_trait]
impl FlightService for ExitFlightServer {
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
        let mut batch_count = 0;
        let mut current_schema: Option<FlightData> = None;

        let start_time = Instant::now();
        
        while let Some(maybe_msg) = stream_pin.next().await {
            let data_msg = maybe_msg.map_err(|e| {
                Status::internal(format!("Error reading FlightData stream: {}", e))
            })?;
            
            message_count += 1;

            if message_count % 50 == 0 {
                println!("Received {} messages so far...", message_count);
            }

            // Check if this is a schema message (has schema but no data body)
            if !data_msg.data_header.is_empty() && data_msg.data_body.is_empty() {
                current_schema = Some(data_msg.clone());
                all_flight_data.push(data_msg);
                continue;
            }

            // Only process data messages that have actual data
            if !data_msg.data_body.is_empty() {
                let batch_receive_start = Instant::now();
                let mut current_data = vec![data_msg.clone()];
                if let Some(ref schema) = current_schema {
                    current_data.insert(0, schema.clone());
                }

                if let Ok(batches) = flight_data_to_batches(&current_data) {
                    if !batches.is_empty() {
                        batch_count += 1;
                        let receive_duration = batch_receive_start.elapsed().as_secs_f64();
                        
                        for batch in &batches {
                            let rows = batch.num_rows();
                            let bytes = batch.get_array_memory_size() as u64;
                            
                            {
                                let mut stats_lock = self.stats.lock().unwrap();
                                stats_lock.add_receive_batch(batch_count, rows, bytes, receive_duration);
                            }
                            
                            if batch_count % 10 == 0 {
                                let mbs = bytes as f64 / 1_000_000.0;
                                println!("Received batch {}: {} rows, {:.2} MB", batch_count, rows, mbs);
                            }
                        }
                    }
                }
            }

            all_flight_data.push(data_msg);
        }

        let elapsed_time = start_time.elapsed();

        println!("Finished receiving stream. Processing {} FlightData messages...", all_flight_data.len());

        // Don't try to convert all data at once - just use stats from individual batches
        let mut total_rows: usize = 0;
        let mut total_bytes: u64 = 0;
        
        {
            let stats_lock = self.stats.lock().unwrap();
            for batch_stat in &stats_lock.receive_batches {
                total_rows += batch_stat.rows;
                total_bytes += batch_stat.bytes;
            }
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
        additional_stats.insert("receiving_rate_mbps".to_string(), json!(rate));
        
        stats_lock.save_to_file(Some(additional_stats)).unwrap_or_else(|e| {
            eprintln!("Failed to save exit stats: {}", e);
            "failed".to_string()
        });

        println!(
            "Received {} batches with {} rows and {:.2} MB in {:.2?} seconds with transfer rate {:.2} MB/s",
            batch_count, total_rows, total_mbs, elapsed_time, rate
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