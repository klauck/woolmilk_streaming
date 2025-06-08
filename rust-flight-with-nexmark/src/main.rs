mod nexmark;
mod flights;
use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::Server;
use std::env;
use std::net::SocketAddr; 

use nexmark::{NexmarkDataGenerator};
use flights::ExitFlightServer;
use flights::ProcessorFlightServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    let mut args = env::args();
    // Skip program name
    let _exe = args.next();
    let mode = match args.next() {
        Some(m) => m,
        None => {
            eprintln!("Usage: {} <entry|processor|exit>", env::args().next().unwrap_or_default());
            std::process::exit(1);
        }
    };

    match mode.as_str(){
        "entry" => {
            let entry_client = flights::EntryClient::new("localhost:8815", 100000, 5000000, flights::entry_client::DataGenerationMode::PreGenerated);
            entry_client.run().await?;
        }
        "processor" => {
            let addr: SocketAddr = "[::1]:8815".parse()?;
            println!("Starting Processor Flight server on {}", addr);

            let processor_server = ProcessorFlightServer {};
            Server::builder()
                .max_frame_size(Some(16_777_215))
                .add_service(
                    FlightServiceServer::new(processor_server)
                    .max_decoding_message_size(usize::MAX)
                    .max_decoding_message_size(usize::MAX)
                )
                .serve(addr)
                .await?;
        }

        other => {
            eprintln!("Unknown mode: {}. Expected 'processor'.", other);
            std::process::exit(1);
        }
    }

    Ok(())
}
