mod nexmark;
mod flights;
use arrow_flight::flight_service_server::FlightServiceServer;
use flights::entry_client::DataGenerationMode;
use tonic::transport::Server;
use std::env;
use std::net::SocketAddr; 
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

    let exit_addr: SocketAddr = "[::1]:8816".parse()?;

    match mode.as_str(){
        "run-query-2"=>{
            nexmark::queries::run_nexmark_query_2().await?;
        }
        "entry" => {
            let records_per_chunk = 500000;
            let no_records = 1000000;
            // get next argument as real time or pre-generated
            let generation_mode = match args.next() {
                Some(mode) => mode,
                None => {
                    eprintln!("Usage: {} entry <real-time|pre-generated>", env::args().next().unwrap_or_default());
                    std::process::exit(1);
                }
            };
            let generation_mode = match generation_mode.as_str() {
                "real-time" => DataGenerationMode::RealTime,
                "pre-generated" => DataGenerationMode::PreGenerated,
                _ => {
                    eprintln!("Unknown generation mode: {}. Expected 'real-time' or 'pre-generated'.", generation_mode);
                    std::process::exit(1);
                }
            };
            
            let entry_client = flights::EntryClient::new("localhost:8815", records_per_chunk, no_records, generation_mode);
            entry_client.run().await?;
        }
        "processor" => {
            let addr: SocketAddr = "[::1]:8815".parse()?;
            println!("Starting Processor Flight server on {}", addr);

            let processor_server = ProcessorFlightServer::new(exit_addr.to_string());
            Server::builder()
                .add_service(
                    FlightServiceServer::new(processor_server)
                    .max_decoding_message_size(usize::MAX)
                    .max_decoding_message_size(usize::MAX)
                )
                .serve(addr)
                .await?;
        }

        "exit" => {
            println!("Starting Exit Flight server on {}", exit_addr);

            let exit_server = ExitFlightServer {};
            Server::builder()
                .add_service(
                    FlightServiceServer::new(exit_server)
                    .max_decoding_message_size(usize::MAX)
                    .max_decoding_message_size(usize::MAX)
                )
                .serve(exit_addr)
                .await?;
        }
        other => {
            eprintln!("Unknown mode: {}. Expected 'processor'.", other);
            std::process::exit(1);
        }
    }

    Ok(())
}
