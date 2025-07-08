mod nexmark;
mod flights;
use arrow_flight::flight_service_server::FlightServiceServer;
use flights::entry_client::DataGenerationMode;
use tonic::transport::Server;
use std::net::SocketAddr; 
use flights::ExitFlightServer;
use flights::ProcessorFlightServer;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "flight-with-nexmark")]
#[command(about = "A Flight server with Nexmark data generation")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    RunQuery2,
    Entry {
        #[arg(long, default_value = "500000")]
        records_per_chunk: usize,
        #[arg(long, default_value = "1000000")]
        no_records: usize,
        /// processor server address to connect to
        #[arg(long, default_value = "localhost:8815")]
        server_address: String,
        #[arg(value_enum)]
        mode: GenerationMode,
    },
    Processor {
        #[arg(long, default_value = "[::1]:8815")]
        bind_address: String,
        #[arg(long, default_value = "localhost:8816")]
        exit_address: String,
        #[arg(value_enum, long, default_value = "real-time")]
        label: LabelMode,
    },
    Exit {
        #[arg(long, default_value = "[::1]:8816")]
        bind_address: String,
        #[arg(value_enum, long, default_value = "real-time")]
        label: LabelMode,
    }
}

#[derive(clap::ValueEnum, Clone)]
enum GenerationMode {
    RealTime,
    PreGenerated,
}

#[derive(clap::ValueEnum, Clone)]
enum LabelMode {
    RealTime,
    PreGenerated,
}

impl From<GenerationMode> for DataGenerationMode {
    fn from(mode: GenerationMode) -> Self {
        match mode {
            GenerationMode::RealTime => DataGenerationMode::RealTime,
            GenerationMode::PreGenerated => DataGenerationMode::PreGenerated,
        }
    }
}

impl From<LabelMode> for String {
    fn from(mode: LabelMode) -> Self {
        match mode {
            LabelMode::RealTime => "REAL-TIME".to_string(),
            LabelMode::PreGenerated => "PRE-GEN".to_string(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    let cli = Cli::parse();

    match cli.command {
        Commands::RunQuery2 => {
            nexmark::queries::run_nexmark_query_2().await?;
        }
        Commands::Entry { records_per_chunk, no_records, server_address, mode } => {
            let generation_mode = DataGenerationMode::from(mode);
            let entry_client = flights::EntryClient::new(&server_address, records_per_chunk, no_records, generation_mode);
            entry_client.run().await?;
        }
        Commands::Processor { bind_address, exit_address, label } => {
            let addr: SocketAddr = bind_address.parse()?;
            println!("Starting Processor Flight server on {}", addr);

            let label_str = String::from(label);
            let processor_server = ProcessorFlightServer::new(exit_address, label_str);
            Server::builder()
                .add_service(
                    FlightServiceServer::new(processor_server)
                    .max_decoding_message_size(usize::MAX)
                    .max_decoding_message_size(usize::MAX)
                )
                .serve(addr)
                .await?;
        }
        Commands::Exit { bind_address, label } => {
            let addr: SocketAddr = bind_address.parse()?;
            println!("Starting Exit Flight server on {}", addr);

            let label_str = String::from(label);
            let exit_server = ExitFlightServer::new(label_str);
            Server::builder()
                .add_service(
                    FlightServiceServer::new(exit_server)
                    .max_decoding_message_size(usize::MAX)
                    .max_decoding_message_size(usize::MAX)
                )
                .serve(addr)
                .await?;
        }
    }

    Ok(())
}
