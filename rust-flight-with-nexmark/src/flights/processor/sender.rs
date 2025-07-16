use arrow_flight::{FlightData, flight_descriptor::DescriptorType, FlightDescriptor};
use arrow_flight::flight_service_client::FlightServiceClient;
use async_stream::stream;
use futures::stream::StreamExt;
use tokio::{sync::mpsc, task::JoinHandle};
use tonic::{Request, metadata::MetadataValue};
use prost::Message;

#[derive(Clone)]
pub struct Sender {
    pub exit_server_addr: String,
    label: String,
}

impl Sender {
    pub fn new(exit_server_addr: String, label: String) -> Self {
        Self { exit_server_addr, label }
    }

    pub async fn setup_exit_connection(&self) -> Result<(mpsc::Sender<FlightData>, JoinHandle<()>), Box<dyn std::error::Error>> {
        if self.exit_server_addr.is_empty() {
            return Err("No exit server address provided".into());
        }

        let client = FlightServiceClient::connect(format!("http://{}", self.exit_server_addr))
            .await
            .map_err(|e| format!("Failed to connect to exit server: {}", e))?;
        
        println!("[EXIT] Connected to exit server at {}", self.exit_server_addr);

        let (tx, mut rx) = mpsc::channel::<FlightData>(100);
        
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
        let handle = tokio::spawn(async move {
            match client_clone.do_put(request).await {
                Ok(response) => {
                    let mut put_results = response.into_inner();
                    while let Some(_put_res) = put_results.next().await {
                        // Process put results if needed
                    }
                    println!("[EXIT] Persistent connection closed successfully");
                }
                Err(e) => {
                    println!("[EXIT] Persistent connection error: {}", e);
                }
            }
        });

        Ok((tx, handle))
    }
}
