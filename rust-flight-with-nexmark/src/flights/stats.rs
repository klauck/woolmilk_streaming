use std::fs;
use serde_json::json;

#[derive(Debug, Clone)]
pub struct BatchStats {
    pub id: usize,
    pub time: f64,
    pub bytes: u64,
    pub rows: usize,
}

#[derive(Debug, Clone)]
pub struct QueryProcessingStats {
    pub id: usize,
    pub input_rows: usize,
    pub output_rows: usize,
    pub processing_time: f64,
}

pub trait Stats {
    fn get_component_name(&self) -> &str;
    fn get_mode(&self) -> &str;
    fn to_json(&self) -> serde_json::Value;
    
    fn save_to_file(&self) -> Result<String, Box<dyn std::error::Error>> {
        let stats = self.to_json();

        let component_name = self.get_component_name().to_uppercase();
        let mode = self.get_mode();
        
        let filename = format!("./stats/{}-{}.json", component_name, mode);
        
        if let Err(e) = fs::create_dir_all("./stats") {
            eprintln!("Failed to create stats directory: {}", e);
        }
        
        fs::write(&filename, serde_json::to_string_pretty(&stats)?)?;
        println!("Stats saved to {}", filename);
        Ok(filename)
    }
}

pub struct EntryStats {
    mode: String,
    pub batches: Vec<BatchStats>,
    total_completion_time: f64,
    total_data_sent: u64,
}

impl EntryStats {
    pub fn new(mode: String) -> Self {
        Self {
            mode,
            batches: Vec::new(),
            total_completion_time: 0.0,
            total_data_sent: 0,
        }
    }

    pub fn add_batch(&mut self, id: usize, time: f64, bytes: u64, rows: usize) {
        let batch_stats = BatchStats {
            id,
            time,
            bytes,
            rows,
        };
        self.batches.push(batch_stats);
        self.total_data_sent += bytes;
    }

    pub fn set_total_completion_time(&mut self, total_time: f64) {
        self.total_completion_time = total_time;
    }
}

impl Stats for EntryStats {
    fn get_component_name(&self) -> &str {
        "entry"
    }
    
    fn get_mode(&self) -> &str {
        &self.mode
    }

    fn to_json(&self) -> serde_json::Value {
        json!({
            "mode": self.mode,
            "batches": self.batches.iter().map(|batch| {
                json!({
                    "id": batch.id,
                    "time": batch.time,
                    "bytes": batch.bytes,
                    "rows": batch.rows
                })
            }).collect::<Vec<_>>(),
            "total_completion_time": self.total_completion_time,
            "total_data_sent": self.total_data_sent
        })
    }
}

pub struct ProcessorStats {
    mode: String,
    pub batches: Vec<BatchStats>,
    pub query_processing: Vec<QueryProcessingStats>,
    pub receive: Vec<BatchStats>,
    pub send: Vec<BatchStats>,
    total_time: f64,
    total_bytes_received: u64,
    total_bytes_sent: u64,
}

impl ProcessorStats {
    pub fn new(mode: String) -> Self {
        Self {
            mode,
            batches: Vec::new(),
            query_processing: Vec::new(),
            receive: Vec::new(),
            send: Vec::new(),
            total_time: 0.0,
            total_bytes_received: 0,
            total_bytes_sent: 0,
        }
    }

    pub fn add_batch(&mut self, id: usize, time: f64, bytes: u64, rows: usize) {
        let batch_stats = BatchStats {
            id,
            time,
            bytes,
            rows,
        };
        self.batches.push(batch_stats);
        self.total_bytes_received += bytes;
    }

    pub fn add_receive_batch(&mut self, id: usize, time: f64, bytes: u64, rows: usize) {
        let batch_stats = BatchStats {
            id,
            time,
            bytes,
            rows,
        };
        self.receive.push(batch_stats);
    }

    pub fn add_send_batch(&mut self, id: usize, time: f64, bytes: u64, rows: usize) {
        let batch_stats = BatchStats {
            id,
            time,
            bytes,
            rows,
        };
        self.send.push(batch_stats);
        self.total_bytes_sent += bytes;
    }

    pub fn add_query_processing(&mut self, id: usize, input_rows: usize, output_rows: usize, processing_time: f64) {
        let query_stats = QueryProcessingStats {
            id,
            input_rows,
            output_rows,
            processing_time,
        };
        self.query_processing.push(query_stats);
    }

    pub fn set_total_time(&mut self, total_time: f64) {
        self.total_time = total_time;
    }
}

impl Stats for ProcessorStats {
    fn get_component_name(&self) -> &str {
        "processor"
    }
    
    fn get_mode(&self) -> &str {
        &self.mode
    }

    fn to_json(&self) -> serde_json::Value {
        json!({
            "mode": self.mode,
            "batches": self.batches.iter().map(|batch| {
                json!({
                    "id": batch.id,
                    "time": batch.time,
                    "bytes": batch.bytes,
                    "rows": batch.rows
                })
            }).collect::<Vec<_>>(),
            "query_processing": self.query_processing.iter().map(|query| {
                json!({
                    "id": query.id,
                    "input_rows": query.input_rows,
                    "output_rows": query.output_rows,
                    "processing_time": query.processing_time
                })
            }).collect::<Vec<_>>(),
            "receive": self.receive.iter().map(|batch| {
                json!({
                    "id": batch.id,
                    "time": batch.time,
                    "bytes": batch.bytes,
                    "rows": batch.rows
                })
            }).collect::<Vec<_>>(),
            "send": self.send.iter().map(|batch| {
                json!({
                    "id": batch.id,
                    "time": batch.time,
                    "bytes": batch.bytes,
                    "rows": batch.rows
                })
            }).collect::<Vec<_>>(),
            "total_time": self.total_time,
            "total_bytes_received": self.total_bytes_received,
            "total_bytes_sent": self.total_bytes_sent
        })
    }
}

pub struct ExitStats {
    mode: String,
    pub batches: Vec<BatchStats>,
    total_completion_time: f64,
    total_data_received: u64,
}

impl ExitStats {
    pub fn new(mode: String) -> Self {
        Self {
            mode,
            batches: Vec::new(),
            total_completion_time: 0.0,
            total_data_received: 0,
        }
    }

    pub fn add_batch(&mut self, id: usize, time: f64, bytes: u64, rows: usize) {
        let batch_stats = BatchStats {
            id,
            time,
            bytes,
            rows,
        };
        self.batches.push(batch_stats);
        self.total_data_received += bytes;
    }

    pub fn set_total_completion_time(&mut self, total_time: f64) {
        self.total_completion_time = total_time;
    }
}

impl Stats for ExitStats {
    fn get_component_name(&self) -> &str {
        "exit"
    }
    
    fn get_mode(&self) -> &str {
        &self.mode
    }

    fn to_json(&self) -> serde_json::Value {
        json!({
            "mode": self.mode,
            "batches": self.batches.iter().map(|batch| {
                json!({
                    "id": batch.id,
                    "time": batch.time,
                    "bytes": batch.bytes,
                    "rows": batch.rows
                })
            }).collect::<Vec<_>>(),
            "total_completion_time": self.total_completion_time,
            "total_data_received": self.total_data_received
        })
    }
}
