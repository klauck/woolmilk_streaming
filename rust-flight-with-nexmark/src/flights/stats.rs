use std::fs;
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct BatchStats {
    pub chunk_id: usize,
    pub rows: usize,
    pub bytes: u64,
    pub time_seconds: f64,
    pub throughput_mbps: f64,
}

#[derive(Debug, Clone)]
pub struct ProcessingStats {
    pub chunk_id: usize,
    pub input_rows: usize,
    pub output_rows: usize,
    pub processing_time_seconds: f64,
    pub query_execution_time_seconds: f64,
}

pub trait Stats {
    fn get_component_name(&self) -> &str;
    fn get_mode(&self) -> &str;
    fn get_records_per_chunk(&self) -> usize;
    fn to_json(&self) -> serde_json::Value;
    
    fn save_to_file(&self, additional_stats: Option<serde_json::Map<String, serde_json::Value>>) -> Result<String, Box<dyn std::error::Error>> {
        let mut final_stats = self.to_json();
        
        if let Some(additional) = additional_stats {
            if let Some(summary) = final_stats.get_mut("summary") {
                for (key, value) in additional {
                    summary[key] = value;
                }
            }
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let component_name = self.get_component_name().to_uppercase();
        let mode = self.get_mode().to_uppercase().replace("-", "_").replace(" ", "_");
        
        let filename = if mode.is_empty() || mode == "PROCESSING" || mode == "RECEIVING" {
            format!("./stats/{}-{}.json", component_name, timestamp)
        } else {
            format!("./stats/{}-{}-{}.json", component_name, mode, timestamp)
        };
        
        if let Err(e) = fs::create_dir_all("./stats") {
            eprintln!("Failed to create stats directory: {}", e);
        }
        
        fs::write(&filename, serde_json::to_string_pretty(&final_stats)?)?;
        println!("Stats saved to {}", filename);
        Ok(filename)
    }
}

pub struct EntryStats {
    batches: Vec<BatchStats>,
    mode: String,
    records_per_chunk: usize
}

impl EntryStats {
    pub fn new(mode: String, records_per_chunk: usize) -> Self {
        Self {
            batches: Vec::new(),
            mode,
            records_per_chunk
        }
    }

    pub fn add_batch(&mut self, chunk_id: usize, rows: usize, bytes: u64, time_seconds: f64) {
        let throughput_mbps = if time_seconds > 0.0 {
            (bytes as f64 / 1_000_000.0) / time_seconds
        } else {
            0.0
        };

        let batch_stats = BatchStats {
            chunk_id,
            rows,
            bytes,
            time_seconds,
            throughput_mbps,
        };

        self.batches.push(batch_stats);
    }
}

impl Stats for EntryStats {
    fn get_component_name(&self) -> &str {
        "entry"
    }

    fn get_mode(&self) -> &str {
        &self.mode
    }

    fn get_records_per_chunk(&self) -> usize {
        self.records_per_chunk
    }

    fn to_json(&self) -> serde_json::Value {
        let total_time: f64 = self.batches.iter().map(|b| b.time_seconds).sum();
        let total_bytes: u64 = self.batches.iter().map(|b| b.bytes).sum();
        let total_rows: usize = self.batches.iter().map(|b| b.rows).sum();
        let total_chunks = self.batches.len();
        let total_mb = total_bytes as f64 / 1_000_000.0;
        let overall_throughput_mbps = if total_time > 0.0 {
            total_mb / total_time
        } else {
            0.0
        };

        json!({
            "component": self.get_component_name(),
            "mode": self.get_mode(),
            "records_per_chunk": self.get_records_per_chunk(),
            "summary": {
                "total_chunks": total_chunks,
                "total_rows": total_rows,
                "total_bytes": total_bytes,
                "total_mb": total_mb,
                "processing_time_seconds": total_time,
                "overall_throughput_mbps": overall_throughput_mbps
            },
            "data": self.batches.iter().map(|batch| {
                json!({
                    "chunk_id": batch.chunk_id,
                    "rows": batch.rows,
                    "bytes": batch.bytes,
                    "mb": batch.bytes as f64 / 1_000_000.0,
                    "time_seconds": batch.time_seconds,
                    "throughput_mbps": batch.throughput_mbps
                })
            }).collect::<Vec<_>>()
        })
    }
}

pub struct ProcessorStats {
    receive_batches: Vec<BatchStats>,
    send_batches: Vec<BatchStats>,
    processing_stats: Vec<ProcessingStats>,
    records_per_chunk: usize
}

impl ProcessorStats {
    pub fn new(records_per_chunk: usize) -> Self {
        Self {
            receive_batches: Vec::new(),
            send_batches: Vec::new(),
            processing_stats: Vec::new(),
            records_per_chunk
        }
    }

    pub fn add_receive_batch(&mut self, chunk_id: usize, rows: usize, bytes: u64, time_seconds: f64) {
        let throughput_mbps = if time_seconds > 0.0 {
            (bytes as f64 / 1_000_000.0) / time_seconds
        } else {
            0.0
        };

        let batch_stats = BatchStats {
            chunk_id,
            rows,
            bytes,
            time_seconds,
            throughput_mbps,
        };

        self.receive_batches.push(batch_stats);
    }

    pub fn add_send_batch(&mut self, chunk_id: usize, rows: usize, bytes: u64, time_seconds: f64) {
        let throughput_mbps = if time_seconds > 0.0 {
            (bytes as f64 / 1_000_000.0) / time_seconds
        } else {
            0.0
        };

        let batch_stats = BatchStats {
            chunk_id,
            rows,
            bytes,
            time_seconds,
            throughput_mbps,
        };

        self.send_batches.push(batch_stats);
    }

    pub fn add_processing_stats(&mut self, chunk_id: usize, input_rows: usize, output_rows: usize, 
                              processing_time_seconds: f64, query_execution_time_seconds: f64) {
        let processing_stats = ProcessingStats {
            chunk_id,
            input_rows,
            output_rows,
            processing_time_seconds,
            query_execution_time_seconds,
        };

        self.processing_stats.push(processing_stats);
    }
}

impl Stats for ProcessorStats {
    fn get_component_name(&self) -> &str {
        "processor"
    }

    fn get_mode(&self) -> &str {
        "processing"
    }

    fn get_records_per_chunk(&self) -> usize {
        self.records_per_chunk
    }

    fn to_json(&self) -> serde_json::Value {
        let receive_total_time: f64 = self.receive_batches.iter().map(|b| b.time_seconds).sum();
        let receive_total_bytes: u64 = self.receive_batches.iter().map(|b| b.bytes).sum();
        let receive_total_rows: usize = self.receive_batches.iter().map(|b| b.rows).sum();
        let receive_total_chunks = self.receive_batches.len();
        let receive_total_mb = receive_total_bytes as f64 / 1_000_000.0;
        let receive_throughput_mbps = if receive_total_time > 0.0 {
            receive_total_mb / receive_total_time
        } else {
            0.0
        };

        let send_total_time: f64 = self.send_batches.iter().map(|b| b.time_seconds).sum();
        let send_total_bytes: u64 = self.send_batches.iter().map(|b| b.bytes).sum();
        let send_total_rows: usize = self.send_batches.iter().map(|b| b.rows).sum();
        let send_total_chunks = self.send_batches.len();
        let send_total_mb = send_total_bytes as f64 / 1_000_000.0;
        let send_throughput_mbps = if send_total_time > 0.0 {
            send_total_mb / send_total_time
        } else {
            0.0
        };

        let total_processing_time: f64 = self.processing_stats.iter().map(|p| p.processing_time_seconds).sum();
        let total_query_time: f64 = self.processing_stats.iter().map(|p| p.query_execution_time_seconds).sum();
        let total_input_rows: usize = self.processing_stats.iter().map(|p| p.input_rows).sum();
        let total_output_rows: usize = self.processing_stats.iter().map(|p| p.output_rows).sum();
        let processing_chunks = self.processing_stats.len();
        let selectivity = if total_input_rows > 0 { 
            (total_output_rows as f64 / total_input_rows as f64) * 100.0 
        } else { 
            0.0 
        };

        json!({
            "component": self.get_component_name(),
            "mode": self.get_mode(),
            "records_per_chunk": self.get_records_per_chunk(),
            "summary": {
                "receive": {
                    "total_chunks": receive_total_chunks,
                    "total_rows": receive_total_rows,
                    "total_bytes": receive_total_bytes,
                    "total_mb": receive_total_mb,
                    "processing_time_seconds": receive_total_time,
                    "overall_throughput_mbps": receive_throughput_mbps
                },
                "send": {
                    "total_chunks": send_total_chunks,
                    "total_rows": send_total_rows,
                    "total_bytes": send_total_bytes,
                    "total_mb": send_total_mb,
                    "processing_time_seconds": send_total_time,
                    "overall_throughput_mbps": send_throughput_mbps
                },
                "processing": {
                    "total_chunks": processing_chunks,
                    "total_input_rows": total_input_rows,
                    "total_output_rows": total_output_rows,
                    "selectivity_percentage": selectivity,
                    "total_processing_time_seconds": total_processing_time,
                    "total_query_execution_time_seconds": total_query_time,
                    "avg_processing_time_per_chunk": if processing_chunks > 0 { total_processing_time / processing_chunks as f64 } else { 0.0 },
                    "avg_query_time_per_chunk": if processing_chunks > 0 { total_query_time / processing_chunks as f64 } else { 0.0 }
                }
            },
            "data": {
                "receive": self.receive_batches.iter().map(|batch| {
                    json!({
                        "chunk_id": batch.chunk_id,
                        "rows": batch.rows,
                        "bytes": batch.bytes,
                        "mb": batch.bytes as f64 / 1_000_000.0,
                        "time_seconds": batch.time_seconds,
                        "throughput_mbps": batch.throughput_mbps
                    })
                }).collect::<Vec<_>>(),
                "send": self.send_batches.iter().map(|batch| {
                    json!({
                        "chunk_id": batch.chunk_id,
                        "rows": batch.rows,
                        "bytes": batch.bytes,
                        "mb": batch.bytes as f64 / 1_000_000.0,
                        "time_seconds": batch.time_seconds,
                        "throughput_mbps": batch.throughput_mbps
                    })
                }).collect::<Vec<_>>(),
                "processing": self.processing_stats.iter().map(|proc| {
                    json!({
                        "chunk_id": proc.chunk_id,
                        "input_rows": proc.input_rows,
                        "output_rows": proc.output_rows,
                        "selectivity_percentage": if proc.input_rows > 0 { (proc.output_rows as f64 / proc.input_rows as f64) * 100.0 } else { 0.0 },
                        "processing_time_seconds": proc.processing_time_seconds,
                        "query_execution_time_seconds": proc.query_execution_time_seconds
                    })
                }).collect::<Vec<_>>()
            }
        })
    }
}

pub struct ExitStats {
    pub receive_batches: Vec<BatchStats>,
    records_per_chunk: usize
}

impl ExitStats {
    pub fn new(records_per_chunk: usize) -> Self {
        Self {
            receive_batches: Vec::new(),
            records_per_chunk
        }
    }

    pub fn add_receive_batch(&mut self, chunk_id: usize, rows: usize, bytes: u64, time_seconds: f64) {
        let throughput_mbps = if time_seconds > 0.0 {
            (bytes as f64 / 1_000_000.0) / time_seconds
        } else {
            0.0
        };

        let batch_stats = BatchStats {
            chunk_id,
            rows,
            bytes,
            time_seconds,
            throughput_mbps,
        };

        self.receive_batches.push(batch_stats);
    }
}

impl Stats for ExitStats {
    fn get_component_name(&self) -> &str {
        "exit"
    }

    fn get_mode(&self) -> &str {
        "receiving"
    }

    fn get_records_per_chunk(&self) -> usize {
        self.records_per_chunk
    }

    fn to_json(&self) -> serde_json::Value {
        let total_time: f64 = self.receive_batches.iter().map(|b| b.time_seconds).sum();
        let total_bytes: u64 = self.receive_batches.iter().map(|b| b.bytes).sum();
        let total_rows: usize = self.receive_batches.iter().map(|b| b.rows).sum();
        let total_chunks = self.receive_batches.len();
        let total_mb = total_bytes as f64 / 1_000_000.0;
        let overall_throughput_mbps = if total_time > 0.0 {
            total_mb / total_time
        } else {
            0.0
        };

        json!({
            "component": self.get_component_name(),
            "mode": self.get_mode(),
            "records_per_chunk": self.get_records_per_chunk(),
            "summary": {
                "total_chunks": total_chunks,
                "total_rows": total_rows,
                "total_bytes": total_bytes,
                "total_mb": total_mb,
                "processing_time_seconds": total_time,
                "overall_throughput_mbps": overall_throughput_mbps
            },
            "data": self.receive_batches.iter().map(|batch| {
                json!({
                    "chunk_id": batch.chunk_id,
                    "rows": batch.rows,
                    "bytes": batch.bytes,
                    "mb": batch.bytes as f64 / 1_000_000.0,
                    "time_seconds": batch.time_seconds,
                    "throughput_mbps": batch.throughput_mbps
                })
            }).collect::<Vec<_>>()
        })
    }
}
