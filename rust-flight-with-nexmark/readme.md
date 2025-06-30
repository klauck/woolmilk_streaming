# Arrow Flight Data Pipeline with Rust

This rust project implements a distributed data streaming pipeline using Apache Arrow Flight. The system consists of three components that work together to process and transfer data in real-time.

## Architecture Overview

```
Entry Client → Processor Server → Exit Server
```

### Components

1. **Entry Client**: Generates and sends Nexmark bids data
2. **Processor Server**: Receives, processes, and forwards data
3. **Exit Server**: Final destination that receives processed data

## How It Works

### 1. Entry Client

-  Generates Nexmark auction bid data
-  Supports two modes:
   -  **Real-time**: Generates data real time during streaming
   -  **Pre-generated**: Generates all data upfront, then streams it
-  Converts data to Arrow Flight format and streams to Processor Server

### 2. Processor Server

-  Receives FlightData stream from Entry Client
-  Processes each batch (For now doing nothing)
-  Forward data to Exit Node

### 3. Exit Server

-  Receives processed data from Processor Server
-  Print statistics (Data speed) calculation

## Prerequisites

-  Rust
-  Cargo package manager

## Command Line Interface

The application uses structured command line arguments. Use `--help` to see all available options:

```bash
cargo run --help
```

## Running the System

The system requires running three separate processes in order. Open three terminal windows:

### Terminal 1: Start Exit Server

```bash
cargo run exit #default bind to localhost:8816

# custom bind address
cargo run exit --bind-address "[::1]:8817"
```

### Terminal 2: Start Processor Server

```bash
cargo run processor #default bind to [::1]:8815, default exit localhost:8816

# custom bind address
cargo run processor --bind-address "[::1]:8818" --exit-address "localhost:8817"
```

### Terminal 3: Run Entry Client

For real-time data generation:

```bash
cargo run entry real-time #connects to localhost:8815

cargo run entry --records-per-chunk 100000 --no-records 2000000 --server-address "localhost:8815" real-time
```

For pre-generated data:

```bash
cargo run entry pre-generated

cargo run entry --records-per-chunk 250000 --no-records 5000000 --server-address "localhost:8815" pre-generated
```

## Additional Features

### Run Nexmark Query 2

You can also run standalone Nexmark Query 2:

```bash
cargo run run-query2
```
