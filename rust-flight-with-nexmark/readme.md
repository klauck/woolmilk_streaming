# Arrow Flight Data Pipeline with Rust

This project implements a distributed data streaming pipeline using Apache Arrow Flight. The system consists of three components that work together to process and transfer data in real-time.

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

## Running the System

The system requires running three separate processes in order. Open three terminal windows:

### Terminal 1: Start Exit Server

```bash
cargo run exit
```

Expected output:

```
Starting Exit Flight server on [::1]:8816
```

### Terminal 2: Start Processor Server

```bash
cargo run processor
```

Expected output:

```
Starting Processor Flight server on [::1]:8815
```

### Terminal 3: Run Entry Client

For real-time data generation:

```bash
cargo run entry real-time
```

For pre-generated data:

```bash
cargo run entry pre-generated
```
