# Nexmark Flight Pipeline (Without DataFusion)

This project sets up a simple Arrow Flight pipeline in Python using a Nexmark data generator.

---

## Features

- A **Nexmark data generator** using the `nexmark` Rust crate.
- A **Flight pipeline** that passes data from:
  - **Entry node** → **Processor node** → **Exit node**.
- This version does **not** include integration with DataFusion yet.

## Prerequisites

Before running the pipeline, ensure the following dependencies are installed:

1. **Install Rust and Cargo (if not already installed):**

   ```bash
   curl https://sh.rustup.rs -sSf | sh
   ```

2. **Install the Nexmark data generator:**

   ```bash
   cargo install nexmark --features bin
   ```

---

## Running the System

1. **Move into py_arrow_flight directory**

    ```bash
    cd py_arrow_flight
    ```

2. **Start the Exit Node:**

   ```bash
   python exit_server.py
   ```

3. **Start the Processor Node:**

   ```bash
   python flight_server.py
   ```

4. **Start the Entry Node:**

   ```bash
   python flight_client.py
   ```

This sets up the full data flow pipeline:  
**Entry Node → Processor Node → Exit Node**