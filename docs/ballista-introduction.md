# Apache Ballista

**Apache Ballista** is a distributed platform implemented in `Rust` and powered by Apache Arrow and DataFusion.

There are two main components in Ballista: the **scheduler** and the **executor**. The scheduler is responsible for coordinating queries, while the executor is responsible for executing them.

## Ballista Scheduler

The scheduler is responsible for coordinating queries and is implemented as a gRPC service. The scheduler is responsible for parsing the logical plan, creating tasks, and scheduling tasks on executors.

## Ballista Executor

The executor is responsible for executing tasks and is implemented as a gRPC service. The executor is responsible for executing tasks and returning the results to the scheduler.

## Ballista Setup

To setup Ballista, you need to install the Ballista binaries on your system. Ballista schedular and executor can be install using `cargo` package manager.

```bash
cargo install --locked ballista-scheduler
cargo install --locked ballista-executor
```

### Usage Example with Ballista & Data Fusion

#### Setup Project

To use Ballista with Data Fusion, you need to create a new project and add the following dependencies to your `Cargo.toml` file:

```toml
[package]
name = "ballista-test"
version = "0.1.0"
edition = "2021"

[dependencies]
ballista = "43.0.0"
ballista-core = "43.0.0"
ballista-executor = "43.0.0"
ballista-scheduler = "43.0.0"
datafusion = "43.0.0"
tokio = "1.43.0"
```

Run this command to install the dependencies:

```bash
cargo update
```

Create a new file `main.rs` at `src/` and add the following code:

```rust
use ballista::prelude::*;
use datafusion::{
    execution::SessionStateBuilder,
    prelude::{CsvReadOptions, SessionConfig, SessionContext},
};

use tokio;

#[tokio::main]
async fn main() -> Result<(), std::io::Error>  {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_ballista_job_name("Remote SQL Example");

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

    ctx.register_csv(
        "data",
        &format!("path/to/data.csv"),
        CsvReadOptions::new(),
    )
    .await?;

    let df = ctx
        .sql(
            r#"
            SELECT
               *
            FROM data
            "#,
        )
        .await?;

    df.show().await?;

    Ok(())
}
```

#### Start Ballista Scheduler and Executor

Then we need to start the scheduler and executor. The scheduler and Executor can be started using the following command:

```bash
ballista-scheduler
```

```bash
ballista-executor
```

#### Run the Project

Finally, run the project using the following command:

```bash
cargo run
```

## Continue Reading

[Ballista Scheduler & Executer]("ballista-scheduler-executer.md")
