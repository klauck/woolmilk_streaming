# Setup and Usage

Below are concise instructions for getting the project running on your machine.

> **Prerequisites**
>
> -  Python **3.8 or newer**
> -  (For entry/processor nodes) a **virtual environment** that already has the  
>    [`datafusion`](https://pypi.org/project/datafusion/) package installed.  
>    You will pass the **absolute path** of that environment to the API as `env_path`.

---

## Installation Steps

1. **Clone** or download this repository.
2. **Open a terminal** in the project’s root directory.

### Install dependencies

```bash
pip install -r requirements.txt
```

### Run the server

```bash
uvicorn main:app --reload
```

The server starts on **<http://127.0.0.1:8000>** by default.

---

# Flight Nodes API

High-level REST interface for managing **Entry Nodes** (data-ingestion servers) and **Processor Nodes** (compute workers).

| Endpoint               | Purpose                                       |
| ---------------------- | --------------------------------------------- |
| `POST /entry-node`     | Start up a local Entry Node.                  |
| `POST /processor-node` | Start up a local Processor Node.              |
| `POST /full`           | Convenience: launch _both_ nodes in one call. |

All endpoints return

```jsonc
{ "message": "a message" }
```

---

## 1. Create Entry Node `POST /entry-node`

| Field (form-data) | Type                 | Required | Notes                                                                                                 |
| ----------------- | -------------------- | -------- | ----------------------------------------------------------------------------------------------------- |
| `name`            | string               | ✓        | Instance name; becomes folder name under `./local_node_setup/entry/`.                                 |
| `node_files`      | **file[]** (Parquet) | ✓        | **Upload exactly one file &mdash; `bids.parquet`.** Only this file is recognised locally.             |
| `parquet_files`   | string (JSON object) | –        | Optional explicit map `<table_name>: "bids.parquet"`. If omitted we auto-map `bids` → `bids.parquet`. |
| `env_path`        | string               | ✓        | Absolute path to an **existing** venv env **containing `datafusion`**.                                |

### cURL example

```bash
curl -X POST http://127.0.0.1:8000/api/local-node-setup/entry-node \
  -F "name=bids-entry" \
  -F "node_files=@/Users/usamabintariq/Documents/GitHub/woolmilk_streaming/data/bids.parquet" \
  -F "env_path=/Users/usamabintariq/Documents/GitHub/woolmilk_streaming/test_env"
```

Successful response:

```json
{
   "message": "Entry node 'auction-entry' created at local_node_setup/entry/auction-entry; parquet_files={'bids': 'bids.parquet'}.  Logs: local_node_setup/entry/auction-entry/entry_stdout.log"
}
```

#### Verify the server is listening

```bash
sudo lsof -i :8815
```

---

## 2. Create Processor Node `POST /processor-node`

| Field      | Type   | Required | Notes                                               |
| ---------- | ------ | -------- | --------------------------------------------------- |
| `name`     | string | ✓        | Folder name under `./local_node_setup/processor/`.  |
| `env_path` | string | ✓        | Must point to an env with **datafusion** installed. |

> The Processor Node requires **no file uploads**

### cURL example

```bash
curl -X POST http://127.0.0.1:8000/api/local-node-setup/processor-node \
  -F "name=auction-processor" \
  -F "env_path=/home/you/venvs/flight-env"
```

---

## 3. Create Both Nodes `POST /full`

| Field                                         | Required? | Description                             |
| --------------------------------------------- | --------- | --------------------------------------- |
| `entry_name`, `node_files[]`, `parquet_files` | Yes/No    | Passed through to `/entry-node`.        |
| `processor_name`                              | Yes       | Passed through to `/processor-node`.    |
| `env_path`                                    | Yes       | Shared environment path for both nodes. |

### cURL example

```bash
curl -X POST http://127.0.0.1:8000/full \
  -F "entry_name=auction-entry" \
  -F "node_files=@bids.parquet" \
  -F "processor_name=auction-processor" \
  -F "env_path=/home/you/venvs/flight-env"
```

---

## Logs

| Node type | Log file               | Location                               |
| --------- | ---------------------- | -------------------------------------- |
| Entry     | `entry_stdout.log`     | `./local_node_setup/entry/<name>/`     |
| Processor | `processor_stdout.log` | `./local_node_setup/processor/<name>/` |

Tail a log live with:

```bash
tail -f ./local_node_setup/entry/<name>/entry_stdout.log
```

---

## Error Handling

| HTTP Code | Reason                                                                                        |
| --------- | --------------------------------------------------------------------------------------------- |
| **400**   | Malformed JSON in `parquet_files`, invalid `env_path`, or the interpreter lacks `datafusion`. |
| **500**   | Unexpected I/O or subprocess errors while saving files or booting nodes.                      |
