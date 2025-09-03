# Setup and Usage

Below are instructions on how to set up and run this project.

Make sure you have Python 3.8+ installed.

## Installation Steps

Clone or download this project.

Navigate to the project directory in your terminal.

### Install dependencies:

`pip install -r requirements.txt`

### Run the server:

`uvicorn main:app --reload`

The server will start on http://127.0.0.1:8000 by default, and the API routes will be available under the /api path.

# Flight Nodes API

High‑level REST interface for managing **Entry Nodes** and **Processor Nodes**

---

## Base path

`/` (root of FastAPI application)

---

## Entry‑Node routes (`/entry-node…`)

### 1. **Create an entry node**

POST /entry-node (multipart/form-data)

Register a new entry node in the DB **and** deploy it on the remote host.  
_Required form fields_

-  `name` `str`
-  `ssh_host` `str`
-  `ssh_port` `int`
-  `ssh_user` `str`
-  `serving_host` `str`
-  `serving_port` `int`
-  `env_name` `str` (name of Python virtual‑env on remote)

_Optional form fields_

-  `ssh_password` `str` (if not provided, SSH key auth is used)
-  `parquet_files` **JSON string** – e.g. `{"bids":"bids.parquet"}`
-  `node_files` `List[UploadFile]` – Parquet files

_Success response_ → **200 OK** with an `EntryNodeOut` object.  
_Failure_ → **400** (validation) or **500** (deployment error).

---

### 2. **List all entry nodes**

GET /entry-node

Returns `List[EntryNodeOut]`.

### 3. **Get one entry node**

GET /entry-node/{node_id}

**Returns** → `EntryNodeOut`

_404_ if the id is missing.

### 4. **Delete an entry node**

DELETE /entry-node/{node_id}

Stops the remote process, removes DB record and returns

**Returns** → `{"detail": "Entry node … deleted."}`

### 5. **Health‑check**

Pings `serving_host:serving_port` (TCP).

Response JSON:

```jsonc
{ "node_id": 3,
  "name": "bids_source",
  "port_status": "open" | "closed",
  "message": "Port 8815 on 1.2.3.4 is open. Node is working." }
```

## Processor‑Node routes (/processor-node…)

### 1. Create a processor node

POST /processor-node (application/json)

Body matches the **ProcessorNodeCreate**.
Deploys a Python script remotely under the given virtual‑env.

**Returns** → ProcessorNodeOut

### 2. List all processor nodes

GET /processor-node

**Returns** → List[ProcessorNodeOut]

### 3. Get one processor node

GET /processor-node/{node_id}

**Returns** → ProcessorNodeOut

### 4. Delete a processor node

DELETE /processor-node/{node_id}

**Returns** → {"detail": "Processor node <name> deleted."}.

## Data models (response bodies)

**EntryNodeOut**

```jsonc
{
   "id": 1,
   "name": "bids_source",
   "ssh_host": "1.2.3.4",
   "ssh_port": 22,
   "ssh_user": "ubuntu",
   "ssh_password": null,
   "serving_host": "0.0.0.0",
   "serving_port": 8815,
   "parquet_files": { "bids": "bids.parquet" },
   "env_name": "pyarrow_env",
   "status": "running", // running | stopped | failed
   "status_message": "Deployed successfully."
}
```

**ProcessorNodeCreate**

```jsonc
{
   "name": "json_processor",
   "ssh_host": "192.168.0.10",
   "ssh_port": 22,
   "ssh_user": "ubuntu",
   "ssh_password": null,
   "exit_host": "10.0.0.2", // optional
   "exit_port": 9000, // optional
   "env_name": "pyarrow_env", // remote Python venv
   "entry_endpoints": [
      {
         "name": "entry1",
         "host": "1.2.3.4",
         "port": 8815,
         "services": ["bids", "auctions"]
      }
   ],
   "queries": [
      {
         "node_id": "bids",
         "queries_string": [
            "SELECT * FROM bids LIMIT 100",
            "SELECT COUNT(*) FROM bids"
         ]
      }
   ]
}
```

**ProcessorNodeOut**

```jsonc
{
   "id": 7,
   "name": "json_processor",
   "ssh_host": "192.168.0.10",
   "ssh_port": 22,
   "ssh_user": "ubuntu",
   "ssh_password": null,
   "exit_host": "10.0.0.2",
   "exit_port": 9000,
   "env_name": "pyarrow_env",
   "entry_endpoints": [
      {
         "name": "entry1",
         "host": "1.2.3.4",
         "port": 8815,
         "services": ["bids", "auctions"]
      }
   ],
   "queries": [
      { "node_id": "bids", "queries_string": ["SELECT * FROM bids LIMIT 100"] }
   ],
   "status": "running",
   "status_message": "Deployed successfully."
}
```

## Error codes

-  400 Bad Request – validation failure, malformed JSON, duplicate names.

-  404 Not Found – specified node_id does not exist.

-  500 Internal Server Error – SSH connection or remote deployment failure; details placed in status_message.
