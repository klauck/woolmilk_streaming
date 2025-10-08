# WoolMilk Streaming with Docker

## How to run it

### Step 1: Build the images

First you need to build all the docker images:

```bash
./build.sh #make sure you do chmod +x ./build.sh so that it becomes executeable
```

This will create 3 images:

-  woolmilk-source:latest
-  woolmilk-processing:latest
-  woolmilk-sink:latest

### Step 2: Run everything

```bash
docker-compose up
```

This starts all 3 containers and they talk to each other.
