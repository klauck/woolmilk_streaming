## Installation

Optionally create a virtual environment
```
cd py_arrow_flight
python3 -m venv woolmilk
source woolmilk/bin/activate
```
Install the Python library for DataFusion and numpy
```
pip install datafusion
pip install numpy
```

## Start local example

**1. Start the sink server**

```
python sink_node.py 8017
```

**2. Start the processing node**
   
```
python processing_node.py 8016 127.0.0.1:8017
```
  
**3. Start the source client**

```
python source_node.py 127.0.0.1:8016
```

