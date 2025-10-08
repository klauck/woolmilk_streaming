FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sink_node.py .

RUN mkdir -p /app/results

CMD ["python", "sink_node.py"]