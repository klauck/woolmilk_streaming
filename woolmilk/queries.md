# Source and Processing Node parameters to run advanced NEXMark queries

## Query 3
```
python woolmilk/processing_node.py \
  --port 8017 \
  --forward-node 127.0.0.1:8027 \
  --query "SELECT P.name, P.city, P.state, A.id FROM person_table P, auction_table A WHERE A.seller = P.id" \
  --query-result-schema '{
    "fields": [
      {"name": "name", "type": "string"},
      {"name": "city", "type": "string"},
      {"name": "state", "type": "string"},
      {"name": "id", "type": "int64"}
    ]
  }' \
  --query-id 3
```
```
python woolmilk/source_node.py --stream nexmark.person --processing-nodes 127.0.0.1:8017
```
```
python woolmilk/source_node.py --stream nexmark.auction --processing-nodes 127.0.0.1:8017
```

## Query 5
```
python woolmilk/processing_node.py \
  --port 8017 \
  --forward-node 127.0.0.1:8027 \
  --query "WITH counts AS (SELECT B1.auction, COUNT(*) AS num FROM window_table AS B1 GROUP BY B1.auction), max_counts AS (SELECT MAX(num) AS max_num FROM counts) SELECT c.auction FROM counts C, max_counts M WHERE C.num = M.max_num" \
  --query-result-schema '{
    "fields": [
      {"name": "id", "type": "int64"}
    ]
  }' \
  --query-id 5
```
```
python woolmilk/source_node.py --stream nexmark.bid --processing-nodes 127.0.0.1:8017
```

## Query 7
```
python woolmilk/processing_node.py \
  --port 8017 \
  --forward-node 127.0.0.1:8027 \
  --query "SELECT B.auction, B.price, B.bidder FROM window_table B WHERE B.price = (SELECT MAX(B1.price) FROM window_table B1)" \
  --query-result-schema '{
    "fields": [
      {"name": "auction", "type": "int64"},
      {"name": "price", "type": "int64"},
      {"name": "bidder", "type": "int64"}
    ]
  }' \
  --query-id 7
```
```
python woolmilk/source_node.py --stream nexmark.bid --processing-nodes 127.0.0.1:8017
```
