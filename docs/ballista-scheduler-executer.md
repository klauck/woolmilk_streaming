# Ballista Scheduler & Executer

Ballista Scheduler is responsible for receiving the query from the client, creating the plan for the query, and sending the plan to the Ballista Executor.

## Query Example

Let's assume we have a simple query to execute on the Ballista.

```sql
SELECT customer.id, sum(order.amount) as total_amount
FROM customer JOIN order ON customer.id = order.customer_id
GROUP BY customer.id
```

The Ballista Scheduler will receive the query, create a plan for it, convert the query into multiple parts, and send it to the Ballista Executor Nodes.

The plan for this query is as follows:

```
Query Stage #4:
  Projection: #customer.id, #total_amount
    HashAggregate: groupBy=[customer.id], aggr=[MAX(max_fare) AS total_amount]
      Query Stage #3: repartition=[]
        HashAggregate: groupBy=[customer.id], aggr=[MAX(max_fare) AS total_amount]
          Join: condition=[customer.id = order.customer_id]
            Query Stage #1: repartition=[customer.id]
              Scan: customer
            Query Stage #2: repartition=[order.customer_id]
              Scan: order
```

### Stage 1 & Stage 2: Repartitioning

```
Run parallel and partition data
Query Stage #1: repartition=[customer.id]
  Scan: customer
Query Stage #2: repartition=[order.customer_id]
  Scan: order
```

These stages will run in parallel because they are independent of each other. When each executor node is done with the processing, they will send the result to the Ballista Scheduler.

### Stage 3: Join and Partial Aggregate

```
Query Stage #3: repartition=[]
  HashAggregate: groupBy=[customer.id], aggr=[MAX(max_fare) AS total_amount]
    Join: condition=[customer.id = order.customer_id]
      Query Stage #1
      Query Stage #2
```

In this stage, the Ballista Scheduler will join the results of the previous stages. This step can't be done in parallel because the data is dependent on each other, so it has to be done on one Executor node.

### Stage 4: Final Aggregation

```
Query Stage #4:
Projection: #customer.id, #total_amount
HashAggregate: groupBy=[customer.id], aggr=[MAX(max_fare) AS total_amount]
QueryStage #3
```

At the end, the Ballista Scheduler combines all results and sends them back to the client.

\*The example above is taken from the [Apache Ballista Architecture Guide](https://datafusion.apache.org/ballista/contributors-guide/architecture.html).

## Continue Reading

[Nexmark Queries Over Ballista]("nexmark-queries-over-ballista.md")
