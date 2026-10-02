# Spark internals walkthrough for this pipeline

> Stage counts below describe the *typical* plan for the default local configuration. Confirm them on your machine
> with `--explain true` and the Spark UI (http://localhost:4040 → Stages / SQL). Adaptive Query Execution can
> coalesce shuffle partitions or change join strategies at runtime.

## Vocabulary

| Term | Meaning | In this project |
|---|---|---|
| Transformation | Lazy; builds the lineage / logical plan | `map`, `flatMap`, `select`, `withColumn`, `filter`, `join`, `groupBy`, window |
| Action | Triggers a job | `collect`, `count`, `write.*`, `show` |
| Narrow dependency | Each output partition depends on one input partition; pipelined in a stage | `flatMap`, `map`, `select`, `filter`, broadcast-join probe side, `coalesce` |
| Wide dependency | Output partition needs data from many inputs → **shuffle** | `reduceByKey`, `groupBy`, window `partitionBy`, `repartition`, sort-merge join |
| Stage | Set of tasks that run without a shuffle in between | split at every `Exchange` |
| Task | One stage × one partition | |

## Component 1 - Clickstream (RDD / Pair RDD)

```
textFile ─► flatMap(parse + filter) ─► map(to (key,value)) ─► reduceByKey(HashPartitioner) ─► map ─► toDF ─► write
└──────────────── Stage 0 (ShuffleMapStage, narrow chain) ───────────────┘ │ shuffle │ └──── Stage 1 (ResultStage) ───┘
```

- **Transformations:** `textFile`, `flatMap`, `map` (narrow); `reduceByKey` (wide).
- **Action:** the Parquet write of the funnel (the earlier `collect()` of product ids is a separate tiny job).
- **Shuffle:** one, at `reduceByKey`. Partial sums are computed in Stage 0 (map-side combine), so only per-product
  partials cross the network.
- **Why not `groupByKey`?** It ships every raw event across the shuffle and only then sums them.
- **Why not `partitionBy(...).reduceByKey(...)`?** `partitionBy` would shuffle every raw record *before* combining.
- **Partitioning:** `textFile(path, n)` sets the minimum input partitions; `HashPartitioner(n)` fixes the output
  partitions - the same key always lands in the same partition.
- **Persistence:** `MEMORY_ONLY` for the funnel; if a partition is evicted it is recomputed from the text file.
- **Shared variables:** broadcast set (valid product ids) and `LongAccumulator` (rejected lines).
  Read the accumulator only after an action; transformation-side updates can double count on retries.

## Component 2 - Clean (DataFrame → Dataset)

- **Narrow:** `select` (trim, cast, normalise), `withColumn(reject_reason)`, `filter`.
- **Wide:** `row_number().over(Window.partitionBy("order_id")...)` → `Exchange hashpartitioning(order_id)` + sort.
- **Typed boundary:** `.as[Order]` turns the DataFrame into `Dataset[Order]`; mismatched column names/types fail
  at analysis time, not mid-job.
- **Actions:** two Parquet writes (valid, rejected) = two jobs, each one shuffle (two stages).
  Both re-run the cheap upstream plan from the raw Parquet.

## Component 3 - Enrich

- **Typed `map`** (`Order → OrderLine`): compile-time safe but opaque to Catalyst; rows are deserialised into
  objects. After `.toDF()` Catalyst optimises again.
- **Broadcast joins:** `BroadcastExchange` collects each small table (products, customers, funnel) to the driver and
  ships it to executors; the order side is joined in place (`BroadcastHashJoin`) - **no shuffle on orders**.
  The broadcast build steps show up as small separate jobs in the UI.
- **UDFs:** `order_size`, `mask_email`. Black boxes → no push-down/code-gen for their bodies.
- **Persistence:** `persist(MEMORY_AND_DISK)` then `count()` (action) to materialise. All later jobs read from the cache
  (Spark UI → Storage tab).
- **Output partitioning:** `repartition(order_date)` is a shuffle that co-locates each date; `partitionBy("order_date")`
  then writes one folder per date, typically with one file each.

## Component 4 - Aggregate

| Table | Wide operations | Notes |
|---|---|---|
| `daily_revenue_by_category` | `groupBy` (partial agg → Exchange → final agg) | `countDistinct` needs an extra aggregation step |
| `daily_revenue_trend` | `groupBy` + un-partitioned window (single partition) | frames: unbounded→current, last 7 rows, `lag` |
| `customer_timeline` | window `partitionBy(customer_id)` | input pre-partitioned by `customer_id` so the shuffle can be reused |
| `customer_summary` | `groupBy` + window `partitionBy(country)` + UDF | `dense_rank` keeps ties on the same rank |
| `top_products_per_category` | `groupBy` + window `partitionBy(category)` | `row_number <= 3` |
| `payment_mix` | `groupBy` + broadcast `crossJoin` of the 1-row grand total | |

Every one of these reads the **cached** enriched DataFrame; without `persist` each table would re-run the whole
read → clean → join lineage.

## What to look at in the UI

1. **Jobs / Stages:** one job per action; stage boundaries match the `Exchange` nodes above.
2. **SQL:** per-query plan with `BroadcastHashJoin`, `Exchange`, `Window`, `InMemoryTableScan` (cache hit).
3. **Storage:** the `MEMORY_AND_DISK` enriched DataFrame - size and fraction cached.
4. **Executors / Stages → shuffle read/write:** small numbers confirm the map-side combine and broadcast joins worked.
