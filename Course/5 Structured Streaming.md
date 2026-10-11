# Structured Streaming – Complete Guide

## 1. Definition

### What is Structured Streaming?

Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine . It allows you to express streaming computations the same way you would express batch computations on static data . The Spark SQL engine runs the computation incrementally and continuously, updating the final result as streaming data arrives.

**Core Philosophy:** Treat a live data stream as an unbounded table that is continuously appended. This enables you to use the same DataFrame/Dataset API for both batch and streaming.

### Why was it created?

Before Structured Streaming, Spark Streaming (DStreams) was the primary API, but it had several limitations:

- **Complexity**: DStreams required reasoning about micro-batches and state management separately
- **Fault-tolerance**: Developers had to manually handle recovery logic
- **Consistency**: Achieving exactly-once semantics was difficult
- **API divergence**: Batch and streaming had different APIs

Structured Streaming was created to solve these problems by building stream processing on top of Spark SQL, providing a unified API for batch and streaming .

### What problem does it solve?

1. **Unified API**: Same DataFrame API for batch and streaming
2. **Exactly-once guarantees**: End-to-end exactly-once fault-tolerance 
3. **Simplified development**: Express queries as you would on static data
4. **Event-time processing**: Native handling of event-time and late data 
5. **State management**: Automatic state handling for aggregations and joins
6. **Integration**: Works with Delta Lake, Kafka, Event Hubs, file sources, and more 

---

## 2. Internal Architecture

### The Unbounded Table Model

Structured Streaming treats streaming data as an unbounded table:

```text
┌─────────────────────────────────────────────────────────────────────┐
│                          Input Table                               │
│                      (Unbounded, append-only)                      │
│                                                                   │
│   New rows are appended continuously as data arrives              │
│   ┌─────┐                                                         │
│   │ 1: A │                                                         │
│   │ 2: B │   Trigger     ┌─────────────────────────────────┐      │
│   │ 3: C │  ──────────► │        Result Table              │      │
│   │ 4: D │              │  (Incrementally updated)         │      │
│   │ 5: E │              │                                 │      │
│   └─────┘              └─────────────────────────────────┘      │
│                                                                   │
└─────────────────────────────────────────────────────────────────────┘
```

### Execution Flow

```text
Streaming Source (Kafka, Files, Event Hubs, etc.)
           │
           ▼
    spark.readStream()
           │
           ▼
    Streaming DataFrame (Unbounded Table)
           │
           ▼
    Transformations (filter, select, groupBy, join, window)
           │
           ▼
    Spark SQL Engine (Catalyst Optimizer)
           │
           ▼
    Micro-batch Execution (or Continuous)
           │
           ▼
    spark.writeStream()
           │
           ▼
    Output Sink (Delta, Console, Kafka, etc.)
           │
           ▼
    Checkpoint (Fault-tolerance, State Recovery)
```

### Micro-batch Processing

**Default mode**: The default processing mode uses micro-batches :

```text
Trigger Interval
     │
     ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Micro-batch │────►│  Micro-batch │────►│  Micro-batch │────► ...
│     #1       │     │     #2       │     │     #3       │
└──────────────┘     └──────────────┘     └──────────────┘
     │                     │                     │
     ▼                     ▼                     ▼
Processes new data    Processes next batch   Processes latest
since last batch      since last batch       since last batch
```

**Characteristics**:
- Latency: As low as 100ms 
- Fault-tolerance: Exactly-once
- Default behavior: Processes as fast as possible

### Continuous Processing (Alternative Mode)

Since Spark 2.3, Continuous Processing provides lower latency :

```text
┌─────────────────────────────────────────────────────────────────────┐
│                      Continuous Processing                         │
│  End-to-end latency as low as 1ms, at-least-once guarantees      │
│                                                                   │
│  Executor continuously processes records as they arrive          │
└─────────────────────────────────────────────────────────────────────┘
```

**Characteristics**:
- Latency: As low as 1ms 
- Fault-tolerance: At-least-once
- Use when: Ultra-low latency is required

### Components Involved

| Component | Role |
|-----------|------|
| **Source** | Provides the streaming data (Kafka, files, Event Hubs)  |
| **SparkSession** | Entry point for streaming operations |
| **Streaming DataFrame** | Represents the unbounded table |
| **Catalyst Optimizer** | Optimizes the query plan |
| **Micro-batch Engine** | Processes data in small batches |
| **Checkpoint** | Stores progress and state for fault-tolerance  |
| **Sink** | Output destination (Delta, Console, Kafka) |
| **State Store** | Maintains state for stateful operations |

---

## 3. Key Concepts

### Vocabulary

| Term | Definition |
|------|------------|
| **Unbounded Table** | A table that continuously grows as new data arrives  |
| **Streaming DataFrame** | DataFrame created with `spark.readStream()` |
| **Trigger** | Controls when data is processed  |
| **Checkpoint** | Stores processing progress for fault-tolerance  |
| **Watermark** | Defines how late data can be before being dropped  |
| **Output Mode** | Defines what gets written to the sink  |
| **Stateful Operation** | Maintains state across batches (aggregations, joins)  |
| **Stateless Operation** | Processes each row independently (filter, map) |
| **Exactly-once** | Each record is processed exactly once  |
| **Micro-batch** | Small batch processing interval |

### Trigger Types

| Trigger | Behavior | Use Case | Status |
|---------|----------|----------|--------|
| **Default (No trigger)** | Processes as fast as possible, new batch when previous completes  | Low-latency streaming | ✅ Current |
| **processingTime="10 seconds"** | Fixed interval processing  | Controlled throughput | ✅ Current |
| **availableNow=True** | Processes all available data, then stops  | Scheduled batch ingestion | ✅ Current |
| **once=True** | Processes one micro-batch, then stops | Deprecated; use availableNow instead | ⚠️ Deprecated  |

### Output Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Append** | Only new rows appended since last trigger  | Most ingestion workloads |
| **Complete** | Entire updated result table is written  | Aggregations (small result sets) |
| **Update** | Only rows updated since last trigger  | Aggregations (large result sets) |

**When to use each mode:**

```text
Append Mode:
  - Simple projections: SELECT col1, col2 FROM stream
  - Filters: SELECT * FROM stream WHERE temperature > 100
  - Not for aggregations where rows can change

Complete Mode:
  - Global aggregations: SELECT COUNT(*) FROM stream
  - Grouped aggregations with small number of groups
  - Use when you need the entire result set

Update Mode:
  - Aggregations with many groups
  - When you only need the changed rows
  - More efficient than Complete for large result sets
```

### Limitations

1. **State size**: Stateful operations can grow unbounded without watermarks
2. **Stream-stream joins**: Require watermarks and time constraints 
3. **Sink idempotency**: Exactly-once requires idempotent sinks 
4. **Checkpoint compatibility**: Changes in query structure may break checkpoint recovery 
5. **latency**: Micro-batch default has ~100ms latency (not sub-millisecond)

---

## 4. Real-World Use Cases

### Use Case 1: IoT Sensor Data Ingestion

**Business Context**: A manufacturing company needs to collect sensor readings from thousands of IoT devices and store them in a Delta Lake for real-time monitoring and historical analysis.

**Why Structured Streaming**:
- Processes each sensor reading within seconds of arrival 
- Auto Loader handles file ingestion for devices that write to cloud storage 
- Checkpoints ensure exactly-once processing if the job fails 

**Alternative Approaches**:
- Batch ingestion would introduce hours of latency
- Manual file processing would be error-prone and not scalable

**Expected Benefits**:
- Near-real-time visibility into manufacturing operations
- Reliable, exactly-once data ingestion
- Unified platform for real-time and batch analytics

### Use Case 2: Clickstream Analytics

**Business Context**: An e-commerce company needs to analyze user behavior on their website in real-time to personalize recommendations and detect issues.

**Why Structured Streaming**:
- Kafka connector reads click events as they occur 
- Stateful aggregations track user sessions in real-time
- Watermarks handle out-of-order events from different time zones

**Alternative Approaches**:
- Batch processing would delay personalization
- Custom streaming solutions would require more development effort

**Expected Benefits**:
- Real-time personalization
- Immediate detection of website issues
- Accurate session analysis even with out-of-order events

### Use Case 3: Financial Transaction Monitoring

**Business Context**: A bank needs to detect fraudulent transactions within seconds of occurrence.

**Why Structured Streaming**:
- Event Hubs receives transactions as they occur
- Streaming joins enrich transactions with customer profiles
- Alerts generated in near-real-time

**Alternative Approaches**:
- Batch fraud detection would miss the window for prevention
- Custom streaming would be complex to build and maintain

**Expected Benefits**:
- Fraud detection within seconds
- Regulatory compliance
- Reduced financial losses

---

## 5. Practical Examples

### Example 1: Reading from Kafka

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("Kafka Stream") \
    .getOrCreate()

# Define schema for incoming JSON data
schema = StructType() \
    .add("device_id", StringType()) \
    .add("temperature", DoubleType()) \
    .add("timestamp", StringType())

# Read from Kafka
df_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "sensor-readings")
    .option("startingOffsets", "latest")
    .load())

# Parse the JSON payload
# Kafka returns `value` as binary data
parsed_df = (df_stream
    .select(col("value").cast("string").alias("json_value"))
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*"))

# Display streaming data to console for debugging
query = (parsed_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="5 seconds")
    .start())

query.awaitTermination()
```

**Code Explanation:**
- `spark.readStream`: Creates a streaming DataFrame
- `format("kafka")`: Uses the Kafka connector
- `option()`: Configures Kafka connection parameters
- `value.cast("string")`: Casts binary data to string
- `from_json()`: Parses JSON data according to schema
- `writeStream`: Writes streaming data to a sink
- `outputMode("append")`: Only outputs new rows
- `trigger(processingTime="5 seconds")`: Processes every 5 seconds

### Example 2: Writing to Delta Lake

```python
from pyspark.sql.functions import col, current_timestamp

# Read from Event Hubs (native connector)
events = (spark.readStream
    .format("eventhubs")
    .option("eventhubs.connectionString", connection_string)
    .load())

# Extract and parse the event payload
payload = (events
    .selectExpr("CAST(body AS STRING) AS payload")
    .select(from_json(col("payload"), schema).alias("data"))
    .select("data.*")
    .withColumn("ingestion_time", current_timestamp()))

# Write to Delta Lake table with checkpoint
checkpoint_path = "/Volumes/bronze/checkpoints/iot"

query = (payload.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("bronze.iot.sensor_readings"))

print(f"Stream active: {query.isActive}")

# Wait for the stream to finish (availableNow will stop automatically)
query.awaitTermination()
```

**Code Explanation:**
- `format("eventhubs")`: Uses the native Event Hubs connector 
- `selectExpr("CAST(body AS STRING) AS payload")`: Extracts event payload
- `option("checkpointLocation")`: Enables fault-tolerance 
- `trigger(availableNow=True)`: Processes all available data and stops 
- `toTable()`: Writes to a Unity Catalog table
- `awaitTermination()`: Blocks until the stream completes

### Example 3: Stateful Aggregation with Watermark

```python
from pyspark.sql.functions import window, col, avg, to_timestamp

# Read streaming data
df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "sensor-readings")
    .load())

# Parse and add event-time column
parsed_df = (df
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp"))))

# Define watermark to handle late data (10 minutes)
# This tells Spark to drop data older than 10 minutes from the latest event
watermarked_df = parsed_df.withWatermark("event_time", "10 minutes")

# Perform windowed aggregation
# Tumbling window: 5-minute windows, 5-minute slide
aggregated_df = (watermarked_df
    .groupBy(
        window(col("event_time"), "5 minutes", "5 minutes"),
        col("device_id")
    )
    .agg(avg("temperature").alias("avg_temperature")))

# Write to Delta table
query = (aggregated_df.writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", "/checkpoints/windowed-agg")
    .trigger(processingTime="1 minute")
    .toTable("silver.iot.device_avg_temperature"))

query.awaitTermination()
```

**Code Explanation:**
- `withWatermark("event_time", "10 minutes")`: Defines lateness threshold 
- `window(col("event_time"), "5 minutes", "5 minutes")`: Tumbling window
- `groupBy()`: Groups by window and device_id
- `avg("temperature")`: Calculates average per window
- `outputMode("append")`: Outputs finalized windows

---

## 6. Interview Questions with Answers

### Common Questions

**Q: What is the difference between Spark Streaming (DStreams) and Structured Streaming?**

**A:** Structured Streaming is the modern streaming engine built on Spark SQL. Key differences:
- Structured Streaming uses the DataFrame API; DStreams use RDD API
- Structured Streaming provides exactly-once semantics; DStreams provide at-least-once
- Structured Streaming supports event-time processing and watermarks
- Structured Streaming is recommended for new applications; DStreams is in maintenance mode 

---

**Q: What is a watermark and why is it important?**

**A:** A watermark defines the maximum allowed lateness for data in a stateful operation . It tells the system how long to wait for late-arriving data before dropping it and cleaning up old state. This prevents unbounded state growth and enables proper handling of out-of-order events.

---

**Q: Explain the difference between Append, Update, and Complete output modes.**

**A:** 
- **Append**: Outputs only new rows since the last trigger. Used for queries where existing rows don't change (e.g., simple projections).
- **Update**: Outputs only rows that were updated since the last trigger. Used for aggregations where results change.
- **Complete**: Outputs the entire result table. Used for global aggregations or when the sink needs the full dataset.

---

### Difficult Questions

**Q: How does Structured Streaming achieve exactly-once semantics?**

**A:**  Structured Streaming achieves exactly-once through:
1. **Checkpointing**: Stores offsets and progress metadata
2. **Replayable sources**: Sources like Kafka allow replaying data
3. **Idempotent sinks**: Sinks that can handle duplicate writes safely
4. **Write-Ahead Logs**: Tracks data being processed

When a failure occurs, the system restores state from the checkpoint and replays any uncommitted data.

---

**Q: What happens to a streaming query if the checkpoint directory is deleted?**

**A:** Deleting the checkpoint directory causes the query to lose its progress state and any stateful operation state. The query would restart from the beginning (if the source allows it) and:
- Lose exactly-once guarantees
- Potentially reprocess data already processed
- For stateful operations, lose all accumulated state

This can lead to data duplication or loss. Checkpoints should be treated as critical infrastructure .

---

### Certification-Style Questions

**Q1: A data engineer is building a streaming pipeline that reads from Kafka and writes to Delta Lake. They want the pipeline to process all available data and then stop. Which trigger should they use?**

- A) `trigger(processingTime="10 seconds")`
- B) `trigger(once=True)`
- C) `trigger(availableNow=True)` ✅
- D) No trigger (default)

**Explanation:** `availableNow` processes all currently available data and then terminates . `once` is deprecated.

---

**Q2: What is the recommended output mode for a streaming query that performs a simple projection (selecting columns) without aggregation?**

- A) Complete mode
- B) Update mode
- C) Append mode ✅
- D) All modes are valid

**Explanation:** For simple projections where existing rows don't change, Append mode is most efficient .

---

**Q3: Which of the following is NOT required for exactly-once processing in Structured Streaming?**

- A) Checkpoint location
- B) Idempotent sink
- C) Replayable source
- D) Processing time trigger ✅

**Explanation:** Processing time triggers control latency but are not required for exactly-once semantics .

---

## 7. Common Mistakes

### Beginner Mistakes

**1. Forgetting to set a checkpoint location for stateful operations**
```python
# ❌ WRONG - No checkpoint for stateful aggregation
agg_df = grouped_df.writeStream \
    .outputMode("update") \
    .format("delta") \
    .start("/path/table")  # No checkpoint!

# ✅ CORRECT - Always set checkpoint for stateful operations
agg_df = grouped_df.writeStream \
    .outputMode("update") \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/my-stream") \
    .start("/path/table")
```

**2. Using `df.show()` on a streaming DataFrame**
```python
# ❌ WRONG - show() doesn't work on streaming DataFrames
df_stream.show()  # Error!

# ✅ CORRECT - Use writeStream to console
df_stream.writeStream.format("console").start()
```

**3. Confusing event-time with processing-time**

Processing-time uses the system clock; event-time uses the timestamp in the data. For accurate analytics, always prefer event-time with watermarks.

### Production Mistakes

**1. Sharing checkpoints between different queries** 
```python
# ❌ WRONG - Same checkpoint location for different queries
query1 = df.writeStream.option("checkpointLocation", "/checkpoints/my-stream").start()
query2 = df2.writeStream.option("checkpointLocation", "/checkpoints/my-stream").start()

# ✅ CORRECT - Each query gets its own unique checkpoint
query1 = df.writeStream.option("checkpointLocation", "/checkpoints/query1").start()
query2 = df2.writeStream.option("checkpointLocation", "/checkpoints/query2").start()
```

**2. Not handling late data in stateful operations**

Without watermarks, state can grow unbounded. Always use watermarks for stateful operations .

**3. Ignoring sink idempotency requirements**

Not all sinks are idempotent. For exactly-once, ensure your sink can handle duplicate writes.

### Certification Traps

- **Append mode cannot be used with aggregations** (unless watermarks finalize windows)
- **Complete mode can cause memory issues** for large result sets
- **Update mode is more efficient** than Complete for grouped aggregations
- **`once` trigger is deprecated**; use `availableNow` instead 

---

## 8. Performance Considerations

### Best Practices

1. **Use appropriate triggers**
   - For near-real-time: Default trigger (as fast as possible)
   - For controlled throughput: `processingTime`
   - For scheduled batch: `availableNow` 

2. **Set watermarks for stateful operations**
   - Prevents unbounded state growth 
   - Enables cleanup of old state

3. **Use `maxFilesPerTrigger` for file sources**
   - Controls micro-batch size
   - Prevents overwhelming the system

4. **Choose the right output mode**
   - Append for ingestion
   - Update for large aggregations
   - Complete for small aggregation result sets

5. **Optimize partitioning**
   - Partition by date in Bronze layer
   - Avoid too many small partitions

### Optimization Techniques

| Technique | When to Use | Benefit |
|-----------|-------------|---------|
| **Watermark** | Stateful aggregations | Controls state size  |
| **`maxFilesPerTrigger`** | File source ingestion | Controls batch size |
| **`spark.sql.shuffle.partitions`** | Shuffles | Manages parallelism |
| **`awaitTermination()`** | Monitoring | Enables graceful shutdown |
| **Checkpoint location** | All production queries | Enables fault-tolerance  |

### Scalability Concerns

**State Size Growth:**
```text
Without watermark: State grows indefinitely as new keys arrive
With watermark: Old state is discarded after watermark threshold
```

**Recommendation:** Always use watermarks for stateful operations .

**Checkpoint Performance:**
- Checkpoint writes are synchronous
- Use high-throughput storage for checkpoints
- Keep checkpoints on fast storage (SSD-backed)

### Cost Considerations

1. **Serverless compute**: Auto-scales; costs scale with usage
2. **Checkpoint storage**: Costs depend on state size and retention
3. **Source and sink costs**: Kafka reads, Delta writes have associated costs
4. **Trigger interval**: Frequent triggers increase compute costs

---

## 9. Comparison Tables

### Structured Streaming vs Batch Processing

| Aspect | Batch | Structured Streaming |
|--------|-------|---------------------|
| API | `spark.read` | `spark.readStream` |
| Data representation | Static DataFrame | Unbounded table |
| Execution | Once | Continuous/incremental |
| Latency | Hours to days | 100ms - seconds |
| Output method | `.write` | `.writeStream` |
| Checkpoint | Not required | Required for recovery  |
| Fault-tolerance | Manual | Automatic exactly-once |

### Trigger Types Comparison

| Trigger | Latency | Cost | Use Case |
|---------|---------|------|----------|
| Default | ~100ms | Higher | Continuous ingestion |
| processingTime | Fixed interval | Moderate | Controlled throughput |
| availableNow | On-demand | Low | Scheduled jobs  |

### Output Modes Comparison

| Mode | Output | State Required | Used For |
|------|--------|----------------|----------|
| Append | New rows only | No | Simple transformations |
| Update | Changed rows only | Yes | Aggregations  |
| Complete | Full result set | Yes | Small aggregations  |

---

## 10. Exam Notes

### Things the Certification Exam Likes to Ask

1. **`readStream` vs `read`**: Streaming uses `readStream`, batch uses `read`
2. **Checkpoint importance**: Required for fault-tolerance and exactly-once 
3. **Output mode selection**: Match mode to query type
4. **Watermark purpose**: State management and late data handling 
5. **`availableNow` trigger**: Replaces deprecated `once` trigger 
6. **Event-time vs processing-time**: Event-time uses data timestamps

### Common Traps

1. **Forgetting checkpoint location**: Required for stateful operations
2. **Sharing checkpoint directories**: Each query needs its own 
3. **Using Append mode with aggregations**: Won't work unless watermark finalizes windows
4. **Using Complete mode for large datasets**: Can cause memory issues
5. **Using deprecated `once` trigger**: Use `availableNow` instead 

### Important Facts to Memorize

| Fact | Detail |
|------|--------|
| **Default trigger** | Processes as fast as possible  |
| **Default latency** | ~100ms  |
| **Exactly-once** | Requires checkpoint and idempotent sinks  |
| **Watermark** | Tells system how late data can be  |
| **State cleanup** | Controlled by watermark threshold  |
| **Checkpoint requirement** | Each query must have its own checkpoint  |

### Scenario-Based Questions

1. **You need to ingest streaming data into Bronze layer with exactly-once guarantees. What must you configure?**
   - Checkpoint location 

2. **Your streaming aggregation state is growing too large. What feature should you use?**
   - Watermark 

3. **You need to process all available data and stop. Which trigger should you use?**
   - `availableNow` 

---

## Summary Cheat Sheet

```text
┌─────────────────────────────────────────────────────────────────────┐
│                   STRUCTURED STREAMING CHEAT SHEET                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                   │
│  READ:                                                            │
│  spark.readStream.format("kafka").option("...").load()            │
│                                                                   │
│  WRITE:                                                           │
│  df.writeStream.format("delta")                                   │
│    .option("checkpointLocation", "/checkpoint")                   │
│    .outputMode("append")                                          │
│    .trigger(processingTime="10 seconds")                         │
│    .toTable("catalog.schema.table")                              │
│                                                                   │
│  TRIGGERS:                                                        │
│  - Default: Processes as fast as possible                        │
│  - processingTime: Fixed interval                                 │
│  - availableNow: Process all, then stop ✅                       │
│  - once: Deprecated ❌                                            │
│                                                                   │
│  OUTPUT MODES:                                                    │
│  - Append: New rows only                                         │
│  - Update: Changed rows only                                     │
│  - Complete: Full result table                                   │
│                                                                   │
│  STATE MANAGEMENT:                                                │
│  - Use watermarks for stateful operations                       │
│  - Always set checkpoint location                                │
│  - Each query needs unique checkpoint                            │
│                                                                   │
└─────────────────────────────────────────────────────────────────────┘
```