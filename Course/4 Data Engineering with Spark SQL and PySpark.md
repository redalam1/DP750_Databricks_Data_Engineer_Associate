# Data Engineering with Spark SQL and PySpark

## 1. DataFrames

### Definition

A DataFrame is a distributed collection of data organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in Python/Pandas, but with richer optimizations under the hood . DataFrames can be constructed from a wide array of sources, including structured data files, tables in Hive, external databases, or existing RDDs .

**Why was it created?**

DataFrames were introduced to address the limitations of RDDs by providing:
- A higher-level abstraction for structured data
- Catalyst optimizer integration for query optimization
- A more user-friendly API than RDDs
- Better performance through Tungsten optimizations

**What problem does it solve?**

DataFrames solve the problem of processing structured data efficiently and expressively. They provide:
- Schema awareness for type safety
- Built-in optimizations via Catalyst
- Automatic data skipping and predicate pushdown
- Seamless integration with Spark SQL

### Internal Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                      User Code                            │
│  - DataFrame API calls (Python, Scala, Java, R)          │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Catalyst Optimizer                      │
│  - Logical Plan: Parse & Resolve                          │
│  - Optimization: Predicate pushdown, column pruning       │
│  - Physical Plan: Cost-based selection                    │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Tungsten Engine                         │
│  - Whole-stage code generation                            │
│  - Cache-aware execution                                 │
│  - Off-heap memory management                            │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      Execution                            │
│  - DAG Scheduler → Stages → Tasks                         │
│  - Executors process data                                 │
│  - Results returned to driver                             │
└─────────────────────────────────────────────────────────────┘
```

### Key Concepts

**Vocabulary:**
- **Row**: A record in a DataFrame
- **Column**: A named field with a data type
- **Schema**: The structure of the DataFrame
- **Transformations**: Lazy operations (filter, select, join)
- **Actions**: Eager operations (count, collect, show)
- **Partition**: Logical chunk of data for parallel processing

**Advantages:**
- Built-in optimizations through Catalyst
- Better performance than RDDs
- SQL-like operations
- Type safety at compile time
- Seamless integration with Spark SQL

**Limitations:**
- Less control than RDDs
- Schema required for some operations
- Serialization overhead for Python UDFs

### Real-World Use Cases

**Use Case 1: Data Transformation Pipeline**

*Business Context:* A financial services company needs to transform daily transaction data for reporting.

*Why DataFrames:* The DataFrame API provides declarative operations that are optimized by Catalyst, making transformations efficient and readable.

*Expected Benefits:* Reduced development time, better performance, easier maintenance.

**Use Case 2: Interactive Data Analysis**

*Business Context:* A data analyst needs to explore a large sales dataset interactively.

*Why DataFrames:* DataFrames allow for intuitive, expressive data manipulation with automatic optimization.

*Expected Benefits:* Fast response times, immediate feedback.

### Practical Examples

**Creating a DataFrame**

```python
# Create from a list of tuples
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
df = spark.createDataFrame(data, ["name", "age"])

# Read from a CSV file
df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/path/to/file.csv")

# Read from a JSON file
df = spark.read.json("/path/to/file.json")
```

**Basic DataFrame Operations**

```python
# Select specific columns
df.select("name", "age").show()

# Filter rows
df.filter(df.age > 30).show()

# Add a new column
df.withColumn("age_plus_10", df.age + 10).show()

# Rename a column
df.withColumnRenamed("age", "employee_age").show()

# Drop a column
df.drop("age").show()
```

### Common Mistakes

1. **Forgetting that DataFrames are lazy** - Operations don't execute until an action is called
2. **Using `.collect()` on large DataFrames** - This brings all data to the driver, causing OOM
3. **Not providing a schema for performance** - Schema inference costs time

### Performance Considerations

- Always provide explicit schemas when possible
- Use `select()` to reduce columns before operations
- Filter early to reduce data volume
- Use caching for reused DataFrames
- Use broadcast joins for small tables

### Comparison Table

| Feature | RDD | DataFrame | Dataset |
|---------|-----|-----------|---------|
| API Level | Low | High | High |
| Schema | None | Inferred/Provided | Provided |
| Catalyst Optimizer | No | Yes | Yes |
| Code Generation | No | Yes | Yes |
| Type Safety | At runtime | At runtime | Compile-time |
| Python Performance | Poor | Good | Good |

### Interview Questions

1. **What is the difference between RDD and DataFrame?**
   - RDDs are low-level, schema-less, and lack optimizations. DataFrames are high-level, schema-aware, and optimized by Catalyst.

2. **Why are DataFrames faster than RDDs?**
   - DataFrames use Catalyst optimizations and Tungsten code generation

3. **What is the role of SparkSession?**
   - It is the entry point for creating DataFrames and executing SQL queries.

### Exam Notes

- DataFrames are built on top of RDDs but with optimizations
- They are immutable and lazy
- They support SQL-like operations
- Catalyst Optimizer improves performance
- Tungsten engine handles memory management and code generation