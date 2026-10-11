# Databricks SQL – Complete Guide

## 1. Definition

### What is Databricks SQL?

Databricks SQL is a cloud data warehouse built on lakehouse architecture . It runs directly on your data lake, supports ANSI SQL with Delta Lake extensions, and provides the tools to build highly performant, cost-effective data warehouses without moving your data .

### Why was it created?

Databricks SQL was created to bring the power of a data warehouse directly to the data lake. Traditional data warehouses require copying data into proprietary formats, creating duplication and sync challenges. Databricks SQL eliminates this by querying data where it already lives .

**Key motivations:**
- **Eliminate data silos**: One copy of data for all workloads 
- **Reduce costs**: Avoid data duplication and replication 
- **Unified platform**: Support both AI and BI on the same data 
- **Simplify architecture**: Lakehouse as the single source of truth 

### What problem does it solve?

| Problem | Solution |
|---------|----------|
| Data movement between systems | Query data directly in the lake |
| Separate BI and AI platforms | Unified lakehouse for both |
| Proprietary formats | Open formats (Parquet, Delta) |
| Costly infrastructure | Serverless, pay-as-you-go |
| Performance tuning overhead | AI-powered automation  |

---

## 2. Internal Architecture

### Databricks SQL Architecture

```text
┌─────────────────────────────────────────────────────────────────────┐
│                      Control Plane                                 │
│  - SQL Editor                                                     │
│  - Dashboards                                                     │
│  - Alerts                                                         │
│  - Query Management                                               │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     SQL Warehouses                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   Serverless    │  │      Pro        │  │    Classic      │  │
│  │ - Instant start │  │ - Custom VPC    │  │ - Legacy        │  │
│  │ - Auto-scaling  │  │ - Predictable   │  │ - Self-managed  │  │
│  │ - AI optimized  │  │ - Isolated      │  │ - Limited use   │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Storage Layer                               │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                    Unity Catalog                           │  │
│  │         Catalogs → Schemas → Tables/Views/Metric Views    │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                              │                                    │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                    Data Lake                               │  │
│  │    Delta Lake tables in cloud storage (S3, ADLS, GCS)    │  │
│  └─────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### SQL Warehouse Architecture

A SQL warehouse is the compute resource that runs SQL queries .

**Three types of SQL warehouses :**

```text
┌─────────────────────────────────────────────────────────────────────┐
│                      SQL Warehouse Types                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                    SERVERLESS (Recommended)                 │  │
│  │  - Fully managed by Databricks                             │  │
│  │  - Instant startup (2-6 seconds)                           │  │
│  │  - Intelligent Workload Management                         │  │
│  │  - Automatic performance optimization                     │  │
│  │  - Lower TCO                                               │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                        PRO                                  │  │
│  │  - Customer-managed VPC support                            │  │
│  │  - Custom networking                                       │  │
│  │  - Used when serverless not available                     │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                       CLASSIC                               │  │
│  │  - Legacy model                                            │  │
│  │  - Basic interactive exploration                           │  │
│  │  - Limited features                                        │  │
│  └─────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Query Execution Flow

```text
User writes SQL query
          │
          ▼
    SQL Editor / Dashboard
          │
          ▼
    Select SQL Warehouse
          │
          ▼
    Query Parser & Validator
          │
          ▼
    Catalyst Optimizer
          │
          ▼
    Photon Engine (Vectorized)
          │
          ▼
    Delta Lake Access
          │
          ▼
    Result Display
```

### AI-Powered Performance Systems 

| System | Function |
|--------|----------|
| **Predictive Optimization** | Automatically handles clustering, file size adjustments, and vacuuming |
| **Predictive I/O** | "Indexless indexing" for scan performance |
| **Intelligent Workload Management** | ML models optimize resources for high-concurrency BI |
| **Liquid Clustering** | Auto-chooses clustering key, replaces partitioning and ZORDER |

---

## 3. Key Concepts

### SQL Warehouses

A SQL warehouse is a compute resource that lets you query and explore data on Databricks .

**Key Configuration Options:**

| Setting | Serverless | Pro/Classic |
|---------|------------|-------------|
| **Auto-stop default** | 10 minutes | 45 minutes |
| **Auto-stop minimum (UI)** | 5 minutes | 10 minutes |
| **Auto-stop minimum (API)** | 1 minute | 10 minutes |
| **Scaling recommendation** | 1 cluster per 10 concurrent queries | Same |
| **Channel** | Preview / Standard | Preview / Standard |

### Permissions

| Permission | Capabilities |
|------------|--------------|
| **CAN VIEW** | View warehouses, query history, and profiles. Cannot run queries  |
| **CAN USE** | Run queries on the warehouse  |
| **CAN MONITOR** | Run queries, view history and profiles  |
| **CAN MANAGE** | Full control including sizing and spend decisions  |

### SQL Editor 

**Features:**
- Autocomplete for schemas, tables, and columns
- Multi-tab editing
- Query saving and drafts (retained for 30 days)
- Tags and favorites
- Visualization creation
- Parameterized queries 

### Dashboards (AI/BI) 

**Two main tabs:**
- **Data tab**: Define datasets via SQL queries or Unity Catalog tables
- **Canvas tab**: Add visualizations, filters, text boxes 

**Dashboard components:**
| Component | Purpose |
|-----------|---------|
| **Dataset** | Underlying data from a query or table |
| **Visualization** | Chart, table, or other graphical representation |
| **Filter** | User-controlled data selection (dropdown, date range) |
| **Parameter** | Query-based value substitution  |
| **Text box** | Context or instructions |

### Alerts 

**Alert statuses:**
- `TRIGGERED`: Condition met
- `OK`: Condition not met
- `ERROR`: Evaluation failed

**Custom templates with variables :**
- `ALERT_STATUS`: Evaluated status
- `ALERT_NAME`: Alert name
- `ALERT_URL`: Alert page URL
- `QUERY_RESULT_VALUE`: Result value
- `ALERT_CONDITION`: Operator

### SQL Functions 

**Two function types:**
- **Scalar functions**: Return single value
- **Table functions**: Return set of rows

**Parameters:**
- Default values supported using `DEFAULT` keyword
- Named parameter invocation
- Python UDFs require Unity Catalog 

---

## 4. Real-World Use Cases

### Use Case 1: Self-Service Analytics Dashboard

**Business Context:** A retail company needs executives to track KPIs (sales, inventory, customer metrics) via interactive dashboards.

**Why AI/BI Dashboards:**
- Low-code interface 
- AI-assisted authoring speeds development
- Interactive filters empower self-service
- Unity Catalog governance ensures security 

**Alternative Approaches:**
- Building custom dashboards in code is slower
- Separate BI tools create additional costs
- Spreadsheets lack interactivity and freshness

**Expected Benefits:**
- Faster insights: 73% performance improvement 
- Democratized access to data
- Single source of truth

### Use Case 2: Data Quality Monitoring

**Business Context:** A financial services firm needs to monitor data quality in real-time and alert on anomalies.

**Why Alerts:**
- Automate query evaluation on schedule 
- Multiple delivery options (email, webhooks)
- Customizable thresholds and conditions

**Alternative Approaches:**
- Manual checks are error-prone and slow
- Custom monitoring requires significant development

**Expected Benefits:**
- Proactive issue detection
- Reduced manual monitoring effort
- SLA compliance

### Use Case 3: Building a Customer 360 View

**Business Context:** A company wants a unified view of customers from multiple systems.

**Why Metric Views :**
- Define consistent business metrics
- Reuse across queries and dashboards
- Governed by Unity Catalog

**Alternative Approaches:**
- Separate SQL logic in each query leads to inconsistency
- Hard-coded values require maintenance

**Expected Benefits:**
- Consistent definitions
- Reduced duplicate work
- Trusted metrics

---

## 5. Practical Examples

### Example 1: Creating and Using a SQL Warehouse

```python
# Note: SQL warehouses are created in the UI or via API.
# This example shows how to configure one programmatically.

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import WarehouseType, Channel

w = WorkspaceClient()

# Create a serverless SQL warehouse
warehouse = w.warehouses.create(
    name="analytics_team",
    warehouse_type=WarehouseType.SERVERLESS,
    cluster_size="2X-Large",
    auto_stop_mins=10,
    max_num_clusters=3,  # Support 30 concurrent queries
    channel=Channel(name="CHANNEL_NAME_CURRENT"),
)

print(f"Created warehouse: {warehouse.id}")

# Note: Servless SQL warehouses are the default when serverless is enabled 
```

### Example 2: Writing and Using SQL Queries with Parameters

```sql
-- Query with a parameter for dynamic filtering
-- The :segment parameter allows dashboard users to filter the data 

SELECT 
    c_custkey,
    c_name,
    c_mktsegment,
    c_acctbal
FROM 
    samples.tpch.customer
WHERE 
    c_mktsegment = :segment
ORDER BY 
    c_acctbal DESC;

-- Save this query as a dataset in a dashboard
-- The parameter will be populated by a filter widget 
```

### Example 3: Creating an Interactive Dashboard

```sql
-- Dataset 1: Main data for visualization
SELECT 
    dayofweek(tpep_pickup_datetime) as weekday,
    tpep_pickup_datetime,
    fare_amount,
    trip_distance,
    pickup_zip
FROM 
    samples.nyctaxi.trips
WHERE 
    trip_distance > 0 AND trip_distance < 10
    AND fare_amount > 0 AND fare_amount < 50;

-- Dataset 2: Dropdown choices for a filter
-- This populates the filter widget with distinct values 
SELECT DISTINCT 
    pickup_zip 
FROM 
    samples.nyctaxi.trips
WHERE 
    pickup_zip IS NOT NULL;
```

### Example 4: Creating a SQL Function

```sql
-- Create a scalar SQL function with default parameters 
CREATE OR REPLACE FUNCTION calculate_tax(
    amount DOUBLE,
    rate DOUBLE DEFAULT 0.10
)
RETURNS DOUBLE
LANGUAGE SQL
COMMENT 'Calculate tax based on amount and rate'
RETURN amount * rate;

-- Use the function
SELECT 
    fare_amount,
    calculate_tax(fare_amount) as default_tax,
    calculate_tax(fare_amount, 0.15) as high_tax
FROM 
    samples.nyctaxi.trips
LIMIT 10;

-- Create a table function 
CREATE OR REPLACE FUNCTION weekly_summary(start_date DATE)
RETURNS TABLE
LANGUAGE SQL
RETURN 
    SELECT 
        week_id,
        SUM(fare_amount) as total_fare
    FROM 
        taxi_trips
    WHERE 
        trip_date >= start_date
    GROUP BY 
        week_id;

-- Use a table function
SELECT * FROM weekly_summary('2024-01-01') 
WHERE total_fare > 1000;
```

### Example 5: Creating a SQL Alert

```sql
-- Alert query 
SELECT 
    to_date(tpep_pickup_datetime) as date,
    SUM(fare_amount) as amount
FROM 
    samples.nyctaxi.trips
GROUP BY 
    ALL
ORDER BY 
    1 DESC;

-- Alert configuration:
-- 1. Condition: Sum of amount > 4000
-- 2. Schedule: Every 5 minutes
-- 3. Notification: Send to team email
-- 4. Custom template:
-- Subject: Alert "{{ALERT_NAME}}" changed to {{ALERT_STATUS}}
-- Body: Value is {{QUERY_RESULT_VALUE}}
```

### Example 6: Query Optimization

```sql
-- ❌ INEFFICIENT: OR condition in JOIN forces nested loop join 
SELECT * 
FROM table1 t1
INNER JOIN table2 t2 
    ON t1.column1 = t2.column1 
    OR t1.column2 = t2.column2;

-- ✅ EFFICIENT: Use UNION with separate equi-joins 
SELECT * 
FROM table1 t1
INNER JOIN table2 t2 ON t1.column1 = t2.column1
UNION
SELECT * 
FROM table1 t1
INNER JOIN table2 t2 ON t1.column2 = t2.column2;

-- ❌ INEFFICIENT: TABLESAMPLE before filtering 
SELECT * FROM large_table
TABLESAMPLE (10 PERCENT)
WHERE date = '2024-01-01';

-- ✅ EFFICIENT: Filter before sampling 
WITH filtered_data AS (
    SELECT * FROM large_table WHERE date = '2024-01-01'
)
SELECT * FROM filtered_data
TABLESAMPLE (10 PERCENT);
```

---

## 6. Interview Questions with Answers

### Common Questions

**Q: What is a SQL warehouse and what types are available?**

**A:** A SQL warehouse is compute optimized for running SQL queries . Types:
- **Serverless** (recommended): Fully managed, instant startup, auto-scaling 
- **Pro**: For custom networking when serverless unavailable
- **Classic**: Legacy, limited use

**Q: What is the difference between a SQL warehouse and a cluster?**

**A:** SQL warehouses are specialized for SQL queries and BI workloads. Clusters (all-purpose) are for notebooks and general compute. SQL warehouses are optimized for SQL performance, support auto-scaling for concurrency, and are managed separately.

**Q: How do I make a dashboard interactive?**

**A:** Use filters. Create query-based parameters with `:parameter_name` syntax . Add filter widgets on the Canvas tab . Use a separate dataset to populate dropdown choices .

### Difficult Questions

**Q: How does Databricks SQL achieve 73% performance improvement?**

**A:** Through AI-powered systems :
- **Predictive Optimization**: Automatic clustering and maintenance
- **Predictive I/O**: Indexless indexing for scans
- **Intelligent Workload Management**: ML-driven resource allocation
- **Liquid Clustering**: Replaces manual ZORDER and partitioning 

**Q: What's the difference between a regular view and a metric view?**

**A:** A **regular view** is a saved query. A **metric view** is a governed semantic layer that defines business metrics with consistent calculations, reusable across queries and dashboards .

### Certification-Style Questions

**Q1: What is the recommended compute type for most BI and ETL SQL workloads?**
- A) Classic SQL warehouse
- B) Pro SQL warehouse
- C) Serverless SQL warehouse ✅
- D) All-purpose cluster

**Explanation:** Serverless SQL warehouses are recommended for most workloads .

**Q2: What is the default auto-stop time for a serverless SQL warehouse?**
- A) 5 minutes
- B) 10 minutes ✅
- C) 45 minutes
- D) 60 minutes

**Explanation:** Serverless warehouses default to 10 minutes; pro/classic default to 45 minutes .

**Q3: Which permission allows a user to run queries but NOT view query history?**
- A) CAN VIEW
- B) CAN USE ✅
- C) CAN MONITOR
- D) CAN MANAGE

**Explanation:** CAN USE only allows running queries .

**Q4: What is the maximum number of rows returned in the SQL editor?**
- A) 1,000
- B) 10,000
- C) 64,000 ✅
- D) Unlimited

**Explanation:** The maximum is 64,000 rows .

---

## 7. Common Mistakes

### Beginner Mistakes

**1. Forgetting to save queries**
```sql
-- Query drafts are retained for 30 days but not saved 
-- Always click Save to persist your query
```

**2. Using wrong identifier escaping**
```sql
-- ❌ WRONG
SELECT * FROM My Table; -- Error: spaces need escaping

-- ✅ CORRECT
SELECT * FROM `My Table`; -- Use backticks 
```

**3. Not using Limit 1000**
```sql
-- Default is 1000 rows with Limit 1000 selected 
-- Deselect to get up to 64,000 rows
-- Add LIMIT clause for custom row limits
```

### Production Mistakes

**1. OR condition in JOIN**
```sql
-- ❌ Slow: Forces nested loop join 
SELECT * FROM t1 JOIN t2 ON t1.a = t2.a OR t1.b = t2.b

-- ✅ Fast: Use UNION 
SELECT * FROM t1 JOIN t2 ON t1.a = t2.a
UNION
SELECT * FROM t1 JOIN t2 ON t1.b = t2.b
```

**2. Using TABLESAMPLE before filtering**
```sql
-- ❌ Slow: Full table scan 
SELECT * FROM large_table TABLESAMPLE (10 PERCENT) WHERE date = '2024-01-01'

-- ✅ Fast: Filter first 
WITH filtered AS (SELECT * FROM large_table WHERE date = '2024-01-01')
SELECT * FROM filtered TABLESAMPLE (10 PERCENT)
```

**3. Using preview channel for production**
```sql
-- ⚠️ Preview channel for testing only 
-- Never use preview in production
-- Indicate preview in warehouse name to prevent mistakes
```

### Certification Traps

- **Serverless auto-stop minimum in UI = 5 minutes**; API allows 1 minute 
- **Pro/classic auto-stop minimum = 10 minutes**; default = 45 minutes 
- **Default result limit = 1,000 rows**; maximum = 64,000 
- **CAN VIEW = view only**; CAN USE = run queries 
- **Alert statuses**: OK, TRIGGERED, ERROR (no UNKNOWN) 

---

## 8. Performance Considerations

### Best Practices

1. **Use serverless SQL warehouses** when available 
2. **Set appropriate auto-stop** to balance cost and performance
3. **Configure scaling** for expected concurrency (1 cluster per 10 queries)
4. **Use metric views** for consistent business logic 
5. **Optimize JOINs**: Use UNION for OR conditions 
6. **Apply filters before sampling** 
7. **Use preview channel for testing only** 

### Optimization Techniques

| Technique | When to Use | Benefit |
|-----------|-------------|---------|
| **Liquid Clustering** | New tables | Auto-data layout  |
| **Predictive I/O** | Selective scans | Indexless indexing  |
| **Intelligent Workload Management** | High concurrency | Auto-resources  |
| **Predictive Optimization** | All tables | Auto-maintenance  |

### Scalability Concerns

**Concurrency scaling :**
- Recommended: 1 cluster per 10 concurrent queries
- Configure min/max clusters based on expected usage

**Query limits:**
- Max rows returned: 64,000 
- Query drafts: retained for 30 days 
- Auto-stop minimums vary by warehouse type 

### Cost Considerations

| Warehouse Type | Cost Model | Best For |
|----------------|------------|----------|
| **Serverless** | Pay-per-use, auto-scaling | Production workloads |
| **Pro** | Pay-per-use | Custom networking |
| **Classic** | Legacy | Limited use |

**Cost optimization:**
- Set appropriate auto-stop (5-10 minutes for serverless) 
- Use serverless for better TCO 
- Monitor usage in Query History

---

## 9. Comparison Tables

### SQL Warehouse Types

| Feature | Serverless | Pro | Classic |
|---------|------------|-----|---------|
| **Management** | Databricks | Customer | Customer |
| **Startup** | Instant (2-6s) | Moderate | Moderate |
| **Scaling** | Auto | Manual | Manual |
| **Custom VPC** | No | Yes | No |
| **Recommendation** | ✅ For most workloads | When needed | ❌ Avoid |

### SQL Warehouse vs All-Purpose Cluster

| Aspect | SQL Warehouse | All-Purpose Cluster |
|--------|---------------|---------------------|
| **Purpose** | SQL queries | Notebooks |
| **Optimization** | SQL | General |
| **Autostart** | Yes | Manual |
| **Auto-scaling** | Yes (serverless) | Yes |

### Alert vs Dashboard vs Query

| Feature | Alert | Dashboard | Query |
|---------|-------|-----------|-------|
| **Purpose** | Monitor conditions | Visualize insights | Run SQL |
| **Schedule** | Yes | No | No |
| **Notifications** | Yes | No | No |
| **Interactive** | No | Yes | Yes |

---

## 10. Exam Notes

### Things the Certification Exam Likes to Ask

1. **Serverless SQL warehouse benefits** 
2. **Auto-stop defaults and minimums** 
3. **Scaling recommendations** (1 cluster per 10 queries)
4. **Permission levels** (CAN USE vs CAN MONITOR) 
5. **Query result limits** (64,000 max) 
6. **SQL function syntax** 
7. **Dashboard filter configuration** 

### Common Traps

| Trap | Truth |
|------|-------|
| Serverless auto-stop min is 1 minute | 5 minutes in UI, 1 via API  |
| Pro/classic auto-stop min is 5 minutes | Minimum is 10 minutes  |
| Serverless default is 45 minutes | Default is 10 minutes  |
| CAN VIEW allows running queries | CAN VIEW is view-only  |
| Unlimited query results | 64,000 rows max  |

### Important Facts to Memorize

| Fact | Detail |
|------|--------|
| **SQL warehouse default** | Serverless  |
| **Serverless auto-stop** | 10 min default, 5 min UI minimum  |
| **Pro/classic auto-stop** | 45 min default, 10 min minimum  |
| **Query draft retention** | 30 days  |
| **Max query rows** | 64,000  |
| **AI/BI dashboards** | Low-code, AI-assisted  |
| **Metric views** | Semantic layer for metrics  |

### Scenario-Based Questions

1. **You need to support 30 concurrent users. How many clusters should you configure for your SQL warehouse?**
   - 3 clusters (1 per 10 concurrent queries)

2. **Your dashboard filter needs to show values from a list. How do you populate it?**
   - Create a separate dataset with DISTINCT values 

3. **Your JOIN query with OR is slow. What do you do?**
   - Rewrite with UNION 

4. **You need a metric used across many dashboards to be consistent.**
   - Use Unity Catalog metric views 

5. **You need to monitor a business metric and get notified automatically.**
   - Create a SQL alert 

---

## Summary Cheat Sheet

```text
┌─────────────────────────────────────────────────────────────────────┐
│                     DATABRICKS SQL CHEAT SHEET                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                   │
│  SQL WAREHOUSES:                                                  │
│  - Serverless: Recommended, instant, auto-scaling    │
│  - Pro: Custom VPC support                                       │
│  - Classic: Legacy, avoid                                        │
│                                                                   │
│  AUTO-STOP:                                                      │
│  ┌─────────────┬───────────┬─────────────┐                      │
│  │ Type        │ Default   │ Minimum (UI)│                      │
│  ├─────────────┼───────────┼─────────────┤                      │
│  │ Serverless  │ 10 min    │ 5 min      │                      │
│  │ Pro/Classic │ 45 min    │ 10 min     │         │
│  └─────────────┴───────────┴─────────────┘                      │
│                                                                   │
│  PERMISSIONS:                                                    │
│  - CAN VIEW: View only                                          │
│  - CAN USE: Run queries                                         │
│  - CAN MONITOR: Run + monitor                                   │
│  - CAN MANAGE: Full control                        │
│                                                                   │
│  QUERY LIMITS:                                                   │
│  - Default rows: 1,000                               │
│  - Max rows: 64,000                                  │
│  - Draft retention: 30 days                          │
│                                                                   │
│  OPTIMIZATION:                                                   │
│  - OR in JOIN → Use UNION                            │
│  - TABLESAMPLE → Filter first                       │
│  - Use serverless for auto-performance               │
│                                                                   │
└─────────────────────────────────────────────────────────────────────┘
```