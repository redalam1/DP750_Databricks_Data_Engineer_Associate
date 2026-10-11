# Databricks Lakehouse Architecture

## 1. Definition

### What is a Lakehouse?

A lakehouse is a data management system that combines the benefits of data lakes and data warehouses . It provides scalable storage and processing capabilities while avoiding the need for isolated systems for different workloads like ML and BI .

### Why was it created?

- **Traditional data warehouses** optimize queries for BI but rely on proprietary formats and limit ML support
- **Data lakes** store data cheaply but lack the reliability and performance needed for BI reporting
- **The lakehouse** solves this by providing a unified platform that eliminates data silos traditionally separating data engineering, analytics, BI, data science, and machine learning 

### What problem does it solve?

- Eliminates data silos by unifying data warehousing and AI use cases on a single platform 
- Reduces redundant costs from maintaining separate systems
- Ensures data freshness and provides consistent user experience across all personas
- Provides a single source of truth across all use cases

## 2. Internal Architecture

### Databricks Platform Architecture

The Databricks lakehouse is built on three key technologies:

1. **Apache Spark**: Massively scalable processing engine
2. **Delta Lake**: Optimized storage layer supporting ACID transactions and schema enforcement
3. **Unity Catalog**: Unified, fine-grained governance solution

```
┌─────────────────────────────────────────────────────────────┐
│                     Control Plane                          │
│  - Workspace management                                    │
│  - Cluster management                                      │
│  - Notebooks, Jobs, SQL Editor                             │
│  - Unity Catalog (governance)                             │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      Data Plane                            │
│  - Compute resources (clusters, SQL warehouses)            │
│  - Apache Spark execution                                  │
│  - Photon engine optimization                              │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      Storage Layer                         │
│  - Cloud object storage (S3, ADLS, GCS)                    │
│  - Delta Lake tables                                       │
│  - Open file formats (Parquet, Iceberg)                    │
└─────────────────────────────────────────────────────────────┘
```

### Well-Architected Framework Pillars

**Shared with cloud frameworks** :
- Operational Excellence
- Security, Privacy, and Compliance
- Reliability
- Performance Efficiency
- Cost Optimization

**Lakehouse-specific** :
- **Data and AI Governance**: Oversight to ensure data and AI bring value and support business strategy. A unified governance solution simplifies data management by consolidating governance controls into a single processing layer 
- **Interoperability and Usability**: Ability of the lakehouse to interact with users and other systems. Provides consistent user experience for all personas, reducing training costs and improving collaboration across functions 

## 3. Key Concepts

### Medallion Architecture

The medallion architecture organizes data into three layers to progressively improve data quality :

```
┌────────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                               │
│  - Raw data exactly as it arrives from source systems             │
│  - Minimal transformation                                         │
│  - Immutable (append-only)                                        │
│  - Schema-on-read approach                                        │
│  - Serves as the single source of truth                           │
└────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER                               │
│  - Cleansed, refined, and aggregated data                         │
│  - Data quality: removes duplicates, handles missing values       │
│  - Enrichment: joins data from multiple sources                   │
│  - Standardization: consistent types, formats, naming            │
│  - Foundation for reporting, dashboards, and ML                   │
└────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────────┐
│                        GOLD LAYER                                 │
│  - Business-ready data products                                   │
│  - Performance-optimized (pre-aggregated, denormalized)           │
│  - Access-controlled (row-level security, column masking)         │
│  - Tailored to specific business use cases                        │
│  - Published as reusable datasets                                 │
└────────────────────────────────────────────────────────────────────┘
```

#### Bronze Layer (Raw Data) 

**Characteristics**:
- **Source of truth**: Raw data exactly as it arrives from source systems
- **Minimal transformation**: Data stored in its original format (or converted to Delta for auditability)
- **Immutable**: Data is append-only, never updated or deleted
- **Schema-on-read**: Flexible schema handling for diverse source systems
- **Audit trail**: Preserves historical archive of source data for reprocessing and compliance

**Best Practices** :
- Preserve all source data fields for complete auditability
- Use Unity Catalog volumes for landing raw files
- Implement incremental ingestion to avoid full reprocessing
- Partition by ingestion date for efficient data management
- Document data sources and ingestion schedules

**Who Uses This Layer**: Data engineers building the ingestion pipelines 

#### Silver Layer (Cleansed and Conformed Data) 

**Characteristics**:
- **Data quality**: Removes duplicates, handles missing values, enforces schema
- **Enrichment**: Joins data from multiple sources to create integrated datasets
- **Standardization**: Applies consistent data types, formats, and naming conventions
- **Business rules**: Implements validation rules and business logic
- **Serves analytics**: Foundation for reporting, dashboards, and machine learning
- **Data modeling**: Typically uses 3rd Normal Form or Data Vault-like models

**Best Practices** :
- Implement data quality checks and monitoring
- Use Unity Catalog managed tables for silver layer data
- Partition by business dimensions (for example, date, region, product)
- Document transformation logic and business rules
- Establish SLAs for data freshness

**Who Uses This Layer**: Data scientists often explore silver tables for analysis and feature engineering; data engineers use it as a foundation for gold layer development 

#### Gold Layer (Business-Ready Data) 

**Characteristics**:
- **Business-specific**: Tailored to specific use cases and consumers
- **Performance-optimized**: Pre-aggregated, denormalized for fast queries
- **Access-controlled**: Implements row-level security and column masking
- **Consumer-ready**: Structured for BI tools, applications, and ML models
- **Data products**: Published as reusable datasets across the organization
- **Data modeling**: Typically uses Kimball-style star schemas or denormalized data marts

**Best Practices** :
- Create separate gold tables for different business units or use cases
- Use dynamic views for row-level and column-level security
- Implement predictive optimization for frequently queried tables
- Document data lineage from bronze through gold
- Publish data products through Unity Catalog with clear ownership

**Who Uses This Layer**: Business analysts and dashboards typically consume gold tables; data scientists may also use them for feature engineering 

### Landing Zone

Pipelines in larger organizations often have an additional landing zone in the cloud. The landing zone receives raw files from external systems before ingestion into the bronze layer .

**Landing Zone Patterns**:
- Cloud object storage (S3, ADLS Gen2, GCS) for file drops
- Unity Catalog volumes: Secure POSIX-style file access with Unity Catalog governance
- Third-party access: External systems can write directly to landing zones
- Notification triggers: Use event notifications to trigger ingestion pipelines 

### Data Ingestion Strategies 

**Batch Ingestion**:
- Schedule regular data loads (hourly, daily, weekly)
- Best for large volumes of historical data
- Lower cost compared to streaming
- Acceptable latency for analytical workloads

**Streaming Ingestion**:
- Continuous data ingestion with low latency
- Use Lakeflow Spark Declarative Pipelines with Auto Loader for streaming file ingestion
- Best for real-time analytics and operational use cases
- Higher compute costs but fresh data

**Change Data Capture (CDC)**:
- Capture and apply incremental changes from source systems
- Efficient for large tables with frequent updates
- Preserves data lineage and audit trail
- Supported by Lakeflow Connect and Lakeflow Spark Declarative Pipelines

### Ingestion Methods 

1. **Lakeflow Connect**: Managed data ingestion service that can regularly sync data from external sources without writing code
2. **Partner ingestion tools** (Fivetran): For sources not supported by Lakeflow Connect
3. **Custom ingestion pipelines**: Lakeflow Spark Declarative Pipelines or notebooks for complex transformations

### Compute Types

| Compute Type | Description | Best For |
|--------------|-------------|----------|
| **Job Compute** | Ephemeral compute for automated workloads | ETL pipelines, scheduled jobs |
| **All-purpose Compute** | Interactive clusters for notebooks | Ad-hoc analysis, development |
| **SQL Warehouse** | Optimized for SQL queries | BI dashboards, reporting |
| **Serverless SQL Warehouse** | Fully managed, instant startup | Ad-hoc SQL, production BI |

### SQL Warehouses

A SQL warehouse is a compute resource that lets you query and explore data on Databricks .

**Key Features**:
- Appears in compute drop-down menus of workspace UIs (query editor, Catalog Explorer, dashboards) 
- Running a query against a stopped warehouse starts it automatically
- Auto-restarts when queries are submitted, jobs are scheduled, or JDBC/ODBC connections are established 

### Serverless SQL Warehouses 

**Key Advantages**:
1. **Instant and elastic compute**: Eliminates waiting for infrastructure resources; Intelligent Workload Management dynamically handles scaling
2. **Minimal management overhead**: Capacity management, patching, upgrades, and performance optimization handled by Databricks
3. **Lower total cost of ownership**: Automatic provisioning and scaling avoids over-provisioning and reduces idle times

**Requirements**:
- Workspace on Premium plan or above
- Serverless Terms of Service accepted in account console
- User must be workspace admin or have unrestricted cluster creation permissions
- Region must support the feature

### Hub-and-Spoke Medallion Design 

For enterprise deployments with multiple domains:

```
Data Hub (Central)
├── Bronze: Organization-wide raw data (SAP, financials, weather)
├── Silver: Curated shared datasets
└── Gold: Enterprise-wide data products

Sales Domain
├── Bronze: Sales-specific raw data + shared hub data
├── Silver: Sales analytics datasets
└── Gold: Sales data products

Engineering Domain
├── Bronze: Engineering telemetry + shared hub data
├── Silver: Engineering metrics
└── Gold: Engineering dashboards
```

**Characteristics** :
- **Data hub**: Ingests, curates, and manages organization-wide assets
- **Data domains**: Each domain reads hub-curated data products and ingests its own domain-specific raw data
- **Publishing models**: Centralized (domains publish back to hub) or distributed (domains publish within own catalogs)

**Best Practices** :
- Use the hub for organization-wide shared data that multiple domains consume
- Allow domains to ingest and curate their own domain-specific data
- Use Unity Catalog catalogs to separate hub and domain data
- Use Databricks-managed OpenSharing to share data products

### Data Quality Strategy 

Regardless of the medallion architecture variant, **data quality must improve as data progresses through the layers**.

**Data Quality Tools**:
- **Constraints**: Verify quality and integrity of data added to a table
- **Primary and foreign keys**: Encode relationships between fields (informational, not enforced)
- **Expectations**: Prevent data quality issues from trickling downstream
- **Lakehouse Monitoring**: Monitor statistical properties and quality of data in all tables

**Best Practices**:
- Implement data quality checks at bronze ingestion (schema validation, null checks)
- Enforce stricter quality rules as data moves to silver and gold
- Monitor data quality metrics and trends over time
- Define data quality SLAs for critical datasets
- Automate alerting for data quality violations 

### Avoiding Data Silos 

Data movement, copy, and duplication may decrease data quality, especially when leading to data silos. When copies become operational with downstream business data products dependent on them, they become data silos that soon become out of sync.

**Best Practices to Avoid Data Silos** :
- Use Unity Catalog views and OpenSharing instead of copying data
- Establish a single source of truth for each dataset
- Discourage department-level data copies and duplicates
- Use Unity Catalog lineage to track data dependencies
- Retire redundant datasets regularly

## 4. Real-World Use Cases

### Use Case 1: Modernizing a Data Warehouse

**Business Context**: A retail company with a legacy data warehouse struggles to handle semi-structured data (clickstream, IoT) and ML workloads.

**Why Lakehouse**: The lakehouse unifies BI and AI on a single platform, eliminating silos and reducing data copies . Delta Lake provides ACID transactions and schema enforcement .

**Alternative Approaches**: Traditional data warehouse would require separate systems for ML, increasing costs and complexity.

**Expected Benefits**: Single source of truth, faster time-to-insight, reduced infrastructure costs.

### Use Case 2: Building a Customer 360 View

**Business Context**: A financial services company needs to combine data from multiple systems (CRM, transactions, support) for a unified customer view.

**Why Medallion Architecture**: The silver layer enables data integration from disparate sources, while the gold layer provides business-ready customer profiles .

**Alternative Approaches**: Point-to-point integrations create data silos and inconsistent views.

**Expected Benefits**: Trusted customer data, improved personalization, regulatory compliance.

### Use Case 3: Real-Time Analytics Dashboard

**Business Context**: An e-commerce company needs real-time inventory and sales dashboards during high-volume periods like Cyber Monday .

**Why Lakehouse**: Structured Streaming enables low-latency ingestion to bronze tables; silver tables clean and deduplicate data; gold tables provide pre-aggregated, performance-optimized views for BI tools .

**Alternative Approaches**: Separate streaming and batch systems create operational complexity and data duplication.

**Expected Benefits**: Fresh data for dashboards, unified operational view, cost optimization.

## 5. Practical Examples

### Creating a Medallion Architecture Pipeline

```python
# Bronze Layer - Raw Ingestion
bronze_df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/Volumes/bronze/schema") \
    .load("/Volumes/landing/raw_data")

bronze_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/Volumes/bronze/checkpoints") \
    .start("/Volumes/bronze/sales")

# Silver Layer - Cleaned and Validated
silver_df = spark.read.table("bronze.sales") \
    .filter("order_amount IS NOT NULL") \
    .withColumn("order_date", to_date("order_timestamp")) \
    .dropDuplicates(["order_id"])

silver_df.write.mode("overwrite") \
    .format("delta") \
    .saveAsTable("silver.sales")

# Gold Layer - Aggregated for BI
gold_df = spark.sql("""
    SELECT 
        order_date,
        region,
        SUM(order_amount) as total_sales,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM silver.sales
    GROUP BY order_date, region
""")

gold_df.write.mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold.sales_summary")
```

### Using SQL Warehouse for BI Queries

```sql
-- Run on a SQL Warehouse for BI dashboards
SELECT 
    region,
    DATE_TRUNC('month', order_date) as month,
    SUM(order_amount) as monthly_sales,
    COUNT(DISTINCT customer_id) as active_customers
FROM gold.sales_summary
WHERE order_date >= CURRENT_DATE() - INTERVAL 12 MONTHS
GROUP BY region, month
ORDER BY month DESC, region;
```

### Enabling Change Data Feed for Incremental Updates

```sql
-- Enable Change Data Feed on silver table for efficient gold updates
ALTER TABLE silver.sales
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = true);

-- Query Change Data Feed to get incremental changes
SELECT *
FROM table_changes('silver.sales', 0)
WHERE _change_type IN ('insert', 'update_postimage');
```

## 6. Interview Questions with Answers

### Common Questions

**1. What is a data lakehouse and how does it differ from a data lake?**

A lakehouse combines the benefits of data lakes (low-cost storage, support for all data types) and data warehouses (ACID transactions, schema enforcement, query performance) . Unlike a data lake, it provides transactional guarantees, schema enforcement, and performance optimizations for BI and ML workloads .

**2. Explain the medallion architecture and its three layers.**

The medallion architecture organizes data into three layers :
- **Bronze**: Raw data exactly as it arrives, immutable, append-only
- **Silver**: Cleansed, validated, and enriched data with schema enforcement
- **Gold**: Business-ready, aggregated data products optimized for consumption

**3. What is the difference between managed and external tables?**

Managed tables are fully managed by Unity Catalog, including storage location and lifecycle. New features like predictive optimization and managed DR are only available for managed tables . External tables are registered in Unity Catalog but the storage is user-managed, used when data must remain in specific paths for regulatory reasons .

**4. What is a SQL warehouse?**

A SQL warehouse is a compute resource optimized for SQL queries on Databricks . It appears in compute drop-down menus across the workspace UI and automatically starts when queries are submitted .

**5. What are serverless SQL warehouses and why are they recommended?**

Serverless SQL warehouses are fully managed by Databricks with instant and elastic compute, minimal management overhead, and lower TCO . They eliminate waiting for infrastructure resources and auto-scale dynamically.

### Advanced Questions

**1. How does the hub-and-spoke medallion design work for multi-domain organizations?**

The hub-and-spoke pattern centralizes shared data assets in a hub (organization-wide data like SAP, financials) while allowing domains to ingest and curate their own data . Domains read hub-curated data and produce domain-specific products, publishing either back to the hub or within their own catalogs .

**2. Why does data quality need to improve progressively through the medallion layers?**

Data quality increases as data progresses through layers, building trust from a business perspective . Bronze preserves raw data for reprocessing; Silver applies quality checks; Gold provides business-ready, curated data .

**3. When should you use streaming vs. batch ingestion?**

Use **streaming** when you need low-latency, real-time data (e.g., IoT, real-time dashboards). Use **batch** for large historical data volumes where latency is acceptable (e.g., daily/weekly financial reports). Streaming has higher compute costs but provides fresher data .

### Certification-Style Questions

**1. A data engineer is designing a new data pipeline and needs to ensure data is reliable for both BI and ML. Which architectural pattern should they use?**
- A) Data Vault
- B) Medallion Architecture ✅
- C) Lambda Architecture
- D) Star Schema

**2. Which compute type is recommended for most BI and ETL SQL workloads due to rapid startup and automatic scaling?**
- A) Classic all-purpose compute
- B) Serverless SQL Warehouse ✅
- C) Job compute
- D) Pro SQL Warehouse

**3. What is the primary purpose of the Silver layer in the medallion architecture?**
- A) Store raw data
- B) Clean, validate, and conform data ✅
- C) Create business-ready aggregations
- D) Land raw files

**4. Which table type supports predictive optimization and managed disaster recovery?**
- A) External tables
- B) Managed tables ✅
- C) Views
- D) Temporary tables

**5. What is the primary benefit of the bronze layer being immutable?**
- A) Better query performance
- B) Ability to rebuild downstream layers from source ✅
- C) Lower storage costs
- D) Automatic schema evolution

## 7. Common Mistakes

### Beginner Mistakes

- **Storing raw data in Silver**: Raw data belongs in Bronze, not Silver 
- **Ignoring data quality**: Quality must improve at each layer 
- **Using external tables for all data**: Managed tables recommended for most workloads 
- **Overlooking the landing zone**: Pipelines need a staging area before Bronze 

### Production Mistakes

- **Creating data silos** by copying data instead of using Unity Catalog views 
- **Not implementing proper partitioning** in Bronze layer
- **Overlooking Unity Catalog for governance**
- **Skipping the Bronze layer** by writing directly to Silver
- **Not documenting data lineage** from Bronze through Gold 

### Certification Traps

- Confusing Bronze vs. Silver vs. Gold purposes
- Misunderstanding managed vs. external tables 
- Overlooking serverless compute benefits
- Forgetting the hub-and-spoke pattern for multi-domain organizations
- Mixing up the lakehouse-specific pillars (Data and AI Governance, Interoperability and Usability)

## 8. Performance Considerations

### Best Practices

- Use **managed tables** for most workloads 
- **Partition by ingestion date** in Bronze layer 
- **Implement incremental ingestion** to avoid full reprocessing 
- Enable **predictive optimization** for frequently queried tables
- Use **Unity Catalog volumes** for landing raw files 

### Optimization Techniques

- Apply **Z-ORDER** on frequently filtered columns
- Use **OPTIMIZE** to compact small files
- Enable **Photon** for accelerated queries
- Implement **data skipping** with Z-order

### Scalability Concerns

- Auto Loader scales to handle billions of files
- Serverless SQL warehouses auto-scale to handle concurrency
- Delta Lake handles large volumes with ACID transactions 
- Partitioning strategies impact performance at scale

### Cost Considerations

- Serverless SQL warehouses reduce TCO through automatic provisioning 
- Auto-stop minimizes costs during idle periods
- Managed tables simplify cost optimization 
- Optimized writes reduce storage costs

## 9. Comparison Tables

### Batch vs Streaming

| Aspect | Batch | Streaming |
|--------|-------|-----------|
| Latency | Hours to days | Milliseconds to seconds |
| Cost | Lower | Higher |
| Use Cases | Historical analysis | Real-time dashboards |
| Pattern | Scheduled loads | Continuous processing |
| Data Freshness | Stale | Fresh |

### Managed vs External Tables

| Feature | Managed | External |
|---------|---------|----------|
| Storage management | Unity Catalog | User-managed |
| Predictive optimization | ✅ | ❌ |
| Managed DR | ✅ | ❌ |
| Simple folder structure | ❌ | ✅ |
| When to use | Most workloads | Regulatory/compliance needs  |

### Medallion Layers Comparison

| Feature | Bronze | Silver | Gold |
|---------|--------|--------|------|
| Data State | Raw | Cleansed | Business-ready |
| Transformations | Minimal | Quality + enrichment | Aggregation |
| Mutability | Append-only | Can be updated | Can be updated |
| Schema | Schema-on-read | Enforced | Enforced |
| Primary Users | Data engineers | Data scientists | Business analysts |

### SQL Warehouse Types

| Feature | Classic | Pro | Serverless |
|---------|---------|-----|------------|
| Startup time | Minutes | Minutes | 2-6 seconds |
| Auto-scaling | Limited | Yes | Intelligent Workload Management |
| Photon support | No | Yes | Yes |
| Management overhead | High | Medium | Minimal  |

## 10. Exam Notes

### Things the Certification Exam Likes to Ask

- **Medallion layer purposes**: Bronze=raw, Silver=cleansed, Gold=business-ready 
- **Managed vs External table tradeoffs**: Managed tables recommended for most workloads 
- **Serverless SQL warehouse benefits**: Instant compute, auto-scaling, minimal management 
- **Unity Catalog capabilities**: Governance, lineage, access control
- **Lakehouse-specific pillars**: Data and AI Governance, Interoperability and Usability 

### Common Traps

- Bronze = **immutable, append-only** 
- Silver = **data quality, enrichment, standardization** 
- Gold = **business-specific, performance-optimized** 
- Lakehouse pillars include **Data and AI Governance** and **Interoperability and Usability** 

### Important Facts to Memorize

- The lakehouse **unifies data warehousing and AI** on a single platform 
- **ACID transactions** come from Delta Lake 
- **Unity Catalog** provides centralized governance
- **Photon** is the vectorized query engine
- **Managed tables** support predictive optimization and managed DR 
- **Serverless SQL warehouses** are recommended when available 

### Scenario-Based Questions

1. **You need to ingest 5TB of JSON files daily. Which ingestion method should you use?**
   - Auto Loader with incremental ingestion 

2. **You need to share data across domains while maintaining governance. Which pattern should you use?**
   - Hub-and-spoke medallion design with Unity Catalog catalogs 

3. **You need to grant a business unit access to sensitive columns only. Which feature should you use?**
   - Dynamic views with column masking in Gold layer 

4. **You need to process data with low latency for real-time dashboards. Which ingestion pattern should you use?**
   - Streaming ingestion with Auto Loader 

5. **Your data must remain in specific cloud storage paths for regulatory reasons. Which table type should you use?**
   - External tables 