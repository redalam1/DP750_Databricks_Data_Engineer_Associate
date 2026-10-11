# Unity Catalog and Governance – Complete Guide

## 1. Definition

### What is Unity Catalog?

Unity Catalog is the unified governance layer for both data and AI in Databricks . It provides a single place to control access, track lineage, discover assets, monitor quality, and govern AI across your entire organization, no matter where your data lives .

Think of Unity Catalog as the "single source of truth" for all your data and AI assets. It brings together:

- **Data governance**: Tables, views, schemas, catalogs
- **AI governance**: ML models, Large Language Models (LLMs), Gen AI tools
- **Access control**: Fine-grained permissions for all assets
- **Data discovery**: Catalog Explorer, AI-generated comments
- **Data lineage**: Column-level tracking of data flow
- **Auditing**: System tables and audit logs for compliance

### Why was it created?

Before Unity Catalog, Databricks had governance challenges:

- **Multiple metastores**: Each workspace had its own Hive metastore
- **Inconsistent permissions**: Managing access across workspaces was complex
- **Limited visibility**: Data lineage and discovery were fragmented
- **AI assets ungoverned**: ML models lacked proper governance

Unity Catalog was created to solve these problems by providing a **centralized, unified governance layer** that works across workspaces, clouds, and data formats .

### What problem does it solve?

1. **Data silos**: Unity Catalog helps avoid data silos by offering a single unified system to manage, govern, and organize all data and AI assets 
2. **Security fragmentation**: Provides a single place to define and enforce access policies
3. **Compliance complexity**: Simplifies auditing and compliance reporting
4. **Discovery challenges**: Makes it easy for users to find data and AI assets
5. **AI governance gap**: Extends governance to AI models, agents, and tools 

> **Key Insight**: Unity Catalog is the industry's only universal catalog for data and AI, supporting any format, engine, and asset .

---

## 2. Internal Architecture

### Three-Level Namespace

Unity Catalog organizes assets using a three-level namespace:

```text
┌─────────────────────────────────────────────────────────────────────┐
│                         CATALOG                                    │
│  - Top-level container for data and AI assets                     │
│  - Logical grouping (e.g., production, development)               │
│  - Can be shared across workspaces                                │
└─────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         SCHEMA                                     │
│  - Second-level container within a catalog                        │
│  - Groups tables, views, models, functions                        │
│  - Similar to a database in traditional RDBMS                    │
└─────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         TABLE / VIEW / MODEL                      │
│  - Third-level: the actual asset                                  │
│  - Table: structured data                                         │
│  - View: virtual table based on query                            │
│  - Model: ML model registered in Unity Catalog                   │
└─────────────────────────────────────────────────────────────────────┘
```

**Full asset reference syntax**:
```sql
<catalog>.<schema>.<asset>
-- Example: prod.sales.transactions
```

### Governance Capabilities Architecture

```text
┌─────────────────────────────────────────────────────────────────────┐
│                    UNITY CATALOG GOVERNANCE                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐          │
│  │  ACCESS     │    │   LINEAGE   │    │  DISCOVERY  │          │
│  │  CONTROL    │    │             │    │             │          │
│  └─────────────┘    └─────────────┘    └─────────────┘          │
│                                                                   │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐          │
│  │  AUDITING   │    │   DATA      │    │     AI      │          │
│  │             │    │  QUALITY    │    │  GOVERNANCE │          │
│  └─────────────┘    └─────────────┘    └─────────────┘          │
│                                                                   │
│  ┌─────────────┐    ┌─────────────┐                               │
│  │    DATA     │    │   DATA      │                               │
│  │  SHARING    │    │  CLASSIF.   │                               │
│  └─────────────┘    └─────────────┘                               │
│                                                                   │
└─────────────────────────────────────────────────────────────────────┘
```

### Integration with MLflow

Unity Catalog integrates with MLflow for AI governance :

```text
┌─────────────────────────────────────────────────────────────────────┐
│                     MLflow + Unity Catalog                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────┐    ┌─────────────────────────────────┐  │
│  │   MLflow Experiment  │───►│   Unity Catalog Governance     │  │
│  │  - Track training    │    │   - Access control             │  │
│  │  - Log parameters    │    │   - Lineage tracking           │  │
│  │  - Capture metrics   │    │   - Version management         │  │
│  └─────────────────────┘    └─────────────────────────────────┘  │
│           │                                   │                   │
│           ▼                                   ▼                   │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                  Registered Model in UC                     │  │
│  │               <catalog>.<schema>.<model>                    │  │
│  └─────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Key Concepts

### Vocabulary

| Term | Definition |
|------|------------|
| **Catalog** | Top-level container for organizing data and AI assets  |
| **Schema** | Container within a catalog that groups tables, views, and models |
| **Managed Table** | Unity Catalog fully manages the storage location and lifecycle |
| **External Table** | Table registered in Unity Catalog but stored in user-managed location |
| **Volume** | Container for unstructured data files  |
| **Storage Credential** | Authentication for accessing cloud storage  |
| **External Location** | Path in cloud storage referenced by external tables |
| **Data Lineage** | Capture and visualization of data flow through the organization  |
| **Governed Tags** | Attributes used for discovery and ABAC policies  |
| **ABAC** | Attribute-Based Access Control for row filtering and column masking  |

### Configuration Options

| Option | Purpose |
|--------|---------|
| **`USE CATALOG`** | Permission to access a catalog |
| **`USE SCHEMA`** | Permission to access a schema |
| **`CREATE TABLE`** | Permission to create tables in a schema |
| **`SELECT`** | Permission to read data from a table |
| **`MODIFY`** | Permission to insert, update, or delete data |
| **`OWNER`** | Full privileges on an object |

### Advantages

- **Centralized governance**: Single place for all data and AI assets
- **Fine-grained access control**: Catalog, schema, table, column levels
- **Cross-workspace**: Works across all workspaces in an account
- **Multi-cloud**: Supports AWS, Azure, GCP
- **Open and interoperable**: Compatible with Delta Lake, Iceberg, Hive 

### Limitations

- **External Hive metastore not supported**: Using an external Hive metastore blocks serverless SQL warehouses
- **Performance**: Some operations (like `SHOW TABLES`) can be slower with many objects
- **Configuration complexity**: Requires proper setup of storage credentials and external locations

---

## 4. Real-World Use Cases

### Use Case 1: Enterprise Data Governance

**Business Context**: A financial institution needs to centrally govern data across multiple business units and workspaces, with strict regulatory compliance.

**Why Unity Catalog**:
- Single place to define and enforce access policies
- Cross-workspace governance
- Data lineage for audit trails

**Alternative Approaches**:
- Multiple Hive metastores would create fragmentation
- Manual access management would be error-prone

**Expected Benefits**:
- Consistent security policies
- Simplified auditing
- Improved compliance

### Use Case 2: AI Governance with MLflow

**Business Context**: A data science team needs to govern ML models across the entire lifecycle, from experimentation to production .

**Why Unity Catalog**:
- Register models with three-level namespace
- Fine-grained access control for models
- Integration with MLflow for experiment tracking 

**Alternative Approaches**:
- MLflow alone lacks robust governance
- Custom solutions would be complex to maintain

**Expected Benefits**:
- Secure model deployment
- Clear version management
- Production-ready MLOps

### Use Case 3: Data Sharing Across Teams

**Business Context**: Multiple teams need to share data while maintaining governance and avoiding data silos.

**Why Unity Catalog**:
- OpenSharing for secure data sharing 
- Centralized governance across teams
- Data discovery for finding assets

**Alternative Approaches**:
- Copying data would create silos
- Sharing via cloud storage lacks governance

**Expected Benefits**:
- Single source of truth
- Reduced data duplication
- Governed data sharing

---

## 5. Practical Examples

### Example 1: Creating a Table in Unity Catalog

```sql
-- Create a managed table in Unity Catalog
CREATE TABLE prod.sales.transactions (
    transaction_id STRING,
    customer_id STRING,
    product_id STRING,
    amount DECIMAL(10,2),
    transaction_date DATE
) USING DELTA
LOCATION '/Volumes/prod/sales/transactions';
```

**Code Explanation:**
- `prod.sales.transactions`: Three-level namespace (catalog.schema.table)
- `USING DELTA`: Specifies Delta Lake format
- `LOCATION`: Optional; if omitted, Unity Catalog manages storage

### Example 2: Granting Permissions

```sql
-- Grant access to a catalog
GRANT USE CATALOG ON CATALOG prod TO `analyst@company.com`;

-- Grant access to a schema
GRANT USE SCHEMA ON SCHEMA prod.sales TO `analyst@company.com`;

-- Grant SELECT on a table
GRANT SELECT ON prod.sales.transactions TO `analyst@company.com`;

-- Grant full control (ownership)
GRANT OWNER ON prod.sales.transactions TO `data_engineer@company.com`;
```

### Example 3: Querying Unity Catalog with PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read from a Unity Catalog table
df = spark.table("prod.sales.transactions")

# Filter and aggregate
result = (df
    .filter(df.transaction_date >= "2024-01-01")
    .groupBy("customer_id")
    .agg({"amount": "sum"})
    .collect())

# Create a view in Unity Catalog
df.createOrReplaceTempView("temp_view")

# Or create a permanent view
spark.sql("""
    CREATE VIEW prod.sales.recent_transactions AS
    SELECT * FROM prod.sales.transactions
    WHERE transaction_date >= CURRENT_DATE() - INTERVAL 30 DAYS
""")
```

---

## 6. Interview Questions with Answers

### Common Questions

**Q: What is the three-level namespace in Unity Catalog?**

**A:** Unity Catalog uses a three-level namespace: `<catalog>.<schema>.<asset>`. Catalogs are top-level containers, schemas group assets within a catalog, and assets include tables, views, models, and functions .

---

**Q: What is the difference between a managed table and an external table?**

**A:** Managed tables are fully managed by Unity Catalog, which controls the storage location and lifecycle . External tables are registered in Unity Catalog but the underlying files are stored in locations specified by the user.

---

**Q: What is data lineage in Unity Catalog?**

**A:** Data lineage captures and visualizes how data flows through your organization, down to the column level . It helps with auditing, troubleshooting, and understanding data dependencies.

---

### Difficult Questions

**Q: How does Unity Catalog integrate with MLflow?**

**A:** Unity Catalog integrates with MLflow by allowing models to be registered in Unity Catalog using the three-level namespace (`<catalog>.<schema>.<model>`) . This combines MLflow's experiment tracking with Unity Catalog's governance capabilities .

---

**Q: What is ABAC and how does it work in Unity Catalog?**

**A:** Attribute-Based Access Control (ABAC) uses governed tags to enforce access policies . Governance teams define tags at the account level, and ABAC uses these tags to enforce row- and column-level security . This enables consistent, scalable access control.

---

### Certification-Style Questions

**Q1: You need to grant a user the ability to create tables in a specific schema. What permissions should they have?**

- A) `USE CATALOG` on the catalog
- B) `CREATE TABLE` on the schema ✅
- C) `SELECT` on the schema
- D) `MODIFY` on the catalog

**Explanation:** To create a table, a user needs `CREATE TABLE` on the schema .

---

**Q2: What is the correct syntax to reference a table in Unity Catalog?**

- A) `<catalog>.<table>`
- B) `<schema>.<catalog>.<table>`
- C) `<catalog>.<schema>.<table>` ✅
- D) `<table>`

**Explanation:** Unity Catalog uses the three-level namespace: `<catalog>.<schema>.<table>` .

---

## 7. Common Mistakes

### Beginner Mistakes

**1. Creating external tables without proper storage credentials**
```sql
-- ❌ WRONG - Missing storage credential
CREATE TABLE prod.sales.external_table
USING DELTA
LOCATION 's3://my-bucket/data';

-- ✅ CORRECT - Storage credential must be defined first
-- (Define storage credential before creating external table)
```

**2. Granting too broad permissions**
```sql
-- ❌ WRONG - Too permissive
GRANT ALL PRIVILEGES ON prod TO `user@company.com`;

-- ✅ CORRECT - Principle of least privilege
GRANT USE CATALOG ON prod TO `user@company.com`;
GRANT SELECT ON prod.sales.transactions TO `user@company.com`;
```

### Production Mistakes

**1. Not setting ownership on important tables**
**Problem:** Without proper ownership, no one can manage permissions effectively.
**Solution:** Always set `OWNER` for critical assets.

**2. Sharing checkpoint directories**
**Problem:** Sharing checkpoints between queries causes corruption.
**Solution:** Each query needs its own unique checkpoint location.

### Certification Traps

- **Not knowing the three-level namespace structure** (catalog.schema.asset)
- **Confusing managed vs external tables**
- **Not knowing what ABAC provides** (row filters and column masks)
- **Ignoring the governance capabilities**

---

## 8. Performance Considerations

### Best Practices

1. **Use managed tables for most workloads** 
2. **Enable predictive optimization** for frequently queried tables 
3. **Use governed tags for consistent classification**
4. **Implement column-level security** with ABAC 
5. **Track data lineage** for audit trails 

### Optimization Techniques

| Technique | Benefit |
|-----------|---------|
| **Managed tables** | Simplified lifecycle management |
| **Predictive optimization** | Faster queries |
| **ABAC policies** | Scalable access control |
| **Governed tags** | Consistent classification |

### Scalability Concerns

- **Namespace depth**: Three-level namespace scales well
- **Permission checks**: May impact performance with large numbers of objects
- **Lineage tracking**: Can affect write performance

### Cost Considerations

- **Storage costs**: Managed tables use Unity Catalog storage
- **Compute costs**: Access control checks add overhead
- **Governance overhead**: More governance = more management

---

## 9. Comparison Tables

### Managed vs External Tables

| Feature | Managed | External |
|---------|---------|----------|
| Storage management | Unity Catalog | User-managed |
| Predictive optimization | ✅ | ❌  |
| Managed DR | ✅ | ❌  |
| Simple folder structure | ❌ | ✅ |
| When to use | Most workloads | Regulatory needs |

### Unity Catalog vs Hive Metastore

| Feature | Unity Catalog | Hive Metastore |
|---------|---------------|----------------|
| Cross-workspace | ✅ | ❌ |
| AI governance | ✅ | ❌ |
| Data lineage | ✅ | ❌ |
| Open API | ✅ | ✅ |

---

## 10. Exam Notes

### Things the Certification Exam Likes to Ask

1. **Three-level namespace**: `<catalog>.<schema>.<asset>` 
2. **Governance capabilities**: Access control, lineage, discovery, auditing, quality, sharing 
3. **Managed vs external tables**: Storage management differences
4. **ABAC**: Row filtering and column masking 
5. **MLflow integration**: Model governance 

### Common Traps

- **Thinking Unity Catalog replaces Hive metastore**: It enhances it
- **Assuming external tables are always better**: Managed tables recommended
- **Not knowing what ABAC does**: Row filters and column masks

### Important Facts to Memorize

| Fact | Detail |
|------|--------|
| **Namespace** | catalog.schema.asset  |
| **Managed tables** | Unity Catalog manages storage |
| **External tables** | User manages storage |
| **ABAC** | Uses governed tags  |
| **Governance Hub** | Centralized command center  |

### Scenario-Based Questions

1. **You need to share data across teams while maintaining governance. What should you use?**
   - OpenSharing with Unity Catalog 

2. **You need to track data flow across your organization. What should you use?**
   - Data lineage in Unity Catalog 

3. **You need to give a user access to a table but not the schema. What should you grant?**
   - `SELECT` on the table

---

## Summary Cheat Sheet

```text
┌─────────────────────────────────────────────────────────────────────┐
│                   UNITY CATALOG CHEAT SHEET                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                   │
│  NAMESPACE:                                                       │
│  <catalog>.<schema>.<asset>                                      │
│                                                                   │
│  ASSET TYPES:                                                     │
│  - Tables (managed/external)                                     │
│  - Views                                                         │
│  - Models (ML/AI)                                                │
│  - Volumes (unstructured)                                        │
│  - Functions                                                     │
│                                                                   │
│  PERMISSIONS:                                                     │
│  - USE CATALOG                                                   │
│  - USE SCHEMA                                                    │
│  - SELECT, INSERT, UPDATE, DELETE                                │
│  - CREATE TABLE, CREATE VIEW, CREATE MODEL                      │
│  - OWNER (full control)                                          │
│                                                                   │
│  GOVERNANCE CAPABILITIES:                                         │
│  - Access control                                                │
│  - Data lineage                                                  │
│  - Data discovery                                                │
│  - Auditing                                                      │
│  - Data quality                                                  │
│  - Data sharing                                                  │
│  - AI governance                                                 │
│                                                                   │
│  BEST PRACTICES:                                                  │
│  1. Use managed tables for most workloads                       │
│  2. Grant least privilege                                       │
│  3. Use ABAC for scalable access control                        │
│  4. Track lineage for audit trails                               │
│                                                                   │
└─────────────────────────────────────────────────────────────────────┘
```