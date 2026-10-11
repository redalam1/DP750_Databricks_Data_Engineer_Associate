# Databricks Workflows and Jobs – Complete Guide

## 1. Definition

### What are Databricks Jobs?

A **Databricks Job** is the primary unit for scheduling and orchestrating production workloads on Azure Databricks . Jobs consist of one or more **tasks** that run your custom logic, including Spark, SQL, Python, ML, and arbitrary code .

**Key Components:**
- **Tasks**: Individual units of logic (notebooks, JARs, SQL queries, dbt, Python scripts)
- **Job**: The container that organizes tasks and defines how they run together
- **Trigger**: Defines when the job runs (schedule, event, manual)

### Why was it created?

Jobs were created to solve several production challenges:

1. **Orchestration**: Run complex workflows with dependencies between steps
2. **Reliability**: Auto-retry failed tasks, handle transient errors
3. **Observability**: Monitor runs, view logs, get notifications
4. **Scheduling**: Run workloads on a schedule or trigger by events
5. **Reproducibility**: Parameterize and version production workloads

### What problem does it solve?

Jobs transform ad-hoc code into **reliable, scheduled, and monitored production pipelines** :

- **Scheduling**: Run workloads at specific times or continuously 
- **Dependencies**: Control execution order between tasks 
- **Fault-tolerance**: Auto-retry failed tasks 
- **Observability**: Monitor runs, get notifications, view logs 
- **Scalability**: Handle thousands of tasks in complex workflows 

---

## 2. Internal Architecture

### Overall Architecture

```text
┌─────────────────────────────────────────────────────────────────────┐
│                      Databricks Workflows                          │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                      Control Flow                           │  │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────────┐  │  │
│  │  │ Trigger │─►│  Job    │─►│ Tasks   │─►│ Dependencies│  │  │
│  │  │(Schedule│  │(Manager)│  │(Runners)│  │  (Graph)    │  │  │
│  │  │ /Event) │  └─────────┘  └─────────┘  └─────────────┘  │  │
│  │  └─────────┘                                             │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                              │                                    │
│                              ▼                                    │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                      Execution Layer                        │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │  │
│  │  │  Task Runner │  │   Compute    │  │  State/Checkpoint│  │  │
│  │  │  (Executor)  │  │  (Cluster)   │  │  (Persistence)   │  │  │
│  │  └──────────────┘  └──────────────┘  └──────────────────┘  │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                              │                                    │
│                              ▼                                    │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                      Output Layer                           │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │  │
│  │  │   Notify     │  │  Log Output  │  │  Metrics/Results │  │  │
│  │  │  (Email/Web) │  │   (Console)  │  │   (System Tables)│  │  │
│  │  └──────────────┘  └──────────────┘  └──────────────────┘  │  │
│  └─────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Lifecycle of a Job Run

```text
1. Job Triggered (Schedule/Event/Manual/API)
         │
         ▼
2. Job Queued (Wait for resources)
         │
         ▼
3. Task DAG Evaluation (Determine execution order)
         │
         ▼
4. Task Execution (Run tasks in order, respecting dependencies)
         │
         ▼
5. Monitoring (Status updates, metrics collection)
         │
         ▼
6. Completion (Success/Failure/Timeout/Retry)
         │
         ▼
7. Notification (Send emails or webhooks based on outcome)
```

### Components Involved

| Component | Role |
|-----------|------|
| **Jobs UI** | Create, configure, and monitor jobs  |
| **Jobs API** | Programmatic management of jobs  |
| **Trigger** | Schedule (cron), event-based, manual, or continuous  |
| **Task** | Unit of logic (notebook, JAR, SQL, dbt, Python)  |
| **Dependency Graph** | DAG defining task execution order  |
| **Compute** | Serverless, job cluster, or existing cluster  |
| **Checkpoint** | State persistence for recovery and exactly-once  |
| **System Tables** | Operational data for monitoring and cost analysis  |

### Important Implementation Details

**Task Dependencies:**
```text
Task Dependencies determine execution order :

        ┌─────┐
        │Task1│ ──────────────────┐
        └─────┘                   │
                                    ▼
        ┌─────┐                ┌──────┐
        │Task2│ ──────────────►│Task4 │
        └─────┘                └──────┘
                                    ▲
        ┌─────┐                   │
        │Task3│ ──────────────────┘
        └─────┘

Task4 depends on Task2 and Task3 completing successfully.
Task1 is independent (root task).
```

---

## 3. Key Concepts

### Vocabulary

| Term | Definition |
|------|------------|
| **Job** | The primary unit for scheduling and orchestrating production workloads  |
| **Task** | A single unit of logic to be run as a step in a job  |
| **Dependency** | Relationship that controls execution order between tasks  |
| **Trigger** | Defines when a job runs (schedule, event, manual)  |
| **Retry** | Number of times to re-run a failed task  |
| **Run if** | Conditional execution based on dependency status  |
| **If/else** | Conditional execution based on boolean expression  |
| **For each** | Loop over an array of values, running a task for each  |
| **Task Values** | Pass information between tasks  |
| **Job Parameters** | Parameterize jobs with default values  |

### Task Types 

| Task Type | Description |
|-----------|-------------|
| **Notebook** | Run a Databricks notebook |
| **JAR** | Run a JAR file with a main class |
| **Python** | Run a Python file |
| **Spark Submit** | Run a spark-submit script |
| **SQL** | Run a SQL query |
| **dbt** | Run dbt commands |
| **Pipeline** | Run a Delta Live Tables pipeline |
| **Run Job** | Trigger another job |
| **If/else** | Conditional branching |
| **For each** | Loop over inputs |

### Trigger Types 

| Trigger | Description | Use Case |
|---------|-------------|----------|
| **Schedule (Quartz Cron)** | Fixed schedule | Daily ETL, hourly jobs |
| **Continuous** | Always-on streaming  | Streaming workloads |
| **Manual** | User-initiated | Ad-hoc runs, migrations |
| **Event-based** | Triggered by conditions | File arrival |
| **API** | Programmatic trigger | External orchestration |

### Compute Options 

| Compute Type | Description | Best For |
|--------------|-------------|----------|
| **Serverless** | Fully managed, auto-scaling | Most workloads  |
| **Job Cluster** | Ephemeral, automated | ETL, batch jobs |
| **Existing Cluster** | Long-running, shared | Interactive workloads |

### Limitations

| Limitation | Value |
|------------|-------|
| Max tasks per job | 100  |
| Max concurrent task runs per workspace | 2000  |
| Max jobs per workspace | 12,000  |
| Max jobs created per hour | 10,000  |
| Max clusters per job | 100  |

---

## 4. Real-World Use Cases

### Use Case 1: Daily ETL Pipeline

**Business Context:** A retail company needs to ingest sales data, clean it, and generate daily reports.

**Why Jobs:**
- Schedule runs daily at 2 AM
- Sequential dependencies: Ingest → Clean → Aggregate → Report
- Auto-retry if ingestion fails
- Email notifications on failure

**Alternative Approaches:**
- Manual execution would be unreliable
- External orchestrators add complexity

**Expected Benefits:**
- Reliable, automated daily processing
- Clear failure notifications
- Consistent scheduling

### Use Case 2: Dynamic Workflow with For Each

**Business Context:** A data platform needs to process data for each of 50 markets .

**Why Jobs:**
- Use `For each` task to loop over markets
- Read configuration table dynamically
- Process each market in parallel

**Alternative Approaches:**
- Hardcoding 50 tasks would be unmaintainable
- Manual iteration is error-prone

**Expected Benefits:**
- Dynamic, maintainable workflow
- Parallel processing
- Configuration-driven

### Use Case 3: Continuous Streaming

**Business Context:** A streaming pipeline processes IoT sensor data .

**Why Jobs:**
- Continuous trigger for always-on processing
- Auto-retries with exponential backoff
- Exactly-once semantics with checkpoints

**Alternative Approaches:**
- Batch would have high latency
- Manual restart would require intervention

**Expected Benefits:**
- Always-on processing
- Automatic recovery
- Low latency

---

## 5. Practical Examples

### Example 1: Creating a Job with Dependencies (API)

```json
{
  "name": "ETL Pipeline",
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "ingest_data",
      "description": "Ingest raw data",
      "notebook_task": {
        "notebook_path": "/Users/user@example.com/ingest"
      },
      "new_cluster": {
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 5
      },
      "timeout_seconds": 3600,
      "max_retries": 3,
      "retry_on_timeout": true
    },
    {
      "task_key": "clean_data",
      "description": "Clean and validate data",
      "notebook_task": {
        "notebook_path": "/Users/user@example.com/clean"
      },
      "depends_on": [
        {"task_key": "ingest_data"}
      ],
      "new_cluster": {
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 10
      },
      "timeout_seconds": 7200,
      "max_retries": 2,
      "retry_on_timeout": true
    },
    {
      "task_key": "run_analysis",
      "description": "Run analysis",
      "notebook_task": {
        "notebook_path": "/Users/user@example.com/analyze"
      },
      "depends_on": [
        {"task_key": "clean_data"}
      ],
      "existing_cluster_id": "1201-my-cluster",
      "timeout_seconds": 3600,
      "max_retries": 1
    },
    {
      "task_key": "send_report",
      "description": "Send report",
      "notebook_task": {
        "notebook_path": "/Users/user@example.com/send_report"
      },
      "depends_on": [
        {"task_key": "run_analysis"}
      ],
      "existing_cluster_id": "1201-my-cluster",
      "run_if": "ALL_SUCCESS"
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "America/New_York"
  },
  "email_notifications": {
    "on_failure": ["eng-team@example.com"],
    "on_success": ["owner@example.com"],
    "on_start": []
  }
}
```

**Code Explanation:**
- `tasks` array defines each task
- `task_key` uniquely identifies each task
- `depends_on` creates dependencies
- `run_if` controls conditional execution
- `new_cluster` creates ephemeral clusters per task
- `existing_cluster_id` reuses a cluster
- `schedule` uses Quartz cron for timing
- `email_notifications` sends alerts

### Example 2: For Each Task with Dynamic Input 

```sql
-- read_markets: SQL task to read configuration
SELECT market, currency, timezone
FROM config.markets
WHERE active = true
ORDER BY market;
```

```python
# process_markets: For each task
# Inputs: {{tasks.read_markets.output.rows}}
# This iterates over each row returned by the SQL task

# Nested task runs for each iteration
# Parameters: market={{input.market}}, currency={{input.currency}}
```

**Code Explanation:**
- SQL task reads configuration table and returns rows
- `For each` task receives the rows as input
- Each row is passed as parameters to the nested task
- Great for dynamic, data-driven workflows 

### Example 3: If/Else Conditional Task

```yaml
# Job definition with If/else
if_else_task:
  task_key: "check_data"
  condition: "{{tasks.load_data.values.row_count}} > 0"
  if_ttask_key: "process_data"
  else_task_key: "no_data_alert"
```

**Code Explanation:**
- `If/else` task evaluates a boolean condition
- Uses task values from upstream tasks
- Runs different branches based on result
- Prevents unnecessary processing 

---

## 6. Interview Questions with Answers

### Common Questions

**Q: What is the difference between a job and a task?**

**A:** A **job** is the top-level container that orchestrates multiple tasks . A **task** is a single unit of logic (notebook, JAR, SQL query) that runs as a step in the job . Tasks can have dependencies, and a job runs tasks in the order defined by those dependencies.

---

**Q: How do you control task execution order?**

**A:** Use `depends_on` to define dependencies between tasks . Tasks can run:
- **Sequentially**: Task B depends on Task A
- **In parallel**: Tasks A and B have no dependencies
- **Conditionally**: Use `run_if` to run based on status of dependencies 

---

**Q: What is the `run_if` condition and what options are available?**

**A:** `run_if` controls when a task runs based on the status of its dependencies :
- `ALL_SUCCESS`: All dependencies succeeded (default)
- `AT_LEAST_ONE_SUCCESS`: At least one dependency succeeded
- `NONE_FAILED`: None of the dependencies failed
- `ALL_DONE`: All dependencies completed (regardless of success)
- `AT_LEAST_ONE_FAILED`: At least one dependency failed
- `ALL_FAILED`: All dependencies failed 

---

**Q: What is the difference between job parameters and task values?**

**A:** 
- **Job parameters**: Static values defined at the job level, passed to tasks
- **Task values**: Dynamic values generated during a task run, passed to downstream tasks

---

**Q: What is the recommended compute option for production jobs?**

**A:** Databricks recommends **serverless compute for jobs** for most workloads . For classic compute, use **job clusters** (ephemeral) rather than all-purpose clusters, as job clusters are billed at lower rates .

---

### Advanced Questions

**Q: What happens when a continuous job fails? How are retries handled?**

**A:** Continuous jobs use exponential backoff for retries :
1. Failed tasks are retried up to 3 times with increasing delays
2. After max retries, the job run is canceled and a new run is triggered
3. Consecutive job failures increase the retry period
4. The backoff continues until a run succeeds 

---

**Q: How do you pass dynamic values between tasks in a job?**

**A:** Use **task values** :
1. Upstream task sets a value: `dbutils.jobs.taskValues.set(key, value)`
2. Downstream task references it: `{{tasks.<task_name>.values.<key>}}`
3. Also works with `For each` tasks: `{{input.field_name}}` 

---

**Q: What are the limitations of Databricks Jobs?**

**A:** 
- 100 tasks max per job
- 12,000 jobs max per workspace
- 2,000 concurrent task runs per workspace
- 10,000 jobs created per hour max
- 100 clusters per job max

---

### Certification-Style Questions

**Q1: A data engineer is creating a job with four tasks that must run sequentially. Task B depends on Task A, Task C depends on Task B, and Task D depends on Task C. What is the simplest way to configure this?**

- A) Use `For each` task to iterate
- B) Set `depends_on` on each task pointing to the previous task ✅
- C) Use `If/else` conditions
- D) No dependencies needed; tasks run in order by default

**Explanation:** Sequential dependencies are created using `depends_on` where each task depends on the previous one .

---

**Q2: A data engineer wants to run a task only if the upstream task fails. Which `run_if` condition should they use?**

- A) `ALL_SUCCESS`
- B) `NONE_FAILED`
- C) `ALL_FAILED`
- D) `AT_LEAST_ONE_FAILED` ✅

**Explanation:** `AT_LEAST_ONE_FAILED` runs a task if at least one dependency failed .

---

**Q3: Which task type is used to iterate over a dynamic list of values in a job?**

- A) `If/else` condition task
- B) `Run if` conditional task
- C) `For each` task ✅
- D) `Run Job` task

**Explanation:** `For each` tasks run a nested task in a loop over an input array .

---

**Q4: What is the maximum number of tasks allowed in a single job?**

- A) 50
- B) 100 ✅
- C) 500
- D) 1,000

**Explanation:** A job can contain at most 100 tasks .

---

**Q5: Which compute option is recommended for most production workloads?**

- A) Existing cluster
- B) All-purpose cluster
- C) Serverless compute ✅
- D) SQL warehouse

**Explanation:** Databricks recommends serverless compute for most automated workloads .

---

## 7. Common Mistakes

### Beginner Mistakes

**1. Forgetting to set task dependencies**
```python
# ❌ WRONG - Tasks run in parallel by default
# Task B runs before Task A completes if no dependency is set

# ✅ CORRECT - Set depends_on for sequential execution
"depends_on": [{"task_key": "ingest_data"}]
```

**2. Using all-purpose clusters for jobs**
```python
# ❌ WRONG - Higher cost, less reliable
"existing_cluster_id": "1201-my-interactive-cluster"

# ✅ CORRECT - Use job clusters for automated workloads
"new_cluster": {
  "spark_version": "15.4.x-scala2.12",
  "node_type_id": "i3.xlarge",
  "num_workers": 5
}
```

**3. Not using service principals** 
```python
# ❌ WRONG - Jobs owned by users can break when users leave
# ✅ CORRECT - Use service principals for production jobs
"run_as": "service-principal@example.com"
```

### Production Mistakes

**1. Sharing checkpoints between different queries** 
```python
# ❌ WRONG - Same checkpoint location for different streaming jobs
# ✅ CORRECT - Each streaming job gets its own unique checkpoint
"checkpoint_location": "/checkpoints/unique-identifier"
```

**2. Not setting retries for transient errors** 
```python
# ❌ WRONG - No retries, job fails on first error
"max_retries": 0

# ✅ CORRECT - Set retries for reliability
"max_retries": 3,
"retry_on_timeout": true
```

**3. Storing production data in DBFS root** 
```python
# ❌ WRONG - All users can access, security risk
# ✅ CORRECT - Use Unity Catalog managed volumes
```

### Certification Traps

1. **`run_if` conditions**: `Skipped` upstream tasks are treated as **successful** when evaluating `run_if` conditions 
2. **Excluded tasks**: Tasks marked as `Excluded` are **skipped** and treated as **successful** 
3. **Continuous jobs**: Cannot use task dependencies 
4. **`once` trigger**: Deprecated; use `availableNow` instead 

---

## 8. Performance Considerations

### Best Practices 

| Practice | Impact |
|----------|--------|
| **Use serverless compute** | No cluster configuration, auto-scaling  |
| **Use service principals** | Jobs continue when users leave  |
| **Use job clusters** | Lower billing rates than interactive  |
| **Restart long-running clusters** | Apply patches and bug fixes  |
| **Use LTS runtime** | Best stability and performance  |
| **Don't store data in DBFS root** | Security risk  |

### Optimization Techniques

**1. Parallel Task Execution:**
```python
# Independent tasks run in parallel automatically
tasks:
  - task_key: "task_a"  # No depends_on
  - task_key: "task_b"  # No depends_on, runs parallel with A
```

**2. Dynamic Concurrency:**
```python
# Allow multiple concurrent runs when needed
"max_concurrent_runs": 3
```

**3. Resource Optimization:**
```python
# Right-size clusters per task
"new_cluster": {
  "node_type_id": "i3.xlarge",  # Choose appropriate size
  "num_workers": 5              # Scale for workload
}
```

### Cost Considerations

| Factor | Impact |
|--------|--------|
| **Compute type** | Serverless vs. classic  |
| **Cluster size** | Larger clusters cost more |
| **Run duration** | Longer runs cost more |
| **Parallelism** | More concurrent tasks cost more |
| **Retries** | Retries add cost  |

### Monitoring with System Tables 

```sql
-- Most expensive jobs (last 30 days)
WITH list_cost_per_job AS (
  SELECT
    workspace_id,
    usage_metadata.job_id,
    COUNT(DISTINCT usage_metadata.job_run_id) as runs,
    SUM(usage_quantity * list_prices.pricing.default) as list_cost
  FROM system.billing.usage
  INNER JOIN system.billing.list_prices ON ...
  WHERE billing_origin_product = "JOBS"
    AND usage_date >= CURRENT_DATE() - INTERVAL 30 DAY
  GROUP BY ALL
)
SELECT name, job_id, runs, list_cost
FROM list_cost_per_job
ORDER BY list_cost DESC;
```

---

## 9. Comparison Tables

### Job vs Task

| Aspect | Job | Task |
|--------|-----|------|
| Definition | Orchestration container | Unit of logic  |
| Contains | 1-100 tasks | Notebook, JAR, SQL, etc.  |
| Scheduling | Has trigger/schedule | Runs as part of job |
| Dependencies | N/A (tasks have deps) | Can depend on other tasks  |

### Compute Options 

| Aspect | Serverless | Job Cluster | Existing Cluster |
|--------|------------|-------------|------------------|
| Setup | None  | Automated | Manual |
| Scaling | Auto  | Configured | Configured |
| Cost | Pay-as-you-go | Lower rate | Higher rate  |
| Management | Databricks | User | User |
| Best For | Most workloads | Batch jobs | Interactive |

### Trigger Types 

| Trigger | Use Case | Complexity |
|---------|----------|------------|
| Schedule (cron) | Regular ETL | Simple |
| Continuous | Streaming  | Complex |
| Manual | Ad-hoc | Simple |
| Event-based | File arrival | Medium |
| API | External orchestration | Medium |

### Control Flow Options 

| Feature | Purpose | Used For |
|---------|---------|----------|
| `depends_on` | Execution order | Sequencing tasks  |
| `run_if` | Conditional based on task status | Error handling  |
| `If/else` | Conditional based on values | Branching logic  |
| `For each` | Iteration over values | Dynamic workflows  |

### Retry Policies 

| Aspect | Task Retry | Continuous Job Retry |
|--------|------------|---------------------|
| Mechanism | `max_retries` | Exponential backoff  |
| Control | User-configurable | System-managed  |
| Max attempts | Configurable | Unlimited  |
| Use Case | All jobs | Continuous jobs  |

---

## 10. Exam Notes

### Things the Certification Exam Likes to Ask

1. **Job structure**: Jobs contain tasks, tasks have dependencies 
2. **Task types**: Notebook, JAR, Python, SQL, dbt, Pipeline, Run Job 
3. **`run_if` conditions**: All options and their behavior 
4. **Retries**: `max_retries`, `retry_on_timeout` 
5. **Compute recommendations**: Serverless for most, job clusters for classic 
6. **Limitations**: 100 tasks per job, 12,000 jobs per workspace 
7. **Continuous mode**: Always-on streaming, exponential backoff 

### Common Traps

1. **`Skipped` tasks are treated as successful** for `run_if` evaluations 
2. **`Excluded` tasks are skipped** and treated as successful 
3. **Continuous jobs cannot use task dependencies** 
4. **`once` trigger is deprecated**; use `availableNow` 

### Important Facts to Memorize

| Fact | Detail |
|------|--------|
| Max tasks per job | 100  |
| Max jobs per workspace | 12,000  |
| Max concurrent task runs | 2,000 per workspace  |
| Default `run_if` | `ALL_SUCCESS`  |
| Default `max_concurrent_runs` | 1  |
| Recommended compute | Serverless  |
| `retry_on_timeout` default | `false` |

### Scenario-Based Questions

1. **You need to run a task only if an upstream task fails. Which `run_if` condition should you use?**
   - `AT_LEAST_ONE_FAILED` 

2. **You need to iterate over a dynamic list of values from a configuration table. Which task type should you use?**
   - `For each` 

3. **You need to run a streaming job continuously with automatic retries. Which trigger type should you use?**
   - Continuous trigger 

4. **You need to pass a value from one task to a downstream task. What should you use?**
   - Task values 

---

## Summary Cheat Sheet

```text
┌─────────────────────────────────────────────────────────────────────┐
│                    DATABRICKS JOBS CHEAT SHEET                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                   │
│  JOB COMPONENTS:                                                  │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐                  │
│  │  Trigger │────►│   Tasks  │────►│   DAG    │                  │
│  │(Schedule)│     │(Notebook)│     │(Depends) │                  │
│  └──────────┘     └──────────┘     └──────────┘                  │
│                                                                   │
│  TASK TYPES:                                                      │
│  - Notebook  - Python  - JAR  - SQL  - dbt  - Pipeline           │
│  - Run Job   - If/else  - For each                   │
│                                                                   │
│  TRIGGERS:                                                        │
│  - Schedule (cron)  - Continuous  - Manual  - API    │
│                                                                   │
│  RUN IF CONDITIONS :                                  │
│  ┌──────────────────┬────────────────────────────────────────────┐│
│  │ ALL_SUCCESS      │ All dependencies succeeded                  ││
│  │ AT_LEAST_ONE_SUCC│ At least one succeeded                    ││
│  │ NONE_FAILED      │ None failed                               ││
│  │ ALL_DONE         │ All completed                             ││
│  │ AT_LEAST_ONE_FAIL│ At least one failed                       ││
│  │ ALL_FAILED       │ All failed                               ││
│  └──────────────────┴────────────────────────────────────────────┘│
│                                                                   │
│  COMPUTE RECOMMENDATIONS :                            │
│  - Serverless: ✅ Most workloads                                 │
│  - Job Cluster: ✅ Batch ETL                                     │
│  - Existing Cluster: ❌ Higher cost                              │
│                                                                   │
│  LIMITATIONS :                                       │
│  - 100 tasks per job                                             │
│  - 12,000 jobs per workspace                                     │
│  - 2,000 concurrent tasks per workspace                         │
│                                                                   │
└─────────────────────────────────────────────────────────────────────┘
```