# Data Infrastructure Optimization Project

## Overview

This project demonstrates data infrastructure optimization techniques using
Apache Spark, PySpark, and Delta Lake.

The project focuses on:

- Spark partition analysis and optimization
- Data distribution and key skew detection
- Skew mitigation using salting
- Broadcast join optimization
- Spark execution plan analysis
- Delta Lake transactional operations
- Delta Lake time travel
- Delta Lake rollback
- Infrastructure and security configuration

## Technologies

- Python
- PySpark
- Apache Spark
- Delta Lake
- YAML
- Git / GitHub

## Testing

The project includes automated tests using `pytest`.

Run the complete test suite:

```bash
python -m pytest -q
```

## Architecture

The project is organized into the following components:

```text
Data Infrastructure Optimization Project
│
├── spark_optimizer.py
│   └── Spark performance and execution analysis
│
├── data_pipeline_optimizer.py
│   └── Data skew, partitioning, salting, joins, benchmarking
│
├── data_lake_transactions.py
│   └── Delta Lake transactions, time travel, rollback
│
├── storage_evaluator.py
│   └── Infrastructure and security evaluation
│
├── Infrastructure_baseline.yaml
│   └── Infrastructure configuration
│
└── tests/
	├── test_data_pipeline_optimizer.py
	├── test_storage_evaluator.py
	└── test_data_lake_transactions.py
```

## Project Components

### 1. Spark Performance Optimization

`spark_optimizer.py` provides reusable utilities for:

- Data distribution analysis
- Partition optimization
- Smart caching
- Performance benchmarking
- Spark execution-plan inspection

Example analysis includes record counts, distinct keys, average records per
key, and skew ratio.

### 2. Data Pipeline Optimization

`data_pipeline_optimizer.py` demonstrates:

- Partition distribution analysis
- Key-level skew detection
- Repartitioning
- Salting for skew mitigation
- Broadcast joins
- Execution-plan analysis
- Performance benchmarking

The skew mitigation benchmark compares normal aggregation with a salted
aggregation strategy.

> Note: In the latest local execution, the salted aggregation completed
> approximately **7.78% faster** than the unsalted aggregation. This result
> is specific to the local test dataset and environment and should not be
> interpreted as a guaranteed performance improvement for larger workloads.

### 3. Delta Lake Transactions

`data_lake_transactions.py` demonstrates:

- Delta table creation
- Atomic updates
- Delta transaction history
- Time travel
- MERGE / UPSERT operations
- Rollback using Delta Lake RESTORE

The execution confirmed multiple Delta Lake transaction versions and a
successful restore operation.

### 4. Infrastructure and Security Evaluation

`storage_evaluator.py` evaluates the infrastructure configuration defined in
`Infrastructure_baseline.yaml`.

The implemented checks include:

- Encryption at rest
- Public access disabled
- IAM authentication
- Least-privilege authorization
- Lifecycle policy
- Network isolation
- Private subnets
- Security groups
- TLS encryption for data in transit

The local evaluation completed successfully for all listed security checks.

## Usage

Run the Spark optimization analysis:

```bash
python spark_optimizer.py
```
