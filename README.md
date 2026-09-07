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

> Note: In the local test, the salted aggregation was slower than the
> unsalted aggregation. The measured result was approximately **26.25% slower**.
> This is expected for a very small test dataset because salting introduces
> additional aggregation overhead. The result is reported as measured rather
> than being presented as a guaranteed performance improvement.

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
