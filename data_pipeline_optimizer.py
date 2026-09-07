"""
Data Pipeline Bottleneck Resolution Toolkit.

This module provides utilities for:
- Detecting data skew.
- Analyzing partition distribution.
- Identifying shuffle-heavy operations.
- Optimizing joins.
- Benchmarking before/after performance.
"""

import time
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class DataPipelineOptimizer:
    """
    Toolkit for diagnosing and optimizing Spark data pipelines.
    """

    def __init__(
        self,
        spark_session: SparkSession,
    ) -> None:
        """
        Initialize the pipeline optimizer.

        Args:
            spark_session: Existing SparkSession.
        """
        self.spark = spark_session
        self.optimization_history: List[Dict[str, Any]] = []

    def analyze_partition_distribution(
        self,
        df: DataFrame,
    ) -> Dict[str, Any]:
        """
        Analyze the number of records in each Spark partition.

        Args:
            df: Spark DataFrame to analyze.

        Returns:
            Dictionary containing partition statistics.
        """
        partition_sizes = (
            df.rdd
            .mapPartitions(lambda rows: [sum(1 for _ in rows)])
            .collect()
        )

        if not partition_sizes:
            return {
                "partition_count": 0,
                "min_records": 0,
                "max_records": 0,
                "average_records": 0.0,
                "skew_ratio": 0.0,
                "skew_detected": False,
            }

        partition_count = len(partition_sizes)
        min_records = min(partition_sizes)
        max_records = max(partition_sizes)

        average_records = (
            sum(partition_sizes) / partition_count
        )

        skew_ratio = (
            max_records / average_records
            if average_records > 0
            else 0.0
        )

        return {
            "partition_count": partition_count,
            "min_records": min_records,
            "max_records": max_records,
            "average_records": round(
                average_records,
                2,
            ),
            "skew_ratio": round(
                skew_ratio,
                2,
            ),
            "skew_detected": skew_ratio >= 5.0,
        }

    def detect_skew(
        self,
        df: DataFrame,
        key_column: str,
    ) -> Dict[str, Any]:
        """
        Detect key-level data skew.

        Args:
            df: Input DataFrame.
            key_column: Column used to evaluate skew.

        Returns:
            Skew analysis results.
        """
        if key_column not in df.columns:
            raise ValueError(
                f"Column '{key_column}' not found in DataFrame."
            )

        total_records = df.count()

        if total_records == 0:
            return {
                "key_column": key_column,
                "total_records": 0,
                "distinct_keys": 0,
                "max_frequency": 0,
                "max_key_percentage": 0.0,
                "average_frequency": 0.0,
                "skew_ratio": 0.0,
                "severity": "none",
                "skew_detected": False,
            }

        key_counts = (
            df.groupBy(key_column)
            .count()
        )

        statistics = (
            key_counts
            .agg(
                F.count("*").alias("distinct_keys"),
                F.max("count").alias("max_frequency"),
                F.avg("count").alias("average_frequency"),
            )
            .first()
        )

        distinct_keys = statistics["distinct_keys"]
        max_frequency = statistics["max_frequency"] or 0
        average_frequency = statistics["average_frequency"] or 0.0

        max_key_percentage = (
            max_frequency / total_records
        ) * 100

        skew_ratio = (
            max_frequency / average_frequency
            if average_frequency > 0
            else 0.0
        )

        if max_key_percentage >= 50:
            severity = "critical"
        elif max_key_percentage >= 30:
            severity = "high"
        elif max_key_percentage >= 20:
            severity = "moderate"
        else:
            severity = "low"

        return {
            "key_column": key_column,
            "total_records": total_records,
            "distinct_keys": distinct_keys,
            "max_frequency": max_frequency,
            "max_key_percentage": round(
                max_key_percentage,
                2,
            ),
            "average_frequency": round(
                average_frequency,
                2,
            ),
            "skew_ratio": round(
                skew_ratio,
                2,
            ),
            "severity": severity,
            "skew_detected": max_key_percentage >= 30,
        }

    def optimize_with_repartitioning(
        self,
        df: DataFrame,
        partition_column: str,
        num_partitions: Optional[int] = None,
    ) -> DataFrame:
        """
        Repartition a DataFrame using a specified column.

        Args:
            df: Input DataFrame.
            partition_column: Column used for repartitioning.
            num_partitions: Optional target partition count.

        Returns:
            Repartitioned DataFrame.
        """
        if partition_column not in df.columns:
            raise ValueError(
                f"Column '{partition_column}' not found."
            )

        if num_partitions is None:
            num_partitions = self.spark.sparkContext.defaultParallelism

        return df.repartition(
            num_partitions,
            partition_column,
        )

    def mitigate_skew_with_salting(
        self,
        df: DataFrame,
        key_column: str,
        num_salt_buckets: int = 10,
    ) -> DataFrame:
        """
        Mitigate severe data skew using key salting.

        A random salt value is added to the skewed key so that records
        belonging to the same key can be distributed across multiple
        partitions.

        Args:
            df: Input DataFrame.
            key_column: Column containing the skewed key.
            num_salt_buckets: Number of salt buckets to create.

        Returns:
            DataFrame containing the salted key column.
        """
        if key_column not in df.columns:
            raise ValueError(
                f"Column '{key_column}' not found."
            )

        if num_salt_buckets <= 0:
            raise ValueError(
                "num_salt_buckets must be greater than zero."
            )

        salted_column = f"{key_column}_salted"

        return df.withColumn(
            salted_column,
            F.concat(
                F.col(key_column).cast("string"),
                F.lit("_"),
                F.floor(
                    F.rand(seed=42) * num_salt_buckets
                ).cast("int"),
            ),
        )

    def broadcast_join(
        self,
        large_df: DataFrame,
        small_df: DataFrame,
        join_column: str,
    ) -> DataFrame:
        """
        Perform a broadcast join between a large and small DataFrame.

        Args:
            large_df: Large DataFrame.
            small_df: Small DataFrame.
            join_column: Join key.

        Returns:
            Joined DataFrame.
        """
        return large_df.join(
            F.broadcast(small_df),
            on=join_column,
            how="inner",
        )

    def optimize_broadcast_join(
        self,
        large_df: DataFrame,
        small_df: DataFrame,
        join_column: str,
    ) -> DataFrame:
        """
        Join a large DataFrame with a small DataFrame using broadcast.

        Broadcasting sends the small DataFrame to each executor so that
        Spark can perform the join without shuffling the large DataFrame.

        Args:
            large_df: Large DataFrame.
            small_df: Small lookup/reference DataFrame.
            join_column: Column used for the join.

        Returns:
            Joined DataFrame.
        """
        if join_column not in large_df.columns:
            raise ValueError(
                f"Join column '{join_column}' "
                "not found in large DataFrame."
            )

        if join_column not in small_df.columns:
            raise ValueError(
                f"Join column '{join_column}' "
                "not found in small DataFrame."
            )

        return large_df.join(
            F.broadcast(small_df),
            on=join_column,
            how="left",
        )

    def analyze_execution_plan(
        self,
        df: DataFrame,
    ) -> Dict[str, Any]:
        """
        Analyze the physical execution plan and identify
        potential performance bottlenecks.

        Args:
            df: Spark DataFrame to analyze.

        Returns:
            Dictionary containing detected issues and recommendations.
        """
        plan = df._jdf.queryExecution().executedPlan().toString()

        recommendations = []
        detected_issues = []

        if "Exchange" in plan:
            detected_issues.append("shuffle_detected")
            recommendations.append(
                "Shuffle detected. Consider repartitioning, "
                "broadcast joins, or reducing unnecessary data movement."
            )

        if "BroadcastHashJoin" in plan:
            detected_issues.append("broadcast_join")
            recommendations.append(
                "Broadcast join detected. Verify that the broadcasted "
                "dataset remains small enough for executor memory."
            )

        if "SortMergeJoin" in plan:
            detected_issues.append("sort_merge_join")
            recommendations.append(
                "Sort-merge join detected. Consider broadcast join "
                "when one side of the join is sufficiently small."
            )

        if "AdaptiveSparkPlan" in plan:
            detected_issues.append("adaptive_execution")
            recommendations.append(
                "Adaptive Query Execution is enabled and can dynamically "
                "optimize partitioning and join strategies."
            )

        if not detected_issues:
            recommendations.append(
                "No major shuffle or join bottlenecks detected "
                "in the current physical plan."
            )

        return {
            "detected_issues": detected_issues,
            "recommendations": recommendations,
            "plan": plan,
        }

    def benchmark_skew_mitigation(
        self,
        df: DataFrame,
        key_column: str,
        num_salt_buckets: int = 10,
    ) -> Dict[str, Any]:
        """
        Compare aggregation performance before and after salting.

        Args:
            df: Input DataFrame containing a skewed key.
            key_column: Column containing the skewed key.
            num_salt_buckets: Number of salt buckets.

        Returns:
            Dictionary containing before/after benchmark metrics.
        """
        if key_column not in df.columns:
            raise ValueError(
                f"Column '{key_column}' not found in DataFrame."
            )

        # Benchmark original aggregation.
        start_time = time.perf_counter()

        (
            df.groupBy(key_column)
            .agg(F.sum("amount").alias("total_amount"))
            .count()
        )

        before_duration = time.perf_counter() - start_time

        # Create salted DataFrame.
        salted_df = self.mitigate_skew_with_salting(
            df,
            key_column,
            num_salt_buckets,
        )

        salted_column = f"{key_column}_salted"

        # Benchmark salted aggregation followed by
        # a final aggregation back to the original key.
        start_time = time.perf_counter()

        salted_aggregation = (
            salted_df.groupBy(salted_column)
            .agg(
                F.sum("amount").alias("partial_amount")
            )
        )

        (
            salted_aggregation
            .withColumn(
                key_column,
                F.regexp_extract(
                    F.col(salted_column),
                    r"^(.*)_\d+$",
                    1,
                ),
            )
            .groupBy(key_column)
            .agg(
                F.sum("partial_amount").alias("total_amount")
            )
            .count()
        )

        after_duration = time.perf_counter() - start_time

        improvement = (
            ((before_duration - after_duration) / before_duration) * 100
            if before_duration > 0
            else 0.0
        )

        return {
            "before_duration_seconds": round(
                before_duration,
                4,
            ),
            "after_duration_seconds": round(
                after_duration,
                4,
            ),
            "improvement_percent": round(
                improvement,
                2,
            ),
            "num_salt_buckets": num_salt_buckets,
        }

    def benchmark(
        self,
        operation,
        *args: Any,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Benchmark a Spark operation.

        Args:
            operation: Callable Spark operation.
            *args: Operation arguments.
            **kwargs: Operation keyword arguments.

        Returns:
            Benchmark metrics.
        """
        start_time = time.perf_counter()

        try:
            result = operation(*args, **kwargs)

            if isinstance(result, DataFrame):
                result_count = result.count()
            else:
                result_count = None

            duration = time.perf_counter() - start_time

            metrics = {
                "status": "success",
                "duration_seconds": round(
                    duration,
                    4,
                ),
                "result_count": result_count,
            }

        except Exception as exc:
            duration = time.perf_counter() - start_time

            metrics = {
                "status": "failed",
                "duration_seconds": round(
                    duration,
                    4,
                ),
                "error": str(exc),
            }

        self.optimization_history.append(metrics)

        return metrics
    
def main() -> None:
    """
    Run a demonstration of pipeline bottleneck analysis.
    """
    spark = (
        SparkSession.builder
        .appName("PipelineBottleneckDemo")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    data = [
        ("user_1", 100.0),
        ("user_1", 200.0),
        ("user_1", 300.0),
        ("user_1", 400.0),
        ("user_2", 50.0),
        ("user_3", 75.0),
    ]

    df = spark.createDataFrame(
        data,
        ["user_id", "amount"],
    )

    optimizer = DataPipelineOptimizer(spark)

    print("=== Partition Distribution ===")

    partition_analysis = (
        optimizer.analyze_partition_distribution(df)
    )

    print(partition_analysis)

    print("\n=== Key Skew Analysis ===")

    skew_analysis = optimizer.detect_skew(
        df,
        "user_id",
    )

    print(skew_analysis)

    print("\n=== Repartitioning ===")

    optimized_df = optimizer.optimize_with_repartitioning(
        df,
        "user_id",
    )

    print(
        "Original partitions:",
        df.rdd.getNumPartitions(),
    )

    print(
        "Optimized partitions:",
        optimized_df.rdd.getNumPartitions(),
    )

    print("\n=== Benchmark ===")

    benchmark = optimizer.benchmark(
        lambda data: data.groupBy("user_id").agg(
            F.sum("amount").alias("total_amount")
        ),
        optimized_df,
    )

    print(benchmark)

    print("\n=== Skew Mitigation ===")

    skewed_data = (
        [("heavy_key", 1.0)] * 80
        + [("key_2", 1.0)] * 10
        + [("key_3", 1.0)] * 10
    )

    skewed_df = spark.createDataFrame(
        skewed_data,
        ["user_id", "amount"],
    )

    print("Before salting:")

    before_skew = optimizer.detect_skew(
        skewed_df,
        "user_id",
    )

    print(before_skew)

    salted_df = optimizer.mitigate_skew_with_salting(
        skewed_df,
        "user_id",
        num_salt_buckets=10,
    )

    print("\nSalted key distribution:")

    salted_df.select(
        "user_id",
        "user_id_salted",
    ).groupBy(
        "user_id_salted"
    ).count().orderBy(
        F.desc("count")
    ).show()

    print("\n=== Skew Mitigation Benchmark ===")

    skew_benchmark = optimizer.benchmark_skew_mitigation(
        skewed_df,
        "user_id",
        num_salt_buckets=10,
    )

    print(skew_benchmark)

    print("\n=== Broadcast Join ===")

    transactions = spark.createDataFrame(
        [
            ("u1", 100.0),
            ("u2", 200.0),
            ("u3", 300.0),
            ("u1", 150.0),
            ("u2", 250.0),
        ],
        ["user_id", "amount"],
    )

    customers = spark.createDataFrame(
        [
            ("u1", "Alice", "US"),
            ("u2", "Bob", "UK"),
            ("u3", "Charlie", "TH"),
        ],
        ["user_id", "customer_name", "country"],
    )

    broadcast_result = optimizer.optimize_broadcast_join(
        transactions,
        customers,
        "user_id",
    )

    broadcast_result.show()

    print("=== Broadcast Join Execution Plan ===")

    broadcast_result.explain(True)

    print("\n=== Execution Plan Analysis ===")

    plan_analysis = optimizer.analyze_execution_plan(
        broadcast_result
    )

    print("Detected issues:")
    for issue in plan_analysis["detected_issues"]:
        print("-", issue)

    print("\nRecommendations:")
    for recommendation in plan_analysis["recommendations"]:
        print("-", recommendation)

    spark.stop()


if __name__ == "__main__":
    main()