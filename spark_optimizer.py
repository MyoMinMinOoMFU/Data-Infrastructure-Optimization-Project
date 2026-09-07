"""
Spark Performance Optimization Toolkit.

This module provides utilities for:
- Analyzing data distribution and detecting skew.
- Optimizing DataFrame partitioning.
- Applying caching based on access patterns.
- Benchmarking Spark operations.

The implementation is designed for the data infrastructure
optimization project.
"""

import time
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


class SparkPerformanceOptimizer:
    """
    Toolkit for analyzing and improving Spark job performance.
    """

    def __init__(
        self,
        app_name: str = "DataInfrastructureOptimizer",
    ) -> None:
        """
        Initialize Spark session and performance tracking.

        Args:
            app_name: Name of the Spark application.
        """
        self.spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config(
                "spark.sql.adaptive.coalescePartitions.enabled",
                "true",
            )
            .getOrCreate()
        )

        self.performance_history: List[Dict[str, Any]] = []

    def analyze_data_distribution(
        self,
        df: DataFrame,
        key_columns: List[str],
    ) -> Dict[str, Any]:
        """
        Analyze data distribution for the specified key columns.

        The method calculates:
        - Number of records.
        - Number of distinct keys.
        - Most frequent key.
        - Maximum key frequency.
        - Average records per key.
        - Skew ratio.
        - Recommended action.

        Args:
            df: Spark DataFrame to analyze.
            key_columns: Columns used to evaluate distribution.

        Returns:
            Dictionary containing distribution and skew metrics.
        """
        if not key_columns:
            raise ValueError("key_columns must contain at least one column.")

        missing_columns = [
            column for column in key_columns if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Columns not found in DataFrame: {missing_columns}"
            )

        total_records = df.count()

        if total_records == 0:
            return {
                "total_records": 0,
                "key_columns": key_columns,
                "distinct_keys": 0,
                "max_key_frequency": 0,
                "average_records_per_key": 0.0,
                "skew_ratio": 0.0,
                "skew_detected": False,
                "recommendation": "Dataset is empty.",
            }

        key_counts = (
            df.groupBy(*key_columns)
            .count()
            .cache()
        )

        distinct_keys = key_counts.count()

        max_frequency_row = (
            key_counts
            .orderBy(F.desc("count"))
            .first()
        )

        max_frequency = (
            max_frequency_row["count"]
            if max_frequency_row
            else 0
        )

        average_records_per_key = (
            total_records / distinct_keys
            if distinct_keys
            else 0.0
        )

        skew_ratio = (
            max_frequency / average_records_per_key
            if average_records_per_key
            else 0.0
        )

        skew_detected = skew_ratio >= 5.0

        if skew_ratio >= 10.0:
            recommendation = (
                "Severe skew detected. Consider salting, "
                "custom partitioning, or key redistribution."
            )
        elif skew_ratio >= 5.0:
            recommendation = (
                "Moderate skew detected. Review partitioning "
                "and consider targeted skew mitigation."
            )
        else:
            recommendation = (
                "Distribution appears reasonably balanced."
            )

        key_counts.unpersist()

        return {
            "total_records": total_records,
            "key_columns": key_columns,
            "distinct_keys": distinct_keys,
            "max_key_frequency": max_frequency,
            "average_records_per_key": round(
                average_records_per_key,
                2,
            ),
            "skew_ratio": round(skew_ratio, 2),
            "skew_detected": skew_detected,
            "recommendation": recommendation,
        }

    def optimize_partitioning(
        self,
        df: DataFrame,
        partition_columns: List[str],
        target_partition_size_mb: int = 128,
    ) -> DataFrame:
        """
        Apply a partitioning strategy to a DataFrame.

        Args:
            df: Input Spark DataFrame.
            partition_columns: Columns used for partitioning.
            target_partition_size_mb:
                Target approximate partition size.

        Returns:
            Repartitioned DataFrame.
        """
        if not partition_columns:
            raise ValueError(
                "partition_columns must contain at least one column."
            )

        missing_columns = [
            column
            for column in partition_columns
            if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Columns not found in DataFrame: {missing_columns}"
            )

        if target_partition_size_mb <= 0:
            raise ValueError(
                "target_partition_size_mb must be greater than zero."
            )

        current_partitions = df.rdd.getNumPartitions()

        # Start with Spark's existing partition count and increase it
        # when the current parallelism is very low.
        target_partitions = max(
            current_partitions,
            self.spark.sparkContext.defaultParallelism,
        )

        optimized_df = df.repartition(
            target_partitions,
            *partition_columns,
        )

        return optimized_df

    def implement_smart_caching(
        self,
        df: DataFrame,
        access_pattern: str = "frequent",
    ) -> DataFrame:
        """
        Apply caching based on expected access frequency.

        Args:
            df: DataFrame to cache.
            access_pattern:
                "frequent", "moderate", or "rare".

        Returns:
            DataFrame with the selected persistence level.
        """
        pattern = access_pattern.lower()

        if pattern == "frequent":
            storage_level = StorageLevel.MEMORY_AND_DISK
        elif pattern == "moderate":
            storage_level = StorageLevel.MEMORY_AND_DISK
        elif pattern == "rare":
            return df
        else:
            raise ValueError(
                "access_pattern must be 'frequent', "
                "'moderate', or 'rare'."
            )

        return df.persist(storage_level)

    def benchmark_performance(
        self,
        operation_func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Benchmark a Spark operation.

        The operation is executed and its runtime is measured.

        Args:
            operation_func: Function representing the operation.
            *args: Positional arguments for operation_func.
            **kwargs: Keyword arguments for operation_func.

        Returns:
            Dictionary containing execution metrics.
        """
        start_time = time.perf_counter()

        try:
            result = operation_func(*args, **kwargs)

            # If the result is a DataFrame, force Spark execution.
            if isinstance(result, DataFrame):
                result_count = result.count()
            else:
                result_count = None

            end_time = time.perf_counter()

            duration_seconds = end_time - start_time

            benchmark_result = {
                "status": "success",
                "duration_seconds": round(
                    duration_seconds,
                    4,
                ),
                "result_count": result_count,
            }

        except Exception as exc:
            end_time = time.perf_counter()

            benchmark_result = {
                "status": "failed",
                "duration_seconds": round(
                    end_time - start_time,
                    4,
                ),
                "error": str(exc),
            }

        self.performance_history.append(
            benchmark_result
        )

        return benchmark_result

    def get_execution_plan(
        self,
        df: DataFrame,
    ) -> str:
        """
        Return the physical execution plan for a DataFrame.

        Args:
            df: Spark DataFrame.

        Returns:
            Formatted execution plan.
        """
        return df._jdf.queryExecution().executedPlan().toString()

    def stop(self) -> None:
        """
        Stop the Spark session.
        """
        self.spark.stop()


def main() -> None:
    """
    Run a small demonstration of the optimizer.
    """
    optimizer = SparkPerformanceOptimizer()

    sample_data = [
        ("user_1", 100.0, "us_west"),
        ("user_1", 200.0, "us_west"),
        ("user_1", 300.0, "us_west"),
        ("user_2", 50.0, "us_east"),
        ("user_3", 75.0, "eu_central"),
    ]

    columns = [
        "user_id",
        "amount",
        "region",
    ]

    df = optimizer.spark.createDataFrame(
        sample_data,
        columns,
    )

    print("=== Data Distribution Analysis ===")

    distribution = optimizer.analyze_data_distribution(
        df,
        ["user_id"],
    )

    print(distribution)

    print("\n=== Partitioning ===")

    optimized_df = optimizer.optimize_partitioning(
        df,
        ["region"],
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

    benchmark = optimizer.benchmark_performance(
        lambda data: data.groupBy("region").agg(
            F.sum("amount").alias("total_amount")
        ),
        optimized_df,
    )

    print(benchmark)

    print("\n=== Execution Plan ===")

    optimized_df.groupBy("region").agg(
        F.sum("amount").alias("total_amount")
    ).explain(True)

    optimizer.stop()


if __name__ == "__main__":
    main()