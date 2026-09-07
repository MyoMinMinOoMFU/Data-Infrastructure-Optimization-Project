import pytest
from pyspark.sql import SparkSession

from data_pipeline_optimizer import DataPipelineOptimizer


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("DataPipelineOptimizerTests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark
    spark.stop()


@pytest.fixture
def optimizer(spark):
	return DataPipelineOptimizer(spark)


def test_detect_skew(optimizer, spark):
	data = [
		("heavy", 1),
		("heavy", 2),
		("heavy", 3),
		("heavy", 4),
		("normal", 5),
		("normal", 6),
	]

	df = spark.createDataFrame(
		data,
		["user_id", "amount"],
	)

	result = optimizer.detect_skew(df, "user_id")

	assert result["total_records"] == 6
	assert result["distinct_keys"] == 2
	assert result["max_frequency"] == 4
	assert result["max_key_percentage"] == 66.67
	assert result["severity"] == "critical"
	assert result["skew_detected"] is True


def test_detect_skew_missing_column(optimizer, spark):
	df = spark.createDataFrame(
		[("u1", 100)],
		["user_id", "amount"],
	)

	with pytest.raises(ValueError):
		optimizer.detect_skew(df, "missing_column")


def test_repartitioning(optimizer, spark):
	data = [
		("u1", 100),
		("u2", 200),
		("u3", 300),
	]

	df = spark.createDataFrame(
		data,
		["user_id", "amount"],
	)

	result = optimizer.optimize_with_repartitioning(
		df,
		"user_id",
		num_partitions=4,
	)

	assert result.rdd.getNumPartitions() == 4


def test_salting(optimizer, spark):
	data = [
		("heavy", 100),
		("heavy", 200),
		("normal", 300),
	]

	df = spark.createDataFrame(
		data,
		["user_id", "amount"],
	)

	result = optimizer.mitigate_skew_with_salting(
		df,
		"user_id",
		num_salt_buckets=10,
	)

	assert "user_id_salted" in result.columns
	assert result.count() == 3


def test_broadcast_join(optimizer, spark):
	large_data = [
		("u1", 100),
		("u2", 200),
	]

	small_data = [
		("u1", "Alice"),
		("u2", "Bob"),
	]

	large_df = spark.createDataFrame(
		large_data,
		["user_id", "amount"],
	)

	small_df = spark.createDataFrame(
		small_data,
		["user_id", "customer_name"],
	)

	result = optimizer.optimize_broadcast_join(
		large_df,
		small_df,
		"user_id",
	)

	rows = result.collect()

	assert len(rows) == 2
	assert rows[0]["customer_name"] in {"Alice", "Bob"}
