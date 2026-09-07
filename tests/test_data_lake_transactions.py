import pytest

from data_lake_transactions import DataLakeTransactionManager


@pytest.fixture(scope="session")
def spark():
	from data_lake_transactions import create_spark_session

	spark = create_spark_session()
	spark.sparkContext.setLogLevel("ERROR")

	yield spark

	spark.stop()


@pytest.fixture
def manager(spark, tmp_path):
	return DataLakeTransactionManager(
		spark_session=spark,
		base_path=str(tmp_path),
	)


@pytest.fixture
def transactions_df(spark):
	data = [
		("tx_001", "u1", 100.0),
		("tx_002", "u2", 200.0),
		("tx_003", "u3", 300.0),
	]

	return spark.createDataFrame(
		data,
		["transaction_id", "user_id", "amount"],
	)


def test_create_versioned_table(manager, transactions_df):
	table_path = manager.create_versioned_table(
		transactions_df,
		"transactions",
	)

	assert table_path.endswith("/transactions")
	assert manager.table_exists("transactions") is True


def test_read_table(manager, transactions_df):
	manager.create_versioned_table(
		transactions_df,
		"transactions",
	)

	result = manager.read_table("transactions")

	assert result.count() == 3
	assert set(result.columns) == {
		"transaction_id",
		"user_id",
		"amount",
	}


def test_atomic_update(manager, transactions_df):
	manager.create_versioned_table(
		transactions_df,
		"transactions",
	)

	result = manager.atomic_update(
		table_name="transactions",
		update_condition="user_id = 'u1'",
		update_data={"amount": 150.0},
	)

	assert result["table_name"] == "transactions"
	assert result["condition"] == "user_id = 'u1'"
	assert result["updated_columns"] == ["amount"]
	assert result["operation"] == "UPDATE"

	updated = manager.read_table("transactions")
	amount = (
		updated
		.filter("user_id = 'u1'")
		.select("amount")
		.first()["amount"]
	)

	assert amount == 150.0


def test_atomic_update_missing_table(manager):
	with pytest.raises(ValueError):
		manager.atomic_update(
			table_name="missing",
			update_condition="user_id = 'u1'",
			update_data={"amount": 150.0},
		)


def test_atomic_update_empty_data(manager, transactions_df):
	manager.create_versioned_table(
		transactions_df,
		"transactions",
	)

	with pytest.raises(ValueError):
		manager.atomic_update(
			table_name="transactions",
			update_condition="user_id = 'u1'",
			update_data={},
		)


def test_merge_transactions(manager, transactions_df, spark):
	manager.create_versioned_table(
		transactions_df,
		"transactions",
	)

	incoming_data = [
		("tx_001", "u1", 175.0),
		("tx_004", "u4", 400.0),
	]

	incoming_df = spark.createDataFrame(
		incoming_data,
		["transaction_id", "user_id", "amount"],
	)

	result = manager.merge_transactions(
		"transactions",
		incoming_df,
	)

	assert result["operation"] == "MERGE"

	current = manager.read_table("transactions")

	assert current.count() == 4

	tx_001 = (
		current
		.filter("transaction_id = 'tx_001'")
		.select("amount")
		.first()["amount"]
	)

	assert tx_001 == 175.0

	assert (
		current
		.filter("transaction_id = 'tx_004'")
		.count()
		== 1
	)


def test_get_history(manager, transactions_df):
	manager.create_versioned_table(
		transactions_df,
		"transactions",
	)

	history = manager.get_history("transactions")

	assert history.count() >= 1
	assert "version" in history.columns
	assert "operation" in history.columns


def test_time_travel_requires_argument(manager):
	with pytest.raises(ValueError):
		manager.time_travel_query("transactions")


def test_time_travel_rejects_both_arguments(manager):
	with pytest.raises(ValueError):
		manager.time_travel_query(
			"transactions",
			version=0,
			timestamp="2026-01-01",
		)


def test_time_travel_by_version(manager, transactions_df):
	manager.create_versioned_table(
		transactions_df,
		"transactions",
	)

	manager.atomic_update(
		table_name="transactions",
		update_condition="user_id = 'u1'",
		update_data={"amount": 150.0},
	)

	historical = manager.time_travel_query(
		"transactions",
		version=0,
	)

	amount = (
		historical
		.filter("user_id = 'u1'")
		.select("amount")
		.first()["amount"]
	)

	assert amount == 100.0


def test_rollback_to_version(manager, transactions_df):
	manager.create_versioned_table(
		transactions_df,
		"transactions",
	)

	manager.atomic_update(
		table_name="transactions",
		update_condition="user_id = 'u1'",
		update_data={"amount": 150.0},
	)

	result = manager.rollback_to_version(
		"transactions",
		version=0,
	)

	assert result["table_name"] == "transactions"
	assert result["restored_version"] == 0

	current = manager.read_table("transactions")

	amount = (
		current
		.filter("user_id = 'u1'")
		.select("amount")
		.first()["amount"]
	)

	assert amount == 100.0
