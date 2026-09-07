"""
Data Lake Transaction Management Toolkit.

This module provides:
- Delta Lake table creation.
- ACID transaction support.
- Version history.
- Time-travel queries.
- Rollback support.

The implementation is designed for the
Data Infrastructure Optimization Project.
"""

from typing import Any, Dict, Optional

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class DataLakeTransactionManager:
    """
    Manage ACID transactions and versioned Delta Lake tables.
    """

    def __init__(
        self,
        spark_session: SparkSession,
        base_path: str,
    ) -> None:
        """
        Initialize the transaction manager.

        Args:
            spark_session:
                Existing SparkSession configured for Delta Lake.
            base_path:
                Root directory where Delta tables are stored.
        """
        self.spark = spark_session
        self.base_path = base_path

    def _table_path(self, table_name: str) -> str:
        """
        Build the storage path for a Delta table.

        Args:
            table_name: Name of the Delta table.

        Returns:
            Full table storage path.
        """
        return f"{self.base_path}/{table_name}"

    def create_versioned_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
    ) -> str:
        """
        Create a Delta Lake table.

        Delta Lake provides ACID transaction support and
        automatically maintains table versions.

        Args:
            df:
                DataFrame to write.
            table_name:
                Name of the Delta table.
            mode:
                Write mode. Defaults to overwrite.

        Returns:
            Path of the created Delta table.
        """
        table_path = self._table_path(table_name)

        (
            df.write
            .format("delta")
            .mode(mode)
            .save(table_path)
        )

        return table_path

    def atomic_update(
        self,
        table_name: str,
        update_condition: str,
        update_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Perform an atomic update on a Delta Lake table.

        Delta Lake applies the update as a transaction and creates
        a new table version.

        Args:
            table_name:
                Name of the Delta table.
            update_condition:
                SQL condition identifying rows to update.
            update_data:
                Dictionary mapping column names to new values.

        Returns:
            Dictionary containing update information.
        """
        table_path = self._table_path(table_name)

        if not self.table_exists(table_name):
            raise ValueError(
                f"Delta table '{table_name}' does not exist."
            )

        if not update_data:
            raise ValueError(
                "update_data must contain at least one column."
            )

        delta_table = DeltaTable.forPath(
            self.spark,
            table_path,
        )

        update_expressions = {
            column: F.lit(value)
            for column, value in update_data.items()
        }

        delta_table.update(
            condition=update_condition,
            set=update_expressions,
        )

        latest_history = (
            delta_table.history(1)
            .select(
                "version",
                "timestamp",
                "operation",
                "operationMetrics",
            )
            .first()
        )

        return {
            "table_name": table_name,
            "condition": update_condition,
            "updated_columns": list(update_data.keys()),
            "version": latest_history["version"],
            "operation": latest_history["operation"],
        }

    def merge_transactions(
        self,
        table_name: str,
        source_df: DataFrame,
    ) -> Dict[str, Any]:
        """
        Merge incoming transaction data into a Delta table.

        Existing transactions are updated and new transactions
        are inserted.
        """
        table_path = self._table_path(table_name)

        if not self.table_exists(table_name):
            raise ValueError(
                f"Delta table '{table_name}' does not exist."
            )

        delta_table = DeltaTable.forPath(
            self.spark,
            table_path,
        )

        (
            delta_table.alias("target")
            .merge(
                source_df.alias("source"),
                "target.transaction_id = source.transaction_id",
            )
            .whenMatchedUpdate(
                set={
                    "user_id": "source.user_id",
                    "amount": "source.amount",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "transaction_id": "source.transaction_id",
                    "user_id": "source.user_id",
                    "amount": "source.amount",
                }
            )
            .execute()
        )

        latest_history = (
            delta_table.history(1)
            .select(
                "version",
                "operation",
                "operationMetrics",
            )
            .first()
        )

        return {
            "table_name": table_name,
            "version": latest_history["version"],
            "operation": latest_history["operation"],
            "operation_metrics": latest_history["operationMetrics"],
        }

    def read_table(
        self,
        table_name: str,
    ) -> DataFrame:
        """
        Read the current version of a Delta table.

        Args:
            table_name: Name of the Delta table.

        Returns:
            Current Delta table as a DataFrame.
        """
        table_path = self._table_path(table_name)

        return (
            self.spark.read
            .format("delta")
            .load(table_path)
        )

    def table_exists(
        self,
        table_name: str,
    ) -> bool:
        """
        Check whether a Delta table exists.

        Args:
            table_name: Name of the Delta table.

        Returns:
            True if the Delta table exists, otherwise False.
        """
        table_path = self._table_path(table_name)

        return DeltaTable.isDeltaTable(
            self.spark,
            table_path,
        )

    def get_history(
        self,
        table_name: str,
    ) -> DataFrame:
        """
        Return the transaction history of a Delta table.

        Args:
            table_name: Name of the Delta table.

        Returns:
            DataFrame containing Delta transaction history.
        """
        table_path = self._table_path(table_name)

        delta_table = DeltaTable.forPath(
            self.spark,
            table_path,
        )

        return delta_table.history()

    def time_travel_query(
        self,
        table_name: str,
        version: Optional[int] = None,
        timestamp: Optional[str] = None,
    ) -> DataFrame:
        """
        Query a historical version of a Delta table.

        Args:
            table_name:
                Name of the Delta table.
            version:
                Historical Delta version number.
            timestamp:
                Historical timestamp.

        Returns:
            Historical DataFrame.

        Raises:
            ValueError:
                If neither version nor timestamp is supplied,
                or if both are supplied.
        """
        if version is not None and timestamp is not None:
            raise ValueError(
                "Specify either version or timestamp, not both."
            )

        if version is None and timestamp is None:
            raise ValueError(
                "Specify either version or timestamp."
            )

        table_path = self._table_path(table_name)

        reader = (
            self.spark.read
            .format("delta")
        )

        if version is not None:
            return (
                reader
                .option("versionAsOf", version)
                .load(table_path)
            )

        return (
            reader
            .option("timestampAsOf", timestamp)
            .load(table_path)
        )

    def rollback_to_version(
        self,
        table_name: str,
        version: int,
    ) -> Dict[str, Any]:
        """
        Restore a Delta table to a previous version.

        Args:
            table_name:
                Name of the Delta table.
            version:
                Version to restore.

        Returns:
            Dictionary containing rollback information.
        """
        table_path = self._table_path(table_name)

        delta_table = DeltaTable.forPath(
            self.spark,
            table_path,
        )

        restore_result = delta_table.restoreToVersion(version)

        return {
            "table_name": table_name,
            "restored_version": version,
            "metrics": restore_result.collect()[0].asDict(),
        }


def create_spark_session() -> SparkSession:
    """
    Create a SparkSession configured for Delta Lake.

    Returns:
        SparkSession configured with Delta Lake dependencies.
    """
    builder = (
        SparkSession.builder
        .appName("DataLakeTransactionDemo")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(
        builder
    ).getOrCreate()


def main() -> None:
    """
    Demonstrate Delta Lake table creation and version history.
    """
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    manager = DataLakeTransactionManager(
        spark_session=spark,
        base_path="./delta_tables",
    )

    data = [
        ("tx_001", "u1", 100.0),
        ("tx_002", "u2", 200.0),
        ("tx_003", "u3", 300.0),
    ]

    df = spark.createDataFrame(
        data,
        ["transaction_id", "user_id", "amount"],
    )

    print("=== Creating Delta Table ===")

    table_path = manager.create_versioned_table(
        df,
        "transactions",
    )

    print("Delta table path:", table_path)

    print("\n=== Current Table ===")

    manager.read_table(
        "transactions"
    ).show()

    print("\n=== Table Exists ===")

    print(
        manager.table_exists("transactions")
    )

    print("\n=== Delta History ===")

    manager.get_history(
        "transactions"
    ).select(
        "version",
        "timestamp",
        "operation",
    ).show(
        truncate=False
    )

    print("\n=== Atomic Update ===")

    update_result = manager.atomic_update(
        table_name="transactions",
        update_condition="user_id = 'u1'",
        update_data={
            "amount": 150.0,
        },
    )

    print(update_result)

    print("\n=== Table After Atomic Update ===")

    manager.read_table(
        "transactions"
    ).show()

    print("\n=== Updated Delta History ===")

    manager.get_history(
        "transactions"
    ).select(
        "version",
        "timestamp",
        "operation",
    ).show(
        truncate=False
    )

    print("\n=== Delta Time Travel ===")

    historical_df = (
        spark.read
        .format("delta")
        .option("versionAsOf", 1)
        .load(table_path)
    )

    historical_df.show()

    print("\n=== MERGE / UPSERT ===")

    incoming_data = [
        ("tx_001", "u1", 175.0),
        ("tx_004", "u4", 400.0),
    ]

    incoming_df = spark.createDataFrame(
        incoming_data,
        ["transaction_id", "user_id", "amount"],
    )

    merge_result = manager.merge_transactions(
        table_name="transactions",
        source_df=incoming_df,
    )

    print(merge_result)

    print("\n=== Table After MERGE ===")

    manager.read_table(
        "transactions"
    ).orderBy(
        "transaction_id"
    ).show()

    print("\n=== Delta History After MERGE ===")

    manager.get_history(
        "transactions"
    ).select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics",
    ).show(
        truncate=False
    )

    print("\n=== Delta Rollback ===")

    rollback_result = manager.rollback_to_version(
        table_name="transactions",
        version=6,
    )

    print(rollback_result)

    print("\n=== Table After Rollback ===")

    manager.read_table(
        "transactions"
    ).orderBy(
        "transaction_id"
    ).show()

    print("\n=== Delta History After Rollback ===")

    manager.get_history(
        "transactions"
    ).select(
        "version",
        "timestamp",
        "operation",
    ).show(
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()