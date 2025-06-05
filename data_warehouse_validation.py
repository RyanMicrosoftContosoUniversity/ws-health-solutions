# Data Warehouse Validation
"""Use this script to count rows in each table and view of a Microsoft Fabric Data Warehouse."""

from pyspark.sql import SparkSession, Row
import argparse


def count_dw_tables(dw_name: str, dw_location: str, save_path: str | None = None):
    """Return counts of all tables and views in the given Data Warehouse.

    Optionally save the results to ``save_path``.
    """
    spark = SparkSession.builder.getOrCreate()
    # Ensure the warehouse is mounted
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dw_name} LOCATION '{dw_location}'")
    tables_df = spark.sql(f"SHOW TABLES IN {dw_name}")
    views_df = spark.sql(f"SHOW VIEWS IN {dw_name}")
    results = []
    for row in tables_df.collect():
        table = row.tableName
        count = spark.read.table(f"{dw_name}.{table}").count()
        results.append(Row(objectType="table", name=table, rowCount=count))
    for row in views_df.collect():
        view = row.viewName
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {dw_name}.{view}").collect()[0].cnt
        results.append(Row(objectType="view", name=view, rowCount=count))
    results_df = spark.createDataFrame(results)
    if save_path:
        results_df.toPandas().to_csv(save_path, index=False)
    return results_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate Fabric Data Warehouse row counts.")
    parser.add_argument("--name", required=True, help="Name of the Fabric Data Warehouse")
    parser.add_argument("--location", required=True, help="abfss location of the Data Warehouse")
    parser.add_argument("--output", help="Optional path to save results CSV")
    args = parser.parse_args()

    df = count_dw_tables(args.name, args.location, args.output)
    df.show(truncate=False)
