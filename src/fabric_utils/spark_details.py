"""Utilities for retrieving Spark environment information."""

from pyspark.sql import SparkSession
from sempy_labs import admin
from sempy_labs import _spark as spark_sempy

from .logging_config import get_logger

logger = get_logger(__name__)


class SparkDetails:
    """Encapsulates Spark details retrieval methods."""

    def __init__(self) -> None:
        self.spark = SparkSession.builder.getOrCreate()

    @staticmethod
    def get_capacity_sku(capacity_name: str) -> str | None:
        """Return the SKU for the given capacity name."""
        admin_pd_df = admin.list_capacities()
        try:
            result = (
                admin_pd_df.loc[
                    admin_pd_df["Capacity Name"] == capacity_name, "Sku"
                ].tolist()[0]
            )
            logger.info("Retrieved SKU for capacity %s", capacity_name)
            logger.info("Capacity SKU: %s", result)


            return result
        except Exception as exc:  # pragma: no cover - thin wrapper
            logger.error("Exception on get_capacity_sku: %s", exc)
            return None

    @staticmethod
    def get_custom_pool_details(
        custom_pool_name: str,
    ) -> tuple[str, str, str, str] | None:
        """Return custom pool details for the provided pool name."""
        custom_pools_pd_df = spark_sempy.list_custom_pools()
        try:
            pool_df = custom_pools_pd_df[
                custom_pools_pd_df["Custom Pool Name"] == custom_pool_name
            ]
            node_size = pool_df["Node Size"].tolist()[0]
            auto_scale_enabled = pool_df["Auto Scale Enabled"].tolist()[0]
            auto_scale_min_node_count = pool_df[
                "Auto Scale Min Node Count"
            ].tolist()[0]
            auto_scale_max_node_count = pool_df[
                "Auto Scale Max Node Count"
            ].tolist()[0]
            logger.info("Retrieved custom pool details for %s", custom_pool_name)

            logger.info(
                "Custom pool details: Node Size: %s, Auto Scale Enabled: %s, "
                "Min Node Count: %s, Max Node Count: %s",
                node_size,
                auto_scale_enabled,
                auto_scale_min_node_count,
                auto_scale_max_node_count,
            )

            return (
                node_size,
                auto_scale_enabled,
                auto_scale_min_node_count,
                auto_scale_max_node_count,
            )
        except Exception as exc:  # pragma: no cover - thin wrapper
            logger.error("Error retrieving custom pool details: %s", exc)
            return None

    def get_spark_details(self) -> dict[str, str]:
        """Return relevant Spark configuration settings."""
        conf = self.spark.conf
        details = {
            "spark.driver.cores": conf.get("spark.driver.cores"),
            "spark.driver.memory": conf.get("spark.driver.memory"),
            "spark.executor.cores": conf.get("spark.executor.cores"),
            "spark.executor.memory": conf.get("spark.executor.memory"),
            "spark.dynamicAllocation.enabled": conf.get(
                "spark.dynamicAllocation.enabled"
            ),
            "spark.dynamicAllocation.minExecutors": conf.get(
                "spark.dynamicAllocation.minExecutors"
            ),
            "spark.dynamicAllocation.maxExecutors": conf.get(
                "spark.dynamicAllocation.maxExecutors"
            ),
            "spark.native.enabled": conf.get("spark.native.enabled"),
        }
        logger.info("Retrieved Spark configuration details")
        logger.info("Spark details: %s", details)
        return details
