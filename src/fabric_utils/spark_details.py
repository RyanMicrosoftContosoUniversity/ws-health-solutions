"""
The following code provides utility functions to retrieve details about Azure Synapse Spark environments, capacities, and custom pools.
"""
from sempy_labs import _capacities as capacities
from sempy_labs import admin
from sempy_labs import _environments as environments
from sempy_labs import _spark as spark_sempy


class SparkDetails:
    """
    A class to encapsulate Spark details retrieval methods.
    """

    def __init__(self):
        pass


@staticmethod
def getCapacitySKU(capacity_name:str)->str:
    """
    Given a Capacity Name (case sensitive) return the SKU if found

    capacity_name:str: Name of the Capacity

    returns:str: SKU of the Capacity
    """
    admin_pd_df = admin.list_capacities()

    try:
        results = admin_pd_df[admin_pd_df['Capacity Name']==current_capacity_name]['Sku'].tolist()[0]
    except Exception as e:
        print(f'Exception on getCapacitySKU: {e}')

@staticmethod
def getCustomPoolDetails(custom_pool_name:str)->tuple[str, str, str, str]:
    """
    Given a custom pool name (case sensitive) return the relevant details

    custom_pool_name:str: The name of the custom pool
    """
    custom_pools_pd_df = spark_sempy.list_custom_pools()

    try:
        node_size = custom_pools_pd_df[custom_pools_pd_df['Custom Pool Name']==custom_pool_name]['Node Size'].tolist()[0]
        auto_scale_enabled = custom_pools_pd_df[custom_pools_pd_df['Custom Pool Name']==custom_pool_name]['Auto Scale Enabled'].tolist()[0]
        auto_scale_min_node_count = custom_pools_pd_df[custom_pools_pd_df['Custom Pool Name']==custom_pool_name]['Auto Scale Min Node Count'].tolist()[0]
        auto_scale_max_node_count = custom_pools_pd_df[custom_pools_pd_df['Custom Pool Name']==custom_pool_name]['Auto Scale Max Node Count'].tolist()[0]
    except Exception as e:
        print(f'Error retrieving custom pool details: {e}')

@staticmethod
def getSparkDetails():
    """
    Get Spark Details like
    Spark Driver Core
    Spark Driver Memory
    Spark Executor Cores
    Spark Executor Memory
    Dynamically Allocate Executors Enabled
    Minimum Number of Executors
    Maximum Number of Executors
    If Native Execution Engine is Enabled
    """
    spark.conf.get('spark.driver.cores')
    spark.conf.get('spark.driver.memory')
    spark.conf.get('spark.executor.cores')
    spark.conf.get('spark.executor.memory')
    spark.conf.get('spark.dynamicAllocation.enabled')
    spark.conf.get('spark.dynamicAllocation.minExecutors')
    spark.conf.get('spark.dynamicAllocation.maxExecutors')
    spark.conf.get('spark.native.enabled')
