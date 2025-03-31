# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1ac4412c-84bb-4fe9-8629-68d6faf28eaf",
# META       "default_lakehouse_name": "metadataLH",
# META       "default_lakehouse_workspace_id": "c0a7b8a9-eb12-495a-b863-2cb583e31154",
# META       "known_lakehouses": [
# META         {
# META           "id": "1ac4412c-84bb-4fe9-8629-68d6faf28eaf"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType
from pyspark.sql.functions import monotonically_increasing_id


schema = StructType([
    StructField("id", StringType(), True),
    StructField("source_dir", StringType(), True),
    StructField("target_dir", StringType(), True),
    StructField("source_folder", StringType(), True),
    StructField("total_files_processed", LongType(), True),
    StructField("processed_dttm", TimestampType(), True)
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = []
df = spark.createDataFrame(data, schema)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format('delta').saveAsTable('dicom_ingest_hist')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
