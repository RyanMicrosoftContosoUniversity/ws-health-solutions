# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "a2c23839-93e8-ad7b-44d7-6fea38ba86ac",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

from delta.tables import *
import yaml
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, TimestampType, BooleanType
import great_expectations as gx
from great_expectations.data_context import DataContext


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_path = r'abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/a9e521a9-66a4-497c-86f4-c23eed99d7df/Files/bronze_silver_address_template.yml'

file_content = spark.read.text(file_path).collect()
yaml_content = "\n".join([row.value for row in file_content])

data = yaml.safe_load(yaml_content)


print(data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data['table_metadata']['format']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data['table_metadata']['path']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# setup a gx data context and data source
context = gx.get_context()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# load table to validate
validated_df = spark.read.format(data['table_metadata']['format']).load(data['table_metadata']['path'])

display(validated_df)

df_gx = gx.dataset.SparkDFDataset(validated_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# validate column types and names

for record in data['table_metadata']['columns']:
    # check name
    df_gx.expect_column_to_exist(record['name'])

    # check type
    df_gx.expect_column_values_to_be_of_type(record['name'], record['type'])

    # check nulls
    if 'nullable' in record and record['nullable']==False:
        df_gx.expect_column_values_to_not_be_null(record['name'])

    # check if is in set
    if 'unique' in record and record['unique']:
        # check uniqueness
        df_gx.expect_column_values_to_be_unique(record['name'])
    
    if 'allowed_values' in record:
        df_gx.expect_column_values_to_be_in_set(record['name'], record['allowed_values'])



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run the validation
validation_results = df_gx.validate()

# Extract only the failed expectations
failed_expectations = [result for result in validation_results['results'] if not result['success']]

# Print the failed expectations
for failure in failed_expectations:
    print(failure)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create Expectations Suite

# CELL ********************

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig
from great_expectations.data_context import BaseDataContext
from great_expectations.core import ExpectationConfiguration



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

context_config = DataContextConfig(
    datasources={
        "fabric_spark_datasource": DatasourceConfig(
            class_name="Datasource",
            execution_engine={
                "class_name": "SparkDFExecutionEngine"
            },
            data_connectors={
                "default_runtime_connector":{
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["batch_id"]
                }
            }
        )
    },
    store_backend_defaults=gx.data_context.types.base.InMemoryStoreBackendDefaults()
)

# create a DataContext object from configuration
context = gx.get_context(project_config=context_config)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# connect to data

# load table to validate
validated_df = spark.read.format(data['table_metadata']['format']).load(data['table_metadata']['path'])

df_gx = gx.dataset.SparkDFDataset(validated_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create an expectation suite

suite_name = "address_suite"
suite = context.add_or_update_expectation_suite(expectation_suite_name=suite_name)


# Initialize lists to hold the expectation dictionaries
expect_column_to_exist_list = []
expect_column_values_to_be_of_type_list = []
expect_column_values_to_not_be_null_list = []
expect_column_values_to_be_unique_list = []
expect_column_values_to_be_in_set_list = []

# Build the expectation dictionaries
for record in data['table_metadata']['columns']:
    # Check name
    expect_column_to_exist_list.append({
        "expectation_type": "expect_column_to_exist",
        "kwargs": {"column": record['name']}
    })

    # Check type
    expect_column_values_to_be_of_type_list.append({
        "expectation_type": "expect_column_values_to_be_of_type",
        "kwargs": {"column": record['name'], "type_": record['type']}
    })

    # Check nulls
    if 'nullable' in record and record['nullable'] == False:
        expect_column_values_to_not_be_null_list.append({
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": record['name']}
        })

    # Check uniqueness
    if 'unique' in record and record['unique']:
        expect_column_values_to_be_unique_list.append({
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": record['name']}
        })

    # Check if is in set
    if 'allowed_values' in record:
        expect_column_values_to_be_in_set_list.append({
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {"column": record['name'], "value_set": record['allowed_values']}
        })

# build expectations dicts
expect_column_to_exist_dict = {
    "expectation_type": "expect_column_to_exist",
    "kwargs": {"column_list": expect_column_to_exist_list}
}

expect_column_values_to_be_of_type_dict = {
    "expectation_type": "expect_column_values_to_be_of_type",
    "kwargs": {"columns": expect_column_values_to_be_of_type_list}
}

expect_column_values_to_not_be_null_dict = {
    "expectation_type": "expect_column_values_to_not_be_null",
    "kwargs": {"column_list": expect_column_values_to_not_be_null_list}
}

expect_column_values_to_be_unique_dict = {
    "expectation_type": "expect_column_values_to_be_unique",
    "kwargs": {"column_list": expect_column_values_to_be_unique_list}
}

expect_column_values_to_be_in_set_dict = {
    "expectation_type": "expect_column_values_to_be_in_set",
    "kwargs": {"columns": expect_column_values_to_be_in_set_list}
}



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_great_expectations_suite_from_yaml(yaml_path:str, suite_name:str)->(suite, context):
    """
    Load metadata from a YAML file and build a Great Expectations suite
    using ExpectationConfiguration objects
    """
    ### load and parse YAML
    file_content = spark.read.text(yaml_path).collect()
    yaml_content = "\n".join([row.value for row in file_content])

    metadata = yaml.safe_load(yaml_content)

    table_metadata = metadata.get('table_metadata', {})
    columns = table_metadata.get('columns', [])

    ### get or create Great Expectations suite
    context_config = DataContextConfig(
    datasources={
        "fabric_spark_datasource": DatasourceConfig(
            class_name="Datasource",
            execution_engine={
                "class_name": "SparkDFExecutionEngine"
            },
            data_connectors={
                "default_runtime_connector":{
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["batch_id"]
                }
            }
        )
    },
    store_backend_defaults=gx.data_context.types.base.InMemoryStoreBackendDefaults()
    )

    # create a DataContext object from configuration
    context = gx.get_context(project_config=context_config)
    
    try:
        # try to create new suite name
        suite = context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    except Exception:
        # if suite already exists, get it
        suite = context.get_expectation_suite(suite_name)
    
    # collect new expectations in this list
    new_expectation_configurations = []

    ### build ExpectationConfiguration objects
    for col in columns:
        column_name = col.get('name')
        expectation_config = ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={
                "column": column_name
            },
            meta={
                "description": f"Check that column '{column_name}' exists. ",
                "notes":{
                    "source": "YAML metadata"
                }
            }
        )
        new_expectation_configurations.append(expectation_config)

    for col in columns:
        column_type = col.get('name')
        ge_type = col.get('type')
        
        expectation_config = ExpectationConfiguration(
            expectation_type = "expect_column_values_to_be_of_type",
            kwargs={
                "column": column_name,
                "type_": ge_type
            },
            meta={
                "description": f"Check that column '{column_name}' is of type '{ge_type}'. ",
                "notes": {
                    "source_data_type": ge_type
                }
            }
        )
        new_expectation_configurations.append(expectation_config)

    for col in columns:
        if col.get('nullable') is False:
            column_name = col.get('name')
            expectation_config = ExpectationConfiguration(
                expectation_type = "expect_column_values_to_not_be_null",
                kwargs={
                    "column": column_name,
                    "mostly": 1.0
                },
                meta={
                    "description": f"Check that column '{column_name}' is never null. ",
                    "notes": {
                        "source": "YAML metadata"
                    }
                } 
            )
            new_expectation_configurations.append(expectation_config)
    
    for col in columns:
        allowed_values = col.get('allowed_values')
        if allowed_values:
            column_name = col.get('name')
            expectation_config = ExpectationConfiguration(
                expectation_type= "expect_column_values_to_be_in_set",
                kwargs={
                    "column": column_name,
                    "value_set": col.get('allowed_values')
                },
                meta={
                    "description": f"""Check that column '{column_name}' only has values '{col.get("allowed_values")}' """,
                    "notes":{
                        "source": "YAML metadata"
                    }
                }
            )
            new_expectation_configurations.append(expectation_config)

    ### add all new expectations to the suite at once
    suite.expectations += new_expectation_configurations

    # save the updated suite
    context.save_expectation_suite(suite, suite_name)

    return suite, context



    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# test it out
suite, context = build_great_expectations_suite_from_yaml(yaml_path='abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/a9e521a9-66a4-497c-86f4-c23eed99d7df/Files/bronze_silver_address_template.yml', suite_name='address_suite')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

suite

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

batch_request = RuntimeBatchRequest(
    datasource_name="fabric_spark_datasource",
    data_connector_name="default_runtime_connector",
    data_asset_name="my_spark_data_asset",
    batch_identifiers={"batch_id": "test"},
    runtime_parameters={"batch_data": validated_df},
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

context.list_expectation_suites()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# debug why empty

validator.get_expectation_suite().expectations

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = validator.validate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

docs_paths = context.build_data_docs()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

context.open_data_docs()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

docs_paths

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Ignore Below
