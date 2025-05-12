from datetime import datetime
from sqlalchemy import text
from pyspark.sql import SparkSession, DataFrame
from src.utils.helper import load_log_msg, dwh_engine, dwh_engine_sqlalchemy

def load_to_dwh(spark: SparkSession, df: DataFrame, table_name: str, source_name: str):
    """
    Load a transformed data from staging into a PostgreSQL warehouse database.

    This function performs two main operations:
    1. Truncates the target warehouse table by deleting all existing rows, while resetting the identity column (primary key).
    2. Loads the transformed data into the target table via JDBC.

    Additionally, the function logs the success or failure of each operation to a logging table.

    Parameters:
        spark (SparkSession): The active Spark session.
        df (DataFrame): The Spark DataFrame containing the data to be loaded into the warehouse table.
        table_name (str): The name of the target warehouse table in the PostgreSQL database.
        source_name (str): The name of the source system used for logging the data origin.

    Raises:
        RuntimeError: If any operation (truncation or data loading) fails, an error will be raised after logging the failure.
    """

    # Set current timestamp for logging
    current_timestamp = datetime.now()

    try:
        # Establish a connection to the warehouse database using SQLAlchemy
        conn = dwh_engine_sqlalchemy()

        with conn.begin() as connection:
            # Truncate the target table and reset the identity column (primary key)
            connection.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))

        print(f"Success truncating table: {table_name}")

    except Exception as e:
        # If truncation fails, log the error and re-raise the exception
        print(f"Error when truncating table: {e}")

        # Log the failure event with error details
        log_message = spark.sparkContext.parallelize([("warehouse", "load", "failed", source_name, table_name, current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])

        load_log_msg(spark=spark, log_msg=log_message)

        # Raise a runtime error to indicate truncation failure
        raise RuntimeError(f"Truncation failed for table {table_name}") from e

    finally:
        # Ensure the connection is closed after the operation
        if 'conn' in locals() and conn:
            conn.dispose()

    try:
        # Retrieve PostgreSQL JDBC connection details
        DWH_DB_URL, DWH_DB_USER, DWH_DB_PASS = dwh_engine()

        # Define JDBC properties for connection
        properties = {
            "user": DWH_DB_USER,
            "password": DWH_DB_PASS,
        }

        # Load the DataFrame into the warehouse table using JDBC
        df.write.jdbc(url=DWH_DB_URL,
                      table=table_name,
                      mode="append",  # Append data to the existing table
                      properties=properties)

        print(f"Load process successful for table: {table_name}")

        # Log the success event for the data load operation
        log_message = spark.sparkContext.parallelize([("warehouse", "load", "success", source_name, table_name, current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])

        load_log_msg(spark=spark, log_msg=log_message)

    except Exception as e:
        # If data loading fails, log the error and re-raise the exception
        print(f"Load process failed: {e}")

        # Log the failure event with error details
        log_message = spark.sparkContext.parallelize([("warehouse", "load", "failed", source_name, table_name, current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])

        load_log_msg(spark=spark, log_msg=log_message)

        # Raise a runtime error to indicate loading failure
        raise RuntimeError(f"Data load failed for table {table_name}") from e
