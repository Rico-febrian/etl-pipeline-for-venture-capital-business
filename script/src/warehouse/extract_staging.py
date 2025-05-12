import os
import requests
from pyspark.sql import SparkSession
from datetime import datetime
from dotenv import load_dotenv
from src.utils.helper import load_log_msg, staging_engine



def extract_staging(spark: SparkSession, table_name: str):
    """
    This function extracts data from a PostgreSQL staging database using JDBC
    and returns it as a Spark DataFrame. It also logs the extraction result
    (success or failure) to the logging database.

    Parameters:
        spark (SparkSession): The active Spark session.
        table_name (str): The name of the table to extract from the staging database.

    Returns:
        DataFrame: Spark DataFrame containing the extracted data.

    Raises:
        ValueError: If required config is missing or data extraction fails.
    """
    # Set current timestamp for logging
    current_timestamp = datetime.now()
    
    try:
        # Get staging DB config (URL, username, password)
        STG_DB_URL, STG_DB_USER, STG_DB_PASS = staging_engine()

        # Set JDBC connection properties
        connection_properties = {
            "user": STG_DB_USER,
            "password": STG_DB_PASS,
            "driver": "org.postgresql.Driver"
        }

        # Read table into DataFrame
        df = spark.read.jdbc(
            url=STG_DB_URL,
            table=table_name,
            properties=connection_properties
        )

        print(f"Extraction process successful for table: {table_name} from staging db")

        # Create success log
        log_message = spark.sparkContext.parallelize([
            ("staging", "extraction", "success", "staging_db", table_name, current_timestamp)
        ]).toDF(["step", "process", "status", "source", "table_name", "etl_date"])

        return df

    except NameError as e:
        raise ValueError(f"Missing required configuration: {e}")

    except Exception as e:
        print(f"Extraction process from staging db has failed: {e}")

        # Create failed log with error message
        log_message = spark.sparkContext.parallelize([
            ("staging", "extraction", "failed", "staging_db", table_name, current_timestamp, str(e))
        ]).toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])

    finally:
        # Save log message into log table
        load_log_msg(spark=spark, log_msg=log_message)