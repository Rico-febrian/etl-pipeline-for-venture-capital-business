import os
import requests
from pyspark.sql import SparkSession
from datetime import datetime
from dotenv import load_dotenv
from src.utils.helper import load_log_msg, source_engine



# Load environment variables from a .env file
load_dotenv("/home/jovyan/work/.env", override=True) 

def extract_database(spark: SparkSession, table_name: str):
    """
    This function extracts data from a PostgreSQL source database using JDBC
    and returns it as a Spark DataFrame. It also logs the extraction result
    (success or failure) to the logging database.

    Parameters:
        spark (SparkSession): The active Spark session.
        table_name (str): The name of the table to extract from the source database.

    Returns:
        DataFrame: Spark DataFrame containing the extracted data.

    Raises:
        ValueError: If required config is missing or data extraction fails.
    """
    # Set current timestamp for logging
    current_timestamp = datetime.now()
    
    try:
        # Get source DB config (URL, username, password)
        SOURCE_DB_URL, SOURCE_DB_USER, SOURCE_DB_PASS = source_engine()

        # Set JDBC connection properties
        connection_properties = {
            "user": SOURCE_DB_USER,
            "password": SOURCE_DB_PASS,
            "driver": "org.postgresql.Driver"
        }

        # Read table into DataFrame
        df = spark.read.jdbc(
            url=SOURCE_DB_URL,
            table=table_name,
            properties=connection_properties
        )

        print(f"Extraction process successful for table: {table_name}")

        # Create success log
        log_message = spark.sparkContext.parallelize([
            ("sources", "extraction", "success", "source_db", table_name, current_timestamp)
        ]).toDF(["step", "process", "status", "source", "table_name", "etl_date"])

        return df

    except NameError as e:
        raise ValueError(f"Missing required configuration: {e}")

    except Exception as e:
        print(f"Extraction process failed: {e}")

        # Create failed log with error message
        log_message = spark.sparkContext.parallelize([
            ("sources", "extraction", "failed", "source_db", table_name, current_timestamp, str(e))
        ]).toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])

    finally:
        # Save log message into log table
        load_log_msg(spark=spark, log_msg=log_message)
        
        

def extract_csv(spark: SparkSession, file_name: str):
    """
    This function reads a CSV file into a Spark DataFrame and logs the result (success or failure).

    Parameters:
        spark (SparkSession): The active Spark session.
        file_name (str): The name of the CSV file to load (must be inside the 'data/' folder).

    Returns:
        DataFrame: Spark DataFrame containing the extracted data.

    Raises:
        ValueError: If reading the CSV file fails.
    """
    # Set the base path where CSV files are stored
    path = "/home/jovyan/work/data/"

    # Set current timestamp for logging
    current_timestamp = datetime.now()

    try:
        # Read the CSV file into a DataFrame (expects header row)
        df = spark.read.csv(path + file_name, header=True)

        print(f"Extraction process successful for file: {file_name}")

        # Create success log
        log_message = spark.sparkContext.parallelize([
            ("sources", "extraction", "success", "csv", file_name, current_timestamp)
        ]).toDF(["step", "process", "status", "source", "table_name", "etl_date"])

        return df

    except Exception as e:
        print(f"Extraction process failed: {e}")

        # Create failed log with error message
        log_message = spark.sparkContext.parallelize([
            ("sources", "extraction", "failed", "csv", file_name, current_timestamp, str(e))
        ]).toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])

        # Raise as ValueError if extraction fails
        raise ValueError(f"Failed to extract CSV file: {e}")

    finally:
        # Save log message into log table
        load_log_msg(spark=spark, log_msg=log_message)
        



def extract_api(spark: SparkSession, start_date: str, end_date: str):
    """
    Extracts data from the Milestones API for the given date range and returns it as a Spark DataFrame.
    Also logs the status of the extraction (success/failure) into a log table.

    Parameters:
        spark (SparkSession): The Spark session object.
        start_date (str): The start date of the range in YYYY-MM-DD format.
        end_date (str): The end date of the range in YYYY-MM-DD format.

    Returns:
        DataFrame or None: Returns a Spark DataFrame if data exists, or None if no data or an error occurred.
    """

    # Set current timestamp for logging
    current_timestamp = datetime.now()

    try:
        # Get the base API URL from environment variable
        api_base_url = os.getenv("API_BASE_URL")
        if not api_base_url:
            raise ValueError("Missing required environment variable: API_BASE_URL")

        # Build full URL with query parameters
        url = f"{api_base_url}?start_date={start_date}&end_date={end_date}"

        # Send HTTP GET request to the API
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError if status is 4xx or 5xx

        # Convert JSON response to Python list
        data = response.json()

        # Check if the API returned any data
        if not data:
            print("There is no data in this range of date.")
            df = None
        else:
            print("Extraction process successful for milestones table.")
            df = spark.createDataFrame(data)

        # Set success log message
        log_message = spark.sparkContext.parallelize([
            ("sources", "extraction", "success", "api", "milestones", current_timestamp)
        ]).toDF(["step", "process", "status", "source", "table_name", "etl_date"])

    except Exception as e:
        # Print error and set df to None if extraction fails
        print(f"Extraction process failed: {e}")
        df = None

        # Create failed log with error message
        log_message = spark.sparkContext.parallelize([
            ("sources", "extraction", "failed", "api", "milestones", current_timestamp, str(e))
        ]).toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])

    finally:
        # Save log message into log table
        load_log_msg(spark=spark, log_msg=log_message)

    return df

