import os
import pyspark
import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime


# ---------- Load Environment Variables ---------- #

load_dotenv("/home/jovyan/work/.env", override=True)

SOURCE_DB_HOST=os.getenv("SOURCE_DB_HOST")
SOURCE_DB_USER=os.getenv("SOURCE_DB_USER")
SOURCE_DB_PASS=os.getenv("SOURCE_DB_PASS")
SOURCE_DB_NAME=os.getenv("SOURCE_DB_NAME")
SOURCE_DB_PORT=os.getenv("SOURCE_DB_PORT")

STG_DB_HOST=os.getenv("STG_DB_HOST")
STG_DB_USER=os.getenv("STG_DB_USER")
STG_DB_PASS=os.getenv("STG_DB_PASS")
STG_DB_NAME=os.getenv("STG_DB_NAME")
STG_DB_PORT=os.getenv("STG_DB_PORT")

DWH_DB_HOST=os.getenv("DWH_DB_HOST")
DWH_DB_USER=os.getenv("DWH_DB_USER")
DWH_DB_PASS=os.getenv("DWH_DB_PASS")
DWH_DB_NAME=os.getenv("DWH_DB_NAME")
DWH_DB_PORT=os.getenv("DWH_DB_PORT")

LOG_DB_HOST=os.getenv("LOG_DB_HOST")
LOG_DB_USER=os.getenv("LOG_DB_USER")
LOG_DB_PASS=os.getenv("LOG_DB_PASS")
LOG_DB_NAME=os.getenv("LOG_DB_NAME")
LOG_DB_PORT=os.getenv("LOG_DB_PORT")

MINIO_ENDPOINT=os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY")



# ---------- Database Connection Functions ---------- #

def source_engine():
    """
    This function builds the JDBC URL (Java Database Connectivity) for connecting to a source PostgreSQL database 
    using predefined variables.
    
    Returns:
        tuple: A tuple containing three items:
            - SOURCE_DB_URL (str): The JDBC URL string used to connect to the database.
            - SOURCE_DB_USER (str): The username for the database.
            - SOURCE_DB_PASS (str): The password for the database.
    
    Raises:
        ValueError: If any of the required variables are not defined or there is an error building the config.                    
    """
    try:
        # Build the JDBC URL string using the host, port, and database name
        SOURCE_DB_URL = f"jdbc:postgresql://{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}"

        # Return the URL, username, and password
        return SOURCE_DB_URL, SOURCE_DB_USER, SOURCE_DB_PASS

    except NameError as e:
        # Raised when one of the variables is not defined at all
        raise ValueError(f"Missing required configuration: {e}")

    except Exception as e:
        # Catch any unexpected error
        raise ValueError(f"Error building source database config: {e}")
 
    

def staging_engine():
    """
    This function builds the JDBC URL (Java Database Connectivity) for connecting to a staging PostgreSQL database 
    using predefined variables.
    
    Returns:
        tuple: A tuple containing three items:
            - STG_DB_URL (str): The JDBC URL string used to connect to the database.
            - STG_DB_USER (str): The username for the database.
            - STG_DB_PASS (str): The password for the database.
    
    Raises:
        ValueError: If any of the required variables are not defined or there is an error building the config.                    
    """
    try:
        # Build the JDBC URL string using the host, port, and database name
        STG_DB_URL = f"jdbc:postgresql://{STG_DB_HOST}:{STG_DB_PORT}/{STG_DB_NAME}"
        
        # Return the URL, username, and password
        return STG_DB_URL, STG_DB_USER, STG_DB_PASS 

    except NameError as e:
        # Raised when one of the variables is not defined at all
        raise ValueError(f"Missing required configuration: {e}")

    except Exception as e:
        # Catch any unexpected error
        raise ValueError(f"Error building source database config: {e}")



def dwh_engine():
    """
    This function builds the JDBC URL (Java Database Connectivity) for connecting to a warehouse PostgreSQL database 
    using predefined variables.
    
    Returns:
        tuple: A tuple containing three items:
            - DWH_DB_URL (str): The JDBC URL string used to connect to the database.
            - DWH_DB_USER (str): The username for the database.
            - DWH_DB_PASS (str): The password for the database.
    
    Raises:
        ValueError: If any of the required variables are not defined or there is an error building the config.                   
    """
    try:
        # Build the JDBC URL string using the host, port, and database name
        DWH_DB_URL = f"jdbc:postgresql://{DWH_DB_HOST}:{DWH_DB_PORT}/{DWH_DB_NAME}"
        
        # Return the URL, username, and password
        return DWH_DB_URL, DWH_DB_USER, DWH_DB_PASS 

    except NameError as e:
        # Raised when one of the variables is not defined at all
        raise ValueError(f"Missing required configuration: {e}")

    except Exception as e:
        # Catch any unexpected error
        raise ValueError(f"Error building source database config: {e}")



def log_engine():
    """
    This function builds the JDBC URL (Java Database Connectivity) for connecting to a log PostgreSQL database 
    using predefined variables.
    
    Returns:
        tuple: A tuple containing three items:
            - LOG_DB_URL (str): The JDBC URL string used to connect to the database.
            - LOG_DB_USER (str): The username for the database.
            - LOG_DB_PASS (str): The password for the database.
    
    Raises:
        ValueError: If any of the required variables are not defined or there is an error building the config.        
    """
    try:
        # Build the JDBC URL string using the host, port, and database name
        LOG_DB_URL = f"jdbc:postgresql://{LOG_DB_HOST}:{LOG_DB_PORT}/{LOG_DB_NAME}"
        
        # Return the URL, username, and password
        return LOG_DB_URL, LOG_DB_USER, LOG_DB_PASS 

    except NameError as e:
        # Raised when one of the variables is not defined at all
        raise ValueError(f"Missing required configuration: {e}")

    except Exception as e:
        # Catch any unexpected error
        raise ValueError(f"Error building source database config: {e}")



def staging_engine_sqlalchemy():
    """
    This function builds a SQLAlchemy connection engine for connecting to a staging PostgreSQL database
    using predefined variables.

    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine object used to interact with the database.

    Raises:
        ValueError: If any of the required variables are not defined or there is an error building the engine.
    """
    try:
        # Build the SQLAlchemy connection string and return the engine
        return create_engine(f"postgresql://{STG_DB_USER}:{STG_DB_PASS}@{STG_DB_HOST}:{STG_DB_PORT}/{STG_DB_NAME}")

    except NameError as e:
        # Raised if any variable is not defined
        raise ValueError(f"Missing required configuration: {e}")

    except Exception as e:
        # Catch other unexpected errors
        raise ValueError(f"Error creating SQLAlchemy engine: {e}")



def dwh_engine_sqlalchemy():
    """
    This function builds a SQLAlchemy connection engine for connecting to a warehouse PostgreSQL database
    using predefined variables.

    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine object used to interact with the database.

    Raises:
        ValueError: If any of the required variables are not defined or there is an error building the engine.
    """
    try:
        # Build the SQLAlchemy connection string and return the engine
        return create_engine(f"postgresql://{DWH_DB_USER}:{DWH_DB_PASS}@{DWH_DB_HOST}:{DWH_DB_PORT}/{DWH_DB_NAME}")

    except NameError as e:
        # Raised if any variable is not defined
        raise ValueError(f"Missing required configuration: {e}")

    except Exception as e:
        # Catch other unexpected errors
        raise ValueError(f"Error creating SQLAlchemy engine: {e}")



# ---------- Logging Functions ---------- #

def load_log_msg(spark: SparkSession, log_msg: pyspark.sql.DataFrame):
    """
    This function loads a log message DataFrame into the 'etl_log' table of a PostgreSQL database using JDBC.

    Parameters:
        spark (SparkSession): The active Spark session.
        log_msg (DataFrame): The DataFrame containing the log messages to write.

    Raises:
        ValueError: If any required variable is not defined or if the write operation fails.
    """
    # Get the JDBC connection info (URL, username, password)
    LOG_DB_URL, LOG_DB_USER, LOG_DB_PASS = log_engine()

    # Validate connection parameters before setting up the connection
    if not LOG_DB_URL or not LOG_DB_USER or LOG_DB_PASS is None:
        raise ValueError("Database connection parameters cannot be None/null")
    
    table_name = "etl_log"

    try:

        # Set the connection properties for JDBC
        connection_properties = {
            "user": LOG_DB_USER,
            "password": LOG_DB_PASS,
            "driver": "org.postgresql.Driver"
        }

        # Write the log messages to the database table using append mode
        log_msg.write.jdbc(
            url=LOG_DB_URL,
            table=table_name,
            mode="append",
            properties=connection_properties
        )

    except NameError as e:
        # Raised if any required variable is not defined
        raise ValueError(f"Missing required configuration: {e}")

    except Exception as e:
        # Catch all other errors (e.g., JDBC connection failure)
        raise ValueError(f"Failed to write log messages to database: {e}")



# ---------- Transformation Functions ---------- #


def clean_address(col_name: str):
    """
    Cleans address values in a DataFrame column through the following steps:
    1. Removing special characters '#' or '.' at the beginning of the string.
    2. Converting the entire string to lowercase for standardization.
    3. Identifying and replacing potentially invalid values with NULL.
       A value is considered invalid if it consists solely of symbols and/or numbers,
       or if its length after trimming leading and trailing spaces is less than or equal to 2 characters.

    Parameters:
        col_name (str): The name of the column containing the address values to clean.

    Returns:
        Column: A PySpark Column containing the cleaned address values.
                Invalid values will be replaced with NULL.
    """

    # Step 1: Convert to lowercase and remove '#' or '.' characters at the start of the string.
    # Example: '#Main St' becomes 'main st', '.Apartment 1A' becomes 'apartment 1a'
    cleaned = F.regexp_replace(F.lower(F.col(col_name)), r"^[#.]+", "")

    # Step 2: Define conditions to identify invalid values.

    # Condition 1: Check if the value (after step 1) consists solely of non-word characters
    #              (symbols, spaces, punctuation), digits (numbers), or underscores.
    #              Examples of values considered invalid: '??', '.323' (after removal becomes '323'), '------', ' !? '
    is_only_symbols = cleaned.rlike(r"^[\W\d_]+$")

    # Condition 2: Check if the length of the value (after step 1 and trimming) is too short.
    #              Values with a length of 2 characters or less after trimming are considered invalid.
    #              Examples of values considered invalid: 'a', ' b ', ''
    is_too_short = F.length(F.trim(cleaned)) <= 2

    # Step 3: Apply the cleaning logic.
    # If a value meets either of the invalid conditions (only symbols or too short),
    # then replace it with NULL. Otherwise, return the cleaned and trimmed value.
    cleaned_data = F.when(
        is_only_symbols | is_too_short,
        F.lit(None)  # Replace invalid values with NULL
    ).otherwise(
        F.trim(cleaned)  # Keep and trim valid values
    )

    return cleaned_data



def clean_name(col_name: str):
    """

    Parameters:
        col_name (str): The name of the column containing the address values to clean.

    Returns:
        Column: A PySpark Column containing the cleaned address values.
                Invalid values will be replaced with NULL.
    """
    # Clean leading non-alphabetic characters like #. or digits
    cleaned = F.regexp_replace(F.lower(F.col(col_name)), r"^[^a-z]+", "")
    
    # Remove any remaining weird characters not allowed (keep only a-z, space, hyphen)
    cleaned = F.regexp_replace(cleaned, r"[^a-z\s\-]", "")
    
    # Condition 1: string is only symbols/numbers after cleaning
    is_only_symbols = cleaned.rlike(r"^[^a-z]+$")
    
    # Condition 2: too short
    is_too_short = F.length(F.trim(cleaned)) <= 2
    
    # Condition 3: starts with a number or special char (extra safe)
    starts_invalid = F.col(col_name).rlike(r"^\s*[\d\W_]+")
    
    # Final decision
    cleaned_data = F.when(
        is_only_symbols | is_too_short | starts_invalid,
        F.lit(None)
    ).otherwise(
        F.trim(cleaned)
    )
    
    return cleaned_data




def extract_warehouse(spark: SparkSession, table_name):
    """
    """
    
    # get config
    DWH_DB_URL, DWH_DB_USER, DWH_DB_PASS = dwh_engine()

    # set config
    connection_properties = {
        "user": DWH_DB_USER,
        "password": DWH_DB_PASS,
        "driver": "org.postgresql.Driver" # set driver postgres
    }
    
    try:
        # read data
        df = spark \
                .read \
                .jdbc(url = DWH_DB_URL,
                        table = table_name,
                        properties = connection_properties)
        return df
    except Exception as e:
        print(e)



def to_usd(currency_col, amount_col):
    """
    """

    exchange_rate = F.round(
        F.when(F.col(currency_col) == "USD", F.col(amount_col))
         .when(F.col(currency_col) == "CAD", F.col(amount_col) * 0.72)
         .when(F.col(currency_col) == "EUR", F.col(amount_col) * 1.14)
         .when(F.col(currency_col) == "SEK", F.col(amount_col) * 0.10)
         .when(F.col(currency_col) == "AUD", F.col(amount_col) * 0.64)
         .when(F.col(currency_col) == "JPY", F.col(amount_col) * 0.007)
         .when(F.col(currency_col) == "GBP", F.col(amount_col) * 1.33)
         .when(F.col(currency_col) == "NIS", F.col(amount_col) * 0.28)
         .otherwise(F.col(amount_col)),
        2
    )

    return exchange_rate




# ---------- Data Profiling Functions ---------

# Check Percentage of Missing Values for each column with pyspark
def check_missing_values(df):
    """
    Check the percentage of missing values in each column of a PySpark DataFrame.
    Including: 
        - NULL
        - Empty string
        - NaN
        
    Args:
        df (DataFrame): The input PySpark DataFrame.
        
    Returns:
        dict: Dictionary with column names as keys and missing value percentages as values.
    """

    # Get total data in DataFrame
    total_data = df.count()
    result = {}

    for col_name, dtype in df.dtypes:

        is_null = F.col(col_name).isNull()
        is_empty_string = F.col(col_name) == ""
        is_nan = F.isnan(F.col(col_name)) if dtype in ["double", "float"] else F.lit(False)

        missing_condition = is_null | is_empty_string | is_nan

        # Count missing values
        missing_count = df.filter(missing_condition).count()

        # Get missing values percentage
        missing_values_pctg = round((missing_count / total_data) * 100, 2) 

        result[col_name] = missing_values_pctg

    return result



# Create a function to save the final report to a JSON file
def save_to_json(dict_result: dict, filename: str) -> None:
    """
    This function saves the data profiling result to a JSON file.

    Args:
        dict_result (dict): Data profiling result to save to a JSON file.
        filename (str): Name of the JSON file to save the data profiling result to.

    Returns:
        None
    """

    try:
        
        # Save the data profiling result to a JSON file
        with open(f'{filename}.json', 'w') as file:
            file.write(json.dumps(dict_result, indent= 4))
    
    except Exception as e:
        print(f"Error: {e}")