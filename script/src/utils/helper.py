import os
import pyspark
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


