from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from src.warehouse.extract_staging import extract_staging
from src.utils.helper import load_log_msg, clean_address, clean_name, extract_warehouse, to_usd
from datetime import datetime


def transform_company(spark: SparkSession, df: DataFrame):
    """
    Transform raw company data into a cleaned and standardized dimension table.

    Args:
        spark: SparkSession
        df: Raw input DataFrame containing company data from staging db

    Returns:
        DataFrame: Cleaned and transformed company dimension table
    """

    # Set up current timestamp for logging
    current_timestamp = datetime.now()

    try:
        # Add a new column 'entity_type' to identify if the record is a 'company' or a 'fund'
        # based on the 'object_id' prefix.
        df = df.withColumn(
            "entity_type",
            F.when(F.col("object_id").startswith("c:"), "company")
             .when(F.col("object_id").startswith("f:"), "fund")
             .otherwise(None)
        )

        # Create cleaned versions of address columns by applying the 'clean_address' function.
        df = df.withColumn("address1_cleaned", clean_address(col_name="address1")) \
               .withColumn("address2_cleaned", clean_address(col_name="address2"))

        # Create the 'full_address' column by concatenating 'address1_cleaned' and 'address2_cleaned'.
        # It handles cases where one or both address columns are null or empty.
        df = df.withColumn(
            "full_address",
            F.when(
                (F.col("address1_cleaned").isNull()) & (F.col("address2_cleaned").isNull()),
                F.lit(None)
            ).when(
                (F.col("address1_cleaned").isNull()) | (F.col("address1_cleaned") == ""),
                F.col("address2_cleaned")
            ).when(
                (F.col("address2_cleaned").isNull()) | (F.col("address2_cleaned") == ""),
                F.col("address1_cleaned")
            ).otherwise(
                F.concat_ws(", ", F.col("address1_cleaned"), F.col("address2_cleaned"))
            )
        )

        # Standardize the values for 'region', 'city', and 'country_code' by:
        # 1. Trimming leading/trailing whitespace.
        # 2. Converting 'region' and 'city' to lowercase.
        # 3. Converting 'country_code' to uppercase.
        region_cleaned = F.trim(F.lower(F.col("region")))
        city_cleaned = F.trim(F.lower(F.col("city")))
        country_code_cleaned = F.trim(F.upper(F.col("country_code")))

        # Update the 'region', 'city', and 'country_code' columns with the cleaned values,
        # setting them to null if the cleaned value is null or empty.
        df = df.withColumn(
            "region",
            F.when(
                (region_cleaned.isNull()) | (region_cleaned == ""), F.lit(None)
            ).otherwise(region_cleaned)
        ).withColumn(
            "city",
            F.when(
                (city_cleaned.isNull()) | (city_cleaned == ""), F.lit(None)
            ).otherwise(city_cleaned)
        ).withColumn(
            "country_code",
            F.when(
                (country_code_cleaned.isNull()) | (country_code_cleaned == ""), F.lit(None)
            ).otherwise(country_code_cleaned)
        )

        # Select the necessary columns for the dimension table and rename 'object_id' to 'nk_company_id'
        # as the natural key.
        dim_company = df.select(
            F.col("object_id").alias("nk_company_id"),
            F.col("entity_type"),
            F.col("full_address"),
            F.col("region"),
            F.col("city"),
            F.col("country_code")
        )

        print("Transformation process successful for table: dim_company")

        # Log a success message with details about the ETL process.
        log_message = spark.sparkContext \
            .parallelize([("warehouse", "transform", "success", "staging", "dim_company", current_timestamp)]) \
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])

        return dim_company

    except Exception as e:
        
        # Log an error message with details about the failure.
        print(e)
        log_message = spark.sparkContext \
            .parallelize([("warehouse", "transform", "failed", "staging", "dim_company", current_timestamp, str(e))]) \
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])

    finally:
        # Save log message into log table
        load_log_msg(spark=spark, log_msg=log_message)




def transform_people(spark, df):
    """
    Transform raw people data into a cleaned and standardized dimension table.

    Args:
        spark: SparkSession
        df: Raw input DataFrame containing people data from staging db

    Returns:
        DataFrame: Cleaned and transformed people dimension table
    """

    # Set up current timestamp for logging
    current_timestamp = datetime.now()

    try:
        # Add cleaned versions of first and last name, using the clean_name function.
        df = df.withColumn("firstname_cleaned", clean_name(col_name="first_name")) \
               .withColumn("lastname_cleaned", clean_name(col_name="last_name"))

        # Create the 'full_name' column by concatenating cleaned first and last names.
        # Handles cases where one or both cleaned names are null or empty.
        df = df.withColumn(
            "full_name",
            F.when(
                (F.col("firstname_cleaned").isNull()) & (F.col("lastname_cleaned").isNull()),
                F.lit(None)
            ).when(
                (F.col("firstname_cleaned").isNull()) | (F.col("firstname_cleaned") == ""),
                F.col("lastname_cleaned")
            ).when(
                (F.col("lastname_cleaned").isNull()) | (F.col("lastname_cleaned") == ""),
                F.col("firstname_cleaned")
            ).otherwise(
                F.concat_ws(" ", F.col("firstname_cleaned"), F.col("lastname_cleaned"))
            )
        )

        # Standardize affiliation name by trimming whitespace and converting to lowercase.
        df = df.withColumn(
            "affiliation_name",
            F.trim(F.lower(F.col("affiliation_name")))
        )

        # Select the columns for the dimension table.
        # Rename 'object_id' to 'nk_people_id'.
        dim_people = df.select(
            F.col("object_id").alias("nk_people_id"),
            F.col("full_name"),
            F.col("affiliation_name")
        )

        print("Transformation process successful for table: dim_people")

        # Log a success message with details about the ETL process.
        log_message = spark.sparkContext \
            .parallelize([("warehouse", "transform", "success", "staging", "dim_people", current_timestamp)]) \
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])

        return dim_people

    except Exception as e:
        
        # Log an error message with details about the failure.
        print(e)
        log_message = spark.sparkContext \
            .parallelize([("warehouse", "transform", "failed", "staging", "dim_people", current_timestamp, str(e))]) \
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])

    finally:
        # Save log message into log table
        load_log_msg(spark=spark, log_msg=log_message)




def transform_funds(spark, df):
    """
    Transform raw funds data into a cleaned and standardized dimension table.

    Args:
        spark: SparkSession
        df: Raw input DataFrame containing fund data from staging db

    Returns:
        DataFrame: Cleaned and transformed funds dimension table
    """

    # Set up current timestamp for logging
    current_timestamp = datetime.now()

    try:
        # Extract the dim_date dimension table.
        dim_date = extract_warehouse(spark, table_name="dim_date")

        # Standardize 'name' and 'source_description' by trimming whitespace and converting to lowercase.
        df = df.withColumn("name", F.trim(F.lower(F.col("name")))) \
               .withColumn("source_description", F.trim(F.lower(F.col("source_description"))))

        # Convert raised amount to USD using the to_usd function.
        df = df.withColumn("raised_amount_usd", to_usd(currency_col="raised_currency_code", amount_col="raised_amount"))

        # Add a foreign key 'funded_date_id' by formatting 'funded_at' to match the 'date_id' in dim_date.
        df = df.withColumn(
            "funded_date_id",
            F.date_format(df.funded_at, "yyyyMMdd").cast("integer")
        )

        # Join with dim_date to get date information based on 'funded_date_id'.
        df = df.join(
            dim_date,
            df.funded_date_id == dim_date.date_id,
            "left"
        )

        # Remove empty strings from 'fund_description' and set them to NULL.
        df = df.withColumn(
            "source_description",
            F.when(F.trim(df.source_description) == "", None)
              .otherwise(df.source_description)
        )

        # Select the columns for the dimension table and rename for clarity.
        dim_fund = df.select(
            F.col("object_id").alias("nk_fund_id"),
            F.col("name").alias("fund_name"),
            F.col("raised_amount_usd"),
            F.col("funded_date_id").alias("funded_at"),
            F.col("source_description").alias("fund_description")
        )

        print("Transformation process successful for table: dim_fund")

        # Log a success message with details about the ETL process.
        log_message = spark.sparkContext \
            .parallelize([("warehouse", "transform", "success", "staging", "dim_fund", current_timestamp)]) \
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])

        return dim_fund

    except Exception as e:
        
        # Log an error message with details about the failure.
        print(e)
        log_message = spark.sparkContext \
            .parallelize([("warehouse", "transform", "failed", "staging", "dim_fund", current_timestamp, str(e))]) \
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])

    finally:
        # Save log message into log table
        load_log_msg(spark=spark, log_msg=log_message)




def transform_relationships(spark, df):
    """
    Transform raw relationships data into a cleaned and standardized dimension table.

    Args:
        spark: SparkSession
        df: Raw input DataFrame containing relationships data from staging db

    Returns:
        DataFrame: Cleaned and transformed bridge people company dimension table
    """

    # Set up current timestamp for logging
    current_timestamp = datetime.now()

    try:
        # Extract dimension tables needed for joins.
        dim_date = extract_warehouse(spark, table_name="dim_date")
        dim_company = extract_warehouse(spark, table_name="dim_company")
        dim_people = extract_warehouse(spark, table_name="dim_people")

        # Join with dim_company to get the company's surrogate key (sk_company_id).
        df = df.join(
            dim_company.select("sk_company_id", "nk_company_id"),
            df.relationship_object_id == dim_company.nk_company_id,
            "inner"
        )

        # Join with dim_people to get the person's surrogate key (sk_people_id).
        df = df.join(
            dim_people.select("sk_people_id", "nk_people_id"),
            df.person_object_id == dim_people.nk_people_id,
            "inner"
        )

        # Convert start and end dates to integer format (yyyyMMdd) for consistency.
        df = df.withColumn("relationship_start_at", F.date_format("start_at", "yyyyMMdd").cast("integer")) \
               .withColumn("relationship_end_at", F.date_format("end_at", "yyyyMMdd").cast("integer"))

        # Alias the dim_date table for joining on start and end dates.
        dim_date_start = dim_date.alias("start_date")
        dim_date_end = dim_date.alias("end_date")

        # Join with dim_date to get date information for start and end dates.
        df = df.join(
            dim_date_start,
            df.relationship_start_at == F.col("start_date.date_id"),
            "inner"
        ).join(
            dim_date_end,
            df.relationship_end_at == F.col("end_date.date_id"),
            "inner"
        )

        # Clean the 'title' column by trimming whitespace, converting to lowercase, and setting '.' to NULL.
        df = df.withColumn(
            "title",
            F.when(F.col("title") == ".", F.lit(None))
             .otherwise(F.trim(F.lower(F.col("title"))))
        )

        # Select the columns for the bridge table.
        bridge_company_people = df.select(
            F.col("sk_company_id"),
            F.col("sk_people_id"),
            F.col("title"),
            F.col("is_past"),
            F.col("relationship_start_at"),
            F.col("relationship_end_at")
        )

        print("Transformation process successful for table: bridge_company_people")

        # Log a success message for the ETL process.
        log_message = spark.sparkContext \
            .parallelize([("warehouse", "transform", "success", "staging", "bridge_company_people", current_timestamp)]) \
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])

        return bridge_company_people

    except Exception as e:
        
        # Log an error message with details about the failure.
        print(e)
        log_message = spark.sparkContext \
            .parallelize([("warehouse", "transform", "failed", "staging", "bridge_company_people", current_timestamp, str(e))]) \
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])

    finally:
        # Save log message into log table
        load_log_msg(spark=spark, log_msg=log_message)




def transform_investments(spark, df):
    """
    Transform raw investments and funding rounds data into a cleaned and standardized fact table.

    Args:
        spark: SparkSession
        df: Raw input DataFrame containing investments data from staging db

    Returns:
        DataFrame: Cleaned and transformed investments fact table
    """

    # Set up current timestamp for logging
    current_timestamp = datetime.now()

    try:
        # Extract dimension tables needed for joins.
        dim_date = extract_warehouse(spark, table_name="dim_date")
        dim_company = extract_warehouse(spark, table_name="dim_company")
        dim_funds = extract_warehouse(spark, table_name="dim_funds")
        stg_funding_rounds = extract_staging(spark, table_name="funding_rounds")

        # Join with dim_company to get the company's surrogate key (sk_company_id).
        df = df.join(
            dim_company.select("sk_company_id", "nk_company_id"),
            df.funded_object_id == dim_company.nk_company_id,
            "inner"
        )

        # Join with dim_fund to get the fund's surrogate key (sk_fund_id).
        df = df.join(
            dim_funds.select("sk_fund_id", "nk_fund_id"),
            df.investor_object_id == dim_funds.nk_fund_id,
            "inner"
        )

        # Prepare staging funding rounds data.
        stg_funding_rounds = stg_funding_rounds.withColumn(
            "funded_at",
            F.date_format("funded_at", "yyyyMMdd").cast("integer")
        )

        # Join with dim_date to get date_id.
        stg_funding_rounds = stg_funding_rounds.join(
            dim_date.select("date_id"),
            stg_funding_rounds.funded_at == dim_date.date_id,
            "inner"
        )

        # Join with the funding rounds staging table to get additional information.
        df = df.join(
            stg_funding_rounds.select(
                "funding_round_id", "funding_round_type", "participants",
                "raised_amount_usd", "raised_currency_code",
                "pre_money_valuation_usd", "post_money_valuation_usd",
                "funded_at"
            ),
            on="funding_round_id",
            how="left"
        )

        # Select the columns for the fact table and rename for clarity.
        fct_investments = df.select(
            F.col("investment_id").alias("dd_investment_id"),
            F.col("sk_company_id"),
            F.col("sk_fund_id"),
            F.col("funded_at"),
            F.col("funding_round_type"),
            F.col("participants").alias("num_of_participants"),
            F.col("raised_amount_usd"),
            F.col("pre_money_valuation_usd"),
            F.col("post_money_valuation_usd"),
        )

        print("Transformation process successful for table: fct_investments")

        # Log a success message for the ETL process.
        log_message = spark.sparkContext \
            .parallelize([("warehouse", "transform", "success", "staging", "fct_investments", current_timestamp)]) \
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])

        return fct_investments

    except Exception as e:
        
        # Log an error message with details about the failure.
        print(e)
        log_message = spark.sparkContext \
            .parallelize([("warehouse", "transform", "failed", "staging", "fct_investments", current_timestamp, str(e))]) \
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])

    finally:
        # Save log message into log table
        load_log_msg(spark=spark, log_msg=log_message)




def transform_ipos(spark, df):
    """
    Transform raw IPOs data into a cleaned and standardized fact table.

    Args:
        spark: SparkSession
        df: Raw input DataFrame containing ipos data from staging db

    Returns:
        DataFrame: Cleaned and transformed ipos fact table
    """
    
    # Set up current timestamp for logging
    current_timestamp = datetime.now()

    try:
        # Extract dimension tables needed for joins.
        dim_date = extract_warehouse(spark, table_name="dim_date")
        dim_company = extract_warehouse(spark, table_name="dim_company")

        # Cast 'ipo_id' to integer.
        df = df.withColumn("ipo_id", F.col("ipo_id").cast("integer"))

        # Join with dim_company to get the company's surrogate key (sk_company_id).
        df = df.join(
            dim_company.select("sk_company_id", "nk_company_id"),
            df.object_id == dim_company.nk_company_id,
            "inner"
        )

        # Add a foreign key 'public_date_id' by formatting 'public_at' to match 'date_id' in dim_date.
        df = df.withColumn(
            "public_date_id",
            F.date_format(df.public_at, "yyyyMMdd").cast("integer")
        )

        # Join with dim_date to get date information based on 'public_date_id'.
        df = df.join(
            dim_date,
            df.public_date_id == dim_date.date_id,
            "left"
        )

        # Convert valuation and raised amounts to USD.
        df = df.withColumn("valuation_amount_usd", to_usd(currency_col="valuation_currency_code", amount_col="valuation_amount"))
        df = df.withColumn("raised_amount_usd", to_usd(currency_col="raised_currency_code", amount_col="raised_amount"))

        # Clean and normalize the stock symbol.
        cleaned_stock_symbol = F.trim(F.lower(F.col("stock_symbol")))
        invalid_symbol = cleaned_stock_symbol.rlike(r"^[\W\d_]+$")  # Identify invalid symbols
        cleaned_data = F.when(invalid_symbol, F.lit(None)).otherwise(cleaned_stock_symbol) # Replace invalid with NULL
        df = df.withColumn("stock_symbol", cleaned_data)

        # Remove unused whitespace and convert values to lowercase.
        df = df.withColumn("source_description", F.trim(F.lower(F.col("source_description"))))

        # Select the columns for the fact table and rename for clarity.
        fct_ipos = df.select(
            F.col("ipo_id").alias("dd_ipo_id"),
            F.col("sk_company_id"),
            F.col("valuation_amount_usd"),
            F.col("raised_amount_usd"),
            F.col("public_date_id").alias("public_at"),
            F.col("stock_symbol"),
            F.col("source_description").alias("ipo_description")
        )

        print("Transformation process successful for table: fct_ipos")

        # Log a success message for the ETL process.
        log_message = spark.sparkContext \
            .parallelize([("warehouse", "transform", "success", "staging", "fct_ipos", current_timestamp)]) \
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])

        return fct_ipos

    except Exception as e:
        
        # Log an error message with details about the failure.
        print(e)
        log_message = spark.sparkContext \
            .parallelize([("warehouse", "transform", "failed", "staging", "fct_ipos", current_timestamp, str(e))]) \
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])

    finally:
        # Save log message into log table
        load_log_msg(spark=spark, log_msg=log_message)




def transform_acquisition(spark, df):
    """
    Transform raw acquisition data into a cleaned and standardized fact table.

    Args:
        spark: SparkSession
        df: Raw input DataFrame containing acquisition data from staging db

    Returns:
        DataFrame: Cleaned and transformed acquisition fact table
    """
    
    # Set up current timestamp for logging
    current_timestamp = datetime.now()

    try:
        # Extract dimension tables needed for joins.
        dim_date = extract_warehouse(spark, table_name="dim_date")
        dim_company = extract_warehouse(spark, table_name="dim_company")

        # Set alias for acquiring and acquired companies from dim_company.
        dim_company_acquiring = dim_company.alias("acq")
        dim_company_acquired = dim_company.alias("acd")

        # Join with dim_company to get surrogate keys for acquiring and acquired companies.
        df = df.join(
            dim_company_acquiring.select(
                F.col("sk_company_id").alias("sk_acquiring_company_id"),
                F.col("nk_company_id").alias("nk_acquiring_company_id")
            ),
            df.acquiring_object_id == F.col("nk_acquiring_company_id"),
            "inner"
        )

        df = df.join(
            dim_company_acquired.select(
                F.col("sk_company_id").alias("sk_acquired_company_id"),
                F.col("nk_company_id").alias("nk_acquired_company_id")
            ),
            df.acquired_object_id == F.col("nk_acquired_company_id"),
            "inner"
        )

        # Add a foreign key 'acquired_date_id' by formatting 'acquired_at' to match 'date_id' in dim_date.
        df = df.withColumn(
            "acquired_date_id",
            F.date_format(df.acquired_at, "yyyyMMdd").cast("integer")
        )

        # Join with dim_date to get date information based on 'acquired_date_id'.
        df = df.join(
            dim_date,
            df.acquired_date_id == dim_date.date_id,
            "left"
        )

        # Convert acquisition price to USD.
        df = df.withColumn("price_amount_usd", to_usd(currency_col="price_currency_code", amount_col="price_amount"))

        # Clean and normalize term code.
        cleaned_term_code = F.trim(F.lower(F.col("term_code")))
        df = df.withColumn("term_code", F.when(cleaned_term_code == "", F.lit(None)).otherwise(cleaned_term_code))

        # Clean the source description.
        cleaned_description = F.trim(F.lower(F.col("source_description")))
        df = df.withColumn(
            "source_description",
            F.when(cleaned_description == "", F.lit(None))
             .otherwise(cleaned_description)
        )

        # Select the columns for the fact table and rename for clarity.
        fct_acquisition = df.select(
            F.col("acquisition_id").alias("dd_acquisition_id"),
            F.col("sk_acquiring_company_id"),
            F.col("sk_acquired_company_id"),
            F.col("price_amount_usd"),
            F.col("acquired_date_id").alias("acquired_at"),
            F.col("term_code"),
            F.col("source_description").alias("acquisition_description")
        )

        print("Transformation process successful for table: fct_acquisition")

        # Log a success message for the ETL process.
        log_message = spark.sparkContext \
            .parallelize([("warehouse", "transform", "success", "staging", "fct_acquisition", current_timestamp)]) \
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])

        return fct_acquisition

    except Exception as e:
        
        # Log an error message with details about the failure.
        print(e)
        log_message = spark.sparkContext \
            .parallelize([("warehouse", "transform", "failed", "staging", "fct_acquisition", current_timestamp, str(e))]) \
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])

    finally:
        # Save log message into log table
        load_log_msg(spark=spark, log_msg=log_message)



