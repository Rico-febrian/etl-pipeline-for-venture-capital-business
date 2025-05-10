from pyspark.sql import SparkSession
from src.staging.extract import extract_database, extract_api, extract_csv
from src.staging.load import load_to_stg


if __name__ == "__main__":
    
    # Create spark session
    spark = SparkSession \
            .builder \
            .appName("Main Pipeline") \
            .getOrCreate()

    
    print("Start extracting data from source database...")
    
    # Extract data from source database, csv and api
    acquisition_df = extract_database(spark=spark, table_name="acquisition")
    company_df = extract_database(spark=spark, table_name="company")
    funding_rounds_df = extract_database(spark=spark, table_name="funding_rounds")
    funds_df = extract_database(spark=spark, table_name="funds")
    investments_df = extract_database(spark=spark, table_name="investments")
    ipos_df = extract_database(spark=spark, table_name="ipos")
    people_df = extract_csv(spark=spark, file_name="people.csv")
    relationship_df = extract_csv(spark=spark, file_name="relationships.csv")
    milestones_df = extract_api(spark=spark, start_date="2014-01-01", end_date="2015-01-01")

    print("Extraction source data success!")
    print()
    print("Start load extracted data to staging database...")

    # Load extracted source data to staging database
    load_to_stg(spark=spark, df=people_df, table_name="people", source_name="csv")
    load_to_stg(spark=spark, df=relationship_df, table_name="relationships", source_name="csv")
    load_to_stg(spark=spark, df=company_df, table_name="company", source_name="source_db")
    load_to_stg(spark=spark, df=funding_rounds_df, table_name="funding_rounds", source_name="source_db")
    load_to_stg(spark=spark, df=funds_df, table_name="funds", source_name="source_db")
    load_to_stg(spark=spark, df=acquisition_df, table_name="acquisition", source_name="source_db")
    load_to_stg(spark=spark, df=ipos_df, table_name="ipos", source_name="source_db")
    load_to_stg(spark=spark, df=investments_df, table_name="investments", source_name="source_db")
    load_to_stg(spark=spark, df=milestones_df, table_name="milestones", source_name="api")

    print("Loading source data to staging database success!")
