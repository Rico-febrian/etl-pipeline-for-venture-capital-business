import logging
from pyspark.sql import SparkSession
from src.staging.extract_sources import extract_database, extract_api, extract_csv
from src.staging.load_to_stg import load_to_stg
from src.warehouse.extract_staging import extract_staging
from src.warehouse.transform import transform_company, transform_people, transform_relationships, transform_funds, transform_investments, transform_ipos, transform_acquisition
from src.warehouse.load_to_dwh import load_to_dwh

# Set up logger

# Get logger instance and set up main logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Set up handlers if doesn't exists 
if not logger.handlers:
    
    # Create handler for console
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # Create handler for file
    fh = logging.FileHandler("pipeline_log/main_pipeline_log.txt")
    fh.setLevel(logging.DEBUG)
    
    # Set up log format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add formatter and logger to each handlers
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)    
    
    logger.addHandler(ch)
    logger.addHandler(fh)

def main_pipeline():

    spark = None

    try:
        # Create spark session 
        logger.info("Creating Spark session...")
        
        spark = SparkSession \
                .builder \
                .appName("Main Pipeline") \
                .getOrCreate()

        logger.info("Spark session created!")

        logger.info("Start extracting data from source database...")

        try:
            # Extract data from source database, csv and api
            logger.info("Extracting acquisition table...")
            acquisition_df = extract_database(spark=spark, table_name="acquisition")
            logger.info("Acquisition table successfully extracted!")

            logger.info("Extracting company table...")
            company_df = extract_database(spark=spark, table_name="company")
            logger.info("Company table successfully extracted!")

            logger.info("Extracting funding rounds table...")
            funding_rounds_df = extract_database(spark=spark, table_name="funding_rounds")
            logger.info("Funding rounds tabel successfully extracted!")

            logger.info("Extracting funds table...")
            funds_df = extract_database(spark=spark, table_name="funds")
            logger.info("Funds table successfully extracted!")

            logger.info("Extracting investments table...")
            investments_df = extract_database(spark=spark, table_name="investments")
            logger.info("Investments table successfully extracted!")

            logger.info("Extracting ipos table...")
            ipos_df = extract_database(spark=spark, table_name="ipos")
            logger.info("IPOs table successfully extracted!")

            logger.info("Extracting people table...")
            people_df = extract_csv(spark=spark, file_name="people.csv")
            logger.info("People table successfully extracted!")

            logger.info("Extracting relationships table...")
            relationship_df = extract_csv(spark=spark, file_name="relationships.csv")
            logger.info("Relationships table successfully extracted!")

            logger.info("Extracting milestones table...")
            milestones_df = extract_api(spark=spark, start_date="2014-01-01", end_date="2015-01-01")
            logger.info("Milestones table successfully extracted!")
            
            logger.info("Extraction source data completed!")

        except Exception as e:
            logger.error(f"Error during source data extraction process: {e}", exc_info=True)
            raise
    
        logger.info("Start load extracted data to staging database...")

        try:
            # Load extracted source data to staging database
            logger.info("Load people table into staging database...")
            load_to_stg(spark=spark, df=people_df, table_name="people", source_name="csv")
            logger.info("People table successfully loaded!")
            
            logger.info("Load relationships table into staging database...")
            load_to_stg(spark=spark, df=relationship_df, table_name="relationships", source_name="csv")
            logger.info("Relationships table successfully loaded!")

            logger.info("Load company table into staging database...")
            load_to_stg(spark=spark, df=company_df, table_name="company", source_name="source_db")
            logger.info("Company table successfully loaded!")
            
            logger.info("Load funding rounds table into staging database...")
            load_to_stg(spark=spark, df=funding_rounds_df, table_name="funding_rounds", source_name="source_db")
            logger.info("Funding rounds table successfully loaded!")

            logger.info("Load funds table into staging database...")
            load_to_stg(spark=spark, df=funds_df, table_name="funds", source_name="source_db")
            logger.info("Funds table successfully loaded!")

            logger.info("Load acquisition table into staging database...")
            load_to_stg(spark=spark, df=acquisition_df, table_name="acquisition", source_name="source_db")
            logger.info("Acquisition table successfully loaded!")
            
            logger.info("Load ipos table into staging database...")
            load_to_stg(spark=spark, df=ipos_df, table_name="ipos", source_name="source_db")
            logger.info("IPOs table successfully loaded!")
            
            logger.info("Load investments table into staging database...")
            load_to_stg(spark=spark, df=investments_df, table_name="investments", source_name="source_db")
            logger.info("Investments table successfully loaded!")
            
            logger.info("Load milestones table into staging database...")
            load_to_stg(spark=spark, df=milestones_df, table_name="milestones", source_name="api")
            logger.info("Milestones table successfully loaded!")
            
            logger.info("Loading source data to staging database completed!")

        except Exception as e:
            logger.error(f"Error while loading source data to staging database: {e}", exc_info=True)
            raise

        logger.info("Start extracting data from staging database...")

        try:
            # Extract data from staging database
            logger.info("Extracting acquisition table from staging database...")
            stg_acquisition = extract_staging(spark=spark, table_name="acquisition")
            logger.info("Acquisition table successfully extracted!")

            logger.info("Extracting company table from staging database...")
            stg_company = extract_staging(spark=spark, table_name="company")
            logger.info("Company table successfully extracted!")

            logger.info("Extracting funding rounds table from staging database...")
            stg_funding_rounds = extract_staging(spark=spark, table_name="funding_rounds")
            logger.info("Funding rounds table successfully extracted!")

            logger.info("Extracting funds table from staging database...")
            stg_funds = extract_staging(spark=spark, table_name="funds")
            logger.info("Funds table successfully extracted!")

            logger.info("Extracting investments table from staging database...")
            stg_investments = extract_staging(spark=spark, table_name="investments")
            logger.info("Investments table successfully extracted!")

            logger.info("Extracting ipos table from staging database...")
            stg_ipos = extract_staging(spark=spark, table_name="ipos")
            logger.info("IPOs table successfully extracted!")

            logger.info("Extracting people table from staging database...")
            stg_people = extract_staging(spark=spark, table_name="people")
            logger.info("People table successfully extracted!")

            logger.info("Extracting relationships table from staging database...")
            stg_relationships = extract_staging(spark=spark, table_name="relationships")
            logger.info("Relationships table successfully extracted!")

            logger.info("Extracting milestones table from staging database...")
            stg_milestones = extract_staging(spark=spark, table_name="milestones")
            logger.info("Milestones table successfully extracted!")
        
            logger.info("Extraction staging data completed!")
    
        except Exception as e:
            logger.error(f"Error during extracting data from staging: {e}", exc_info=True)
            raise

        logger.info("Start transforming data from staging database...")

        try:
            # Transform extracted data from staging database
            logger.info("Transforming company table...")
            dim_company = transform_company(spark, df=stg_company)
            logger.info("Company table successfully transformed!")
        
            logger.info("Transforming people table...")
            dim_people = transform_people(spark, df=stg_people)
            logger.info("People table successfully transformed!")
        
            logger.info("Transforming funds table...")
            dim_funds = transform_funds(spark, df=stg_funds)
            logger.info("Funds table successfully transformed!")
            
            logger.info("Transforming relationships table...")
            bridge_company_people = transform_relationships(spark, df=stg_relationships)
            logger.info("Relationships table successfully transformed!")
        
            logger.info("Transforming investments table...")
            fct_investments = transform_investments(spark, df=stg_investments)
            logger.info("Investments table successfully transformed!")
        
            logger.info("Transforming ipos table...")
            fct_ipos = transform_ipos(spark, df=stg_ipos)
            logger.info("IPOs table successfully transformed!")
        
            logger.info("Transforming acquisition table...")
            fct_acquisition = transform_acquisition(spark, df=stg_acquisition)
            logger.info("Acquisition table successfully transformed!")
            
            logger.info("Transformation process success!") 

        except Exception as e:
            logger.error(f"Error during transformation data from staging: {e}", exc_info=True)
            raise

        logger.info("Start load transformed data from staging to warehouse database...")

        try:
            # Load transformed data from staging to warehouse database
            logger.info("Load company table into warehouse database...")
            load_to_dwh(spark, df=dim_company ,table_name="dim_company", source_name="staging_db")
            logger.info("Dim company table successfully loaded!")

            logger.info("Load people table into warehouse database...")
            load_to_dwh(spark, df=dim_people, table_name="dim_people", source_name="staging_db")
            logger.info("Dim people table successfully loaded!")
            
            logger.info("Load funds table into warehouse database...")
            load_to_dwh(spark, df=dim_funds, table_name="dim_funds", source_name="staging_db")
            logger.info("Dim funds table successfully loaded!")
            
            logger.info("Load relationships table into warehouse database...")
            load_to_dwh(spark, df=bridge_company_people, table_name="bridge_company_people", source_name="staging_db")
            logger.info("Bridge company people table successfully loaded!")
            
            logger.info("Load investments table into warehouse database...")
            load_to_dwh(spark, df=fct_investments, table_name="fct_investments", source_name="staging_db")
            logger.info("Fct investments table successfully loaded!")
            
            logger.info("Load ipos table into warehouse database...")
            load_to_dwh(spark, df=fct_ipos, table_name="fct_ipos", source_name="staging_db")
            logger.info("Fct ipos table successfully loaded!")
                
            logger.info("Load acquisition table into warehouse database...")
            load_to_dwh(spark, df=fct_acquisition, table_name="fct_acquisition", source_name="staging_db")
            logger.info("Fct acquisition table successfully loaded!")
            
            logger.info("Loading staging data to warehouse database completed!")

        except Exception as e:
            logger.error(f"Error while loading transformed staging data to warehouse database: {e}", exc_info=True)
            raise

        logger.info("The ETL pipeline has run successfully!")

    except Exception as e:
        logger.critical(f"Error during ETL process: {e}", exc_info=True)

    finally:
        if spark:
            logger.info("Stopping Spark session...")
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main_pipeline()