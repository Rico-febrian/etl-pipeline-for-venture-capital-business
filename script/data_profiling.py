import logging
from pyspark.sql import SparkSession
from src.utils.helper import check_missing_values, save_to_json
from src.staging.extract_sources import extract_database, extract_api, extract_csv
from datetime import datetime


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
    fh = logging.FileHandler("pipeline_log/data_profiling_log.txt")
    fh.setLevel(logging.DEBUG)
    
    # Set up log format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add formatter and logger to each handlers
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)    
    
    logger.addHandler(ch)
    logger.addHandler(fh)

def data_profiling():

    spark = None

    try:
        # Create spark session 
        logger.info("Creating Spark session...")
        
        spark = SparkSession \
                .builder \
                .appName("Data Profiling Pipeline") \
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

        logger.info("Creating profiling report...")

        try:
            data_profiling_report = {
                "Created by" : "Rico Febrian",
                "Checking Date" : datetime.now().strftime('%d/%m/%y'),
                "Column Information": {
                    "Acquisition": {"count": len(acquisition_df.columns), "columns": acquisition_df.columns},
                    "Company": {"count": len(company_df.columns), "columns": company_df.columns},
                    "Funding Rounds": {"count": len(funding_rounds_df.columns), "columns": funding_rounds_df.columns},
                    "Funds": {"count": len(funds_df.columns), "columns": funds_df.columns},
                    "Investments": {"count": len(investments_df.columns), "columns": investments_df.columns},
                    "IPOS": {"count": len(ipos_df.columns), "columns": ipos_df.columns},
                    "People": {"count": len(people_df.columns), "columns": people_df.columns},
                    "Relationships": {"count": len(relationship_df.columns), "columns": relationship_df.columns},
                    "Milestones": {"count": len(milestones_df.columns), "columns": milestones_df.columns}
                },
                "Check Data Size": {
                    "Acquisition": acquisition_df.count(),
                    "Company": company_df.count(),
                    "Funding Rounds": funding_rounds_df.count(),
                    "Funds": funds_df.count(),
                    "Investments": investments_df.count(),
                    "IPOS": ipos_df.count(),
                    "People": people_df.count(),
                    "Relationships": relationship_df.count(),
                    "Milestones": milestones_df.count()
                },
                "Data Type For Each Column" : {
                    "Acquisition": acquisition_df.dtypes,
                    "Company": company_df.dtypes,
                    "Funding Rounds": funding_rounds_df.dtypes,
                    "Funds": funds_df.dtypes,
                    "Investments": investments_df.dtypes,
                    "IPOS": ipos_df.dtypes,
                    "People": people_df.dtypes,
                    "Relationships": relationship_df.dtypes,
                    "Milestones": milestones_df.dtypes
                },
                "Check Missing Value" : {
                    "Acquisition": check_missing_values(acquisition_df),
                    "Company": check_missing_values(company_df),
                    "Funding Rounds": check_missing_values(funding_rounds_df),
                    "Funds": check_missing_values(funds_df),
                    "Investments": check_missing_values(investments_df),
                    "IPOS": check_missing_values(ipos_df),
                    "People": check_missing_values(people_df),
                    "Relationships": check_missing_values(relationship_df),
                    "Milestones": check_missing_values(milestones_df)
                }
            }

        except Exception as e:
            logger.error(f"Error while creating profiling report: {e}", exc_info=True)
            raise

        save_to_json(dict_result=data_profiling_report, filename="profiling_result/data_profiling_report")

        logger.info("Profiling report has been created!")

    except Exception as e:
        logger.critical(f"Error during data profiling: {e}", exc_info=True)

    finally:
        if spark:
            logger.info("Stopping Spark session...")
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    data_profiling()