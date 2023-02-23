from dependencies.spark import start_spark
from dependencies import dbUtility, processUtility
from entities import taxiSchema
import os
from pathlib import Path

def main():
    # SETUP configs, logging and spark instance    
    spark, logger, configs, outputDir, dbconn = start_spark()    
    # END

    try:        
        logger.info("Starting ETL Job....")
        
       
        
        logger.info("Finished...")
        spark.stop()
    except Exception as e:
        logger.error(str(e))
        spark.stop()





























# entry point for PySpark ETL application
if __name__ == '__main__':
    main()