import __main__
from datetime import datetime 
from dependencies import logUtility,configUtility, sparkUtility
import psycopg2

def start_spark():
    # START - setup configs, logging and spark instance    
    # load configurations
    configs = configUtility.loadConfig()
    # instanciate logger
    logger = logUtility.initLogger(configs['default_log_path']) 
    # instanciate pyspark session
    spark = sparkUtility.initSpark(configs['master_instance'],
                                   configs['app_name'],
                                   configs["sparkExecutorMemory"],
                                   configs["sparkExecutorCores"],
                                   configs["sparkExecutorCoresMax"],
                                   configs["sparkDriverMemory"]
                                   )
    
    logger.info("Starting ..."+ configs['app_name'])  
    # compute output folder
    now = datetime.now()        
    dt_string = now.strftime('%d-%m-%Y-%H-%M-%S')
    outputDir  = dt_string+"/"
    conn = psycopg2.connect(
        database = configs['database'],
        user = configs['dbUser'],
        password = configs['dbPass'],
        host=configs['host'],
        port=configs['dbPort']
    )
    return spark, logger, configs, outputDir, conn