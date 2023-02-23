import os
from pathlib import Path
import psycopg2
from dependencies import importUtility, exportUtility, configUtility
from datetime import datetime
from entities import dimensionSchemas

def loadDimensionsLocal(spark,configs):
     # declarations
    dimension_tables = {}

    # recursively read folder files
    filePath = configs["dimension_files_location"]
    dir_list = os.listdir(filePath)
    for fileName in dir_list:
        dimensionName = Path(fileName).stem
        temp_dim = importUtility.readCsvToDataframe(spark,filePath+fileName)
        dimension_tables[dimensionName]=temp_dim
    return None
    

def loadDataFrameFromPostgreSQL(spark,tableName):
    configs = configUtility.loadConfig()
    url = configs["dbUrl"]
    # properties = {"user": configs["dbUser"], "password": configs["dbPass"], "driver": configs["dbDriver"]}          
    df = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", tableName) \
    .option("user", configs["dbUser"]) \
    .option("password", configs["dbPass"]) \
    .option("driver", configs["dbDriver"]) \
    .load()
    return df;

def loadDimensionCsv(spark,dbconn,logger,dimensionName):
    # check if table exists else run sql to create table
    exists = checkIfTableExists(logger,dbconn,dimensionName)
    if(exists==False):
        logger.info("Dimension table "+dimensionName+" not found..Creating New")        
        # load from local csv
        fileName = "data/dim_data/"+dimensionName+".csv"
        # schema = dimensionSchemas.getSchemaForDimension(dimensionName)
        df = importUtility.readCsvToDataframe(spark,fileName)
        df = dimensionSchemas.getSchemaForDimensionExtd(dimensionName,df)        
        #store dataframe to postgresql db
        exportUtility.stageDataToDb(df,dimensionName,"overwrite")
    # load from database 
    df = loadDataFrameFromPostgreSQL(spark,dimensionName)
    return df;

def loadDimensionDb(spark,dbconn,logger,dimensionName):
    # check if table exists else run sql to create table
    exists = checkIfTableExists(logger,dbconn,dimensionName)
    if(exists==False):
        logger.info("Dimension table "+dimensionName+" not found..Creating New")        
        cur = dbconn.cursor()
        sqlFilePath = "sql/"+dimensionName+".sql"
        cur.execute(open(sqlFilePath,"r").read())
        dbconn.commit()
        cur.close()
    # load from database 
    df = loadDataFrameFromPostgreSQL(spark,dimensionName)
    return df;

def loadDimensionsFromDb(spark,dbconn,logger):
    dim_vendor_codes = loadDimensionDb(spark,dbconn,logger,"dim_vendor_codes")
    dim_payment_types = loadDimensionDb(spark,dbconn,logger,"dim_payment_types")
    dim_rate_codes = loadDimensionDb(spark,dbconn,logger,"dim_rate_codes")
    dim_taxi_zones = loadDimensionCsv(spark,dbconn,logger,"dim_taxi_zones")
    dim_date = loadDimensionDb(spark,dbconn,logger,"dim_date")
    dim_time = loadDimensionDb(spark,dbconn,logger,"dim_time")
    return dim_vendor_codes,dim_payment_types,dim_rate_codes,dim_taxi_zones,dim_date,dim_time

def checkIfTableExists(logger,dbconn,tableName):
    dbconn.autocommit = True
    cur = dbconn.cursor()
    try:
        result = cur.execute("select * from information_schema.tables where table_name=%s", (tableName,))
        return bool(cur.rowcount)
    except psycopg2.Error as e:
        logger.error(e.pgerror)
        return False

def getBatchNumber(logger,dbconn):
    dbconn.autocommit = True
    cur = dbconn.cursor()
    try:
        cur.execute("""select max("BatchId") from batch""")
        result = cur.fetchone()
        if(result[0]==None):
            return 0
        else:
            return result[0]
    except psycopg2.Error as e:
        logger.error(e.pgerror)
        return 0

def initBatchStats(logger,dbconn,filename):  
    # check if batch table exists else create
    dbconn.autocommit = True
    cur = dbconn.cursor()
    exists = checkIfTableExists(logger,dbconn,"batch") 
    try: 
        if(exists==False):
            logger.info("Batch Table Not Found : Creating a new one...")
            sqlFilePath = "sql/create_batch_table.sql"
            cur.execute(open(sqlFilePath,"r").read())       
        logger.info("Creating batch....")        
        cur.execute("""INSERT INTO batch("FileName","TotalGoodRecords","TotalBadRecords","StartDateTime") VALUES (%s,%s,%s,%s);""",(filename,0,0,datetime.now()))      
        cur.close()
    except psycopg2.Error as e:
        logger.error(e.pgerror)

def updateBatchStats(logger,dbconn,totalGoodRecords,totalBadRecords,batchId,status):    
    cur = dbconn.cursor()
    try:        
        cur.execute("""UPDATE batch SET "TotalGoodRecords"=%s, "TotalBadRecords"=%s, "EndDateTime"=%s, "Status"=%s WHERE "BatchId"=%s; """,(totalGoodRecords, totalBadRecords, datetime.now(), status, batchId))
        dbconn.commit()
        cur.close()
    except psycopg2.Error as e:
        logger.error(e.pgerror)

def getSeedIndex(logger,dbconn,tableName):
    exists = checkIfTableExists(logger,dbconn,tableName)
    if(exists==True):
        # print("Table exists")
        dbconn.autocommit = True
        cur = dbconn.cursor()
        cur.execute("""select count(*) from yellow_taxi_fleet;""")
        result = cur.fetchone()
        if(result[0]==None):
            return 1
        else:
            return result[0] +1      
    else:
        return 1
