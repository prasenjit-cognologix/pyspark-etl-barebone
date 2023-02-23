from pyspark.sql.functions import current_timestamp

def exportBadDataToCsv(dataFrame,path):    
    dataFrame.coalesce(1).write.json(path)

def logBadDataToDb(dataFrame):
    url = "jdbc:postgresql://localhost:5432/nycTaxiDb"
    properties = {"user": "postgres", "password": "mypassword", "driver": "org.postgresql.Driver"}
    dataFrame = dataFrame.select("custom_errors")
    dataFrame = dataFrame.withColumn("logtime",current_timestamp())    
    dataFrame.write.jdbc(url=url, table="errorlogs", mode="append", properties=properties)

def stageDataToDb(dataFrame, tableName,mode):
    url = "jdbc:postgresql://localhost:5432/nycTaxiDb"
    properties = {"user": "postgres", "password": "mypassword", "driver": "org.postgresql.Driver", "Schema":"public","numPartitions":"20", "batchSize":"50000", "fetchSize":"10"}          
    dataFrame.write.jdbc(url=url, table=tableName, mode=mode, properties=properties)

def stageParquetToPgDb(spark, filePath):
    spark   \
    .read.format("parquet") \
    .load(filePath) \
    .write.format("postgres")   \
    .option("host","localhost")  \
    .option("port","5432")  \
    .option("password", "mypassword")   \
    .option("partitions", 4)    \
    .option("table","yellowTaxiStage") \
    .option("user","postgres")  \
    .option("database","nycTaxiDb") \
    .option("schema","public") \
    .loada()

