from pyspark.sql import functions as func
from pyspark.sql.functions import col
import os
from pathlib import Path

# read from parquet file
def readFromParquetToDataframe(spark,fileName):
    df = spark.read.parquet(fileName)
    for col in df.columns:            
        df = df.withColumn(col, func.ltrim(func.rtrim(df[col])))
    return df

def readFromParquetToDataframeExtd(spark,fileName,schema,errorColumnName):
    df = spark.read \
        .schema(schema)    \
        .option("header", "true")   \
        .option("inferSchema", "true")  \
        .option("mode", "PERMISSIVE")   \
        .option("dateformat","dd.MM.yyyy")  \
        .option("columnNameOfCorruptRecord",errorColumnName)\
        .parquet(fileName)
    for col in df.columns:            
        df = df.withColumn(col, func.ltrim(func.rtrim(df[col])))               
    return df

def readCsvToDataframeWithSchema(spark,fileName,schema):
    df = spark.read \
        .schema(schema) \
        .option("header", "true")   \
        .option("mode", "PERMISSIVE")   \
        .option("dateformat","dd.MM.yyyy")  \
        .option("columnNameOfCorruptRecord", "corrupted_records")   \
        .csv(fileName)
    for col in df.columns:            
        df = df.withColumn(col, func.ltrim(func.rtrim(df[col])))               
    return df

# read from csv
def readCsvToDataframe(spark,fileName):
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(fileName)
        for col in df.columns:            
            df = df.withColumn(col, func.ltrim(func.rtrim(df[col])))               
        return df

def readParquetFilesToDataframes(spark,configs):
     # recursively read folder files
    filePath = configs["data_files_location"]
    dir_list = os.listdir(filePath)
    for fileName in dir_list:
        factName = Path(fileName).stem
        if(factName.__contains__("yellow_")):
            yellowTaxiDF = readFromParquetToDataframe(spark,filePath+fileName)
        if(factName.__contains__("green_")):
            greenTaxiDF = readFromParquetToDataframe(spark,filePath+fileName)
        if(factName.__contains__("fhvhv_")):
            fhvhvDF = readFromParquetToDataframe(spark,filePath+fileName)
        if(factName.__contains__("fhv_")):
            fhvDF = readFromParquetToDataframe(spark,filePath+fileName)
            
    return yellowTaxiDF, greenTaxiDF, fhvDF, fhvhvDF
