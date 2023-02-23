import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession

def initSpark(instance,appName,sparkExecutorMemory,sparkExecutorCores,sparkExecutorCoresMax,sparkDriverMemory):
    config = pyspark.SparkConf().setAll([('spark.executor.memory', sparkExecutorMemory),
                                         ('spark.executor.cores', sparkExecutorCores),  
                                         ('spark.cores.max', sparkExecutorCoresMax),
                                         ('spark.driver.memory',sparkDriverMemory)
                                        ])
    
    spark = SparkSession    \
        .builder    \
        .master(instance)   \
        .appName(appName)   \
        .config(conf=config)    \
        .getOrCreate()
    return spark