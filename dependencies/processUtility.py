from dependencies import importUtility,exportUtility,dbUtility
from entities.factEntities import YellowTaxiSchema
from pyspark.sql.functions import col,when, date_format,to_date,row_number, monotonically_increasing_id,current_timestamp, lit
from pyspark.sql.window import Window

# Process yellow taxi ~older v1
def processYellowTaxiTransformAndLoad(spark,configs,logger,dbconn,yellowTaxiDF):
    schema = YellowTaxiSchema(
            error_column_name="custom_errors",
            split_errors=False
        )
    logger.info("validating yellow taxi data...")
    validDF, errorDF = schema.validate_df(yellowTaxiDF)
    validDF = validDF.withColumn("custom_errors", when(col("custom_errors")=="",None).otherwise(col("custom_errors")))
    goodData = validDF.filter(col("custom_errors").isNull())
    badData  = validDF.filter(col("custom_errors").isNotNull())
    if(badData.count() > 0):
        logger.info("exporting bad data to pg db")
        exportUtility.logBadDataToDb(badData)

    yellowTaxiDF = goodData
    logger.info("processing yellow taxi data...")
    yellowTaxiDF = yellowTaxiDF.withColumn("pickup_time_only",date_format('tpep_pickup_datetime', 'HH:mm'))
    yellowTaxiDF = yellowTaxiDF.withColumn("drop_time_only",date_format('tpep_pickup_datetime', 'HH:mm'))
    yellowTaxiDF = yellowTaxiDF.withColumn("pickup_date_only",to_date('tpep_pickup_datetime'))
    yellowTaxiDF = yellowTaxiDF.withColumn("drop_date_only",to_date('tpep_pickup_datetime'))
    yellowTaxiDF.createOrReplaceTempView("YellowTaxi")
    yellowTaxiDF = spark.sql('select yt.VendorID as VendorDimensionId,  \
                             yt.RatecodeID as RateCodeDimensionId,  \
                             yt.PULocationID as PickupLocationDimensionId,  \
                             yt.DOLocationID as DropLocationDimensionId,    \
                             yt.payment_type as PaymentTypeDimensionId, \
                             yt.pickup_date_only as PickupDateDimensionId,  \
                             yt.drop_date_only as DropDateDimensionId,  \
                             yt.pickup_time_only as PickupTimeDimensionId,    \
                             yt.drop_time_only as DropTimeDimensionId,  \
                             yt.store_and_fwd_flag, yt.fare_amount, yt.extra, yt.mta_tax,   \
                             yt.tip_amount, yt.tolls_amount, yt.improvement_surcharge, yt.total_amount, \
                             yt.congestion_surcharge,yt.airport_fee     \
                             from YellowTaxi yt \
                             left outer join dim_taxi_zones dtz on yt.PULocationID == dtz.LocationID \
                             left outer join dim_taxi_zones dtz1 on yt.DOLocationID == dtz1.LocationID \
        ')

    logger.info("exporting to external db...")
    exists = dbUtility.checkIfTableExists(spark,configs,logger,dbconn,"yellow_taxi_fleet")
    if(exists==True):
        exportUtility.stageDataToDb(yellowTaxiDF,"yellow_taxi_fleet","append")
    else:
        exportUtility.stageDataToDb(yellowTaxiDF,"yellow_taxi_fleet","overwrite")


# new version for v2
def processYellowTaxiV2(spark,configs,logger,dbconn,yellow_taxi_schema,fileName,fileNameWithExtension):
    try:
        # make entry to batch table
        dbUtility.initBatchStats(logger,dbconn,fileNameWithExtension)
        batchNumber = dbUtility.getBatchNumber(logger,dbconn)
        logger.info("Batch Number generated :" + str(batchNumber))                
        logger.info("Processing file ..... : "+fileName)
        filePath = configs["data_files_location"]
        # read and validate source file into dataframe
        yellowTaxiDF = importUtility.readFromParquetToDataframeExtd(spark,filePath+fileNameWithExtension,yellow_taxi_schema,"custom_errors")
        # filter out good and bad data into seperate datagrames
        goodData = yellowTaxiDF.filter(col("custom_errors").isNull())
        badData  = yellowTaxiDF.filter(col("custom_errors").isNotNull())
        # keep the record counts
        totalGoodRecords = goodData.count()
        totalBadRecords = badData.count()
        logger.info("Total good records :" + str(totalGoodRecords))
        logger.info("Total bad records :" + str(totalBadRecords))
        logger.info("Completed validation for  : "+fileName)
        # log the bad data if available for future review
        if(totalBadRecords > 0):
            logger.info("exporting bad data to pg db")
            badData = badData.withColumn("BatchNo",lit(batchNumber))
            exportUtility.logBadDataToDb(badData)

        yellowTaxiDF = goodData
        logger.info("transforming yellow taxi data...")
                    
        # adding columns
        yellowTaxiDF = yellowTaxiDF.withColumn("pickup_time_only",date_format('tpep_pickup_datetime', 'HH:mm'))
        yellowTaxiDF = yellowTaxiDF.withColumn("drop_time_only",date_format('tpep_dropoff_datetime', 'HH:mm'))
        yellowTaxiDF = yellowTaxiDF.withColumn("pickup_date_only",to_date('tpep_pickup_datetime'))
        yellowTaxiDF = yellowTaxiDF.withColumn("drop_date_only",to_date('tpep_dropoff_datetime'))
        # add batch column                
        yellowTaxiDF = yellowTaxiDF.withColumn("BatchNo",lit(batchNumber))
        # add updated datetime column
        yellowTaxiDF = yellowTaxiDF.withColumn("LastUpdatedOn",current_timestamp())
        # create temp view to work with pyspark sql
        yellowTaxiDF.createOrReplaceTempView("YellowTaxi")
                    
        # data transformation
        yellowTaxiDF = spark.sql('select Id,    \
                                cast(yt.VendorID as integer) as VendorDimensionId,  \
                                cast(yt.RatecodeID as integer) as RateCodeDimensionId,  \
                                cast(yt.PULocationID as integer) as PickupLocationDimensionId,  \
                                cast(yt.DOLocationID as integer) as DropLocationDimensionId,    \
                                yt.pickup_date_only as PickupDate,    \
                                yt.drop_date_only as DropDate,  \
                                yt.pickup_time_only as PickupTime,  \
                                yt.drop_time_only as DropTime,  \
                                cast(yt.payment_type as integer) as PaymentTypeDimensionId, \
                                cast(yt.store_and_fwd_flag as varchar(1)) as StoreAndForwardFlag, \
                                cast(yt.passenger_count as integer) as PassengerCount, \
                                cast(yt.fare_amount as decimal(10,2)) as FareAmount, \
                                cast(yt.extra as decimal(10,2)) as ExtraAmount,  \
                                cast(yt.mta_tax as decimal) as MtaTaxAmount,   \
                                cast(yt.tip_amount as decimal(10,2)) as TipAmount,   \
                                cast(yt.tolls_amount as decimal(10,2)) as TollsAmount,   \
                                cast(yt.improvement_surcharge as decimal(10,2)) as ImprovementSurcharge, \
                                cast(yt.total_amount as decimal(10,2)) as TotalAmount, \
                                cast(yt.congestion_surcharge as decimal(10,2)) as CongestionSurcharge,   \
                                cast(yt.airport_fee as decimal(10,2)) as AirportFee,   \
                                BatchNo, LastUpdatedOn  \
                                from YellowTaxi yt \
                                left outer join dim_taxi_zones dtz on yt.PULocationID == dtz.LocationID \
                                left outer join dim_taxi_zones dtz1 on yt.DOLocationID == dtz1.LocationID \
                        ')
                    
        # add identity column - this will find the Id initial value as per sequence with step size = 1
        seedIndex = dbUtility.getSeedIndex(logger,dbconn,"yellow_taxi_fleet")
        logger.info("Setting Insert index="+str(seedIndex))
        # update the Id(index) value as per records starting with seedIndex and step size = 1
        yellowTaxiDF = yellowTaxiDF.withColumn("Id",row_number().over(Window.orderBy(monotonically_increasing_id()+seedIndex)))
        # total records inside dataframe
        totalRecords = yellowTaxiDF.count()
        # check if records exist if yes push them to postgresql db
        if(totalRecords > 0):
            logger.info("exporting to external db...")
            exists = dbUtility.checkIfTableExists(logger,dbconn,"yellow_taxi_fleet")
            if(exists==True):
                exportUtility.stageDataToDb(yellowTaxiDF,"yellow_taxi_fleet","append")
            else:
                exportUtility.stageDataToDb(yellowTaxiDF,"yellow_taxi_fleet","overwrite")
        # update the batch table
        logger.info("Processed Batch No. "+str(batchNumber))
        logger.info("Total Records :"+str(totalBadRecords+totalGoodRecords))
        logger.info("Total Good Records :"+ str(totalGoodRecords))
        logger.info("Total Bad Records :"+str(totalBadRecords))
        dbUtility.updateBatchStats(logger,dbconn,totalGoodRecords,totalBadRecords,batchNumber,"success")
    except Exception as e:
        dbUtility.updateBatchStats(logger,dbconn,totalGoodRecords,totalBadRecords,batchNumber,"failed")

