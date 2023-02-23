from pyspark.sql.types import StructType, StructField,StringType,DoubleType,LongType,TimestampType,IntegerType

def yellowTaxiSchema():
    yellow_taxi_schema = StructType([            
            StructField("VendorID",LongType()),
            StructField("tpep_pickup_datetime",TimestampType(),False),
            StructField("tpep_dropoff_datetime",TimestampType(),False),
            StructField("passenger_count",DoubleType()),
            StructField("trip_distance",DoubleType()),
            StructField("RatecodeID",DoubleType(),False),
            StructField("store_and_fwd_flag",StringType()),
            StructField("PULocationID",LongType(),False),
            StructField("DOLocationID",LongType(),False),
            StructField("payment_type",LongType(),False),
            StructField("fare_amount",DoubleType()),
            StructField("extra",DoubleType()),
            StructField("mta_tax",DoubleType()),
            StructField("tip_amount",DoubleType()),
            StructField("tolls_amount",DoubleType()),
            StructField("improvement_surcharge",DoubleType()),
            StructField("total_amount",DoubleType()),
            StructField("congestion_surcharge",DoubleType()),
            StructField("airport_fee",DoubleType()),            
            StructField("custom_errors",StringType()),
            StructField("Id",IntegerType())                               
        ])
    return yellow_taxi_schema