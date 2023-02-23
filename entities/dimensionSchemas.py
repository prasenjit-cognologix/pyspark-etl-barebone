from pyspark.sql.types import StructType, StructField,StringType,DoubleType,LongType,TimestampType,IntegerType
from marshmallow_pyspark import Schema
from marshmallow import fields, ValidationError
from pyspark.sql.functions import col,when

class DimensionTaxiZone(Schema):
    LocationID=fields.Integer(required=True, allow_none=False)
    Borough=fields.String()
    Zone = fields.String()
    service_zone = fields.String()



def getSchemaForDimension(dimensionName):
    if(dimensionName == "dim_taxi_zones"):
        schema = StructType([
                    StructField("LocationID",LongType(),False),
                    StructField("Borough",StringType()),
                    StructField("Zone", StringType()),
                    StructField("service_zone", StringType())
                ])
        return schema

def getSchemaForDimensionExtd(dimensionName,dataFrame):
    if(dimensionName == "dim_taxi_zones"):
        schema = DimensionTaxiZone(
            error_column_name="custom_errors",
            split_errors=False
        )
        validDF, errorDF = schema.validate_df(dataFrame)
        validDF = validDF.withColumn("custom_errors", when(col("custom_errors")=="",None).otherwise(col("custom_errors")))
        goodData = validDF.filter(col("custom_errors").isNull())
        goodData = goodData.drop("custom_errors")
        return goodData