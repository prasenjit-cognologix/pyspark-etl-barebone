from marshmallow_pyspark import Schema
from marshmallow import fields, ValidationError
from datetime import datetime
from dateutil.parser import parse
   
class YellowTaxiSchema(Schema):
    VendorID = fields.Integer(required=True, allow_none=False)
    tpep_pickup_datetime = fields.DateTime(required=True, allow_none=False)
    tpep_dropoff_datetime = fields.DateTime(required=True, allow_none=False)
    passenger_count = fields.Number(required=False, allow_none=True)
    trip_distance= fields.Number(required=False, allow_none=True)
    # RatecodeID= fields.Number()
    RatecodeID= fields.Integer()
    store_and_fwd_flag= fields.String(required=False, allow_none=True)
    PULocationID= fields.Integer()
    DOLocationID= fields.Integer()
    payment_type= fields.Integer()
    fare_amount= fields.Number()
    extra= fields.Number(required=False, allow_none=True)
    mta_tax= fields.Number()
    tip_amount= fields.Number(required=False, allow_none=True)
    tolls_amount= fields.Number(required=False, allow_none=True)
    improvement_surcharge= fields.Number(required=False, allow_none=True)
    total_amount= fields.Number()
    congestion_surcharge= fields.Number(required=False, allow_none=True)
    airport_fee= fields.Number(required=False, allow_none=True)