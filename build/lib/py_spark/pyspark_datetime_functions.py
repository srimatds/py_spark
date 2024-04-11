from pyspark.sql import SparkSession
from dataset import *
from datetime import datetime, date
#import pandas as pd
#import numpy as np
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import current_timestamp
import pyspark.sql.functions as F
import sys

##Creating Spark Session
spark = SparkSession.builder.getOrCreate()

##import dataset from dataset.py
##dataset with schema
##creating dataframe for dataset with schema
df_withschema=spark.createDataFrame(dataset_withschema)
print("printing car dataset with Schema")
df_withschema.show()

##In printschema we could see relevant datatypes
print("printing schema which has relevant datatypes ")
df_withschema.printSchema()


##creating dataframe for dataset without schema
df_withoutschema=spark.createDataFrame(dataset_withoutschema,col_names)
print("printing car dataset without casting to relevant datatypes")
df_withoutschema.show()

## check the datatypes which displays string dataype even for date column(manufactured_date)
print("printing schema without casting to date")
df_withoutschema.printSchema()

##Now cast the data type from string  to date using the below method
##Note spark date format is 'yyyy-MM-dd'
def convertstringtodate(df,colname):
    # setting this parameter to enable the casting of string datatype to date datatype
    spark.conf.set('spark.sql.legacy.timeParserPolicy', 'LEGACY')
    df_datecasting = df.withColumn(colname, to_date(
        unix_timestamp(col(colname),'yyyy-MM-dd').cast('timestamp')))
    print("returning date dataset after conversion")
    return df_datecasting

##converting string to date data type by passing the required to the method
df_castedtodate=convertstringtodate(df_withoutschema,"manufactured_date")

##check the datatype after it gets converted to date datatype.
df_castedtodate.printSchema()

def convertstringtodatetime(df,colname):
    # setting this parameter to enable the casting of string datatype to date datatype
    spark.conf.set('spark.sql.legacy.timeParserPolicy', 'LEGACY')
    df_timestamp = df.withColumn(colname, to_timestamp(colname))
    print("returning datetimestamp dataset after conversion")
    return df_timestamp


##converting string to datetimestamp data type by passing the required to the method
df_castedtodatetimestamp=convertstringtodatetime(df_withoutschema,"sold_timestamp")

##check the datatype after it gets converted to datetimestamp datatype.
df_castedtodatetimestamp.printSchema()

# df_with_ts = df.withColumn("curr_timestamp", current_timestamp())


df_timestamp_added=df_castedtodatetimestamp.withColumn('timestamp_c', \
         current_timestamp()) 

df_timestamp_added.show(truncate=False)
