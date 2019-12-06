#!/usr/bin/env python
# coding: utf-8

import os
import re
import csv
import sys
import json
import time
import pyspark
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import regexp_extract, col
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import DoubleType
from copy import deepcopy
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.functions import udf, unix_timestamp, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, TimestampType



# Input & output directories
#file_list = "../cluster3.txt"
file_list = "/home/ted/school/big_data/project/big_data_course_project/cluster3.txt"

# use these if running locally (Jupyter, etc)
inputDirectory = "../raw_data/"
outputDirectory = "../output_data/"#sys.argv[2]

# use these if running on DUMBO
#inputDirectory = "/user/hm74/NYCColumns/"#sys.argv[1]
#outputDirectory = "/user/" + this_user + "/project/task1/"#sys.argv[2]


if __name__ == "__main__":
    # Setting spark context and 
    sc = SparkContext()
    spark = SparkSession         .builder         .appName("project_task1")         .config("spark.some.config.option", "some-value")         .getOrCreate()
    sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

    # Current user path
    env_var = os.environ
    this_user = env_var['USER']

    # Input & output directories. Use these if running on DUMBO
    #inputDirectory = "/user/hm74/NYCOpenData/"#sys.argv[1]
    #outputDirectory = "/user/" + this_user + "/project/task1/"#sys.argv[2]
    
    # use these if running locally (Jupyter, etc)
    inputDirectory = "../raw_data/"
    outputDirectory = "../output_data/"#sys.argv[2]

# Importing cluster3 format it and put it into a list
raw_data = sc.textFile("cluster3.txt")
raw_list = raw_data.flatMap(lambda x: x.split(",")).collect()
raw_list = [re.sub("\[|\]|\'|\'|" "", "", item)for item in raw_list]
raw_list = [re.sub(" " "", "", item)for item in raw_list]

# Iteration over dataframes begins bu using dataframe file names
processCount = 1

# Create schema for raw data before reading into df 
customSchema = StructType([
           StructField("val", StringType(), True),
           StructField("count", IntegerType(), True)])

#Testing first 50 files
for filename in raw_list[2:3]:

    print("Processing Dataset =========== : ", str(processCount) + ' - ' +filename)
    df = sqlContext.read.format("csv").option("header",
    "false").option("inferSchema", "true").option("delimiter", 
    "\t").schema(customSchema).load(inputDirectory + filename)

    # Count all val in df with count 
    count_all = df.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]

    # UDF for converting column type from vector to double type
    unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())
   
    for i in ["count"]:
        # VectorAssembler Transformation - Converting column to vector type
        assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")

        # MinMaxScaler Transformation
        scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")

        # Pipeline of VectorAssembler and MinMaxScaler
        pipeline = Pipeline(stages=[assembler, scaler])

        # Fitting pipeline on dataframe
        df = pipeline.fit(df).transform(df).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")

    print("After Scaling :")
    df.sort(col("count").desc()).show()










