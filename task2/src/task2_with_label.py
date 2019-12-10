#!/usr/bin/env python
# coding: utf-8

# --- NOTES -------------------------------------------------------------------
# 1. Update the datasets, dataList
# -----------------------------------------------------------------------------

import os
import re
import sys
import json
import time
import pyspark
from ast import literal_eval
from copy import deepcopy
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.functions import udf, unix_timestamp, col ,length
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, FloatType, DateType, TimestampType
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline 
from collections import Counter

def find_count_for_each_label(df): 
    df.createOrReplaceTempView("df")
    df_no_label = spark.sql("SELECT SUM(count) FROM df WHERE val!='' AND val != 'NULL' ")
    count_type = df_no_label.rdd.map(lambda x: (1,x[0])).collect()[0][1]
    return count_type

# -----------------------------------------------------------------------------
# --- MAIN --------------------------------------------------------------------

if __name__ == "__main__":
    # Setting spark context and 
    sc = SparkContext()
    spark = SparkSession \
        .builder \
        .appName("project_task2_classified") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

    # Current user path
    env_var = os.environ
    this_user = env_var['USER']

    # Input & output directories
    inputDirectory = "task2_classified/"#sys.argv[1]
    outputDirectory = "/user/" + this_user + "/Project_Task2_Manual-Labels/"#sys.argv[2]

    # Output JSON Semantic Schema
    jsonSchema = {
        "column_name": "",
        "semantic_type": []
    }

    # Inner semantic schema 
    semanticSchema = {
        "semantic_type": "",
        "label": "",
        "count": 0 
    }

    # Create schema for raw data before reading into df 
    customSchema = StructType([
                StructField("val", StringType(), True),
                StructField("count", IntegerType(), True)])

#Testing first 10 files
    for filename in os.listdir(inputDirectory):
        print(filename)
        # Read csv file to df
        df = sqlContext.read.format("csv").option("header","false").option("inferSchema", "true").option("delimiter", "\t").schema(customSchema).load(inputDirectory + filename)
        
        # Get the true class from first row
        df_lable = df.limit(1).select(col("val"))
        label = df_lable.rdd.map(lambda x: (1,x[0])).collect()[0][1]
    #     print(label)
        #Get dataset without first row 
        count_label = find_count_for_each_label(df)
    #     print(count_label)
        outJSON = deepcopy(jsonSchema)
        col_name = filename.split('.', 3)
        col_name = col_name[0] + '.' + col_name[1]
        print(col_name)
        outJSON = deepcopy(jsonSchema)
        outJSON["column_name"]= col_name 
        outJSON_semantic =  deepcopy(semanticSchema)
        if (label is "house_number"):
            outJSON_semantic["label"] = label
        else:
            outJSON_semantic["semantic_type"] = label
        outJSON_semantic["count"] = count_label
        outJSON["semantic_type"].append(outJSON_semantic)
        outJSON = sc.parallelize([json.dumps(outJSON)])
        outJSON.saveAsTextFile(outputDirectory + col_name + '.json')