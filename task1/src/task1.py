#!/usr/bin/env python
# coding: utf-8

# --- NOTES -------------------------------------------------------------------
# 1. In line 147 of this code, dataList[0:1] is set to iterate over only the
#    first dataset for testing. We can change the range to test over more datasets.
# 2. Please use .copy() method to make a copy of intSchema, realSchema, dateTimeSchema
#    or textSchema for JSON.
# 3. Check the output dire
# -----------------------------------------------------------------------------

import sys
import os
import json
import pyspark
from pyspark.sql.functions import unix_timestamp
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, TimestampType
# -----------------------------------------------------------------------------
# --- Function Definitions Begin ----------------------------------------------

# Please check something called UDF (from pyspark.sql.functions import udf) for
# making user defined functions so that our functions are also parallelizable
# in Big Data sense.

def get_key_columns_candidates(df):
    keyCols = []
    for col in df.columns:
        if df.select(col).count() == df.select(col).distinct().count():
            keyCols.append(col)
            #keyCols.append((df.select(col).count(), df.select(col).distinct().count()))
    return keyCols

#functions for all data types
def number_non_empty_cells(col):
    eCol = df.select(col)
    currCol = eCol.createOrReplaceTempView("currCol")
    number_non_empty_cells = spark.sql("SELECT COUNT(`" + col + "`) AS number_non_empty_cells FROM currCol WHERE `" + col + "` = '' ")
    res = number_non_empty_cells.rdd.flatMap(lambda x: x).collect()
    return res 

def number_distinct_values(col):
    eCol = df.select(col)
    currCol = eCol.createOrReplaceTempView("currCol")
    number_distinct_values = spark.sql("SELECT COUNT(DISTINCT *) AS number_distinct_values FROM currCol")
    res = number_distinct_values.rdd.flatMap(lambda x: x).collect()
    return res

def frequent_values(col):
    eCol = df.select(col)
    currCol = eCol.createOrReplaceTempView("currCol")
    frequent_values = spark.sql("SELECT `" + col + "` AS frequent_values FROM (SELECT `" + col + "`, COUNT (`" + col + "`) as f FROM currCol GROUP BY `" + col + "` ORDER BY f DESC LIMIT 5)")
    res = frequent_values.rdd.flatMap(lambda x: x).collect()
    return res 

def count(col): 
    eCol = df.select(col)
    currCol = eCol.createOrReplaceTempView("currCol")
    text_count = spark.sql("SELECT COUNT(*) as text_count FROM currCol WHERE `" + col + "` != '' AND `" + col + "` is NOT NULL")
    res = text_count.rdd.flatMap(lambda x: x).collect()
    return res

#functions for string type
def shortest_values(col): 
    eCol = df.select(col)
    currCol = eCol.createOrReplaceTempView("currCol")
    shortest_values = spark.sql("SELECT `" + col + "` FROM (SELECT `" + col + "`, char_length(`" + col + "`) AS Col_Length_MIN FROM currCol WHERE `" + col + "` != '' AND `" + col + "` is NOT NULL ORDER BY Col_Length_MIN ASC LIMIT 5)" )
    res = shortest_values.rdd.flatMap(lambda x: x).collect()
    return res

def longest_values(col): 
    eCol = df.select(col)
    currCol = eCol.createOrReplaceTempView("currCol")
    longest_values = spark.sql("SELECT `" + col + "` FROM (SELECT `" + col + "`, char_length(`" + col + "`) AS Col_Length_MIN FROM currCol WHERE `" + col + "` != '' AND `" + col + "` is NOT NULL ORDER BY Col_Length_MIN DESC LIMIT 5)" )
    res = longest_values.rdd.flatMap(lambda x: x).collect()
    return res

def average_length(col): 
    eCol = df.select(col)
    currCol = eCol.createOrReplaceTempView("currCol")
    average_length = spark.sql("SELECT AVG(char_length(`" + col + "`)) FROM currCol WHERE `" + col + "` != '' AND `" + col + "` is NOT NULL")
    res = average_length.rdd.flatMap(lambda x: x).collect()
    return res

#functions for real and int type
def max_value_for_real_int(col):
    eCol = df.select(col)
    currCol = eCol.createOrReplaceTempView("currCol")
    max_value_for_real_int = spark.sql("SELECT MAX(*) FROM currCol WHERE `" + col + "` is NOT NULL")
    res = max_value_for_real_int.rdd.flatMap(lambda x: x).collect()
    return res

def min_value_for_real_int(col):
    eCol = df.select(col)
    currCol = eCol.createOrReplaceTempView("currCol")
    min_value_for_real_int = spark.sql("SELECT MIN(*) FROM currCol WHERE `" + col + "` is NOT NULL")
    res = min_value_for_real_int.rdd.flatMap(lambda x: x).collect()
    return res

def mean_for_real_int(col):
    eCol = df.select(col)
    currCol = eCol.createOrReplaceTempView("currCol")
    mean_for_real_int = spark.sql("SELECT AVG(*) FROM currCol WHERE `" + col + "` is NOT NULL")
    res = mean_for_real_int.rdd.flatMap(lambda x: x).collect()
    return res

def stddev_for_real_int(col):
    eCol = df.select(col)
    currCol = eCol.createOrReplaceTempView("currCol")
    stddev_for_real_int = spark.sql("SELECT STD(*) FROM currCol WHERE `" + col + "` is NOT NULL")
    res = stddev_for_real_int.rdd.flatMap(lambda x: x).collect()
    return res

#functions for date/time type
def min_value(col): 
    eCol = df.select(col)
    currCol = eCol.createOrReplaceTempView("currCol")
    min_value = spark.sql("SELECT * FROM currCol ORDER BY ASC LIMIT 1 ")
    res = min_value.rdd.flatMap(lambda x: x).collect()
    return res

def max_value(col): 
    eCol = df.select(col)
    currCol = eCol.createOrReplaceTempView("currCol")
    max_value = spark.sql("SELECT * FROM currCol ORDER BY DESC LIMIT 1 ")
    res = max_value.rdd.flatMap(lambda x: x).collect()
    return res

# --- Function Definitions End ------------------------------------------------
# -----------------------------------------------------------------------------



# -----------------------------------------------------------------------------
# --- MAIN --------------------------------------------------------------------

if __name__ == "__main__":
    # Setting spark context and 
    sc = SparkContext()
    spark = SparkSession \
        .builder \
        .appName("project_task1") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    #sqlContext = SQLContext(spark)
    sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)


    env_var = os.environ
    this_user = env_var['USER']

    # Input & output directories
    inputDirectory = "/user/hm74/NYCOpenData/"#sys.argv[1]
    outputDirectory = "/user/" + this_user + "/Project/task1/"#sys.argv[2]

    # Output JSON Schema - PARENT
    jsonSchema = {
        "dataset_name": "",
        "columns": [],
        "key_column_candidates": []
    }
    # Output JSON DataType Schema - INTEGER
    intSchema = {
        "column_name": "",
        "number_non_empty_cells": 0,
        "number_distinct_values": 0,
        "frequent_values": [],
        "data_types": {
            "type": "INTEGER (LONG)",
            "count": 0,
            "max_value": 0,
            "min_value": 0,
            "mean": 0,
            "stddev": 0
        },
        "semantic_types": {
            "semantic_type": "",
            "count": 0
        }

    }
    # Output JSON DataType Schema - REAL
    realSchema = {
        "column_name": "",
        "number_non_empty_cells": 0,
        "number_distinct_values": 0,
        "frequent_values": [],
        "data_types": {
            "type": "REAL",
            "count": 0,
            "max_value": 0,
            "min_value": 0,
            "mean": 0,
            "stddev": 0            
        },
        "semantic_types": {
            "semantic_type": "",
            "count": 0
        }
    }
    # Output JSON DataType Schema - DATE/TIME
    dateTimeSchema = {
        "column_name": "",
        "number_non_empty_cells": 0,
        "number_distinct_values": 0,
        "frequent_values": [],
        "data_types": {
            "type": "DATE/TIME",
            "count": 0,
            "max_value": "",
            "min_value": ""
        },
        "semantic_types": {
            "semantic_type": "",
            "count": 0
        }
    }
    # Output JSON DataType Schema - TEXT
    textSchema = {
        "column_name": "",
        "number_non_empty_cells": 0,
        "number_distinct_values": 0,
        "frequent_values": [],
        "data_types": {
            "type": "TEXT",
            "count": 0,
            "shortest_values": [],
            "longest_values": [],
            "average_length": 0
        },
        "semantic_types": {
            "semantic_type": "",
            "count": 0
        }
    }

    # Importing dataframe from HDFS with datasetnames
    #datasets = sqlContext.read.format("csv").option("header", "false").option("delimiter", "\t").load(inputDirectory + "datasets.tsv")
    datasets = sqlContext.read.format("csv").option("header", 
        "false").option("delimiter", "\t").load(inputDirectory + "datasets.tsv")

    # List of dataset file names
    dataList = [str(row._c0) for row in datasets.select('_c0').collect()]
    # Iteration over dataframes begins bu using dataframe file names
    for filename in dataList[0:4]:
        df = sqlContext.read.format("csv").option("header",
        "true").option("inferSchema", "true").option("delimiter", 
            "\t").load(inputDirectory + filename + ".tsv.gz")

        df.createOrReplaceTempView("df")
        #sqlContext.cacheTable("df")
        #df_result = spark.sql("select * from df").show()
        
        # Copy of the jsonSchema for current iteration 
        outJSON = jsonSchema.copy()


                
        # ---------------------------------------------------------------------
        # --- ENTER FUNCTION CALLS FROM HERE ----------------------------------

        # 01) Setting the "dataset_name" attribute
        outJSON["dataset_name"] = filename
        # 03) Finding "colomns" attribute for each column

        for col in df.columns: 
            type = df.schema[col].dataType.typeName()
            print(type)
            if type == 'datetime':
                outJSON1 = dateTimeSchema.copy()
                outJSON1["column_name"] = col
                outJSON1["number_non_empty_cells"] = ', '.join(map(str, number_non_empty_cells(col)))
                outJSON1["number_distinct_values"] = ', '.join(map(str, number_distinct_values(col)))
                outJSON1["frequent_values"] = ', '.join(map(str, frequent_values(col)))
                outJSON1["data_types"]["count"] = ', '.join(map(str, count(col)))
                outJSON1["data_types"]["max_value"] = ', '.join(map(str,  max_value(col)))
                outJSON1["data_types"]["min_value"] = ', '.join(map(str,  min_value(col)))
            if type == 'string': 
                outJSON = textSchema.copy()
                outJSON["column_name"] = col
                outJSON["number_non_empty_cells"] = ', '.join(map(str, number_non_empty_cells(col)))
                outJSON["number_distinct_values"] = ', '.join(map(str, number_distinct_values(col)))
                outJSON["frequent_values"] = ', '.join(map(str, frequent_values(col)))
                outJSON["data_types"]["count"] = ', '.join(map(str, count(col)))
                outJSON["data_types"]["shortest_values"] = ', '.join(map(str, shortest_values(col)))
                outJSON["data_types"]["longest_values"] = ', '.join(map(str,  longest_values(col)))
                outJSON["data_types"]["average_length"] = ', '.join(map(str,  average_length(col)))
            if type == 'double':
                outJSON = realSchema.copy()
                outJSON["column_name"] = col
                outJSON["number_non_empty_cells"] = ', '.join(map(str, number_non_empty_cells(col)))
                outJSON["number_distinct_values"] = ', '.join(map(str, number_distinct_values(col)))
                outJSON["frequent_values"] = ', '.join(map(str, frequent_values(col)))
                outJSON["data_types"]["count"] = ', '.join(map(str, count(col)))
                outJSON["data_types"]["max_value"] = ', '.join(map(str, max_value_for_real_int(col)))
                outJSON["data_types"]["min_value"] = ', '.join(map(str,  min_value_for_real_int(col)))
                outJSON["data_types"]["mean"] = ', '.join(map(str,  mean_for_real_int(col)))
                outJSON["data_types"]["stddev"] = ', '.join(map(str,  stddev_for_real_int(col)))
            if type == 'long' or type == 'string':
                outJSON = intSchema.copy()
                outJSON["column_name"] = col
                outJSON["number_non_empty_cells"] = ', '.join(map(str, number_non_empty_cells(col)))
                outJSON["number_distinct_values"] = ', '.join(map(str, number_distinct_values(col)))
                outJSON["frequent_values"] = ', '.join(map(str, frequent_values(col)))
                outJSON["data_types"]["count"] = ', '.join(map(str, count(col)))
                outJSON["data_types"]["max_value"] = ', '.join(map(str, max_value_for_real_int(col)))
                outJSON["data_types"]["min_value"] = ', '.join(map(str,  min_value_for_real_int(col)))
                outJSON["data_types"]["mean"] = ', '.join(map(str,  mean_for_real_int(col)))
                outJSON["data_types"]["stddev"] = ', '.join(map(str,  stddev_for_real_int(col)))     

        # 03) Finding "key_columns_candidates" attribute
        outJSON["key_columns_candidates"] = get_key_columns_candidates(df)

        # --- FUNCTION CALLS END HERE -----------------------------------------
        # ---------------------------------------------------------------------
        
        # Exporting the JSON for current dataset
        outJSON = sc.parallelize([outJSON])
        outJSON.saveAsTextFile(outputDirectory + filename + '.json')
