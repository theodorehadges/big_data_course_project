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
import re
from pyspark.sql.functions import unix_timestamp
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, TimestampType
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.functions import udf
# -----------------------------------------------------------------------------
# --- Function Definitions Begin ----------------------------------------------

# Please check something called UDF (from pyspark.sql.functions import udf) for
# making user defined functions so that our functions are also parallelizable
# in Big Data sense.
def parse_Date(x):
    for fmt in ('%Y-%m-%d', '%m-%d-%Y', '%d/%m/%Y','%Y-%m-%d %H:%M:%S','%m/%d/%Y','%m/%d/%Y','%m/%d/%y','%d/%m/%Y'):
        try:
            return str(datetime.strptime(x,fmt).replace(tzinfo=pytz.UTC).strftime("%Y/%m/%d %H:%M:%S"))
        except ValueError:
            pass

def get_key_columns_candidates(df):
    keyCols = []
    for col in df.columns:
        if df.select(col).count() == df.select(col).distinct().count():
            keyCols.append(col)
            #keyCols.append((df.select(col).count(), df.select(col).distinct().count()))
    return keyCols

#functions for all data types
def number_non_empty_cells(df):
    eCol = df.select(col)
    currCol = eCol.createOrReplaceTempView("currCol")
    number_non_empty_cells = spark.sql("SELECT COUNT(`" + col + "`) AS number_non_empty_cells FROM currCol WHERE `" + col + "` != ''")
    res = number_non_empty_cells.rdd.flatMap(lambda x: x).collect()
    return res

def number_empty_cells(df):
    eCol = df.select(col)
    currCol = eCol.createOrReplaceTempView("currCol")
    number_empty_cells = spark.sql("SELECT COUNT(`" + col + "`) AS number_empty_cells FROM currCol WHERE `" + col + "` = '' ")
    res = number_empty_cells.rdd.flatMap(lambda x: x).collect()
    return res

def number_distinct_values(df):
    number_distinct_values = spark.sql("SELECT COUNT(DISTINCT `" + col + "`) AS number_distinct_values FROM df")
    res = number_distinct_values.rdd.flatMap(lambda x: x).collect()
    return res  

def count(df_col): 
    df_col = df_col.createOrReplaceTempView("df_col")
    count = spark.sql("SELECT COUNT(*) FROM df_col WHERE `" + col + "` != '' AND `" + col + "` is NOT NULL")
    res = count.rdd.flatMap(lambda x: x).collect()
    return res

def frequent_values(df_col):
    df_col = df_col.createOrReplaceTempView("df_col")
    frequent_values = spark.sql("SELECT `" + col + "` AS frequent_values FROM (SELECT `" + col + "`, COUNT (`" + col + "`) as f FROM df_col GROUP BY `" + col + "` ORDER BY f DESC LIMIT 5)")
    res = frequent_values.rdd.flatMap(lambda x: x).collect()
    return res 

#functions for string type
def shortest_values(df_col): 
    df_col = df_col.createOrReplaceTempView("df_col")
    shortest_values = spark.sql("SELECT `" + col + "` FROM (SELECT `" + col + "`, char_length(`" + col + "`) AS Col_Length_MIN FROM df_col WHERE `" + col + "` != '' AND `" + col + "` is NOT NULL ORDER BY Col_Length_MIN ASC LIMIT 5)" )
    res = shortest_values.rdd.flatMap(lambda x: x).collect()
    return res

def longest_values(df_col): 
    df_col = df_col.createOrReplaceTempView("df_col")
    longest_values = spark.sql("SELECT `" + col + "` FROM (SELECT `" + col + "`, char_length(`" + col + "`) AS Col_Length_MIN FROM df_col WHERE `" + col + "` != '' AND `" + col + "` is NOT NULL ORDER BY Col_Length_MIN DESC LIMIT 5)" )
    res = longest_values.rdd.flatMap(lambda x: x).collect()
    return res

def average_length(df_col): 
    df_col = df_col.createOrReplaceTempView("df_col")
    average_length = spark.sql("SELECT AVG(char_length(`" + col + "`)) FROM df_col WHERE `" + col + "` != '' AND `" + col + "` is NOT NULL")
    res = average_length.rdd.flatMap(lambda x: x).collect()
    return res

#functions for real and int type
def max_value_for_real_int(df_col):
    df_col = df_col.createOrReplaceTempView("df_col")
    max_value_for_real_int = spark.sql("SELECT MAX(*) FROM df_col WHERE `" + col + "` is NOT NULL")
    res = max_value_for_real_int.rdd.flatMap(lambda x: x).collect()
    return res

def min_value_for_real_int(df_col):
    df_col = df_col.createOrReplaceTempView("df_col")
    min_value_for_real_int = spark.sql("SELECT MIN(*) FROM df_col WHERE `" + col + "` is NOT NULL")
    res = min_value_for_real_int.rdd.flatMap(lambda x: x).collect()
    return res

def mean_for_real_int(df_col):
    df_col = df_col.createOrReplaceTempView("df_col")
    mean_for_real_int = spark.sql("SELECT AVG(*) FROM df_col WHERE `" + col + "` is NOT NULL")
    res = mean_for_real_int.rdd.flatMap(lambda x: x).collect()
    return res

def stddev_for_real_int(df_col):
    df_col = df_col.createOrReplaceTempView("df_col")
    stddev_for_real_int = spark.sql("SELECT STD(*) FROM df_col WHERE `" + col + "` is NOT NULL")
    res = stddev_for_real_int.rdd.flatMap(lambda x: x).collect()
    return res

#functions for date/time type
def max_value(df_col): 
    df_col = df_date.createOrReplaceTempView("df_col")
    max_value = spark.sql("SELECT * FROM df_col ORDER BY `" + col + "` DESC LIMIT 1")
    res = max_value.rdd.flatMap(lambda x: x).collect()
    return res

def min_value(df_col): 
    df_col = df_col.createOrReplaceTempView("df_col")
    min_value = spark.sql("SELECT * FROM df_col ORDER BY `" + col + "` ASC LIMIT 1")
    res = min_value.rdd.flatMap(lambda x: x).collect()
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
    # Output JSON Schema - For EACH COL
    colSchema = {
        "column_name": "",
        "number_non_empty_cells": 0,
        "number_empty_cells" : 0,
        "number_distinct_values": 0,
        "frequent_values": []  
    }

    # Output JSON DataType Schema - INTEGER - FOR EACH DATA TYPE
    intSchema = {
        "data_types": {
            "type": "INTEGER (LONG)",
            "count": 0,
            "max_value": 0,
            "min_value": 0,
            "mean": 0,
            "stddev": 0
        }
    }
    # Output JSON DataType Schema - REAL
    realSchema = {
        "data_types": {
            "type": "REAL",
            "count": 0,
            "max_value": 0,
            "min_value": 0,
            "mean": 0,
            "stddev": 0            
        }
    }
    # Output JSON DataType Schema - DATE/TIME
    dateTimeSchema = {
        "data_types": {
            "type": "DATE/TIME",
            "count": 0,
            "max_value": "",
            "min_value": ""
        }
    }
    # Output JSON DataType Schema - TEXT
    textSchema = {
        "data_types": {
            "type": "TEXT",
            "count": 0,
            "shortest_values": [],
            "longest_values": [],
            "average_length": 0
        }
    }
    # Output JSON Semantic Schema
    
    semanticSchema = {
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
        outJSON["dataset_name"] = filename
        outJSON["columns"] = df.columns
        outJSON["key_columns_candidates"] = get_key_columns_candidates(df)
        # Create an empty list for the final result for each dataset 
        outAllRes = []
        outAllRes.append(outJSON)
   
        # ---------------------------------------------------------------------
        # --- ENTER FUNCTION CALLS FROM HERE ----------------------------------

        # 0Finding "colomns" attribute for each column

        for col in df.columns:
            # Create an empty list for the temp result for each column 
            outColRes = []
            outJSON_col = colSchema.copy()
            outJSON_col["column_name"] = col
            outJSON_col["number_non_empty_cells"] = ', '.join(map(str, number_non_empty_cells(df)))
            outJSON_col["number_empty_cells"] = ', '.join(map(str, number_empty_cells(df)))
            outJSON_col["number_distinct_values"] = ', '.join(map(str, number_distinct_values(df)))
            outJSON_col["frequent_values"] = ', '.join(map(str, frequent_values(df)))
            outColRes.append(outJSON_col)   

            # Define of different types regex list:

            date_list = ['\d{4}-\d{2}-\d{2}', 
                         '\d{2}\/\d{2}\/\d{4}',
                         '\d{4}\/\d{2}\/\d{2}',
                         '\d{2}-\d{2}-\d{4}',
                        ]

            time_list = ['\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',
                         '\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}',
                         '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s[A-Z].',
                         '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s[a-z].',
                         '\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\s[A-Z].',
                         '\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\s[a-z].',
                         '\d{2}:\d{2}:\d{2}',
                         '\d{2}:\d{2}:\d{2}\s[A-Z].',
                         '\d{2}:\d{2}:\d{2}\s[a-z].',
                         '\d{2}:\d{2}',
                         '\d{2}:\d{2}\s[A-Z].',
                         '\d{2}:\d{2}\s[a-z].',
                         '\d{1}:\d{2}\s[A-Z].',
                         '\d{1}:\d{2}\s[a-z].'
                        ]

            integer_list = ['^[-+]?[0-9]+$']

            real_list = ['[-+]?([0-9]*\.[0-9]+)']
            
            # Create empty dict for each types for output 

            matchDate = []
            matchString = []
            matchInt = []
            matchReal = []
            res = df.select(col).rdd.collect()

            # Go over every cell in for each col
            for row in res:
                strrow = str(row[0])
                for r in date_list:
                    match = re.findall(r, strrow)
                    matchstr = ' '.join(match)
                    if match: 
                    # If match, cast all possible formates to timeStamp str
                        matchstr = parse_Date(matchstr)
                        matchDateTime.append(matchstr)
                for r in time_list:
                    match =  re.findall(r, strrow)
                    matchstr = ' '.join(match)
                    if match: 
                        matchDateTime.append(matchstr)
                for r in integer_list:
                    match = re.findall(r, strrow)
                    matchstr = ' '.join(match)
                    if match: 
                        matchInt.append(matchstr)
                for r in real_list:
                    match = re.findall(r, strrow)
                    matchstr = ' '.join(match)
                    if match: 
                        matchReal.append(matchstr)
                # All the entries that not match Date/Time, Integer and Real are String(Text)
                if strrow not in matchInt and strrow not in matchDateTime and strrow not in matchReal: 
                    matchString.append(strrow)

            # If data type is Date and Time: 
             if matchDateTime:
                name = col
                rdd = spark.sparkContext.parallelize(matchDateTime)
                df_date = rdd.map(lambda x: (x, )).toDF()
                df_date = df_date.withColumnRenamed("_1", name)
                outJSON_DATE = dateTimeSchema.copy()
                outJSON_DATE["data_types"]["count"] = ', '.join(map(str, count(df_date)))
                outJSON_DATE["data_types"]["max_value"] = ', '.join(map(str,  max_value(df_date)))
                outJSON_DATE["data_types"]["min_value"] = ', '.join(map(str,  min_value(df_date)))
                outColRes.append(outJSON_DATE)
            # If data type is Int and Long
            if matchInt:
                name = col 
                rdd = spark.sparkContext.parallelize(matchInt)
                df_int = rdd.map(lambda x: (x, )).toDF()
                df_int = df_int.withColumnRenamed("_1", name)
                outJSON_int = intSchema.copy()
                outJSON_int["data_types"]["count"] = ', '.join(map(str, count(df_int)))
                outJSON_int["data_types"]["max_value"] = ', '.join(map(str, max_value_for_real_int(df_int)))
                outJSON_int["data_types"]["min_value"] = ', '.join(map(str,  min_value_for_real_int(df_int)))
                outJSON_int["data_types"]["mean"] = ', '.join(map(str,  mean_for_real_int(df_int)))
                outJSON_int["data_types"]["stddev"] = ', '.join(map(str,  stddev_for_real_int(df_int)))
                outColRes.append(outJSON_int)
             # If data type is Real
            if matchReal:
                name = col 
                rdd = spark.sparkContext.parallelize(matchReal)
                df_real = rdd.map(lambda x: (x, )).toDF()
                df_real = df_real.withColumnRenamed("_1", name)
                outJSON_real = realSchema.copy()
                outJSON_real["data_types"]["count"] = ', '.join(map(str, count(df_real)))
                outJSON_real["data_types"]["max_value"] = ', '.join(map(str, max_value_for_real_int(df_real)))
                outJSON_real["data_types"]["min_value"] = ', '.join(map(str,  min_value_for_real_int(df_real)))
                outJSON_real["data_types"]["mean"] = ', '.join(map(str,  mean_for_real_int(df_real)))
                outJSON_real["data_types"]["stddev"] = ', '.join(map(str,  stddev_for_real_int(df_real)))
                outColRes.append(outJSON_real)
            # If data type is TEXT
            if matchString:
                name = col
                rdd = spark.sparkContext.parallelize(matchString)
                df_string = rdd.map(lambda x: (x, )).toDF()
                df_string = df_string.withColumnRenamed("_1", name)
                outJSON_string = textSchema.copy()
                outJSON_string["data_types"]["count"] = ', '.join(map(str, count(df_string)))
                outJSON_string["data_types"]["shortest_values"] = ', '.join(map(str, shortest_values(df_string)))
                outJSON_string["data_types"]["longest_values"] = ', '.join(map(str,  longest_values(df_string)))
                outJSON_string["data_types"]["average_length"] = ', '.join(map(str,  average_length(df_string)))
                outColRes.append(outJSON_string)

        # TODO: add output for semanticSchema
        

        # Append all column's result to the data set's final result 
            outAllRes.append(outColRes)

        # --- FUNCTION CALLS END HERE -----------------------------------------
        # ---------------------------------------------------------------------
        
        # Exporting the JSON for current dataset
        outAllRes = sc.parallelize(outAllRes)
        outAllRes.saveAsTextFile(outputDirectory + filename + '.json')
