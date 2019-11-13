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
import string
import unicodedata
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



def get_col_meta(df):
    metaCols = []
    for col, dtype in df.dtypes: # change to map function later
        if dtype is 'int':
          intJSON = intSchema.copy()
          intJSON["column_name"] = col

          # anything non-numeric will be set to null since it can't be casted
          
          # Real numbers: any val which can be casted to float
          reals = spark.sql("select float(`" + col + "`) from df")
          reals.createOrReplaceTempView("reals")

          # Ints: any real number divisible by 1
          ints = spark.sql("select `" + col + "` \
              from reals where `" + col + "` % 1 == 0")

          num_non_empty = spark.sql("select sum(case when `" + col + "` \
              is not null then 1 end) from df")
          intJSON['number_non_empty_cells'] = num_non_empty

          num_distinct_vals = spark.sql("select count(distinct `" + col + "`) from df")
          intJSON['number_distinct_values'] = num_distinct_vals

          result = spark.sql("select `" + col + "`, count(`" + col + "`) \
              as frequency from df group by `" + col + "` \
              order by frequency desc limit 5")
          frequent_vals = result.select(col).rdd.flatMap(lambda x: x).collect()
          intJSON['frequent_values'] = frequent_vals
          stats_list = df.describe(col).rdd.flatMap(lambda x: x).collect()

          # might need to manually calculate these rather than use describe()
          #intJSON['data_types']['count'] = int(stats_list[1])
          #intJSON['data_types']['max_value'] = stats_list[9]
          #intJSON['data_types']['min_value'] = 
          #intJSON['data_types']['mean'] = 
          #intJSON['data_types']['stddev'] = 

          #metaCols.append(intJSON) # but doesn't work rn since parallelized
        metaCols.append(dtype) # just putting dtypes for now to fill the slot
    return(metaCols)


    


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
        # 02) Finding "key_columns_candidates" attribute
        outJSON["key_columns_candidates"] = get_key_columns_candidates(df)
        outJSON["columns"] = get_col_meta(df)




        # --- FUNCTION CALLS END HERE -----------------------------------------
        # ---------------------------------------------------------------------
        
        # Exporting the JSON for current dataset
        outJSON = sc.parallelize([outJSON])
        outJSON.saveAsTextFile(outputDirectory + filename + '.json')
