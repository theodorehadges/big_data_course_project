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
    this_user = env_var['USER'] # this_user instead of our netID

    # Input & output directories
    #inputDirectory = "/user/hm74/NYCOpenData/"#sys.argv[1]

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
    # List of dataset file names
    #dataList = [str(row._c0) for row in datasets.select('_c0').collect()]
    # Iteration over dataframes begins bu using dataframe file names
    #for filename in dataList[0:4]:
    df = sqlContext.read.format("csv").option("header",
        "true").option("inferSchema", "false").option("delimiter",
        "\t").load("dummy_set.tsv")

    col_names = df.columns
    print(col_names)

    df.createOrReplaceTempView("df")
    sqlContext.cacheTable("df")
    df_result = spark.sql("select * \
              from df \
              ").show()


    

    for col_name in col_names: # can change to map later
      col_df = df.groupBy(col_name). \
          count(). \
          orderBy('count', ascending=False)
      col_df.rdd.map(lambda x: print(x))

      # number of distinct values in column
      #print(col_df.select(col_name).distinct().count())
        
      
      # try to cast to int. all ints will show, all others will be null
      #numerics = spark.sql("select int("' + col + '") \
    #                    from df").show()

   
    #result = df.rdd.map(lambda x: x.Recurring)
    #print(result.collect())
    #df.describe().show()
    #df.printSchema()
    #for row in df['Recurring']:
    #  print(row, dtype)

    #spark.sql("select * from df").show()
    
    #for name, dtype in df.dtypes:
    #  print(name, dtype)
      
      #if dtype is 'integer':
      #  colJSON = intSchema.copy()
      #  colJSON["column_name"] = name
        #colJSON["


      
      # Copy of the jsonSchema for current iteration 
      #outJSON = jsonSchema.copy()
      
      # ---------------------------------------------------------------------
      # --- ENTER FUNCTION CALLS FROM HERE ----------------------------------
      
      #filename = "output_dummy"
      # 01) Setting the "dataset_name" attribute
      #outJSON["dataset_name"] = filename
      # 02) Finding "key_columns_candidates" attribute
      #outJSON["key_columns_candidates"] = get_key_columns_candidates(df)




      # --- FUNCTION CALLS END HERE -----------------------------------------
      # ---------------------------------------------------------------------
      
      # Exporting the JSON for current dataset
      #outJSON = sc.parallelize([outJSON])
      #outJSON.saveAsTextFile(outputDirectory + filename + '.json')
