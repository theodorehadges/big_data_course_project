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
from copy import deepcopy
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.functions import udf, unix_timestamp, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, TimestampType

# -----------------------------------------------------------------------------
# --- Function Definitions Begin ----------------------------------------------


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
    sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

    # Current user path
    env_var = os.environ
    this_user = env_var['USER']

    # Input & output directories
    inputDirectory = "/user/hm74/NYCOpenData/"#sys.argv[1]
    outputDirectory = "/user/" + this_user + "/project/task1/"#sys.argv[2]

    # Output JSON Semantic Schema
    semanticSchema = {
        "semantic_type": "",
        "count": 0
    }

    # Define of different types regex dict:
    expr_dic = {"Street": "ROAD|STREET|PLACE|DRIVE|BLVD|%ST%|%RD%|DR|AVENUE",
                "Website" : "WWW.|.COM|HTTP://",
                "BuildingCode" : "([A-Z])\d\-",
                "PhoneBumber":"\d\d\d\d\d\d\d\d\d\d|\(\d\d\d\)\d\d\d\d\d\d\d|\d\d\d\-\d\d\d\-\d\d\d\d",
                "ZipCode":"\d\d\d\d\d|\d\d\d\d\d\-\d\d\d|\d\d\d\d\d\d\d\d",
                "Lat_Lon" : "\([-+]?[0-9]+\.[0-9]+\,\s*[-+]?[0-9]+\.[0-9]+\)",
                "SchoolName" : "SCHOOL|ACADEMY|HS|ACAD|I.S.|IS|M.S.|P.S|PS",
                }

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
    for filename in raw_list[0:50]:

        print("Processing Dataset =========== : ", str(processCount) + ' - ' +filename)
        df = sqlContext.read.format("csv").option("header",
        "false").option("inferSchema", "true").option("delimiter", 
        "\t").schema(customSchema).load(inputDirectory + filename)

        # Count all val in df with count 
        count_all = df.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]

        for k,v in expr_dic.items():
        df_filtered = df.filter(df["val"].rlike(v))
        if (df_filtered.count() is not 0):
            count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
#             print("count_filtered:",count_filtered)
            percentage = float(count_filtered/count_all)
#             print("percentage of rows contains:", v , "is",percentage )
            #Can change the percentage threshold later 
            if (percentage > 0.85) :
                print("semantic type is" , k)

        # # Reading the task1 JSON
        # outJSON = sc.textFile(outputDirectory + filename + '.json')
        # outJSON = json.load(outJSON.collect()[0])
        # # Spark SQL view
        # df.createOrReplaceTempView("df")
        # # Datatypes dictionary from InferSchema
        # df_dtypes = {i:j for i,j in df.dtypes}
        # # Copy of semantic types schema
        # sem_types = deepcopy(semanticSchema)
        # # ---------------------------------------------------------------------
        # # --- ENTER FUNCTION CALLS FROM HERE ----------------------------------

        # # Finding "colomns" attribute for each column
        # print("Number of Columns ============ : ", len(df.columns))
        # columnCount = 1
        # for coln in df.columns:
        #     print("Processing Column ============ : ", str(columnCount) + ' - ' + coln)
        #     col_type = df_dtypes[coln]
        #     # Handle integers decimal(10,0)
        #     if (col_type in ['int', 'bigint', 'tinyint', 'smallint']) or (('decimal' in col_type) and col_type[-2]=='0'):
        #         #print('1 '+col_type)
        #         pass
        #     # Handle real numbers
        #     elif (col_type in ['float', 'double']) or (('decimal' in col_type) and col_type[-2]!='0'):
        #         #print('2 '+col_type)
        #         pass
        #     # Handle timestamps
        #     elif col_type in ['timestamp', 'date', 'time', 'datetime']:
        #         #print('3 '+col_type)
        #         pass
        #     # Handle strings 
        #     elif col_type in ['string', 'boolean']:
        #         #print('4 '+col_type)
        #         pass
        #     else:
        #         #print('NOT FOUND' +col_type)
        #         pass

        # columnCount+=1
        
        # # USE ME to append all semantic information to the JSON
        #     for i in range(len(outJSON["columns"])):
        #         if outJSON["columns"][i]["column_name"]== coln:
        #             outJSON["columns"][i]["semantic_types"].append(sem_types)

        # # --- FUNCTION CALLS END HERE -----------------------------------------
        # # ---------------------------------------------------------------------
        
        # USE ME to export the JSON for current dataset
        print("Saving Dataset =============== : ", str(processCount) + ' - ' +filename)
        processCount += 1
        outJSON = sc.parallelize([json.dumps(outJSON)])
        outJSON.saveAsTextFile(outputDirectory + filename + '.json')
