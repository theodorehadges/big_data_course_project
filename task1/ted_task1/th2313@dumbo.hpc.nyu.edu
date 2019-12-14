#!/usr/bin/env python
# coding: utf-8

# --- NOTES -------------------------------------------------------------------
# 1. For Date interpretations from strings, there are 3 functions. Use them in
# correct pairs. Don't mix. They have a trade-off between accuracy and speed.
# -----------------------------------------------------------------------------

import os
import re
import sys
import json
import time
import numpy
import pyspark
import datetime
from copy import deepcopy
from dateutil import parser
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.functions import udf, unix_timestamp, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, TimestampType, LongType, DoubleType

# -----------------------------------------------------------------------------
# --- Function Definitions Begin ----------------------------------------------

# --- Functions for all data types --------------------------------------------

def number_distinct_values(coln):
    res = spark.sql("SELECT COUNT(DISTINCT `" + coln + "`) AS number_distinct_values FROM df").rdd.collect()[0]['number_distinct_values']
    return res

def frequent_values(coln):
    res = spark.sql("SELECT `" + coln + "` AS frequent_values FROM (SELECT `" + coln + "`, COUNT (`" + coln + "`) as f FROM df GROUP BY `" + coln + "` ORDER BY f DESC LIMIT 5)").collect()
    res = list(map(lambda x: x['frequent_values'], res))
    for i in range(len(res)):
        if type(res[i]) == datetime.datetime:
             res[i]=str(res[i])
    return res 

# --- UDFs for extracting various dtypes from string columns ------------------

# UDF to interpret int. Returns int() obj.
def interpret_int(val):
    val = str(val)
    if val  and '.' not in val:
        try:
            return int(val)
        except ValueError:
            return None
    else:
        return val

interpret_int_udf = udf(interpret_int, LongType())

# UDF to interpret float. Returns float() obj.
def interpret_float(val):
    val = str(val)
    if val and '.' in val:
        try:
            return float(val)
        except ValueError:
            return None
    else:
        return val

interpret_float_udf = udf(interpret_float, DoubleType())

# UDF to interpret datetime. Returns datetime() obj.
# LOW ACCURACY - HIGH SPEED
# def interpret_datetime(val):
#     if val:
#         try:
#             int(val)
#             return None
#         except ValueError:
#             try:
#                 float(val)
#                 return None
#             except ValueError:
#                 try:
#                     return parser.parse(val)
#                 except:
#                     return None
#     else:
#         return val

# MODERATE ACCURACY - MODERATE SPEED
# def interpret_datetime(val):
#     if val:
#         try:
#             int(val)
#             return None
#         except:
#             try:
#                 float(val)
#                 return None
#             except:
#                 try:
#                     flag=False
#                     for i in ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']:
#                         if i in val.lower() and 0<val.count('-')<=2:
#                             flag=True
#                     if val.count('-')==2 or 1<=val.count(':')<=2 or val.count('/')==2 or flag:
#                         return parser.parse(val)
#                     else:
#                         return None
#                 except:
#                     return None
#     else:
#         return val

# HIGH ACCURACY - SLOW SPEED
def interpret_datetime(val):
    val = str(val)
    if val:
        try:
            int(val)
            return None
        except:
            try:
                float(val)
                return None
            except:
                try:
                    flag=False
                    if '-' in val and not flag:
                        for i in date_list_1:
                            if bool(re.findall(i, val)):
                                flag=True
                    if '/' in val and not flag:
                        for i in date_list_2:
                            if bool(re.findall(i, val)):
                                flag=True
                    if not flag:
                        for i in date_list_3:
                            if bool(re.findall(i, val)):
                                flag=True
                    if flag or ':' in val:
                        return parser.parse(val)
                    else:
                        return None
                except:
                    return None
    else:
        return val

interpret_datetime_udf = udf(interpret_datetime, TimestampType())

# UDF to interpret strings. Returns str() obj.
# LOW ACCURACY - HIGH SPEED
# def interpret_str(val):
#     if val:
#         try:
#             int(val)
#             return None
#         except ValueError:
#             try:
#                 float(val)
#                 return None
#             except ValueError:
#                 try:
#                     parser.parse(val)
#                     return None
#                 except:
#                     return val
#     else:
#         return val

# MODERATE ACCURACY - MODERATE SPEED
# def interpret_str(val):
#     if val:
#         try:
#             int(val)
#             return None
#         except:
#             try:
#                 float(val)
#                 return None
#             except:
#                 try:
#                     flag=False
#                     for i in ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']:
#                         if i in val.lower() and 0<val.count('-')<=2:
#                             flag=True
#                     if val.count('-')==2 or 1<=val.count(':')<=2 or val.count('/')==2 or flag:
#                         parser.parse(val)
#                         return None
#                     else:
#                         return val
#                 except:
#                     return val
#     else:
#         return val 

# HIGH ACCURACY - SLOW SPEED
def interpret_str(val):
    val = str(val)
    if val:
        try:
            int(val)
            return None
        except:
            try:
                float(val)
                return None
            except:
                try:
                    flag=False
                    if '-' in val and not flag:
                        for i in date_list_1:
                            if bool(re.findall(i, val)):
                                flag=True
                    if '/' in val and not flag:
                        for i in date_list_2:
                            if bool(re.findall(i, val)):
                                flag=True
                    if not flag:
                        for i in date_list_3:
                            if bool(re.findall(i, val)):
                                flag=True
                    if flag or ':' in val:
                        parser.parse(val)
                        return None
                    else:
                        return val
                except:
                    return val
    else:
        return val 

interpret_str_udf = udf(interpret_str, StringType())

# -----------------------------------------------------------------------------

#functions for real and int type
def max_value_for_real_int(df_col, coln):
    df_col.createOrReplaceTempView("df_col")
    max_value_for_real_int = spark.sql("SELECT MAX(*) AS max_real_int FROM df_col WHERE `" + coln + "` is NOT NULL")
    res = max_value_for_real_int.rdd.collect()[0]['max_real_int']
    if numpy.isnan(res):
        return 0
    else:
        return res

def min_value_for_real_int(df_col, coln):
    df_col.createOrReplaceTempView("df_col")
    min_value_for_real_int = spark.sql("SELECT MIN(*) AS min_real_int FROM df_col WHERE `" + coln + "` is NOT NULL")
    res = min_value_for_real_int.rdd.collect()[0]['min_real_int']
    if numpy.isnan(res):
        return 0
    else:
        return res

def mean_for_real_int(df_col, coln):
    df_col.createOrReplaceTempView("df_col")
    mean_for_real_int = spark.sql("SELECT AVG(*) AS mean_real_int FROM df_col WHERE `" + coln + "` is NOT NULL")
    res = mean_for_real_int.rdd.collect()[0]['mean_real_int']
    if numpy.isnan(res):
        return 0
    else:
        return res

def stddev_for_real_int(df_col, coln):
    df_col.createOrReplaceTempView("df_col")
    stddev_for_real_int = spark.sql("SELECT STD(*) AS stddev_real_int FROM df_col WHERE `" + coln + "` is NOT NULL")
    res = stddev_for_real_int.rdd.collect()[0]['stddev_real_int']
    if numpy.isnan(res):
        return 0
    else:
        return res



#functions for date/time type
def max_value(df_col, coln, isInterpreted): 
    df_date.createOrReplaceTempView("df_col")
    if isInterpreted:
        max_value = spark.sql("SELECT `" + coln + "` FROM df_col ORDER BY Interpreted_Datetime DESC LIMIT 1")
        res = max_value.rdd.collect()[0][coln]
    else:
        max_value = spark.sql("SELECT `" + coln + "` FROM df_col ORDER BY `" + coln + "` DESC LIMIT 1")
        res = str(max_value.rdd.collect()[0][coln])
    return res

def min_value(df_col, coln, isInterpreted): 
    df_col.createOrReplaceTempView("df_col")
    if isInterpreted:
        min_value = spark.sql("SELECT `" + coln + "` FROM df_col ORDER BY Interpreted_Datetime ASC LIMIT 1")
        res = min_value.rdd.collect()[0][coln]
    else:
        min_value = spark.sql("SELECT `" + coln + "` FROM df_col ORDER BY `" + coln + "` ASC LIMIT 1")
        res = str(min_value.rdd.collect()[0][coln])
    return res



#functions for string type
def shortest_values(df_col, coln): 
    df_col.createOrReplaceTempView("df_col")
    shortest_values = spark.sql("SELECT `" + coln + "` FROM (SELECT `" + coln + "`, char_length(`" + coln + "`) AS Col_Length_MIN FROM df_col WHERE `" + coln + "` != '' AND `" + coln + "` is NOT NULL ORDER BY Col_Length_MIN ASC LIMIT 5)" )
    res = shortest_values.rdd.flatMap(lambda x: x).collect()
    return res

def longest_values(df_col, coln): 
    df_col.createOrReplaceTempView("df_col")
    longest_values = spark.sql("SELECT `" + coln + "` FROM (SELECT `" + coln + "`, char_length(`" + coln + "`) AS Col_Length_MIN FROM df_col WHERE `" + coln + "` != '' AND `" + coln + "` is NOT NULL ORDER BY Col_Length_MIN DESC LIMIT 5)" )
    res = longest_values.rdd.flatMap(lambda x: x).collect()
    return res

def average_length(df_col, coln): 
    df_col.createOrReplaceTempView("df_col")
    average_length = spark.sql("SELECT AVG(char_length(`" + coln + "`)) FROM df_col WHERE `" + coln + "` != '' AND `" + coln + "` is NOT NULL")
    res = average_length.rdd.flatMap(lambda x: x).collect()[0]
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
    sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

    # Current user path
    env_var = os.environ
    this_user = env_var['USER']

    # Input & output directories
    inputDirectory = "/user/hm74/NYCOpenData/"#sys.argv[1]
    outputDirectory = "/user/" + this_user + "/Project/"#sys.argv[2]

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
        "frequent_values": [],
        "data_types": []
    }

    # Output JSON DataType Schema - INTEGER - FOR EACH DATA TYPE
    intSchema = {
            "type": "INTEGER (LONG)",
            "count": 0,
            "max_value": 0,
            "min_value": 0,
            "mean": 0,
            "stddev": 0
    }
    # Output JSON DataType Schema - REAL
    realSchema = {
            "type": "REAL",
            "count": 0,
            "max_value": 0,
            "min_value": 0,
            "mean": 0,
            "stddev": 0            
    }
    # Output JSON DataType Schema - DATE/TIME
    dateTimeSchema = {
        "type": "DATE/TIME",
        "count": 0,
        "max_value": "",
        "min_value": ""
    }
    # Output JSON DataType Schema - TEXT
    textSchema = {
        "type": "TEXT",
        "count": 0,
        "shortest_values": [],
        "longest_values": [],
        "average_length": 0
    }
    # Output JSON Semantic Schema
    semanticSchema = {
        "semantic_type": "",
        "count": 0
    }

    # Defining different types of regex list:

    date_list_1 = ['\d{2}-\d{2}-\d{4}',
                '\d{4}-\d{2}-\d{2}',
                '\d{2}-\d{2}-\d{2}'
                '\d{2}-\d{4}',
                '\d{4}-\d{2}',
                '\d{2}-[a-zA-Z]{3}',
                '[a-zA-Z]{3}-\d{2}']

    date_list_2 = ['\d{2}/\d{2}/\d{4}',
                '\d{4}/\d{2}/\d{2}',
                '\d{2}/\d{2}/\d{2}'
                '\d{2}/\d{4}',
                '\d{4}/\d{2}',
                '\d{2}/[a-zA-Z]{3}',
                '[a-zA-Z]{3}/\d{2}']
                
    date_list_3 = ['\d{2} \d{2} \d{4}',
                '\d{4} \d{2} \d{2}',
                '\d{2} \d{2} \d{2}'
                '\d{2} \d{4}',
                '\d{4} \d{2}',
                '\d{2} [a-zA-Z]{3}',
                '[a-zA-Z]{3} \d{2}']

    # Importing dataframe from HDFS with datasetnames
    datasets = sqlContext.read.format("csv").option("header", 
        "false").option("delimiter", "\t").load(inputDirectory + "datasets.tsv")

    # List of dataset file names
    dataList = [str(row._c0) for row in datasets.select('_c0').collect()]
    # Iteration over dataframes begins bu using dataframe file names
    processCount = 1
    t000=time.time()
    
    # -------------------------------------------------------------------------
    # --- DATASET ITERATION BEGINS HERE ---------------------------------------
    # -------------------------------------------------------------------------

    for filename in dataList[0:154]:
        #filename = 'vyxt-abab'
        if filename not in ['pbk5-6r7z', 'xjtr-h2x2', 'nc67-uf89', 't29m-gskq', 'biws-g3hs', 'erm2-nwe9', 'am94-epxh']:
            print("Processing Dataset =========== : ", str(processCount) + ' - ' +filename)

            df = sqlContext.read.format("csv").option("header",
            "true").option("inferSchema", "true").option("delimiter", 
                "\t").load(inputDirectory + filename + ".tsv.gz")
            # Spark SQL view
            df.createOrReplaceTempView("df")
            # Datatypes dictionary from InferSchema
            df_dtypes = {i:j for i,j in df.dtypes}
            # Copy of the jsonSchema for current iteration 
            outJSON = deepcopy(jsonSchema)
            outJSON["dataset_name"] = filename

            # ---------------------------------------------------------------------
            # --- ENTER FUNCTION CALLS FROM HERE ----------------------------------

            # Finding "colomns" attribute for each column
            print("Number of Columns ============ : ", len(df.columns))
            columnCount = 1
            for coln in df.columns:
                #coln = '`'+coln+'`'
                t00= time.time()
                print("Processing Column ============ : ", str(columnCount) + ' - ' + coln)
                # Create an empty list for the temp result for each column 
                outJSON_col = deepcopy(colSchema)
                outJSON_col["column_name"] = coln
                outJSON_col["number_non_empty_cells"] = df.where(col('`'+coln+'`').isNotNull()).count()
                outJSON_col["number_empty_cells"] = df.where(col('`'+coln+'`').isNull()).count()
                outJSON_col["number_distinct_values"] = number_distinct_values(coln)
                outJSON_col["frequent_values"] = frequent_values(coln)
                # Candidate key check
                if outJSON_col["number_non_empty_cells"] == outJSON_col["number_distinct_values"]:
                    outJSON["key_column_candidates"].append(coln)
                    # Uncomment following if frequent values are NONE for candidate keys i.e. all vals unique
                    # outJSON_col["frequent_values"] = []
                print('Common Stage Done')
                t11= time.time()
                col_count_total = outJSON_col["number_non_empty_cells"]
                col_type = df_dtypes[coln]
                # Handle integers
                if (col_type in ['int', 'bigint', 'tinyint', 'smallint']) or (('decimal' in col_type) and col_type[-2]=='0'):
                    #print('1 '+col_type)
                    df_int = df.select('`'+coln+'`')
                    outJSON_int = deepcopy(intSchema)
                    outJSON_int["count"] = outJSON_col["number_non_empty_cells"]
                    outJSON_int["max_value"] = max_value_for_real_int(df_int, coln)
                    outJSON_int["min_value"] = min_value_for_real_int(df_int, coln)
                    outJSON_int["mean"] = mean_for_real_int(df_int, coln)
                    outJSON_int["stddev"] = stddev_for_real_int(df_int, coln)
                    outJSON_col["data_types"].append(outJSON_int)
                # Handle real numbers
                elif (col_type in ['float', 'double']) or (('decimal' in col_type) and col_type[-2]!='0'):
                    #print('2 '+col_type)
                    df_real = df.select('`'+coln+'`')
                    outJSON_real = deepcopy(realSchema)
                    outJSON_real["count"] = outJSON_col["number_non_empty_cells"]
                    outJSON_real["max_value"] = max_value_for_real_int(df_real, coln)
                    outJSON_real["min_value"] = min_value_for_real_int(df_real, coln)
                    outJSON_real["mean"] = mean_for_real_int(df_real, coln)
                    outJSON_real["stddev"] = stddev_for_real_int(df_real, coln)
                    outJSON_col["data_types"].append(outJSON_real)
                # Handle timestamps
                elif col_type in ['timestamp', 'date', 'time', 'datetime']:
                    #print('3 '+col_type)
                    df_date = df.select('`'+coln+'`').where(col('`'+coln+'`').isNotNull())
                    outJSON_DATE = deepcopy(dateTimeSchema)
                    outJSON_DATE["count"] = outJSON_col["number_non_empty_cells"]
                    outJSON_DATE["max_value"] = max_value(df_date, coln, False)
                    outJSON_DATE["min_value"] = min_value(df_date, coln, False)
                    outJSON_col["data_types"].append(outJSON_DATE)
                # Handle strings if 
                elif col_type in ['string', 'boolean']:
                    if col_type == 'boolean':
                        df = df.withColumn(coln, col(coln).cast('string'))
                        df.createOrReplaceTempView("df")
                    # Calling UDFs on the string column
                    df_string = df.select('`'+coln+'`', interpret_int_udf('`'+coln+'`').alias('Interpreted_Int'), 
                    interpret_float_udf('`'+coln+'`').alias('Interpreted_Float'), 
                    interpret_datetime_udf('`'+coln+'`').alias('Interpreted_Datetime'), 
                    interpret_str_udf('`'+coln+'`').alias('Interpreted_String'))
                    # Count of strings
                    col_count_str = df_string.where(col('Interpreted_String').isNotNull()).count()
                    # Calling the corresponding dtype functions
                    if (col_count_str != 0) and (col_count_str==col_count_total):
                        print('Str found in '+str(coln)+ ' '+str(col_type)+ ' ' + str(col_count_str))
                        df_str = df_string.select('`'+coln+'`')
                        #if col_type == 'boolean':
                        #    df_str = df_str.withColumn(coln, col('`'+coln+'`').cast('string'))
                        outJSON_string = deepcopy(textSchema)
                        outJSON_string["count"] = outJSON_col["number_non_empty_cells"]
                        outJSON_string["shortest_values"] = shortest_values(df_str, coln)
                        outJSON_string["longest_values"] = longest_values(df_str, coln)
                        outJSON_string["average_length"] = average_length(df_str, coln)
                        outJSON_col["data_types"].append(outJSON_string)
                    else:
                        col_count_int = df_string.where(col('Interpreted_Int').isNotNull()).count()
                        col_count_real = df_string.where(col('Interpreted_Float').isNotNull()).count()
                        col_count_DATE = df_string.where(col('Interpreted_Datetime').isNotNull()).count()
                        if col_count_int != 0:
                            print('Int found in '+str(coln)+ ' '+str(col_type)+ ' ' + str(col_count_int))
                            df_int = df_string.select('Interpreted_Int')
                            outJSON_int = deepcopy(intSchema)
                            outJSON_int["count"] = col_count_int
                            outJSON_int["max_value"] = max_value_for_real_int(df_int, 'Interpreted_Int')
                            outJSON_int["min_value"] = min_value_for_real_int(df_int, 'Interpreted_Int')
                            outJSON_int["mean"] = mean_for_real_int(df_int, 'Interpreted_Int')
                            outJSON_int["stddev"] = stddev_for_real_int(df_int, 'Interpreted_Int')
                            outJSON_col["data_types"].append(outJSON_int)
                        if col_count_real != 0:
                            print('Float found in '+str(coln)+ ' '+str(col_type)+ ' ' + str(col_count_real))
                            df_real = df_string.select('Interpreted_Float')
                            outJSON_real = deepcopy(realSchema)
                            outJSON_real["count"] = col_count_real
                            outJSON_real["max_value"] = max_value_for_real_int(df_real, 'Interpreted_Float')
                            outJSON_real["min_value"] = min_value_for_real_int(df_real, 'Interpreted_Float')
                            outJSON_real["mean"] = mean_for_real_int(df_real, 'Interpreted_Float')
                            outJSON_real["stddev"] = stddev_for_real_int(df_real, 'Interpreted_Float')
                            outJSON_col["data_types"].append(outJSON_real)
                        if col_count_DATE != 0:
                            print('Datetime found in '+str(coln)+ ' '+str(col_type)+ ' ' + str(col_count_DATE))
                            df_date = df_string.select('`'+coln+'`', 'Interpreted_Datetime').where(col('Interpreted_Datetime').isNotNull())
                            outJSON_DATE = deepcopy(dateTimeSchema)
                            outJSON_DATE["count"] = col_count_DATE
                            outJSON_DATE["max_value"] = max_value(df_date, coln, True)
                            outJSON_DATE["min_value"] = min_value(df_date, coln, True)
                            outJSON_col["data_types"].append(outJSON_DATE)
                        if col_count_str != 0:
                            print('Str found in '+str(coln)+ ' '+str(col_type)+ ' ' + str(col_count_str))
                            df_str = df_string.select('Interpreted_String')
                            if col_type == 'boolean':
                                df_str = df_str.withColumn('Interpreted_String', col('Interpreted_String').cast('string'))
                            outJSON_string = deepcopy(textSchema)
                            outJSON_string["count"] = col_count_str
                            outJSON_string["shortest_values"] = shortest_values(df_str, 'Interpreted_String')
                            outJSON_string["longest_values"] = longest_values(df_str, 'Interpreted_String')
                            outJSON_string["average_length"] = average_length(df_str, 'Interpreted_String')
                            outJSON_col["data_types"].append(outJSON_string)
                else:
                    pass
                    #print('Couldnt Identify : ' +col_type)
                print('String Intpt Time = ', str(time.time()-t11))
                print('Total Column Time = ', str(time.time()-t00))

            # Append all column's result to the data set's final result 
                outJSON["columns"].append(outJSON_col)
                columnCount += 1
            print('Total Time '+str(time.time()-t000))

            # --- FUNCTION CALLS END HERE -----------------------------------------
            # ---------------------------------------------------------------------
            
            # Exporting the JSON for current dataset
            print("Saving Dataset =============== : ", str(processCount) + ' - ' +filename)
            processCount += 1
            outJSON = sc.parallelize([json.dumps(outJSON)])
            outJSON.saveAsTextFile(outputDirectory + filename + '/task1.json')

