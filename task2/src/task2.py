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
from pyspark.sql.functions import udf, unix_timestamp, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, FloatType, DateType, TimestampType
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline 
from collections import Counter
#import spacy
#from spacy import displacy
#import en_core_web_sm
# -----------------------------------------------------------------------------
# --- Function Definitions Begin ----------------------------------------------

# Function to find mean and stdv for all files
def mean_stdv(df):
    unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())
    for i in ["count"]:
        assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")
        scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")
        pipeline = Pipeline(stages=[assembler, scaler])
        df = pipeline.fit(df).transform(df).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")
        df_stats = df.select(_mean(col('count_Scaled')).alias('mean'),_stddev(col('count_Scaled')).alias('std')).collect()
        mean = df_stats[0]['mean']
        std = df_stats[0]['std']
        return df_stats 

# Function to sum all count of values for all files
def count_all_values(df):
    res = df.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
    return res

# Regex function to check website type
def re_find_website(df,count_all,found_type):
    web_re_rexpr = "WWW\.|\.COM|HTTP\:"
    df_filtered = df.filter(df["val"].rlike(web_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        if (res >= 0.85): 
            found_type = found_type + ["website"]
        return res, found_type
    else:
        return 0, found_type

# Regex function to check zip type
def re_find_zipCode(df,count_all,found_type):
    zip_re_rexpr = "\d\d\d\d\d|\d\d\d\d\d\-\d\d\d|\d\d\d\d\d\d\d\d"
    df_filtered = df.filter(df["val"].rlike(zip_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        if (res >= 0.85): 
            found_type = found_type + ["zip_code"]
        return res, found_type
    else:
        return 0, found_type

# Regex function to check buildingCode type
def re_find_buildingCode(df,count_all,found_type):
    bc_re_rexpr = "([A-Z])\d\-"
    df_filtered = df.filter(df["val"].rlike(bc_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        if (res >= 0.85): 
            found_type = found_type + ["building_classification"]
        return res, found_type
    else:
        return 0, found_type

# Regex function to check phone number type
def re_find_phoneNum(df,count_all,found_type):
    phone_re_rexpr = "\d\d\d\d\d\d\d\d\d\d|\(\d\d\d\)\d\d\d\d\d\d\d|\d\d\d\-\d\d\d\-\d\d\d\d"
    df_filtered = df.filter(df["val"].rlike(phone_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        if (res >= 0.85): 
            found_type = found_type + ["phone_number"]
        return res, found_type
    else:
        return 0, found_type

# Regex function to check lat_lon type
def re_find_lat_lon(df,count_all,found_type):
    ll_re_rexpr = "\([-+]?[0-9]+\.[0-9]+\,\s*[-+]?[0-9]+\.[0-9]+\)"
    df_filtered = df.filter(df["val"].rlike(ll_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        if (res >= 0.85): 
            found_type = found_type + ["lat_lon_cord"]
        return res, found_type
    else:
        return 0, found_type

# Regex function to check street_addrees type
def re_find_street_address(df,count_all,found_type):
    st_re_rexpr = "\sROAD|\sSTREET|\sPLACE|\sDRIVE|\sBLVD|\sST|\sRD|\sDR|\sAVENUE"
    df_filtered = df.filter(df["val"].rlike(st_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        if (res >= 0.5): 
            found_type = found_type + ["address", "street"]
        return res, found_type
    else:
        return 0, found_type

# Regex function to check school name type
def re_find_school(df,count_all,found_type):
    school_re_rexpr = "SCHOOL|ACADEMY|HS\s|ACAD|I.S.\s|IS\s|M.S.\s|P.S\s|PS\s"
    df_filtered = df.filter(df["val"].rlike(school_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        if (res >= 0.5): 
            found_type = found_type + ["school_name"]
        return res, found_type
    else:
        return 0, found_type


# --- Function Definitions End ------------------------------------------------
# -----------------------------------------------------------------------------



# -----------------------------------------------------------------------------
# --- MAIN --------------------------------------------------------------------

if __name__ == "__main__":
    # Setting spark context and 
    sc = SparkContext()
    spark = SparkSession \
        .builder \
        .appName("project_task2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

    # Current user path
    env_var = os.environ
    this_user = env_var['USER']

    # Input & output directories
    inputDirectory = "/user/hm74/NYCColumns/"#sys.argv[1]
    outputDirectory = "/user/" + this_user + "/project/task2/"#sys.argv[2]

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

    # Importing cluster3 format it and put it into a list
    raw_data = sc.textFile("/user/aj2885/Project_Resource/cluster3_labels.tsv")
    raw_list = raw_data.map(lambda x: x.split("\t")).collect()

    # Iteration over dataframes begins bu using dataframe file names
    processCount = 1

    # Create schema for raw data before reading into df 
    customSchema = StructType([
                StructField("val", StringType(), True),
                StructField("count", IntegerType(), True)])

#Testing first 50 files
    for filerow in raw_list[0:1]:
        filename = filerow[0]
        labels = literal_eval(filerow[1])
        print("Processing Dataset =========== : ", str(processCount) + ' - ' +filename)
        # Read file to dataset and apply all regex functions
        found_type = []
        fileinfo = []
        regex_res = []
        df = sqlContext.read.format("csv").option("header","false").option("inferSchema", "true").option("delimiter", "\t").schema(customSchema).load(inputDirectory + filename)
        df_stats = mean_stdv(df)
        mean = df_stats[0]['mean']
        std = df_stats[0]['std']
        count_all = count_all_values(df)
        precentage_website, found_type = re_find_website(df,count_all,found_type)
        precentage_zip, found_type= re_find_zipCode(df,count_all,found_type)
        percentage_buildingCode, found_type = re_find_buildingCode(df,count_all,found_type)
        percentage_phoneNum, found_type = re_find_phoneNum(df,count_all,found_type)
        percentage_lat_lon, found_type = re_find_lat_lon(df,count_all,found_type)
        percentage_add_st, found_type = re_find_street_address(df,count_all,found_type)
        percentage_add_school, found_type = re_find_school(df,count_all,found_type)
        fileinfo.extend([filename,mean,std,count_all,precentage_website, precentage_zip, percentage_buildingCode, percentage_phoneNum,percentage_lat_lon,percentage_add_st,percentage_add_school,found_type])
        regex_res.append(fileinfo)

    # Output regex function result 
    rdd = sc.parallelize(regex_res)
    row_rdd = rdd.map(lambda x: Row(x))
    df = row_rdd.toDF()
    df = df.select(col('_1').alias('coln'))
    length = len(df.select('coln').take(1)[0][0])
    df = df.select([df.coln[i] for i in range(length)])
    df = df.select(col('coln[0]').alias('filename'),col('coln[1]').alias('mean'),col('coln[2]').alias('stdv'),
                    col('coln[3]').alias('count_all'),col('coln[4]').alias('precentage_website'),col('coln[5]').alias('precentage_zip'),
                    col('coln[6]').alias('percentage_buildingCode'),col('coln[7]').alias('percentage_phoneNum'),col('coln[8]').alias('percentage_lat_lon'),
                    col('coln[9]').alias('percentage_add_st'),col('coln[10]').alias('percentage_add_school'),col('coln[11]').alias('types'),
                    )

    types_found_count = df.where(col('types') > " ").count()
    print(types_found_count)



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
    #outJSON = sc.parallelize([json.dumps(outJSON)])
    #outJSON.saveAsTextFile(outputDirectory + filename + '.json')
