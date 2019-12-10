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
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0

# Regex function to check zip type
def re_find_zipCode(df,count_all,found_type):
    zip_re_rexpr = "^\d{5}?$|^\d{5}?-\d\d\d$|^\d{8}?$"
    df_filtered = df.filter(df["val"].rlike(zip_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        if (res >= 0.85): 
            found_type = found_type + ["zip_code"]
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0

# Regex function to check buildingCode type
def re_find_buildingCode(df,count_all,found_type):
    bc_re_rexpr = "([A-Z])\d\-"
    df_filtered = df.filter(df["val"].rlike(bc_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        if (res >= 0.85): 
            found_type = found_type + ["building_classification"]
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0 

# Regex function to check phone number type
def re_find_phoneNum(df,count_all,found_type):
    phone_re_rexpr = "^\d{10}?$|^\(\d\d\d\)\d\d\d\d\d\d\d$|^\d\d\d\-\d\d\d\-\d\d\d\d$"
    df_filtered = df.filter(df["val"].rlike(phone_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        if (res >= 0.85): 
            found_type = found_type + ["phone_number"]
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0

# Regex function to check lat_lon type
def re_find_lat_lon(df,count_all,found_type):
    ll_re_rexpr = "\([-+]?[0-9]+\.[0-9]+\,\s*[-+]?[0-9]+\.[0-9]+\)"
    df_filtered = df.filter(df["val"].rlike(ll_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        if (res >= 0.85): 
            found_type = found_type + ["lat_lon_cord"]
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0

# Regex function to check street_addrees type
def re_find_street_address(df,count_all,col_length,found_type):
    st_re_rexpr = "\sROAD|\sSTREET|\sPLACE|\sDRIVE|\sBLVD|\sST|\sRD|\sDR|\sAVENUE|\sAVE"
    df_filtered = df.filter(df["val"].rlike(st_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        if (res >= 0.8): 
            if (col_length >= 15):
                found_type = found_type + ["address"]
            elif (col_length < 15):
                found_type = found_type + ["street"]
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0

# Regex function to check school name type
def re_find_school(df,count_all,found_type):
    school_re_rexpr = "\sSCHOOL|\sACADEMY|HS\s|ACAD|I.S.\s|IS\s|M.S.\s|P.S\s|PS\s|ACADEMY\s"
    df_filtered = df.filter(df["val"].rlike(school_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        if (res >= 0.5): 
            found_type = found_type + ["school_name"]
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0

# Regex function for checking house number 
def re_find_houseNo(df,count_all,found_type):
    houseNo_re_rexpr = "^\d{2}?$|^\d{3}?$|^\d{4}?$"
    df_filtered = df.filter(df["val"].rlike(houseNo_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        if (res >= 0.85): 
            found_type = found_type + ["house number"]
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0

# Regex function for checking school subject
def re_find_school_subject(df,count_all,found_type):
    school_subj_re_rexpr = "^ENGLISH$|^ENGLISH\s[0-9]?$|^MATH\s[A-Z$]|^MATH$|^SCIENCE$|^SOCIAL\sSTUDIES$|^ALGEBRA\s[A-Z]$|\
                            ^CHEMISTRY$|^ASSUMED\sTEAM\sTEACHING$|^EARTH\sSCIENCE$|^GEOMETRY$|^ECONOMICS$|^GLOBAL HISTORY$|\
                            ^GLOBAL\sHISTORY[A-Z]$|^LIVING ENVIRONMENT$|^PHYSICS$|^US\sGOVERNMENT$|^US\sGOVERNMENT$|^US\sGOVERNMENT\s&|\
                            ^US\SHISTORY$|^GLOBAL HISTORY\s[0-9]?$"
    df_filtered = df.filter(df["val"].rlike(school_subj_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        print(res)
        if (res >= 0.5): 
            found_type = found_type + ["school subject"]
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0

# Regex function for checking school level 
def re_find_schoolLevel(df,count_all,found_type):
    schlvl_re_rexpr = "^[K]\-\d?$|^HIGH SCHOOL$|^ELEMENTARY$|^ELEMENTARY SCHOOL$|^MIDDLE SCHOOL$|^TRANSFER\sSCHOOL$|^MIDDLE$|^HIGH\sSCHOOL\sTRANSFERL$|^YABC$|^[K]\-[0-9]{2}$"
    df_filtered = df.filter(df["val"].rlike(schlvl_re_rexpr))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        if (res >= 0.85): 
            found_type = found_type + ["school level"]
        return res, found_type, count_filtered
    else:
        return 0, found_type, 0

# --- Functions FOR NLP Starts HERE -------------------------------------------
def nlp_find_person(df,count_all,found_type):
    #Your Code HERE: 
    #Use count_all for percentage calculation
    #Please return two values: (1)percentage of such type in this col AND (2)the type found for this column
    #if found:
#         found_type = found_type + ["person"]
    #if not found:
    return 0, found_type, 0

def nlp_find_business_name(df,count_all,found_type):
    #Your Code HERE:
    return 0, found_type, 0

def nlp_find_vehicle_type(df,count_all,found_type):
    #Your Code HERE:
    return 0, found_type, 0

def nlp_find_color(df,count_all,found_type):
    #Your Code HERE:
    return 0, found_type, 0

def nlp_find_car_make(df,count_all,found_type):
    #Your Code HERE:
    return 0, found_type, 0

def nlp_find_car_model(df,count_all,found_type):
    #Your Code HERE:
    return 0, found_type, 0

def nlp_find_neighborhood(df,count_all,found_type):
    #Your Code HERE:
    return 0, found_type, 0

def nlp_find_borough(df,count_all,found_type):
    #Your Code HERE:
    return 0, found_type, 0

def nlp_find_city(df,count_all,found_type):
    #Your Code HERE:
    return 0, found_type, 0

# --- Function FOR NLP End ------------------------------------------------

# --- Functions FOR LIST COMPARISON Starts HERE -------------------------------
def list_find_school_subject(df,count_all,found_type):
    #Your Code HERE: 
    #Use count_all for percentage calculation
    #Please return two values: (1)percentage of such type in this col AND (2)the type found for this column
    #if found:
    #found_type = found_type + ["school subject"]
    #if not found:
    return 0, found_type, 0

def list_find_business_name(df,count_all,found_type):
    #Your Code HERE: 
    return 0, found_type, 0

def list_find_neighborhood(df,count_all,found_type):
    #Your Code HERE: 
    return 0, found_type, 0

def list_find_area_of_study(df,count_all,found_type):
    #Your Code HERE: 
    return 0, found_type, 0

def list_find_agency(df,count_all,found_type):
    #Your Code HERE: 
    return 0, found_type, 0

def list_find_location_type(df,count_all,found_type):
    #Your Code HERE: 
    return 0, found_type, 0

def list_find_parks_playgrounds(df,count_all,found_type):
    #Your Code HERE: 
    return 0, found_type, 0

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
        "semantic_type": [],
        "count": 0
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

#Testing first 10 files
    for filerow in raw_list[0:10]:
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

        #added col_length which is the average length of the col
        df_length = df.select(_mean(length(col("val"))).alias('avg_length'))
        col_length= df_length.collect()[0][0]

        percentage_website, found_type, type_count_web = re_find_website(df,count_all,found_type)
        percentage_zip, found_type, type_count_zip = re_find_zipCode(df,count_all,found_type)
        percentage_buildingCode, found_type,type_count_building = re_find_buildingCode(df,count_all,found_type)
        percentage_phoneNum, found_type, type_count_phone = re_find_phoneNum(df,count_all,found_type)
        percentage_lat_lon, found_type, type_count_lat_lon = re_find_lat_lon(df,count_all,found_type)
        percentage_add_st, found_type, type_count_add_st = re_find_street_address(df,count_all,col_length,found_type)
        percentage_school_name, found_type, type_count_school_name= re_find_school(df,count_all,found_type)
        percentage_house_no, found_type ,type_count_house_no= re_find_houseNo(df,count_all,found_type)
        percentage_school_lvl, found_type, type_count_school_lvl= re_find_schoolLevel(df,count_all,found_type)
        percentage_school_subject, found_type, type_count_school_subject= re_find_school_subject(df,count_all,found_type)
        
        type_count = type_count_web + type_count_zip + type_count_building + type_count_phone + type_count_lat_lon + type_count_add_st + type_count_school_name +type_count_house_no +type_count_school_lvl + type_count_school_subject
        
        #give a default value for all other precentages 
        percentage_person = 0
        percentage_business_name = 0
        percentage_vehicle_type = 0
        percentage_color = 0
        percentage_car_make = 0
        percentage_car_model = 0
        percentage_neighborhood = 0
        percentage_borough= 0 
        percentage_city = 0
        percentage_area_of_study = 0
        percentage_location = 0
        percentage_agency = 0
        percentage_parks_playgrounds = 0

        #STEP TWO: NLP LABEL AND LIST CHECK
        # if not found_type:
        #     #ANKUSH PART: NLP CHECK TYPES
        #     percentage_person, found_type, type_count_person = nlp_find_person(df,count_all,found_type)
        #     percentage_business_name, found_type, type_count_business = nlp_find_business_name(df,count_all,found_type)
        #     percentage_vehicle_type, found_type, type_count_vehicle_type = nlp_find_vehicle_type(df,count_all,found_type)
        #     percentage_color, found_type, type_count_color = nlp_find_color(df,count_all,found_type)
        #     percentage_car_make, found_type, type_count_car_make = nlp_find_car_make(df,count_all,found_type)
        #     percentage_car_model, found_type, type_count_car_model = nlp_find_car_model(df,count_all,found_type)
        #     percentage_neighborhood, found_type, type_count_neighborhood = nlp_find_neighborhood(df,count_all,found_type)
        #     percentage_borough, found_type, type_count_borough = nlp_find_borough(df,count_all,found_type)
        #     percentage_city, found_type, type_count_city = nlp_find_city(df,count_all,found_type)
        
        #     #TED PART: LIST or SIMILARITY CHECK TYPEs
        #     percentage_school_subject, found_type, type_count_school_subject= list_find_school_subject(df,count_all,found_type)
        #     percentage_business_name, found_type, type_count_business= list_find_business_name(df,count_all,found_type)
        #     percentage_neighborhood, found_type, type_count_neighborhood= list_find_neighborhood(df,count_all,found_type)
        #     percentage_area_of_study, found_type, type_count_area_of_study = list_find_area_of_study(df,count_all,found_type)
        #     percentage_agency, found_type, type_count_agency= list_find_agency(df,count_all,found_type)
        #     percentage_location, found_type, type_count_location= list_find_location_type(df,count_all,found_type)
        #     percentage_parks_playgrounds, type_count_location_parks_playgrounds = list_find_parks_playgrounds(df,count_all,found_type
        # !!! NOTE: Please remeber to add type_count_XXX back to type_count in LINE 347
        fileinfo.extend([filename,mean,std,count_all,col_length, percentage_website, percentage_zip,percentage_buildingCode,percentage_phoneNum,percentage_lat_lon,percentage_add_st,percentage_school_name,percentage_house_no,percentage_school_lvl,percentage_person,percentage_school_subject,percentage_vehicle_type, percentage_color,percentage_car_make,percentage_car_model,percentage_neighborhood,percentage_borough,percentage_city,percentage_business_name,percentage_area_of_study,percentage_location,percentage_parks_playgrounds,found_type, type_count])
        regex_res.append(fileinfo)
        print(regex_res)
        # USE ME to export the JSON for current dataset
        print("Saving Dataset =============== : ", str(processCount) + ' - ' +filename)
        processCount += 1
        outJSON = deepcopy(jsonSchema)
        outJSON["column_name"] = filename
        outJSON["semantic_type"] = found_type
        outJSON["count"] = type_count
        outJSON = sc.parallelize([json.dumps(outJSON)])
        outJSON.saveAsTextFile(outputDirectory + filename + '/task2.json')



    # Output regex function result 
    rdd = sc.parallelize(regex_res)
    row_rdd = rdd.map(lambda x: Row(x))
    df = row_rdd.toDF()
    df = df.select(col('_1').alias('coln'))
    length = len(df.select('coln').take(1)[0][0])
    df = df.select([df.coln[i] for i in range(length)])
    df = df.select(col('coln[0]').alias('filename'),col('coln[1]').alias('mean'),col('coln[2]').alias('stdv'),
               col('coln[3]').alias('count_all'),col('coln[4]').alias('col_length'),col('coln[5]').alias('precentage_website'),
               col('coln[6]').alias('precentage_zip'),col('coln[7]').alias('percentage_buildingCode'),col('coln[8]').alias('percentage_phoneNum'),
               col('coln[9]').alias('percentage_lat_lon'),col('coln[10]').alias('percentage_add_st'),col('coln[11]').alias('percentage_school_name'),
               col('coln[12]').alias('percentage_houseNo'),col('coln[13]').alias('percentage_school_lvl'),col('coln[14]').alias('percentage_person'),
               col('coln[15]').alias('percentage_school_subject'),col('coln[16]').alias('percentage_vehicle_type'),col('coln[17]').alias('percentage_color'),
               col('coln[18]').alias('percentage_car_make'),col('coln[19]').alias('percentage_car_model'),
               col('coln[20]').alias('percentage_neighborhood'),col('coln[21]').alias('percentage_borough'),col('coln[22]').alias('percentage_city'),
               col('coln[23]').alias('percentage_business_name'),col('coln[24]').alias('percentage_area_of_study'),col('coln[25]').alias('percentage_location_type'),
               col('coln[26]').alias('percentage_parks_playgrounds'),col('coln[27]').alias('types'), col('coln[28]').alias('types_count')
               )

    types_found_count = df.where(col('types') > " ").count()
    print(types_found_count)
    df.write.csv('regex_res.csv')
