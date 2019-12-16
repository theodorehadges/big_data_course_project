#!/usr/bin/env python
# coding: utf-8

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
from collections import Counter, defaultdict
from itertools import groupby
#import spacy
import en_core_web_md
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
        # mean = df_stats[0]['mean']
        # std = df_stats[0]['std']
        return df_stats 

# Function to sum all count of values for all files
def count_all_values(df):
    res = df.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
    return res

# Soundex Algorithm
# Source: https://www.rosettacode.org/wiki/Soundex#Python
def soundex(word):
    word = str(word)
    word = re.sub(r'[^\w]', '', word)
    if word != '' and word != None:
        codes = ("bfpv","cgjkqsxz", "dt", "l", "mn", "r")
        soundDict = dict((ch, str(ix+1)) for ix,cod in enumerate(codes) for ch in cod)
        cmap2 = lambda kar: soundDict.get(kar, '9')
        sdx =  ''.join(cmap2(kar) for kar in word.lower())
        sdx2 = word[0].upper() + ''.join(k for k,g in list(groupby(sdx))[1:] if k!='9')
        sdx3 = sdx2[0:4].ljust(4,'0')
        return sdx3
    else:
        return ''

# Function to limit string to n characters:
def limitN(s, n):
    if s != '':
        s = s.split()[0]
        if len(s)>n:
            return s[:n]
        else:
            return s
    else:
        return ''

# Function to explicit type cast any Unicode error counts
def intchk(s):
    if type(s) != int:
        return 0
    else:
        return int(s)

# Regex function to check website type
def re_find_website(df,count_all,found_type):
    web_re_rexpr = "WWW\.|\.COM|HTTP\:|HTTPS\:"
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
        if (res >= 0.75): 
            if (col_length >= 15):
                found_type = found_type + ["address"]
            elif (col_length < 15):
                found_type = found_type + ["street_name"]
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
            found_type = found_type + ["house_number"]
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
            found_type = found_type + ["subject_in_school"]
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
            found_type = found_type + ["school_level"]
        return res, found_type, count_filtered
    else:
        return 0, found_type, 0

# --- Functions with NLP start HERE -------------------------------------------


# Named Entity Recognizer for city and person_name classification
def nlp_find_city_person(df,count_all,found_type):
    rdd1 = df.rdd.map(lambda x: (x[0], x[1]))
    rdd1 = rdd1.collect()
    labels = []
    labelCounts = defaultdict(int)
    labelCounts['GPE'] = 0
    labelCounts['PERSON'] = 0
    for i in range(len(rdd1)):
        labels += [[j.label_, rdd1[i][1]] for j in nlp(rdd1[i][0]).ents]
    labels = sc.parallelize(labels)
    labels = labels.reduceByKey(lambda x,y: x+y ).collect()
    for i in labels:
        if i[0] in ['GPE', 'PERSON', 'ORG']:
            labelCounts[i[0]]=i[1]
    if labelCounts['GPE']!=0 and labelCounts['PERSON']!=0:
        found_type=[]
        gpeRatio = labelCounts['GPE']/(labelCounts['GPE']+labelCounts['PERSON'])
        personRatio = labelCounts['PERSON']/(labelCounts['GPE']+labelCounts['PERSON'])
        if gpeRatio >= personRatio and 'city ' in filename.lower():
            validCount = labelCounts['GPE'] + labelCounts['ORG']
            found_type += ['city']
            return gpeRatio, validCount, 0, 0, found_type
        elif gpeRatio < personRatio and (('name' in filename.lower() and 'last' in filename.lower()) or ('name' in filename.lower() and 'first' in filename.lower()) or ('.mi.' in filename.lower()) or ('initial' in filename.lower()) or ('candmi' in filename.lower())):
            validCount = labelCounts['PERSON'] + labelCounts['ORG']
            found_type += ['person_name']
            return 0, 0, personRatio, validCount, found_type
    else:
        return 0, 0, 0, 0, found_type

# Color determination using SOUNDEX
def nlp_find_color(df,count_all,found_type):
    colors = {'W300':'WHITE', 'B420':'BLACK', 'G600':'GREY', 'B400':'BLUE', 'G650':'GREEN', 'S416':'SILVER', 'Y400':'YELLOW', 'R300':'RED', 'O652':'ORANGE', 'B650':'BROWN'}
    colors_ab = {'W000':'WHITE', 'B200':'BLACK', 'G000':'GREY', 'B400':'BLUE', 'G600':'GREEN'}
    rdd1 = df.rdd.map(lambda x: (soundex(x[0]), x[1]))
    if rdd1.count() !=0:
        rdd1 = rdd1.reduceByKey(lambda x,y: x+y).sortBy(keyfunc= lambda x: x[1], ascending=False)
        # Count of color names that got mapped from above list
        conf1 = rdd1.filter(lambda x: x[0] in colors.keys()).count()
        # Count of per color frequency
        conf2 = rdd1.filter(lambda x: (x[0] in colors.keys()) or (x[0] in colors_ab.keys())).map(lambda x: x[1])
        if conf2.count() != 0:
            conf2 = conf2.reduce(lambda x,y: x+y)
            res = conf2/count_all
            if conf1/len(colors.keys())>=0.5 and res >=0.5:
                # Number of valid colors. ignoring colors starting from numbers, symbols or empty strings
                colors.update(colors_ab)
                count_filtered = rdd1.filter(lambda x: re.match('^[A-Za-z]', x[0]) ).map(lambda x: x[1]).reduce(lambda x,y: x+y)
                examples = rdd1.filter(lambda x: (x[0] in colors.keys()) or (x[0] in colors_ab.keys())).map(lambda x: (colors[x[0]], x[1])).reduceByKey(lambda x,y: x+y).collect()
                examples.append(('OTHERS', count_all-conf2))
                found_type = found_type+['color']
                return res, found_type, count_filtered
    return 0, found_type, 0

# Car make determination using SOUNDEX
def nlp_find_car_make(df,count_all,found_type):
    #Check for n84m-kx4j.VEHICLE_MAKE.txt. It has most different names
    cntR = df.rdd.map(lambda x: len(x[0])).count()
    sumR = df.rdd.map(lambda x: len(x[0])).sum()
    # Determining the make vs model on the basis of string length
    if sumR/cntR > 7: 
        fType = 'car_model'
    else:
        fType = 'car_make'
    makes = {'F630':'FORD', 'T300':'TOYOTA', 'H530':'HONDA', 'N200':'NISSAN', 'C160':'CHEVROLET', 'M100':'MERCEDES BENZ', 'D320': 'DODGE', 'F600': 'FRUEHAUF', 'B500': 'BMW', 'I220':'ISUZU'}
    rdd1 = df.rdd.map(lambda x: (soundex(limitN(x[0],5)), x[1]))
    rdd1 = rdd1.reduceByKey(lambda x,y: x+y).sortBy(keyfunc= lambda x: x[1], ascending=False)
    conf1 = rdd1.filter(lambda x: x[0] in makes.keys()).count()
    # Count per make frequency
    conf2 = rdd1.filter(lambda x: x[0] in makes.keys()).map(lambda x: x[1])
    if conf2.count() != 0:
        conf2 = conf2.reduce(lambda x,y: x+y)
        res=conf1/len(makes.keys())
        # ANCHOR
        if res>=0.5 and 'vehicle' in filename.lower():
            # Number of valid makes. ignoring makes starting from numbers, symbols or empty strings
            count_filtered = rdd1.filter(lambda x: re.match('^[A-Za-z]', x[0]) ).map(lambda x: x[1]).reduce(lambda x,y: x+y)
            examples = rdd1.filter(lambda x: x[0] in makes.keys()).map(lambda x: (makes[x[0]], x[1])).reduceByKey(lambda x,y: x+y).collect()
            examples.sort(key = lambda x: x[1], reverse=True)
            examples.append(('OTHERS', count_all-conf2))
            found_type = found_type+[fType]
            return res, found_type , count_filtered
    return 0, found_type, 0

# def nlp_find_agency(df,count_all,found_type):
#     #Your Code HERE:
#     return 0, found_type, 0

# Function to find middle name initials
def nlp_find_initials(df,count_all,found_type):
    cntR = df.rdd.map(lambda x: len(str(x[0]))).count()
    sumR = df.rdd.map(lambda x: len(str(x[0]))).sum()
    if sumR/cntR <=2 and df.count()>20: 
        found_type += ['person_name_initials']
        return 100, found_type, count_all
    return 0, found_type, 0

# Function to find boroughs
def nlp_find_borough(df,count_all,found_type):
    boro_ab = ['K', 'M', 'Q', 'R', 'X']
    boro = ['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND', 'NEW YORK']
    rdd1 = df.rdd.map(lambda x: (x[0], x[1]))
    sumR = rdd1.filter(lambda x: (x[0] in boro_ab) or (x[0] in boro)).map(lambda x: x[1]).sum()
    res = sumR/count_all
    examples = rdd1.filter(lambda x: (x[0] in boro_ab) or (x[0] in boro)).collect()
    if len(examples) >= 5:
        found_type+=['borough']
        return( res, found_type, sumR)
    else:
        return(0, found_type, 0)

# --- Function with NLP End ---------------------------------------------------

# --- Functions FOR LIST COMPARISON Starts HERE -------------------------------
def list_find_school_subject(df,count_all,found_type):
    df_filtered = df.filter(df["val"].isin(ss_keywords))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        #print(res)
        if (res >= 0.4): 
            found_type = found_type + ["subject_in_school"]
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0

def list_find_business_name(df,count_all,found_type):
    df_filtered = df.filter(df["val"].isin(biz_keywords))
    df_filtered.show()
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        #print(res)
        if (res >= 0.1): 
            found_type = found_type + ["business_name"]
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0


def list_find_neighborhood(df,count_all,found_type):
    df_filtered = df.filter(df["val"].isin(nh_keywords))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        #print(res)
        if (res >= 0.1): 
            found_type = found_type + ["neighborhood"]
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0

def list_find_area_of_study(df,count_all,found_type):
    df_filtered = df.filter(df["val"].isin(aos_keywords))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        #print(res)
        if (res >= 0.25): 
            found_type = found_type + ["area_of_study"]
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0
    

def list_find_agency(df,count_all,found_type):
    df_filtered = df.filter(df["val"].isin(ca_keywords))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        #print(res)
        if (res >= 0.1): 
            found_type = found_type + ["city_agency"]
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0

def list_find_location_type(df,count_all,found_type):
    df_filtered = df.filter(df["val"].isin(lt_keywords))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        #print(res)
        if (res >= 0.1): 
            found_type = found_type + ["location_type"]
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0

def list_find_parks_playgrounds(df,count_all,found_type):
    df_filtered = df.filter(df["val"].isin(pp_keywords))
    if (df_filtered.count() is not 0):
        count_filtered = df_filtered.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        res = float(count_filtered/count_all)
        #print(res)
        if (res >= 0.1): 
            found_type = found_type + ["park_playground"]
        return res, found_type, count_filtered 
    else:
        return 0, found_type, 0

def import_keyword_list(inputDir):
    klist = sc.textFile(inputDir)
    klist = klist.flatMap(lambda x: x.split(",")).collect()
    klist = [x.strip('"') for x in klist]
    klist = [re.sub("\[|\]|\'|\'|" "", "", item)for item in klist]
    klist = [re.sub(" " "", "", item)for item in klist]
    return(klist)

def read_regex_file(inputFile):
    with open(inputFile) as f:
        return(f.read())
    
def get_regex_from_list(lst):
    regex = ""
    for word in lst:
        regex += "\\s"
        regex += word
        regex += "|"
    return(regex)

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
    
    # Spacy NLP wrapper
    nlp = en_core_web_sm.load()

    # Current user path
    env_var = os.environ
    this_user = env_var['USER']

    # Input & output directories
    inputDirectory = "/user/hm74/NYCColumns/"#sys.argv[1]
    outputDirectory = "/user/" + this_user + "/Project_Results/"#sys.argv[2]

    # Lists for different semantics
    input_pp_keywords = "/user/aj2885/Project_Resource/park_playground_keywords"
    input_aos_keywords = "/user/aj2885/Project_Resource/area_of_study_keywords"
    input_ca_keywords = "/user/aj2885/Project_Resource/city_agency_keywords"
    input_ss_keywords = "/user/aj2885/Project_Resource/school_subject_keywords"
    input_sn_keywords = "/user/aj2885/Project_Resource/school_name_keywords"
    input_lt_keywords = "/user/aj2885/Project_Resource/location_type_keywords"
    input_nh_keywords = "/user/aj2885/Project_Resource/neighborhood_keywords"
    input_biz_keywords = "/user/aj2885/Project_Resource/business_keywords"
    
    pp_keywords = import_keyword_list(input_pp_keywords) # parks & playgrounds
    aos_keywords = import_keyword_list(input_aos_keywords) # area of study
    ca_keywords = import_keyword_list(input_ca_keywords) # city agency
    ss_keywords = import_keyword_list(input_ss_keywords) # school subject
    sn_keywords = import_keyword_list(input_sn_keywords) # school name
    lt_keywords = import_keyword_list(input_lt_keywords) # location type
    nh_keywords = import_keyword_list(input_nh_keywords) # neighborhood
    biz_keywords = import_keyword_list(input_biz_keywords) # business name 

    # Parent JSON Wrapper
    parentJSON = []
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
    raw_list = raw_list[:-1]

    # Iteration over dataframes begins bu using dataframe file names
    processCount = 1

    # Create schema for raw data before reading into df 
    customSchema = StructType([
                StructField("val", StringType(), True),
                StructField("count", IntegerType(), True)])
    # List containing data for precision recall analysis
    analysisPR = []

#Testing first 10 files
    for filerow in raw_list:
        filename = filerow[0]
        labels = literal_eval(filerow[1])
        print("\n\nProcessing Dataset =========== : ", str(processCount) + ' - ' +filename)
        # Read file to dataset and apply all regex functions
        found_type = []
        fileinfo = []
        regex_res = []
        df = sqlContext.read.format("csv").option("header","false").option("inferSchema", "true").option("delimiter", "\t").schema(customSchema).load(inputDirectory + filename)
        try:
            df_stats = mean_stdv(df)
            mean = df_stats[0]['mean']
            std = df_stats[0]['std']
            count_all = count_all_values(df)
        except:
            df = df.rdd.map(lambda x: [str(x[0].encode('utf-8'))[2:-1], intchk(x[1])]).toDF(['val', 'count'])
            df_stats = mean_stdv(df)
            mean = df_stats[0]['mean']
            std = df_stats[0]['std']
            count_all = count_all_values(df)

        # Added col_length which is the average length of the col
        df_length = df.select(_mean(length(col("val"))).alias('avg_length'))
        col_length= df_length.collect()[0][0]

        # Regex based classifications
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

        # Default value for all other precentages 
        percentage_area_of_study = 0
        percentage_location = 0
        percentage_agency = 0
        percentage_parks_playgrounds = 0
        percentage_business_name = 0
        percentage_neighborhood = 0
        percentage_person = 0
        percentage_vehicle_type = 0
        percentage_color = 0
        percentage_car_make = 0
        percentage_car_model = 0
        percentage_borough= 0 
        percentage_city = 0
        percentage_initials = 0
        type_count_initials = 0
        type_count_color = 0
        type_count_car_make = 0
        type_count_borough = 0
        type_count_city = 0
        type_count_person = 0
        type_count_area_of_study = 0
        type_count_agency = 0
        type_count_location = 0
        type_count_neighborhood = 0
        type_count_parks_playgrounds = 0
        type_count_business = 0

        if not found_type:
            # NLP - NER & Soundex based classifications
            # percentage_business_name, found_type, type_count_business = nlp_find_business_name(df,count_all,found_type)
            percentage_initials, found_type, type_count_initials = nlp_find_initials(df,count_all,found_type)
            percentage_color, found_type, type_count_color = nlp_find_color(df,count_all,found_type)
            percentage_car_make, found_type, type_count_car_make = nlp_find_car_make(df,count_all,found_type)
            #percentage_car_model, found_type, type_count_car_model = nlp_find_car_model(df,count_all,found_type)
            if df.count() <=10:
                percentage_borough, found_type, type_count_borough = nlp_find_borough(df,count_all,found_type)
                percentage_city = 0
                type_count_city = 0
                percentage_person = 0
                type_count_person = 0
            else:
                percentage_city, type_count_city, percentage_person, type_count_person, found_type = nlp_find_city_person(df,count_all,found_type)
                percentage_borough = 0
                type_count_borough = 0
    
            # Lists & Dictionary based classifications
            percentage_area_of_study, found_type, type_count_area_of_study = list_find_area_of_study(df,count_all,found_type)
            # percentage_school_subject, found_type, type_count_school_subject= list_find_school_subject(df,count_all,found_type)
            percentage_agency, found_type, type_count_agency= list_find_agency(df,count_all,found_type)
            percentage_location, found_type, type_count_location= list_find_location_type(df,count_all,found_type)
            percentage_neighborhood, found_type, type_count_neighborhood= list_find_neighborhood(df,count_all,found_type)
            percentage_parks_playgrounds, found_type, type_count_parks_playgrounds = list_find_parks_playgrounds(df,count_all,found_type)
            percentage_business_name, found_type, type_count_business= list_find_business_name(df,count_all,found_type)

        type_count = type_count_web + type_count_zip + type_count_building + \
                type_count_phone + type_count_lat_lon + type_count_add_st + \
                type_count_school_name + type_count_house_no + type_count_school_lvl + \
                type_count_school_subject +type_count_area_of_study + type_count_neighborhood + \
                type_count_agency + type_count_location + type_count_parks_playgrounds + type_count_business + \
                type_count_initials + type_count_color + type_count_car_make + type_count_borough + type_count_city + type_count_person

        countDict = {"website": type_count_web, 
                    "zip_code": type_count_zip,
                    "building_classification": type_count_building,
                    "phone_number": type_count_phone,
                    "lat_lon_cord": type_count_lat_lon,
                    "address": type_count_add_st,
                    "street_name": type_count_add_st,
                    "school_name": type_count_school_name,
                    "house_number": type_count_house_no,
                    "subject_in_school": type_count_school_subject,
                    "school_level": type_count_school_lvl,
                    "city": type_count_city,
                    "person_name": type_count_person,
                    "person_name_initials": type_count_initials,
                    "car_model": type_count_car_make,
                    "car_make": type_count_car_make,
                    "borough": type_count_borough,
                    "color": type_count_color,
                    "business_name": type_count_business,
                    "neighborhood": type_count_neighborhood,
                    "area_of_study": type_count_area_of_study,
                    "city_agency": type_count_agency,
                    "location_type": type_count_location,
                    "park_playground": type_count_parks_playgrounds}
        

        fileinfo.extend([filename,mean,std,count_all,col_length, percentage_website, percentage_zip,percentage_buildingCode,percentage_phoneNum,percentage_lat_lon,percentage_add_st,percentage_school_name,percentage_house_no,percentage_school_lvl,percentage_person,percentage_school_subject,percentage_initials, percentage_color,percentage_car_make,percentage_car_model,percentage_neighborhood,percentage_borough,percentage_city,percentage_business_name,percentage_area_of_study,percentage_location,percentage_parks_playgrounds,found_type, type_count])
        regex_res.append(fileinfo)
        print(regex_res)
        # Export the JSON for current dataset
        print("Saving Dataset =============== : ", str(processCount) + ' - ' +filename)
        processCount += 1
        outJSON = deepcopy(jsonSchema)
        outJSON["column_name"] = filename
        # Creating type wise JSON
        for i in found_type:
            outSchema = deepcopy(semanticSchema)
            if i not in ['house_number', 'person_name_initials']:
                outSchema['semantic_type'] = i
                outSchema['count'] = countDict[i]
            else:
                outSchema['label'] = i
                outSchema['count'] = countDict[i]
            outJSON["semantic_type"].append(outSchema)
        outJSON["count"] = type_count
        print(outJSON)
        parentJSON.append(outJSON)
        analysisPR.append([filename, found_type])
        
    out = {"predicted_types": parentJSON}
    out = sc.parallelize([json.dumps(out)])
    out.saveAsTextFile(outputDirectory + '/task2.json')
    aPR = sc.parallelize(analysisPR)
    aPR = aPR.toDF(['filename', 'pred'])
    aPR.write.csv('analysis_PR.csv')
    # Output results in the form of a CSV report
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
