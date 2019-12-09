import re
import csv
import numpy as np
import scipy
import pandas as pd
import json
import math
from scipy.cluster.hierarchy import linkage, fcluster
from nltk.util import ngrams
from collections import Counter
#from similarity import *


# note: cosine similarity function is from:
# https://gist.github.com/gaulinmp/da5825de975ed0ea6a24186434c24fe4
def cosine_similarity_ngrams(a, b):
    vec1 = Counter(a)
    vec2 = Counter(b)
    
    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])

    sum1 = sum([vec1[x]**2 for x in vec1.keys()])
    sum2 = sum([vec2[x]**2 for x in vec2.keys()])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)

    if not denominator:
        return 0.0
    return float(numerator) / denominator

def compute_jaccard_similarity(s1, s2):
    numerator = len(set(s1).intersection(set(s2)))
    denominator = len(set(s1).union(set(s2)))
    if denominator is 0:
        return(0.0)
    return(numerator/denominator)

def write_to_json(cluster_id_dic):
    with open('../../intermediary_data/filename_clusters.json', 'w') as fp:
        json.dump(cluster_id_dic, fp, sort_keys=True, indent=4)

def write_list_to_txt(file_list):
    with open('../../intermediary_data/filelist_axis_sim_matrix', 'w') as f:
        wr = csv.writer(f, quoting=csv.QUOTE_ALL)
        wr.writerow(file_list)

def write_matrix_to_csv(M):
    np.savetxt('../../intermediary_data/filename_similarity_matrix.csv', M, delimiter=',')

def clean_string(s):
    s = s.lower()
    s = re.sub(r'[^a-zA-Z0-9\s]', ' ', s)
    return(s)

def get_ngram_list_from_string(s):
    clean_s = clean_string(s)
    tokens = [token for token in s]
    output = list(ngrams(tokens, 3)) 
    return(output)

if __name__ == "__main__":

    filelist = "../cluster3.txt"
    
    with open(filelist, 'r') as f:
        raw_list = f.read().split(",")

    raw_list = raw_list = [re.sub("\[|\]|\'|\'|" "", "", item)for item in raw_list]
    raw_list = [re.sub(" " "", "", item)for item in raw_list] 
    raw_list = list(set(raw_list)) # remove duplicate filenames 
   
    clean_list = [x.split(".")[1] for x in raw_list] 

    # Use this small sample set for sanity check
    #test_list = ["hello", "hi", "cool", "school"]
    #axis_len = len(test_list)
    
    axis_len = len(clean_list)
    M = np.empty((axis_len, axis_len)) # init an empty matrix 
        
    for i, name_x in enumerate(clean_list):
        for j, name_y in enumerate(clean_list):
            a = get_ngram_list_from_string(name_x)
            b = get_ngram_list_from_string(name_y)

            #print(a, b)
            #M[i][j] = compute_jaccard_similarity(a, b)
            #print(M[i][j])
            M[i][j] = cosine_similarity_ngrams(a, b)


    linkage_matrix = scipy.cluster.hierarchy.linkage(M)
    fcluster = fcluster(linkage_matrix, t=0.8)

    cluster_id_dic = {str(x): [] for x in fcluster}

    df = pd.DataFrame()
    df['filename'] = raw_list
    df['cluster_id'] = fcluster

    for index, row in df.iterrows():
        cluster_id_dic[str(row['cluster_id'])].append(row['filename'])

    
    # write cluster dic to json
    write_to_json(cluster_id_dic)

    # write similarity matrix to csv
    #write_matrix_to_csv(M):

    # write file list to text file (the list represents the axes of the sim matrix)
    #write_list_to_txt(clean_list):

