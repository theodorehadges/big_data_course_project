import re
import csv
import numpy as np
import scipy
import pandas as pd
import json
from scipy.cluster.hierarchy import linkage, fcluster

def compute_jaccard_similarity(s1, s2):
    return(len(set(s1.lower()).intersection(set(s2.lower())))/len(set(s1.lower()).union(set(s2.lower()))))

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
            M[i][j] = compute_jaccard_similarity(name_x, name_y)

    
    linkage_matrix = scipy.cluster.hierarchy.linkage(M)
    fcluster = fcluster(linkage_matrix, t=1.12)

    cluster_id_dic = {str(x): [] for x in fcluster}

    df = pd.DataFrame()
    df['filename'] = raw_list
    df['cluster_id'] = fcluster

    for index, row in df.iterrows():
        cluster_id_dic[str(row['cluster_id'])].append(row['filename'])

    # write cluster dic to json:
    with open('../../intermediary_data/filename_clusters.json', 'w') as fp:
        json.dump(cluster_id_dic, fp, sort_keys=True, indent=4)

    # write similarity matrix to csv
    np.savetxt('../../intermediary_data/filename_similarity_matrix.csv', M, delimiter=',')

    # write file list to text file (the list represents the axes of the sim matrix)
    with open('../../intermediary_data/filelist_axis_sim_matrix', 'w') as f:
        wr = csv.writer(f, quoting=csv.QUOTE_ALL)
        wr.writerow(clean_list)

        
