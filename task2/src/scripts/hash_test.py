import re
from nltk import ngrams

s1 = "agency_name"
s2 = "agency"

def compute_jaccard_similarity(s1, s2):
    return(len(set(s1.lower()).intersection(set(s2.lower())))/len(set(s1.lower()).union(set(s2.lower()))))

def compute_jaccard_similarity_of_shingles(l1, l2):
    tot_sim = 0
    
    return(len(set(s1).intersection(set(s2)))/len(set(s1).union(set(s2))))



def jaccard_distance(a, b):
"""Calculate the jaccard distance between sets A and B"""
    a = set(a)
    b = set(b)
    return(1.0 * len(a&b)/len(a|b))


# following function from:
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



def get_shingles(s):
    #s = s.lower()
    #s = re.sub(r'[^a-zA-Z0-9\s]', ' ', s)
    tokens = [token for token in s] 
    output = list(ngrams(tokens, 3)) 
    return(output)


shing1 = get_shingles(s1)
shing2 = get_shingles(s2)

print(shing1)
print(shing2)

print(compute_jaccard_similarity(s1, s2))
print(compute_jaccard_similarity_of_shingles(shing1, shing2))

