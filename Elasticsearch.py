from elasticsearch import Elasticsearch 
from elasticsearch import helpers, Elasticsearch
import csv
import os
import glob
import pandas as pd
import re
import string
import itertools
import nltk
import time
from nltk.stem import PorterStemmer
ps =nltk.PorterStemmer()
from nltk.corpus import stopwords
stops = set(stopwords.words("english")) 
from nltk.tokenize import word_tokenize

# Connect to the elastic cluster
es=Elasticsearch([{'host':'localhost','port':9200}])
prefix_path = '/Users/Manam/Search_Engine/Dataset'
extension = 'csv'
os.chdir(prefix_path)
filenames = glob.glob('*.{}'.format(extension))

for file in filenames:
        complete_path = prefix_path + '/' + str(file)
        try:
            with open(complete_path) as f:
                reader = csv.DictReader(f)
                helpers.bulk(es, reader, index='my-index', doc_type='my-type')
        except:
            #print("File is Empty :" + complete_path)
            continue

index_exists = es.indices.exists(index='my-index')
indices_names = []
for elem in es.cat.indices(format="json"):
    indices_names.append( elem['index'] )

dict_index_fields = {}

for index in indices_names: 
    mapping = es.indices.get_mapping(index)
    dict_index_fields[index] = []
    for field in mapping[index]['mappings']['properties']:
        dict_index_fields[index].append(field) 

print("Enter Query: ")
query=input()
start=time.time()
data=es.search(index="my-index", body={"from":0, "size" : 10000 , "min_score":0, "query": { "match" :  {'Snippet':query}}})
for i in range(len(data['hits']['hits'])):
        print("Show ID: ",data['hits']['hits'][i]['_source']['IAShowID'])
        print("Snippet: ",data['hits']['hits'][i]['_source']['Snippet'])
        print("\n\n")
end=time.time()

print("Search Time: ", end-start)

