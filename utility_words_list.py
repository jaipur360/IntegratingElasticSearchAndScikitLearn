################################################
################################################
######### CALCULATE CountVectorizer fo n-grams ##############
################################################
################################################


import gc
import re
import logging
from datetime import datetime
import simplejson as json
from elasticsearch import Elasticsearch
from nltk.util import ngrams
from collections import Counter
import string
import time
import os
import re
import pprint
import pandas as pd
import numpy as np
from pandasql import sqldf

es = Elasticsearch()

logFile = 'utility_words_list.log'
logging.basicConfig(filename=logFile,level=logging.ERROR, format='%(asctime)s %(message)s')

def delete_all_files(mydir):
	filelist = os.listdir(mydir)
	for f in filelist:
	    os.remove(os.path.join(mydir, f))
#delete_all_files(mydir='./data/')
def create_single_file(filename,filecorpus):
	f = open(filename,'w',encoding="utf-8")
	f.write(filecorpus)
	f.close()
#create_single_file('./data/abc.txt','hi oppppp')
def save_all_files(mydir,bagarray):
	delete_all_files(mydir)
	count = 1
	for item in bagarray:
		filename= str(mydir) + 'doc' + str(count) + ".txt"
		#print(filename,item)
		create_single_file(filename,str(item))
		count = count + 1
	returnfilelist = []
	filelist = os.listdir(mydir)
	for item in filelist:
		returnfilelist.append(str(mydir) + str(item))
	return(returnfilelist)
#save_all_files('./data/',["This is very strange","This is very nice", "wikipedia is very good", "pls read wikipedia"])
def load_doc_from_file(doc):
	f = open(doc,'r')
	message = f.read()
	f.close()
	return(message)
def make_corpus(doc_files):
    for doc in doc_files:
        yield load_doc_from_file(doc) #load_doc_from_file is a custom function for loading a doc from file


### Calculate TF-IDF score
def getScoreFromCorpusUsingFiles(file_list):
	from sklearn.feature_extraction.text import CountVectorizer
	from sklearn.feature_extraction import text
	from scipy.sparse.csr import csr_matrix #need this if you want to save tfidf_matrix
	from nltk.corpus import stopwords
	stop_words = set(stopwords.words('english'))


	from config_stopwords import config_stopwords
	obj_stopwords = config_stopwords()
	data_stopwords = obj_stopwords.read()
	my_stop_words = text.ENGLISH_STOP_WORDS.union(data_stopwords)
	my_stop_words = list(set(my_stop_words))

	corpus = make_corpus(file_list)
	tf = CountVectorizer(analyzer='word', ngram_range=(1,3),decode_error='ignore',stop_words=my_stop_words)
	tfidf_matrix =  tf.fit_transform(corpus)
	feature_names = tf.get_feature_names()
	doc = 0
	feature_index = tfidf_matrix[doc,:].nonzero()[1]
	tfidf_scores = zip(feature_index, [tfidf_matrix[doc, x] for x in feature_index])
	scoreArray = []
	for w, s in [(feature_names[i], s) for (i, s) in tfidf_scores]:
		mydict = {}
		mydict['word'] = w
		mydict['score'] = round(s,4)
		mydict['score'] = int(mydict['score'])
		scoreArray.append(mydict)

	return(scoreArray)

def getList(es,LowerParam,UpperParam):
	arrList = []
	res = es.search(body={"size" : 50000, "_source" : ["reddit_title","py_boiler_html","py_created_utc"], "query": { "range" : { "py_created_utc" : { "gte" : UpperParam , "lt" : LowerParam } } }})
	if (res['hits']['total'] > 0):
		for doc in res['hits']['hits']:
			if(doc['_index']!='.kibana'):
				arrList.append(doc['_source'])
	return(arrList)


##### function calculateAnomalyScore
## Takes 2 parameters as dayRangeLower, dayRangeUpper
## Get Corpus from ES
## Calculates TF-IDF score using getScoreOfFirstDocumentFromCorpus
## Sorts and Saves into file

def calculateAnomalyScore(dayRangeLower,dayRangeUpper):
	try:
		dayRangeLowerParam = "now-"+str(dayRangeLower*24)+"h"
		dayRangeUpperParam = "now-"+str(dayRangeUpper*24)+"h"

		print(dayRangeLowerParam,dayRangeUpperParam)

		time.sleep(1)
		itemsLower = getList(es,dayRangeLowerParam,dayRangeUpperParam)
		createLowerFileList = []
		for item in itemsLower:
			bagWords = ''
			bagWords = bagWords + item['reddit_title'].lower()
			if(item['py_boiler_html'] is not None):
				bagWords = bagWords + (item['py_boiler_html'].lower())

			bagWords = ' '.join(bagWords.split())
			createLowerFileList.append(bagWords)

		mydir = './data/'
		filelist = save_all_files(mydir,createLowerFileList)
		scoreLowerArray = (getScoreFromCorpusUsingFiles(filelist))
		fileName = str(dayRangeLower) + '_' + str(dayRangeUpper) + ".json"
		
		with open(fileName, 'w',encoding="utf-8") as outfile:
		    json.dump(scoreLowerArray, outfile)
		print('fileName created :: ', fileName)
	except Exception as e:
		logging.error(e)
		print(e)


MAX=30
count = 1
while(count <= MAX):
	calculateAnomalyScore(dayRangeLower=count,dayRangeUpper=count+1)
	time.sleep(1)
	count = count + 1

	