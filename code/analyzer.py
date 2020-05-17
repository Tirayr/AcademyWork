from finbert.finbert import predict
from pytorch_pretrained_bert.modeling import BertForSequenceClassification
import argparse
from pathlib import Path
import datetime
import os
from os.path import expandvars
import re
import ast
import random
import string
import nltk
import logging



BASE_DIR  = '$HOME/pythonscripts/MyPjOne'
PROGNAME  = os.path.basename(__file__)
LOG_TIME  = datetime.datetime.now().strftime("%H:%M:%S")
LOG_DATE  = datetime.datetime.now().strftime("%Y%m%d")
LOG_DIR   = '{}/logs/{}'.format(BASE_DIR,LOG_DATE)
os.makedirs('{}'.format(expandvars(LOG_DIR)),exist_ok = True)
LOG_FILE  = '{}/{}_{}.log'.format(LOG_DIR,PROGNAME,LOG_TIME)

DATA_DATE = datetime.datetime.now().strftime("%Y%m%d")
DATA_DIR  = expandvars('{}/data/daily/{}'.format(BASE_DIR,DATA_DATE))
MODEL_DIR = expandvars('{}/models/classifier_model/finbert-sentiment'.format(BASE_DIR))

logging.basicConfig(
	filename = expandvars(LOG_FILE),
	level    = logging.INFO,
	format   = "%(asctime)s:%(levelname)s:%(lineno)d:%(message)s")


class SentimentAnalyser(object):


	def __init__(self,enames,start,end):
		self.startDate  = start
		self.endDate    = end
		self.input_dir  = '{}/Final_FinNews'.format(DATA_DIR)
		self.output_dir = '{}/Final_FinNews_Analysed'.format(DATA_DIR)
		self.comp_list  = enames
		self.model_path = MODEL_DIR
		logging.info('#######STARTING#####::{}'.format(PROGNAME))
		nltk.download('punkt')
		self.Analyse()

	def Analyse(self):
		if not os.path.exists(self.output_dir):
			os.mkdir(self.output_dir)
		model = BertForSequenceClassification.from_pretrained(self.model_path,num_labels=3,cache_dir=None)
		sep   = ':::'
		files = []
		for path, subdirs, file in os.walk(self.input_dir):
			for f in file:
				files.append(os.path.join(path,f))
		if files == []:
			print('No Files Found')
			logging.info('No Files Found')
			return

		try:
			for keyword in self.comp_list:
				print('For company={}'.format(keyword))
				logging.info('For company={}'.format(keyword))
				output_file = os.path.join(self.output_dir,str(keyword)+'_'+str(self.startDate)+'_'+str(self.endDate)+'.csv')
				for file in files:
					logging.info("{}{}Reading File={}".format(sep,sep,file))
					if keyword.lower() in file.lower():
						try:

							news = [line.rstrip('\n') for line in open(file)]
							for raw in news:
								raw           = ast.literal_eval(raw)
								raw_timeIndex = raw[0]
								raw_txt       = raw[1]
								predict(raw_timeIndex,raw_txt,model,write_to_csv=True,path=output_file)

						except Exception as e:
							print('Exception was trown while reading {}:::{}'.format(file,e))
							logging.error('Exception was trown while reading {}:::{}'.format(file,e))
		except Exception as e:
			print('Exception was trown::{}'.format(e))
			logging.error('Exception was trown::{}'.format(e))
