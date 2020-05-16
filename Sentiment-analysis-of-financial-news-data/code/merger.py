import os
from os.path import expandvars
import glob
import datetime
from difflib import SequenceMatcher
import logging
 
BASE_DIR = '$HOME/pythonscripts/Thesis_work/Sentiment-analysis-of-financial-news-data'
PROGNAME = os.path.basename(__file__)

LOG_TIME = datetime.datetime.now().strftime("%H:%M:%S")
LOG_DATE = datetime.datetime.now().strftime("%Y%m%d")
LOG_DIR  = '{}/logs/{}'.format(BASE_DIR,LOG_DATE)
os.makedirs('{}'.format(expandvars(LOG_DIR)),exist_ok = True)
LOG_FILE = '{}/{}_{}.log'.format(LOG_DIR,PROGNAME,LOG_TIME)

DATA_DATE = datetime.datetime.now().strftime("%Y%m%d")
DATA_DIR = expandvars('{}/data/{}'.format(BASE_DIR,DATA_DATE))
os.makedirs('{}'.format(expandvars(DATA_DIR)),exist_ok = True)

class Merger(object):
	"""
	It does two functions.
	1. Merges all the different runs inside the archive folder of a company,with the 
	   relevant google search results if they are also present.
	2. Filters the final list of news by removing the duplicate news.
	"""

	def __init__(self,enames):
		self.base_path = DATA_DIR #os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','data')
		self.comp_list = enames
		logging.info('#######STARTING#####::{}::Merge all files'.format(PROGNAME))
		self.listGoogleFiles()
		logging.info("Get full data")
		self.inp()

	def inp(self):

		for company in self.comp_list:
			print("Getting full data for:",company)
			file = 'results_'+company+'_full.data'
			links = [line.rstrip('\n') for line in open(os.path.join(self.base_path,'FinNews',company,file))]
			links = list(set(links)) #assuming date will be same for archive as well as google results
			print(len(links))
			self.writeToFile(links,company)
			final_link = os.path.join(self.base_path,'Final_FinNews')
			if(not os.path.exists(final_link)):
				os.makedirs(final_link)
				print("made it")
			f = open(os.path.join(final_link,"results_"+company+'_unique.data'),'a+')
			for i in links:
				f.write(str(i)+"\n")
			f.close()
			print("Done for:",company)
		print("All Done!")
		return None

	def listArchiveFiles(self,company):
		for path, subdirs, files in os.walk(os.path.join(self.base_path,'FinNews',company,'archive')):
			if len(files) > 0:
				return files[0]

	def countFiles(self,company):
		a = []
		# b = []
		self.writeToFile([0],company,'random')#creating random file..function stops working without a file in folder
		filew = os.path.join(self.base_path,'FinNews',company,"results_random_unique.data")
		for path, subdirs, files in os.walk(os.path.join(self.base_path,'FinNews',company)):
				# print(path, subdirs, files)
				if len(files) > 0:
					for i in subdirs:
						cnt = sum([len(files) for r, d, files in os.walk(os.path.join(self.base_path,'FinNews',company,i))])
						a.append(cnt)
		os.remove(filew)#removing random file
		# print(a)
		return a				

	def listGoogleFiles(self):
		for company in self.comp_list:
			logging.info('Merging files for::{}'.format(company))
			if company != None:
				a = []
				b = []
				# k = 0
				p = 2
				s = {}
				index = 1
				count = self.countFiles(company)
				# print(count,'count')
				max_size = len(count)
				# print(len(count))
				for path, subdirs, files in os.walk(os.path.join(self.base_path,'FinNews',company)):
					# print(path, subdirs, files)
					if len(files) > 0:
						
						for file in files:
							# print(file)
							if 'archive' in file.lower():
								b.append(os.path.join(path,file))
							else:
								a.append(os.path.join(path,file))
								if index < max_size :
									if len(a) == count[index]:
										temp = []
										s.update({p:temp+a})
										a.clear()
										p+=1
										index+=1
								
				files = []
				s[1] = b
				# print(s,'s')
				websites = {1:'archive',2:'economictimes.indiatimes.com',3:'moneycontrol.com',4:'ndtv.com',5:'reuters.com',6:'thehindu.com',7:'thehindubusinessline.com'}
			
				for key in s:
					print(key,'key')
					# print(key,'key')
					if key == 1:#for archive
						self.getUniqueLinks(s[key],websites[key],company) 
					else:
						self.getUniqueLinks(s[key],websites[key],company)

				files.clear()
				for file in os.listdir(os.path.join(self.base_path,'FinNews',company)):
					if file.endswith('.data'):
						files.append(file)
				
				self.getLinks(files,company)
			

	def writeToFile(self,links,company,name=None):
		if name == None:
			f = open(os.path.join(self.base_path,'FinNews',company,"results_"+company+'_unique.data'),'a+')
		else:
			f = open(os.path.join(self.base_path,'FinNews',company,"results_"+name+'_unique.data'),'a+')
		for i in links:
			f.write(str(i)+"\n")
		f.close()

	def getLinks(self,files,company):
		#print(files)
		file = os.path.join(self.base_path,'FinNews',company,'results_'+company+'_full.data')
		with open(file, 'wb') as outfile:
			for f in files:
				with open(os.path.join(self.base_path,'FinNews',company,f), "rb") as infile:
					outfile.write(infile.read())
		return file

	def getUniqueLinks(self,files,website,company):
		logging.info(':::getUniqueLinks For files={},website={},company={}:::'.format(files,website,company))
		filew = os.path.join(self.base_path,'FinNews',company,'results_'+website+'_unique.data')
		logging.info("Working for:{}".format(website))
		print("Working for: ",website)
		with open(filew, 'w+') as outfile:
			for file in files:
				print(file)
				links = [line.rstrip('\n') for line in open(file)]
				if website == 'archive':
					links = self.checkUnique(company,links)
					print(file)
					self.writeToFile(links,company,str(file).split('/')[-1].split('_')[1])
				for i in links:
					outfile.write(str(i)+"\n")

		outfile.close()

	def checkUnique(self,company,links):
		logging.info(':::checkUnique For company={}:::'.format(company))
		# if company != None:
		# 	file = listGoogleFiles(company)
			
		# 	links = [line.rstrip('\n') for line in open(file)]
		stats={0.2:0,0.4:0,0.6:0,0.8:0,1:0}
		count=0
		a = len(links)
		# print(a)
		d = a*a
		

		c = 0
		for i in links:		
			for j in links:
				if(i!=j):
					value = SequenceMatcher(None, i, j).ratio()
					if(value<=0.2):
						stats[0.2]+=1
					elif(value<=0.4):
						stats[0.4]+=1
					elif(value<=0.6):
						stats[0.6]+=1
					elif(value<=0.7):
						stats[0.8]+=1
					elif(value>0.8):
						stats[1]+=1
						links.remove(j)
				c+=1
				b = len(links)*len(links)
			print("%\r",int((1-(b-c)/d)*100),end='')

		logging.info("Stats for this file)")
		logging.info("Similarity Score(<=)\t  occurances")
		print("Stats for this file\n\nSimilarity Score(<=) \t occurances")
		for i in stats:
			logging.info('\t'+str(i)+'\t\t\t'+str(stats[i]))
			print('\t'+str(i)+'\t\t\t'+str(stats[i]))
		logging.info("final count {}".format(len(links)))
		print("final count ",len(links))
		
		return links

# merge_object = Merger()
# merge_object.inp()
# merge_object.listGoogleFiles()