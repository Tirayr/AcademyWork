import json
import argparse
import os
from archive_scraper import *
from merger          import *
from analyzer        import *

BASE_DIR = '$HOME/pythonscripts/MyPjOne'

def main():
	parser = argparse.ArgumentParser(description="Runs the entire pipeline from scraping to extracting sentiments.",
									 formatter_class=argparse.RawTextHelpFormatter)
	parser.add_argument('-s','--start',type=str,help="Start date,format-(dd/mm/yy)\n",required=True)
	parser.add_argument('-e','--end',type=str,help="End date,format-(dd/mm/yy)",required = True)
	parser.add_argument('-w','--web',type=int,nargs='*',required = True, help="Specify the website number to scrape from separated by space\
						\n0=reuters.com\
						\n1=FinancialTimes.com")
	parser.add_argument('-r','--regexp',type=str,help="Complete path to the regex list file for companies.\n \
		                  For template refer regesList file at root directory of this repo.\n \
		                  By default,it runs the regexList file present at root directory of this repo."
		                  ,default=os.path.join(BASE_DIR,'regexList'))
	# parser.add_argument('-m','--mode',type=int,help="Which operation to perform.")

	args  = parser.parse_args()
	start = args.start
	end   = args.end
	wpage = args.web
	reg   = args.regexp

	## using archive scraper to get the news url
	archive_sc = Archive_Scraper(start,end,reg)
	for option in wpage:
		if(option==0):
			print("Scraping from reuters")
			# archive_sc.reuters()			
		elif(option==1):
			print("Scraping from FinancialTimes")
			# archive_sc.financial_times()
		elif(option==2):
			print("Scraping from Economictimes")
			archive_sc.econ_times()
		elif(option==3):
			print("Scraping from NDTV Yolo")
			archive_sc.ndtv()			
		elif(option==4):
			print("Scraping BusinessLine")
			archive_sc.businessLine()
		elif(option==5):		
			print("Scraping from thehindu")
			archive_sc.thehindu()
	## using merger to merge different versions of archive runs and also remove duplicates
	print("merger")
	entity_names = archive_sc.collection.keys()
	merge = Merger(entity_names)

	# ## running analyser
	print('analyser')
	analyzer=SentimentAnalyser(entity_names,start,end)



if __name__ == '__main__':
	main()
