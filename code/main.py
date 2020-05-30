import argparse
import os
from os.path import expandvars
from scraper         import *
from merger          import *
from analyzer        import *

BASE_DIR = '$HOME/GitProjects/AcademyWork'


parser = argparse.ArgumentParser(description="Runs the entire pipeline from scraping to extracting sentiments.",
								 formatter_class=argparse.RawTextHelpFormatter)
	
parser.add_argument('-s','--start',    type=str,help="startDate,format-(yymmdd)\n",required=True)
parser.add_argument('-e','--end',      type=str,help="\nendDate,format-(yymmdd)\n\n",required = True)
parser.add_argument('-w','--web',      type=int,help="Specify the website number to scrape from separated by space\
													 \n0=reuters.com\
													 \n1=FinancialTimes.com\n\n",
                                       nargs='*',required = True)

parser.add_argument('-l','--regexp',   type=str,help="Complete path to the regex list file for companies.\
	                                                  \nFor template refer regesList file at root directory of this repo.\n\n",
	                                   default=expandvars('{}/regexList'.format(BASE_DIR)))
parser.add_argument('-r','--runStatus',type=str,help="Sets all steps for the run with comma separated",
	                                   default='scrp=Y,merg=Y,anlz=Y',required = True)

args          = parser.parse_args()
start         = args.start
end           = args.end
wpage         = args.web
reg           = args.regexp
runStatus     = args.runStatus

def setRunStatus():
	runStatusList={"scrp":"N","merg":"N","anlz":"N"}
	for sss in runStatus.split(','):
		runStatusList[sss.split('=')[0]] = sss.split('=')[1]
	return runStatusList


def main():
	runStatusList=setRunStatus()

	if runStatusList["scrp"]=="Y":
		print("Scraper")
		archive_sc = Archive_Scraper(start,end,reg,wpage)

	if runStatusList["merg"]=="Y":
		print("Merger")
		entity_names = archive_sc.collection.keys()
		merge = Merger(entity_names)

	if runStatusList["anlz"]=="Y":
		print('Analyser')
		analyzer=SentimentAnalyser(entity_names,start,end)


if __name__ == '__main__':
	main()