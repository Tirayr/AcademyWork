from bs4 import BeautifulSoup
import requests
import datetime
import lxml
import os
import json
from os.path import expandvars
import time
from lxml import html
import pandas as pd
import re
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


logging.basicConfig(
    filename = expandvars(LOG_FILE),
    level    = logging.INFO,
    format   = "%(asctime)s:%(levelname)s:%(lineno)d:%(message)s"
    )
'''
	 the starting and ending dates are inclusive
'''

class Archive_Scraper:
	def __init__(self,start_dt,end_dt,path):

		self.sd,self.sm,self.sy=start_dt
		self.ed,self.em,self.ey=end_dt
		self.relist,self.collection=self.getRegex(path)
		logging.info('#######STARTING#####::{}'.format(PROGNAME))

	def generate_ym_pair(self,y,sm,em,need_zero):
		year_mon=[]
		for m in range(sm,em+1):
			if(m<10):
				if(need_zero):
					year_mon.append((y,"0"+str(m)))
				else:
					year_mon.append(y,str(m))
			else:
				year_mon.append((y,m))
		return year_mon

	def year_m(self,non_zero):
		year_month=[]
		for y in range(self.sy,self.ey+1):
			if(self.sy==self.ey):
					year_month.extend(self.generate_ym_pair(y,self.sm,self.em,non_zero))
			if(self.sy!=self.ey):
					if(y==self.sy):
						year_month.extend(self.generate_ym_pair(y,self.sm,12,non_zero))
					elif (y==self.ey):
						year_month.extend(self.generate_ym_pair(y,1,self.em,non_zero))
					else:
						year_month.extend(self.generate_ym_pair(y,1,12,non_zero))
		return year_month

	def scrape(self,url):
		r = requests.get(url)
		print(url)
		a,b = url.split('//')
		soup = BeautifulSoup(r.content,"html.parser")
		s = ''
		#ndtv.com
		#print(b)
		if b.startswith('www.ndtv')or b.startswith('ndtv'):
			date_data = soup.findAll('span',{"itemprop":'dateModified'})
			#print(date_data)
			j=2
			for i in date_data:
				s = i.get_text().split(" ")
				s[j] = s[j][:-1]
				s = s[j]+'-'+s[j-1][:3]+'-'+s[j+1]
				print(s)
		elif b.startswith('gadgets') or b.startswith('www.gadgets'):#gadgets.ndtv
			date_data = soup.find_all(class_='dtreviewed')
			for i in date_data:
				s = i.get_text().split('"')[0]
				s = s.split(" ")
				s  = s[0]+"-"+(s[1])[0:3]+"-"+s[2]
				print("date",s)
				#s = datetime.datetime.strptime(s, "%d %B %Y")
		elif b.startswith('auto')or b.startswith('www.auto'):
			date_data = soup.find_all(class_='article__pubdate')
			j=0
			for i in date_data:
				if('|' in i.get_text()):
					j=2
				s = i.get_text().split(" ")
				s[j+1] = s[j+1][:-1]
				s = s[j+1]+'-'+s[j]+'-'+s[j+2]
				print(s)
		else:
			date_data = soup.find_all(class_='dateline') #profit.ndtv
			for i in date_data:
				s = i.get_text().split(':')[1]
				a = s.split(' ')
				s = a[2].split(',')
				s = s[0]+'-'+(a[1])[0:3]+'-'+a[3]
				#s = datetime.datetime.strptime(s, "%d-%B-%Y")
				print("date",s)
		
		return s
	def reuters(self):
		
		for key in self.collection:             # Cleaning collector
			self.collection[key]=[]
		sep       = ':::'
		
		logging.info('{}{}{}Scraping from reuters{}{}{}'.format(sep,sep,sep,sep,sep,sep))
		start_timestamp = datetime.datetime(self.sy,self.sm,self.sd).timestamp()*1000000000
		end_timestamp   = datetime.datetime(self.ey,self.em,self.ed).timestamp()*1000000000
		try:	
			for keyword in self.relist:

				print('For company={}'.format(keyword))
				logging.info('For company={}'.format(keyword))

				symbol        = self.relist[keyword][2]
				tillDate      = end_timestamp
				response_code = 200

				while response_code == 200 and tillDate>start_timestamp:
					visit_url     = 'https://wireapi.reuters.com/v8/feed/rcom/us/marketnews/ric:{symbol}?until={timestamp}'.format(symbol=symbol,timestamp=int(tillDate))
					response      = requests.get(visit_url)
					response_code = response.status_code
					json_data     = json.loads(response.content)
					logging.info("{}{}Visitng Url={}".format(sep,sep,visit_url))
					newsDateList  = []
					try:
						for i in json_data['wireitems']:
							for j in i['templates']:
								try:
									pubtime       = time.strftime('%Y-%m-%d %H:%M:%S%z', time.gmtime(int(i['wireitem_id'])/1000000000))
									head_value    = j['story']['hed']+sep+sep
									summary_value = j['story']['lede']
									self.collection[keyword].append([pubtime,head_value+summary_value])
									newsDateList.append(int(i['wireitem_id']))
								except KeyError as e:
									pass
					except KeyError as e:
						logging.warning('Item Search Outputed KeyError for {}{}'.format(sep,e))
					try:
						tillDate=min(newsDateList)
					except ValueError as e:
						logging.warning('{}Last PublishDate found at {}'.format(sep,tillDate))
		except Exception as e:
			logging.error('{}Nothing Found for any of Companies{}'.format(sep,sep))
		finally:
			self.collected()
			self.writeToFile(self.collection,"reutersArchive",self.sd,self.sm,self.sy)

	def financial_times(self):
		for key in self.collection: # Cleaning collector
			self.collection[key]=[]
		sep       = ':::'

		logging.info('{}{}{}Scraping from reuters{}{}{}'.format(sep,sep,sep,sep,sep,sep))
		start_date= datetime.date(self.sy,self.sm,self.sd)
		end_date  = datetime.date(self.ey,self.em,self.ed)

		base_url  = "https://www.ft.com/search?q={cmpName}&page={page}&dateTo={end_date}&dateFrom={start_date}&concept={concept}&sort=relevance&expandRefinements=true"
		try:
			for keyword in self.relist:

				print('For company={}'.format(keyword))
				logging.info('For company={}'.format(keyword))

				cmpName=self.relist[keyword][0]
				concept=self.relist[keyword][1]
				page_list=[]

				#################################Extracting number of pages to look#############################
				PageNumCheck_resonse= requests.get(base_url.format(cmpName=cmpName,page=1,concept=concept,start_date=start_date,end_date=end_date))
				PageNumCheck_content= PageNumCheck_resonse.content
				PageNumCheck_xtree  = html.fromstring(PageNumCheck_content)
				PageNumCheck_value  = PageNumCheck_xtree.xpath('//*[@id="site-content"]/div/div/div/span/text()[2]')
				PageCount           = list(map(int, re.findall(r'\d+', str(PageNumCheck_value))))[0]
				logging.info('{}{} pages found For comany={}'.format(sep,PageCount,keyword))
				#################################################################################################

				for i in range(1,PageCount+1):                              #Creating urls for each page of found news list
					page_list.append(base_url.format(cmpName=cmpName,page=i,concept=concept,start_date=start_date,end_date=end_date))

				for page in page_list:		
					try:
						response=requests.get(page)                         
						xpath_basic='//*[@id="site-content"]/div/ul/li'
						xtree=html.fromstring(response.content)
						# base_xpaths=xtree.xpath(xpath_basic)
						logging.info("{}Visitng Url={}".format(sep,page))
						for i in range(1,26):                               #maximum 25 elements can be found in each page
							try:
								text_part1        = '{}[{}]/div/div/div/div/div/a/span/mark/text()'.format(xpath_basic,i)
								text_part2        = '{}[{}]/div/div/div/div/div/a/span/text()'.format(xpath_basic,i)
								text_part3        = '{}[{}]/div/div/div/div/div/a/text()'.format(xpath_basic,i)
								text_highlight1   = '{}[{}]/div/div/div/div/p/a/span/text()[1]'.format(xpath_basic,i)
								text_highlight2   = '{}[{}]/div/div/div/div/p/a/span/text()[2]'.format(xpath_basic,i)
								text_highlight3   = '{}[{}]/div/div/div/div/p/a/text()'.format(xpath_basic,i)
								pub_time          = '{}[{}]/div/div/div/div/div/time/@datetime'.format(xpath_basic,i)
								if  xtree.xpath(text_part1)!=[]:
									mytext =xtree.xpath(text_part1)+xtree.xpath(text_part2)
									pubtime=xtree.xpath(pub_time)[0]
									highlight=''
									h1=xtree.xpath(text_highlight1)
									h2=xtree.xpath(text_highlight2)
									h3=xtree.xpath(text_highlight3)
									for i in [h1,h2,h3]:
										if i != []:
											highlight=highlight+sep.join(i)
											highlight=highlight.replace('\t','').replace('\n','')
									self.collection[keyword].append([pubtime,'{}{}{}'.format(sep.join(mytext),sep,highlight)])
								elif xtree.xpath(text_part3) != []:
									mytext =xtree.xpath(text_part3)
									pubtime=xtree.xpath(pub_time)[0]
									highlight=''
									h1=xtree.xpath(text_highlight1)
									h2=xtree.xpath(text_highlight2)
									h3=xtree.xpath(text_highlight3)
									for i in [h1,h2,h3]:
										if i != []:
											highlight=highlight+sep.join(i)
											highlight=highlight.replace('\t','').replace('\n','')
									self.collection[keyword].append([pubtime,'{}{}{}'.format(sep.join(mytext),sep,highlight)])
								else:
									pass
							except Exception as e:
								print('Error:::',e)
								logging.error(visit_url)
					except Exception as e:
						print('Error:::',e)
						logging.error(visit_url)
		finally:
			self.collected()
			self.writeToFile(self.collection,"FinancialTimesArchive",self.sd,self.sm,self.sy)

	def econ_times(self):
		base_url="http://economictimes.indiatimes.com/archivelist/year-{year},month-{month},starttime-{start_t}.cms"
		start_time=36892
		start_date=datetime.date(self.sy,self.sm,self.sd)
		days=(start_date-datetime.date(2001,1,1)).days + start_time
		count_days=(datetime.date(self.ey,self.em,self.ed)-start_date).days+1
		visit_url=''
		try:
			for day in range(count_days):
				date_diff=datetime.timedelta(day)
				final_date=date_diff+start_date
				visit_url=base_url.format(year=final_date.year,month=final_date.month,start_t=days+day)
				try:
					page=requests.get(visit_url)
					print(visit_url)
					logging.info(visit_url)
					xtree=html.fromstring(page.content)
					# print(xtree)
					links=xtree.xpath("*//section[@id='pageContent']//li/a/@href") #//*[@id="pageContent"]/span/table[2]/tbody/tr/td[1]/ul/li[5]
					for link in links:                                           #//*[@id="pageContent"]/span/table[2]/tbody/tr/td[1]/ul/li/a
						# print(link,'link')									#//*[@id="site-content"]/div/ul/li/div/div/div/'
						try:
							for keyword in self.relist:
								# print(link.lower(),self.relist[keyword],type(self.relist[keyword]))
								if(self.search_key(link.lower(),self.relist[keyword])):
									# print(link.lower(),self.relist[keyword])
									linked='http://economictimes.indiatimes.com'+link
									date_,month_,year_=final_date.strftime("%d-%b-%Y").split('-')
									now_d=date_+"-"+month_+"-"+year_
									print(now_d,linked,keyword)
		# 							self.collection[keyword].append(now_d+"::"+linked)
						except Exception as e:
							# print("#"*10)
							logging.warning(str(link))
							# print("#"*10)
				except Exception as e:
					# print("#"*10)
					logging.error(visit_url)
					# print("#"*10)
								
								
		except Exception as e:
			# print("#"*10)
			logging.critical(e)
			# print("#"*10)
		finally:
			print(self.collected())
		# 	self.writeToFile(self.collection,"econtimesArchive",self.sd,self.sm,self.sy)


	def thehindu(self):
		base_url="http://www.thehindu.com/archive/web/{year}/{month}/{day}/"
		year_month=self.year_m(True)
		start_date=datetime.date(self.sy,self.sm,self.sd)
		count_days=(datetime.date(self.ey,self.em,self.ed)-start_date).days+1
		#collection=[]
		try:
			for d in range(count_days):
				date_diff=datetime.timedelta(d)
				final_date=date_diff+start_date
				print (final_date),
				visit_url=base_url.format(year=final_date.year,month=final_date.month,day=final_date.day)
				# this webpage loads very slow and becomes unresponsive sometimes hence a timeout of None
				try:
					page=requests.get(visit_url,timeout=None)
					# print(visit_url)
					xtree=html.fromstring(page.content)
					links=xtree.xpath("*//ul[@class='archive-list']//a/@href")
					# print (len(links))
					for link in links:
						try:
							for keyword in self.relist:
								if(self.search_key(link.lower(),self.relist[keyword])):
									# print(link.lower(),keyword)
									date_,month_,year_=final_date.strftime("%d-%b-%Y").split('-')
									now_d=date_+"-"+month_+"-"+year_
									print(now_d,link,keyword)
									# self.collection[keyword].append(now_d+"::"+link)
						except Exception as e:
							print("#"*10)
							logging.warning(str(link))
							print("#"*10)						
				except Exception as e:
					print("#"*10)
					logging.error(visit_url)
					print("#"*10)
		except Exception as e:
			print("#"*10)
			logging.critical(e)
			print("#"*10)
		finally:
			self.collected()
			self.writeToFile(self.collection,"thehinduArchive",self.sd,self.sm,self.sy)

	def ndtv(self):
		base_url="http://archives.ndtv.com/articles/{year}-{month}.html"
		year_month=self.year_m(True)
		start_date=datetime.date(self.sy,self.sm,self.sd)
		end_date = datetime.date(self.ey,self.em,self.ed)
				
		try:
			for d in range(start_date.year-int(self.ey) +1):
				print("Scraping for the year",start_date.year-d)
				try:
					for x in range(start_date.month,end_date.month+1):
						visit_url=base_url.format(year=start_date.year-d,month=str(x).zfill(2))
						page=requests.get(visit_url,timeout=None)
						print(visit_url)
						xtree=html.fromstring(page.content)
						
						links=xtree.xpath("//*[@id='main-content']/ul/li/a/@href")
						print(len(links))
						try:
							for link in links:
								if 'khabar.ndtv' in link or 'hi.ndtv' in link or 'hi.gadgets' in link or 'gadgets.ndtv' in link or 'movies.ndtv' in link or 'food.ndtv' in link:
									continue

								for keyword in self.relist:
									if(self.search_key(link.lower(),self.relist[keyword])):
										print(link,keyword)
										# self.collection[keyword].append(self.scrape(link)+'::'+link)
						except Exception as e:
							print("#"*10)
							logging.warning(str(link))
							print("#"*10)
							
				except Exception as e:
					print("#"*10)
					logging.error(str(visit_url))
					print("#"*10)
		except Exception as e:
			print("#"*10)
			logging.critical(e)
			print("#"*10)
		finally:
				self.collected()
				self.writeToFile(self.collection,"ndtvArchive",self.sd,self.sm,self.sy)

		

	def businessLine(self):
		base_url="http://www.thehindubusinessline.com/today/?date={date}"
		year_month=self.year_m(True)
		start_date=datetime.date(self.sy,self.sm,self.sd)
		end_date = datetime.date(self.sy,self.em,self.ed)
		print(start_date)
		print(end_date.year)
		try:	
			for i in range(start_date.year-int(self.ey)+1):
				start_date=datetime.date(self.sy-i,self.sm,self.sd)
				end_date = datetime.date(self.sy-i,self.em,self.ed)
				daterange = pd.date_range(start_date, end_date)
				for d in daterange:
						try:
							d  = str(d).split(' ')[0]
							visit_url=base_url.format(date=d)
							page=requests.get(visit_url,timeout=None)
							print(visit_url)
							xtree=html.fromstring(page.content)
							
							links=xtree.xpath("//*[@id='printhide']//a/@href")
							print(len(links))
							for link in links:
								try:
									for keyword in self.relist:
										if(self.search_key(link.lower(),self.relist[keyword])):
											dt=str(datetime.datetime.strptime(d,'%Y-%m-%d').strftime('%d-%b-%Y'))
											print(dt,link)
											self.collection[keyword].append(dt+"::"+link)
								except Exception as e:
									print("#"*10)
									logging.warning(str(link))
									print("#"*10)
							
						except Exception as e:
							print("#"*10)
							logging.error(visit_url)
							print("#"*10)
		except Exception as e:
			print("#"*10)
			logging.critical(e)
			print("#"*10)
		finally:
			self.collected()
			self.writeToFile(self.collection,"businessLineArchive",self.sd,self.sm,self.sy)
#change for others also for write to file definition
	def writeToFile(self,links,webp,date,month,year):
					# self.writeToFile(self.collection,"econtimesArchive",self.sd,self.sm,self.sy)
	#	BASE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','data')
		try:
			os.makedirs('{}'.format(DATA_DIR),exist_ok = True)
		except Exception as e:
			logging.warning(e)
			pass

		for company in links:
			try:
				os.makedirs(os.path.join(DATA_DIR,"FinNews",company,'archive'))
			except Exception as e:
				pass
			
			f = open(os.path.join(DATA_DIR,"FinNews",company,"archive","results_"+webp+"_"+company+"_"+str(date)+"_"+str(month)+"_"+str(year)+'.data'),'a+')
			for i in links[company]:
				f.write(str(i)+"\n")
			f.close()

	def search_key(self,input_string,exp):
		exprs=re.compile(exp) 

		if(exprs.search(input_string)!=None):
			return True
		else:
			return False
	def getRegex(self,path):
		relist={}
		collection={}
		with open(os.path.join(path),'r+') as regexFile:
			for line in regexFile:
				try:
					temp=line.split("::")
					relist[temp[0]]=[temp[1].split("|")[0],temp[1].split("|")[1],temp[1].split("|")[2]]
					collection[temp[0]]=[]
				except Exception as e:
					print(e)
					print(line)
		return relist,collection
	def collected(self):
		count={}
		total=0
		for i in self.collection:
			count[i]=len(self.collection[i])
			total+=count[i]
		for i in count:
			print(i+" Collected -"+str(count[i]))
		print("Total Urls collected-"+str(total))



