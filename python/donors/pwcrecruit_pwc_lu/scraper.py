#coding=utf-8

import scraperwiki
from bs4 import BeautifulSoup
import time
from pprint import pprint

#parse https://pwcrecruit.pwc.lu

class Scraper:

    storage = None
    logger = None
    url = 'https://pwcrecruit.pwc.lu/eRecrutementJobs/view/home.xhtml'
    links = []
    dataBuffer = []

    def __init__(self, logger, storage=None):
        self.logger = logger
        self.storage = storage

    def run(self, clear_old=False):
        if clear_old:
            print 'Deleting old data...'
            self.logger.info('Deleting old data...')
            self.deleteData()
        self.processingPage(self.url)
        self.saveData(self.dataBuffer)

    def test(self):
        url = 'https://pwcrecruit.pwc.lu/eRecrutementJobs/view/job.xhtml?jobId=1652'
        data = self.parsePage(url)
        pprint(data)
        print time.ctime(data['publishDate'])

    def processingPage(self, url):
        self.dataBuffer = []
        url = 'https://pwcrecruit.pwc.lu/eRecrutementJobs/view/home.xhtml'
        try:
            html = scraperwiki.scrape(url)
            soup = BeautifulSoup(html, "html.parser")
            for li in soup.find_all('li', attrs={"data-id": True}):
                data_id = li['data-id']
                link = 'https://pwcrecruit.pwc.lu/eRecrutementJobs/view/job.xhtml?jobId=' + data_id
                self.dataBuffer.append(self.parsePage(link))
        except Exception, ex:
            print ex
            self.logger.error(ex)

    def parsePage(self, url):
        print '...Parsing link: %s ...' % url
        self.logger.info( '...Parsing link: %s ...' % url)
        data = {}
        try:
            html = scraperwiki.scrape(url)
            soup = BeautifulSoup(html, "html.parser")
        except Exception, ex:
            print ex
            self.logger.error(ex)
            return data
        data['url'] = url
        data['title'] = self.getTitle(soup)
        data['category'] = self.getCategory(soup)
        data['publishDate'] = self.getPublishData(soup)
        data['contructType'] = self.getContractType(soup)
        data['description'] = self.getDescription(soup)
        data['tasks'] = self.getTasks(soup)
        data['requirements'] = self.getRequirements(soup)
        data['utime'] = int(time.time())
        return data

    def getTitle(self, soup):
        try:
            title = soup.find('h2', class_="title").text.strip()
        except Exception, ex:
            self.logger.error(ex)
            title = None
        return title

    def getCategory(self, soup):
        try:
            category = soup.find('div', class_='etiquette').text.strip()
        except Exception, ex:
            self.logger.error(ex)
            category = None
        return category

    def getPublishData(self, soup):
        months = {u'Jan':1, u'Feb':2, u'Mar':3, u'Apr':4, u'May':5, u'Jun':6, u'Jul':7, u'Aug':8, u'Sep':9, u'Oct':10,	u'Nov':11, u'Dec':12}
        try:
            date_string = soup.find('h5', class_='desktop-only').span.text.strip()
            day, month, year = date_string.split(' ')
            day = int(day)
            month = months.get(month, 1)
            year = int(year)
            date = int(time.mktime((year, month, day, 0, 0, 0, 0, 0, 0)))
            return date
        except Exception, ex:
            self.logger.error(ex)
            return None

    def getContractType(self, soup):
        try:
            ul = soup.find('ul', class_="share")
            h5 = ul.findNext('h5')
            h4 = h5.findNext('h4')
            contruct_type = h4.text
        except Exception, ex:
            contruct_type = None
            self.logger.error(ex)
        return contruct_type

    def getDescription(self, soup):
        try:
            desc = soup.find('p', class_="boilerplate").text.strip()
        except Exception, ex:
            desc = None
            self.logger.error(ex)
        return desc

    def getTasks(self, soup):
        try:
            tasks = soup.find('div', class_="mission").text.strip()
        except Exception, ex:
            self.logger.error(ex)
            tasks = None
        return tasks

    def getRequirements(self, soup):
        try:
            section = soup.find('section', class_="job-profile")
            ul = section.findNext('ul')
            req = ul.text.strip()
        except Exception, ex:
            self.logger.error(ex)
            req = None
        return req


    def saveData(self, data):
        try:
            if self.storage is None:
                scraperwiki.sqlite.save(unique_keys=['title', 'category', 'description'], data=data, table_name="data")
            else:
                self.storage.save(unique_keys={'title':1, 'category':2, 'description':5}, data=data)
        except Exception, ex:
            print ex
            self.logger.error(ex)


    def deleteData(self):
        try:
            if self.storage is None:
                scraperwiki.sqlite.execute("DELETE FROM data")
            else:
                self.storage.delete(table="data")
        except Exception, ex:
            print ex
            self.logger.error(ex)
