#coding=utf-8

import scraperwiki
from bs4 import BeautifulSoup
import time
from pprint import pprint

#parse http://reseaualliance.fr/

class Scraper:

    storage = None
    url = 'http://reseaualliance.fr/offres/1'
    links = []
    dataBuffer = []

    def __init__(self, storage=None):
        self.storage = storage

    def run(self, clear_old=False):
        if clear_old:
            print 'Deleting old data...'
            self.deleteData()
        next_url = self.processingPage(self.url)
        self.saveData(self.dataBuffer)

        while next_url != "#":
            next_url = self.processingPage(next_url)
            self.saveData(self.dataBuffer)

    def test(self):
        url = 'http://reseaualliance.fr/offre/7743'
        data = self.parsePage(url)
        pprint(data)
        print time.ctime(data['publishDate'])

    def processingPage(self, url):
        self.dataBuffer = []
        html = scraperwiki.scrape(url)
        soup = BeautifulSoup(html, "html.parser")
        print 'Processing page %s ...' % url
        for a in soup.find_all('a', class_="row list-group-item"):
            link = a['href']
            #print link
            self.links.append(link)
            self.dataBuffer.append(self.parsePage(link))
        li_next = soup.find('li', class_="next")
        next_url = li_next.a['href']
        return next_url


    def parsePage(self, url):
        print '...Parsing link: %s ...' % url
        data = {}
        html = scraperwiki.scrape(url)
        soup = BeautifulSoup(html, "html.parser")
        data['url'] = url
        data['title'] = self.getTitle(soup)
        data['category'] = self.getCategory(soup)
        data['publishDate'] = self.getPublishData(soup)
        data['contructType'] = self.getContractType(soup)
        data['description'] = self.getDescription(soup)
        data['utime'] = int(time.time())
        return data

    def getTitle(self, soup):
        try:
            title = soup.find('h2', class_="job-situation").text.strip()
        except Exception, ex:
            title = None
        return title

    def getCategory(self, soup):
        try:
            category = soup.find('span', class_="job-activity").text
        except Exception, ex:
            category = None
        return category

    def getPublishData(self, soup):
        months = {u'Janvier':1, u'Février':2, u'Mars':3, u'Avril':4, u'Mai':5, u'Juin':6, u'Juillet':7, u'Aout':8, u'Septembre':9, u'Octobre':10,	u'Novembre':11, u'Décembre':12}
        try:
            div = soup.find('div', class_="col-xs-12 col-sm-4 text-muted text-right")
            date_string = div.small.b.text
            day, month, year = date_string.split(' ')
            day = int(day)
            month = months.get(month, 1)
            year = int(year)
            date = int(time.mktime((year, month, day, 0, 0, 0, 0 ,0, 0)))
            return date
        except Exception, ex:
            return None

    def getContractType(self, soup):
        try:
            contruct_type = soup.find('h3', class_="job-contract-type").span.text
        except Exception, ex:
            contruct_type = None
        return contruct_type

    def getDescription(self, soup):
        try:
            desc = soup.find('div', class_="job-details").text
        except Exception, ex:
            desc = None
        return desc


    def saveData(self, data):
        if self.storage is None:
            scraperwiki.sqlite.save(unique_keys=['title', 'category', 'description'], data=data, table_name="data")
        else:
            self.storage.save(unique_keys={'title':1, 'category':2, 'description':5}, data=data)


    def deleteData(self):
        if self.storage is None:
            scraperwiki.sqlite.execute("DELETE FROM data")
        else:
            self.storage.delete(table="data")