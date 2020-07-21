
import  crawler.CompanyCrawler as cc
import  crawler.CompanyCrawler2  as cc2
from  concurrent.futures import  ThreadPoolExecutor
from crawler.HistoryDateCrawler import HistoryDateCrawler

class CrawlerMannager():

    def __init__(self):
        self.crawlers = self.loadCrawlers()
        self.threadPool = ThreadPoolExecutor(6)

    def loadCrawlers(self):
        # 如何做动态加载？
        crawlers = []
       # crawlers.append(cc.CompanyCrawler())
        crawlers.append(HistoryDateCrawler())
        crawlers.append(cc2.CompanyCrawler2())
        print('loadCrawlers {}', len(crawlers))
        return  crawlers

    def excute(self):

        for crawler in self.crawlers :
            result = self.threadPool.submit(crawler.excute())
            print(result)

        print('crawler finished')


