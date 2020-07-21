from concurrent.futures import wait
from concurrent.futures import ThreadPoolExecutor

import baostock as bs
import time

import crawler.ICrawler
from orm.Company import Company
from orm.ErrorTask import ErrorTask
from  orm.HistoryDate import HistoryDate
from orm.ORMManager import ORMManager
from crawler.CrawlerHistoryManager import CrawlerHisotryManager
import constant
import baostock.common.contants as cons


class HistoryDateCrawler(crawler.ICrawler.ICrawler):

    def __init__(self):
        # baostock 只支持单线程
        self.threadPool = ThreadPoolExecutor(1)

    def excute(self):

        results =  ORMManager.queryAll(Company)

        print('local company size {}', len(results))
        crawlerHistoryManager = CrawlerHisotryManager()
        tasks = []

        self.doLogin()

        for result in results :
            code = result.code
            rtn = self.threadPool.submit(self.doExcute, code, crawlerHistoryManager)
            tasks.append(rtn)

        wait(tasks)
        print('HistoryDateCrawler excute')
        pass

    def doExcute(self, code, crawlerHistoryManager):

        if crawlerHistoryManager.isItemContains(constant.crawler_history_key_history_date, code):
            pass

        self.excutePerHistory(code)

        crawlerHistoryManager.addValue(constant.crawler_history_key_history_date, code)

    def excutePerHistory(self, code):
        start_date = '{}-01-01'
        end_date = '{}-12-31'
        curYear = 2020

        while True:
            rs = self.getPerYearHistory(code, start_date.format(curYear), end_date.format(curYear))
            if  rs.error_code is cons.BSERR_SUCCESS:
                yearrHistoryDate = rs.get_data()
                rtn = yearrHistoryDate.to_sql(HistoryDate.__tablename__, ORMManager.getConnection(), if_exists='append', index = False)
                print(rtn)
            else :
                # sava to db
                params = code+';'+start_date.format(curYear)+';'+end_date.format(curYear)
                ORMManager.insert(ErrorTask('getPerYearHistory', params,rs.error_msg, constant.current_time_in_formate))

            curYear = curYear -1
            if curYear < 2000:
                break


    def getPerYearHistory(self, code , startDate, endDate):

        rs = bs.query_history_k_data_plus(code,
                                          constant.crawler_history_key_bao_stock_params,
                                          start_date=startDate, end_date= endDate,
                                          frequency="d", adjustflag="3")
        print('query_history_k_data_plus respond {}, {}:' , rs.error_code, rs.error_msg)

        if rs.error_code is cons.BSERR_NO_LOGIN:
            self.doLogin()
        elif rs.error_code is (cons.BSERR_CONNECT_TIMEOUT \
                or cons.BSERR_SENDSOCK_FAIL \
                or cons.BSERR_SENDSOCK_TIMEOUT \
                or cons.BSERR_SOCKET_ERR \
                or cons.BSERR_RECVCONNECTION_CLOSED \
                or cons.BSERR_RECVCONNECTION_CLOSED \
                or cons.BSERR_RECVSOCK_TIMEOUT):
            self.getPerYearHistory(code , startDate, endDate)

        return rs

    def doLogin(self):
        bs.logout()
        bs.login()