import  pandas as pd
import baostock as bs
import  os
import time
import crawler.ICrawler
from  orm.Company import Company
import crawler.engine.BaostockEngine as BE
import logging
import baostock.common.contants as cons

class CompanyCrawler(crawler.ICrawler.ICrawler):
    def __init__(self):
        self.bs = BE.BasestockEngine().getEngine()

    def excute(self):
        rs = self.bs.query_stock_industry()
        if None is rs:
            logging.error('query_stock_industry  is None')
            return None

        logging.info('query_history_k_data_plus respond {}, {}:'.format(rs.error_code, rs.error_msg))

        if rs.error_code is cons.BSERR_NO_LOGIN:
            self.bs.do_login()
            return self.excute()
        elif rs.error_code is (cons.BSERR_CONNECT_TIMEOUT \
                                       or cons.BSERR_SENDSOCK_FAIL \
                                       or cons.BSERR_SENDSOCK_TIMEOUT \
                                       or cons.BSERR_SOCKET_ERR \
                                       or cons.BSERR_RECVCONNECTION_CLOSED \
                                       or cons.BSERR_RECVCONNECTION_CLOSED \
                                       or cons.BSERR_RECVSOCK_TIMEOUT):
            return self.excute()

        return rs.get_data()




    def appendCompanyDetail(self, company):

        rs = bs.query_stock_basic(code_name=company.short_name)
        print('query_stock_industry {} {} :', rs.error_code, rs.error_msg)


        if rs.error_code == '0':
            company.ipo_date = rs.get_row_data()[2]

        return company