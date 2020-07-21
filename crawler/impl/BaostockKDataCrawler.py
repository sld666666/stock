import baostock.common.contants as cons
import constant
import crawler.IKDataCrawler
import crawler.engine.BaostockEngine as BE
import logging
import calendar

class BaostockKDataCrawler(crawler.IKDataCrawler.IKDataCrawler):
    def __init__(self):
        self.bs = BE.BasestockEngine().getEngine()

    def query_history_day_k_data(self, code, year, month):
        startDate = '{}-{}-01'.format(year, month)
        endDate = '{}-{}-{}'.format(year, month, self.get_month_max_day(year, month))

        rs = self.bs.query_history_k_data_plus(code,
                                          constant.crawler_history_key_bao_stock_params,
                                          start_date=startDate, end_date=endDate,
                                          frequency="d", adjustflag="3")
        if None is rs:
            logging.error('query error {}, {}, {}'.format(code, startDate, endDate))
            return  None

        logging.info('query_history_k_data_plus respond {}, {}:'.format(rs.error_code, rs.error_msg))

        if rs.error_code is cons.BSERR_NO_LOGIN:
            self.bs.do_login()
            return self.query_history_day_k_data(code, year, month)
        elif rs.error_code is (cons.BSERR_CONNECT_TIMEOUT \
                or cons.BSERR_SENDSOCK_FAIL \
                or cons.BSERR_SENDSOCK_TIMEOUT \
                or cons.BSERR_SOCKET_ERR \
                or cons.BSERR_RECVCONNECTION_CLOSED \
                or cons.BSERR_RECVCONNECTION_CLOSED \
                or cons.BSERR_RECVSOCK_TIMEOUT):
            return self.query_history_day_k_data(code , year, month)

        return rs.get_data()

    def get_month_max_day(self,year, month):
        return calendar.monthrange(year, month)[1]

