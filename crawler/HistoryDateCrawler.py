
import datetime
import crawler.ICrawler
import constant
import baostock.common.contants as cons

import orm.mongobase


class HistoryDateCrawler(crawler.ICrawler.ICrawler):


    def _preList(self):
        print('ICrawler._preList')
        return constant.observation_list.keys()

    def _getTableName(self):
        return 'history_data'

    def _excute(self, code):
        self._excutePerHistory(code)
    def _excutePerHistory(self, code):
        start_date = '{}-01-01'
        end_date = '{}-12-31'
        curYear = datetime.datetime.now().year
        history_years = list(range(2000, curYear + 1))

        for year in history_years:
            history_years = orm.mongobase.get('history_{}'.format(self._getTableName()), {'code':code, 'year':year})
            if history_years is not None and  len(history_years) >  0:
                continue

            rs = self._getPerYearHistory(code, start_date.format(year), end_date.format(year))
            if rs.error_code is cons.BSERR_SUCCESS:
                yearrHistoryDate = rs.get_data()
                orm.mongobase .df_to_mongo_inserts(self._getTableName(), yearrHistoryDate)
                value = {"$set": {'code':code, 'year':year}}
                orm.mongobase.update('history_{}'.format(self._getTableName()), value, {'code':code, 'year':year})
                print(yearrHistoryDate)
            else :
                # sava to db
                params = code+';'+start_date.format(year)+';'+end_date.format(year)
                value = {'msg':rs.error_msg, 'params':params}
                orm.mongobase.update( 'error_{}'.format(self._getTableName), value, {'code':code, 'year':year})

    def _getPerYearHistory(self, code , startDate, endDate):

        rs = self._getClient().query_history_k_data_plus(code,
                                          constant.crawler_history_key_bao_stock_params,
                                          start_date=startDate, end_date= endDate,
                                          frequency="d", adjustflag="3")
        print('query_history_k_data_plus respond {}, {}:' , rs.error_code, rs.error_msg)

        return rs


if __name__=='__main__':
    crawler = HistoryDateCrawler()
    crawler.run()
