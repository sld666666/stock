import datetime

import constant
import orm.mongobase as om
import crawler.impl.BaostockKDataCrawler as bdc
import logging

KEY_START_DATE = 'start_date'
KEY_END_DATE = 'end_date'
class HistoryDataManager():

    def __init__(self):
        self.stockKDataCrawler = bdc.BaostockKDataCrawler()
        pass

    def get_day_k_history(self, code, start_date, end_date):
        value = om.get(constant.table_name_k_data_history, {'code': code})

        target_start_date = datetime.datetime.strptime(start_date, constant.key_date_format)
        target_end_date = datetime.datetime.strptime(end_date, constant.key_date_format)
        if value is None:
            self._update_day_k_data_in_local(code, target_start_date, target_end_date)
            self._update_k_data_history(code, target_start_date, target_end_date)
        else :
            local_start_date = datetime.datetime.strptime(value[KEY_START_DATE], constant.key_date_format)
            local_end_date = datetime.datetime.strptime(value[KEY_END_DATE], constant.key_date_format)

            history_start_date = local_start_date
            history_end_date = local_end_date

            if target_start_date < local_start_date:
                self._update_day_k_data_in_local(code, target_start_date, local_start_date)
                history_start_date = target_start_date
            if local_end_date < target_end_date:
                self._update_day_k_data_in_local(code, local_end_date, target_end_date)
                history_end_date = target_end_date

            if history_start_date is not  local_start_date or \
                history_end_date is not local_end_date:
                self._update_k_data_history(code, history_start_date, history_end_date)


        df = om.df_from_mongo(constant.table_name_k_data, {'code':code},
                              translate_types={'date': datetime.date})
        if df is not  None and len(df) > 0 :
            df = df.loc[(df["date"] >= target_start_date.date()) & (df["date"] <= target_end_date.date())]


        return  df


    def _update_day_k_data_in_local(self, code, start_date, end_date):

        year_length = end_date.year - start_date.year + 1
        for i in range(year_length):
            for j in range(0, 12):
                cur_month = j + 1
                if 0 is i and cur_month < start_date.month:
                    continue
                elif i is (year_length - 1) and cur_month > end_date.month:
                    continue
                else:
                    df = self.stockKDataCrawler.query_history_day_k_data(code, start_date.year + i, cur_month)
                    query_keys = ['code', 'date']
                    om.df_to_mongo(constant.table_name_k_data, df,query_keys)
                    logging.info(' done query_history_day_k_data {}, {}--{}'.format(code, start_date.year + i, cur_month))


    def _update_k_data_history(self, code, start_date, end_date):
        query = {'code': code}
        start_date_str = datetime.datetime.strftime(start_date, constant.key_date_format)
        end_date_str = datetime.datetime.strftime(end_date, constant.key_date_format)
        k_day_history = {'code': code, 'start_date': start_date_str, 'end_date':end_date_str}
        values = {'$set': k_day_history}
        return om.update(constant.table_name_k_data_history, values, query)