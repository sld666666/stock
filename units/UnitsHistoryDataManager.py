import datetime
import unittest

import constant
from manager import HistoryDataManager
import base

base.init_applicaiton()

class UnitsJQDataManager(unittest.TestCase):

        def test_init(self):
            pass

        def test_update_day_k_data_in_local(self):
            manager = HistoryDataManager.HistoryDataManager()
            local_start_date = datetime.datetime.strptime('2019-05-01', constant.key_date_format)
            local_end_date = datetime.datetime. strptime('2020-05-07', constant.key_date_format)
            rtn = manager._update_day_k_data_in_local('sh.600016',local_start_date,local_end_date)
            print(rtn)

        def test_get_day_k_history(self):
            manager = HistoryDataManager.HistoryDataManager()
            rtn = manager.get_day_k_history('sh.600006', '2020-06-8', '2020-06-08')
            print(rtn[['code','date','peTTM', 'psTTM']])

