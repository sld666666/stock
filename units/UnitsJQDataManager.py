
import unittest
from crawler.JQDataCrawlerManager import JQDataCrawlerManager

class UnitsJQDataManager(unittest.TestCase):

        def test_init(self):
           self.manager = JQDataCrawlerManager()

        def test_get_finace(self):
            manager = JQDataCrawlerManager()
            rtn = manager.get_finace('000001.XSHE')
            print(rtn)

        def test_get_total_liability(self):
            manager = JQDataCrawlerManager()
            rtn = manager.get_total_liability('000001.XSHE')
            print(rtn)

        def test_get_dividends(self):
            manager = JQDataCrawlerManager()
            rtn = manager.get_dividends('000001.XSHE')
            print(rtn)
            assert (len(rtn) > 0)

        def test_get_incomes(self):
            manager = JQDataCrawlerManager()
            rtn = manager.get_incomes('sh.600052')
            print(rtn)
            assert (len(rtn) > 0)

        def test_get_capitals(self):
            manager = JQDataCrawlerManager()
            rtn = manager.get_capitals('sh.600052')
            print(rtn)
            assert (len(rtn) > 0)

        def test_get_balances(self):
            manager = JQDataCrawlerManager()
            rtn = manager.get_balances('sh.600052')
            print(rtn)
            assert (len(rtn) > 0)