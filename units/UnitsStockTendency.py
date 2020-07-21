import unittest

from analyze.StockTendency import StockTendency
from orm import ORMManager
import base
base.init_applicaiton()

class UnitsStockTendency(unittest.TestCase):

    def test__is_filter_dividend(self):
        stockTendency = StockTendency(ORMManager.ORMManager.getConnection())
        stockTendency._is_filter_dividend('sh.600001')

    def test_is_filter_cashflow(self):
        stockTendency = StockTendency(ORMManager.ORMManager.getConnection())
        rtn =  stockTendency._is_filter_cashflow('sh.600052', 2019, 100000)

        assert (rtn == False)

    def test__is_filter_assets_bigger_than_liability(self):
        stockTendency = StockTendency(ORMManager.ORMManager.getConnection())
        rtn = stockTendency._is_filter_assets_bigger_than_liability('sh.600052')

        print(rtn)
