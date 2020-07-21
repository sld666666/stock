import unittest

from analyze.GrahamTendency import GrahamTendency, GrahamBalance, GrahamPeTTM, GrahamPeTTMRatio, \
    GrahamSafeTotalLiability, GrahamSafeCurrentRatio, GrahamSafeProfitIncrement, GrahamSafeEveryYearProfitIncrement
from orm import ORMManager
import base
base.init_applicaiton()

class UnitsStockTendency(unittest.TestCase):

    def test_is_peTTM_support(self):
        stockTendency = GrahamPeTTM()
        stockTendency.is_supported('sh.600483')

    def test_is_peTTM_ratio_support(self):
        stockTendency = GrahamPeTTMRatio()
        stockTendency.is_supported('sh.600714')

    def test_is_peTTM_ratio_support(self):
        stockTendency = GrahamBalance()
        stockTendency.is_supported('sh.600714')

    def test_is_safetotalliability_support(self):
        stockTendency = GrahamSafeTotalLiability()
        stockTendency.is_supported('sh.600714')

    def test_is_safecurrentratio_support(self):
        stockTendency = GrahamSafeCurrentRatio()
        stockTendency.is_supported('sh.600714')

    def test_is_safeprofitincrement_support(self):
        stockTendency = GrahamSafeProfitIncrement()
        stockTendency.is_supported('sh.600714')

    def test_is_safeeveryyearprofitincrement_support(self):
        stockTendency = GrahamSafeEveryYearProfitIncrement()
        stockTendency.is_supported('sh.600714')
