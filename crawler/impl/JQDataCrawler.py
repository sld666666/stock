from operator import and_

from jqdatasdk import *
from jqdatasdk import finance
import constant
from constant import time_since
from orm import mongobase
from orm.ORMManager import ORMManager
from orm.CompanyDividend import CompanyDividend
import datetime
import pandas as pd
from orm.Valuation import Valuation


class JQDataCrawler():

    def __init__(self):
        if not is_auth():
            rtn = auth('15168324351', '324351')
            print(rtn)

    def excute(self, code):
        pass

    def get_keys(self):
        pass

    def get_translate_types(self):
        pass

    def format_code_jq(self, local_code):

        rtn = self.translate_if_matched(local_code, 'sh.', '.XSHG')
        if rtn is local_code:
            rtn = self.translate_if_matched(local_code, 'sz.', '.XSHE')

        return rtn

    def format_code_db(self, local_code):
        rtn = self.translate_if_matched(local_code, '.XSHG', 'sh.', start_with=False)
        if rtn is local_code:
            rtn = self.translate_if_matched(local_code, '.XSHE', 'sz.', start_with=False)

        return rtn

    def translate_if_matched(self, code, matched, replaced, start_with=True):

        if start_with is True:
            if code.startswith(matched):
                code = code.replace(matched, '')
                code += replaced
        else:
            if code.endswith(matched):
                code = code.replace(matched, '')
                code = replaced + code

        return code


class ValuationsCrawler(JQDataCrawler):
    def get_keys(self):
       return ['code','date']

    def get_translate_types(self):
        return ['date']

    def excute(self, code):
        valuations = finance.run_query(
            query(finance.SW1_DAILY_VALUATION.code,
                  finance.SW1_DAILY_VALUATION.date,
                  finance.SW1_DAILY_VALUATION.pe,
                  finance.SW1_DAILY_VALUATION.pb,
                  finance.SW1_DAILY_VALUATION.average_price,
                  finance.SW1_DAILY_VALUATION.dividend_ratio)
                .filter(
                and_(finance.SW1_DAILY_VALUATION.code == self.format_code_jq(code),
                     finance.SW1_DAILY_VALUATION.date > time_since)))

        return valuations

class DividendsCrawler(JQDataCrawler):
    def get_keys(self):
       return ['code','report_date']

    def get_translate_types(self):
        return ['report_date']

    def excute(self, code):
        dividend = finance.run_query(
            query(finance.STK_XR_XD.code,
                  finance.STK_XR_XD.bonus_ratio_rmb,  # 派息比例，人民币
                  finance.STK_XR_XD.bonus_type,
                  finance.STK_XR_XD.report_date)
                .filter(
                and_(finance.STK_XR_XD.code == self.format_code_jq(code),
                     finance.STK_XR_XD.report_date > time_since)))

        return dividend

class IncomesCrawler(JQDataCrawler):
    def get_keys(self):
        return ['code', 'report_date']

    def get_translate_types(self):
        return ['report_date']

    def excute(self, code):
        return finance.run_query(
            query(finance.STK_INCOME_STATEMENT.code,
                  finance.STK_INCOME_STATEMENT.report_date,
                  finance.STK_INCOME_STATEMENT.total_operating_revenue,  # 营业总收入
                  finance.STK_INCOME_STATEMENT.total_operating_cost,  # 营业总成本
                  finance.STK_INCOME_STATEMENT.operating_profit,  # 营业利润
                  finance.STK_INCOME_STATEMENT.total_profit,  # 利润总额
                  finance.STK_INCOME_STATEMENT.eps,  # 每股收益
                  finance.STK_INCOME_STATEMENT.basic_eps)  # 本每股收益
                .filter(
                finance.STK_INCOME_STATEMENT.code == self.format_code_jq(code)))

class CashflowCrawler(JQDataCrawler):
    def get_keys(self):
        return ['code', 'pub_date']

    def get_translate_types(self):
        return ['pub_date']

    def excute(self, code):
        return finance.run_query(
            query(finance.STK_BALANCE_SHEET.code,
                  finance.STK_BALANCE_SHEET.pub_date,
                  finance.STK_BALANCE_SHEET.total_liability,  # 负债合计
                  finance.STK_BALANCE_SHEET.total_current_assets,
                  finance.STK_BALANCE_SHEET.total_current_liability)
                .filter(finance.STK_BALANCE_SHEET.code == self.format_code_jq(code)))

class CapitalCrawler(JQDataCrawler):
    def get_keys(self):
        return ['code', 'pub_date']

    def get_translate_types(self):
        return ['pub_date']

    def excute(self, code):
        return finance.run_query(
            query(finance.STK_CAPITAL_CHANGE.code,
                  finance.STK_CAPITAL_CHANGE.pub_date,
                  finance.STK_CAPITAL_CHANGE.share_total)
                .filter(
                finance.STK_CAPITAL_CHANGE.code == self.format_code_jq(code)))

class BalanceCrawler(JQDataCrawler):
    def get_keys(self):
        return ['code', 'pub_date']

    def get_translate_types(self):
        return ['pub_date', 'start_date','end_date','report_date']

    def excute(self, code):
        return finance.run_query(
            query(finance.STK_BALANCE_SHEET_PARENT)
                .filter(
                finance.STK_BALANCE_SHEET_PARENT.code == self.format_code_jq(code)))



