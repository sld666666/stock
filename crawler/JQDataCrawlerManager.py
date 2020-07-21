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
from crawler.JQDataCrawlerManagerPrivate import JQDataCrawlerManagerPrivate


class JQDataCrawlerManager():
    def __init__(self):
        self.engine = ORMManager.getConnection()
        self.jqDataCrawlerManagerPrivate = JQDataCrawlerManagerPrivate()

    def do_get_from(self, code, table_name, fun_query, translate_types=None):
        df_rtn = mongobase.df_from_mongo(table_name, {'code': self.jqDataCrawlerManagerPrivate.format_code_db(code)}, translate_types)
        if len(df_rtn) is 0:
            self.jqDataCrawlerManagerPrivate.do_get_from_jq(code,table_name, fun_query)
            df_rtn = mongobase.df_from_mongo(table_name, {'code': self.jqDataCrawlerManagerPrivate.format_code_db(code)}, translate_types)

        return df_rtn

    def get_finace(self, code):
        rtn = finance.run_query(query(finance.STK_BALANCE_SHEET).filter(finance.STK_BALANCE_SHEET.code == code).limit(100))
        return rtn

    def get_cashflows(self, code):
        return self.do_get_from(code,
                                  constant.talbe_name_cashflow,
                                    self.jqDataCrawlerManagerPrivate.do_query_cashflow,
                                  translate_types={'pub_date': datetime.date})


    def get_incomes(self, code):
        return  self.do_get_from(code,
                                   constant.talbe_name_incomes,
                                    self.jqDataCrawlerManagerPrivate.do_query_incomes,
                                   translate_types={'report_date': datetime.date})
    def get_balances(self, code):
        return  self.do_get_from(code,
                                   constant.table_name_balance,
                                    self.jqDataCrawlerManagerPrivate.do_get_balance,
                                   translate_types={'pub_date': datetime.date,
                                                    'end_date': datetime.date,
                                                    'report_date': datetime.date})
    def get_capitals(self, code):
        return self.do_get_from(code,
                                  constant.talbe_name_capital,
                                self.jqDataCrawlerManagerPrivate.do_get_capital,
                                  translate_types={'pub_date': datetime.date})

    def get_valuation(self, code, date):
        valuation = ORMManager.queryOne(Valuation,
                                        (and_ (Valuation.code == self.jqDataCrawlerManagerPrivate.format_code_db(code), Valuation.date == date )))
        if valuation is None:
            self.jqDataCrawlerManagerPrivate.do_get_valuations(code)

        valuation = ORMManager.queryOne(Valuation,
                                        (and_(Valuation.code == self.jqDataCrawlerManagerPrivate.format_code_db(code), Valuation.date == date)))
        return valuation

    def get_valuations(self, code):
        df_rtn = pd.read_sql_query('SELECT * FROM valuation_daily where code = \'{}\''.format(code), self.engine)
        if len(df_rtn) is 0:
            self.jqDataCrawlerManagerPrivate.do_get_valuations(code)

        df_rtn = pd.read_sql_query('SELECT * FROM valuation_daily where code = \'{}\''.format(code), self.engine)
        return df_rtn

    def get_dividends(self, code):
        df_rtn = pd.read_sql_query('SELECT * FROM company_dividend ',self.engine)
        if len(df_rtn) is 0 :
            self.jqDataCrawlerManagerPrivate.do_get_dividends(code)
            df_rtn = pd.read_sql_query('SELECT * FROM company_dividend ', self.engine)
        return df_rtn
