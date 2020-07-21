import datetime
import logging
import  pandas as pd
import numpy as np
import constant
import crawler.impl.JQDataCrawler as cj
from manager import HistoryDataManager

import  orm.mongobase as om

import base

# ﻿经典价值投资策略的价值五法
# A. 股票的盈利回报率（市盈率倒数） 应大于美国 AAA 级债券收益的 2 倍。例
# 如某只股票的市盈率为 10 倍，则盈利回报率为 10%，如 AAA 债券收益率为 4%, 则该只股票的盈利回报率满足条件。
# B. 股票的市盈率应小于其过去五年最高市盈率的 40%。
# C. 股票派息率应大于美国 AAA 级债券收益率的 2/3.
# D. 股价要低于每股有形资产净值的 2/3.
# E．股价要低于每股净流动资产（流动资产-总负债）的 2/3.

# 经典价值投资策略的安全五法
#A. 总负债要小于有形资产净值。
#B. 流动比率（流动资产/流动负债） 要大于 2
#C. 总负债要要小于净流动资产的 2 倍本研究报告仅通过邮件提供给 华安财保资管 使用。 9
# D． 过去 10 年的平均年化盈利增长率要大于 7%
# E 过去十年中不能超过 2 次的盈利增长率小于-5%。
from manager.BonusDataManager import BonusDataManager

CHINA_AAA = 3.8


# 从 df中取出当前年的第一行
def get_value_year(df, year, year_name, names):
    date_start = datetime.date(year, 1, 1)
    date_end = datetime.date(year, 12, 30)
    df = df.loc[(df[year_name] > date_start) & (df[year_name] < date_end)]
    if df is not None and len(df) > 0:
        df = df.sort_values(by=[year_name], ascending=False).head(1)
        rtn = {}
        for name in names:
            rtn.update({name: df[name].iloc[0]})
        return rtn

class GrahamTendency():

    def __init__(self):
        self.historyManager = HistoryDataManager.HistoryDataManager()
        self.jqDataCrawler = cj.JQDataCrawler()
        self.bonusDataManager = BonusDataManager()

    def get_tmp_support_name(self):
        raise NotImplementedError("Must override method")

    def is_supported(self,code, date):
        raise NotImplementedError("Must override method")

    def filter(self, codes):
        df_support_history = om.df_from_mongo(self.get_tmp_support_name())

        for code in codes:
            if base.is_df_validate(df_support_history) and \
                            len(df_support_history.loc[df_support_history['code'] == code]) > 0:
                logging.info(" history is supported in  table: {} code {}".format(self.get_tmp_support_name(), code))
                continue

            status = 0
            try:
                yesterday = datetime.date.today() - datetime.timedelta(days=1)
                if self.is_supported(code, yesterday):
                    status = 1
                    logging.info(' {} is supported code :{}'.format(self.get_tmp_support_name(), code))
            except Exception as e:
                logging.error(' is  supported  error :{}'.format(e.args))
                status = 0

            value = {'code': code,  'status': status,'last_modify': constant.current_time_in_formate}
            om.update(self.get_tmp_support_name(), value, {'code': code})

    def _get_close(self, code):
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        # 为了寻找工作日，往前推14进行排序
        df = self.historyManager.get_day_k_history(code, str(yesterday - datetime.timedelta(days=14)),
                                                   str(yesterday))

        if df is None or len(df) <= 0:
            logging.error('get_day_k_history is None {}'.format(code))
            return None

        df = df.sort_values(by=['date'], ascending=False).head(1)
        return df['close'].astype(float).iloc[0]

    # 有形资产=总资产-无形资产
    def _get_totle_tangibles(self, code, year):
        df_balance = self.bonusDataManager.get_data(code, constant.table_name_balance, cj.BalanceCrawler())
        if base.is_df_validate(df_balance):
            df_balance = df_balance.sort_values(by=['report_date'], ascending=False).head(1)
            totle_tangibles = df_balance['total_assets'].iloc[0].astype(float) - df_balance['intangible_assets'].iloc[0]
            return  totle_tangibles


class OrderGrahamTendency():
    def __init__(self):
        self.grahams = {'pettm_status': GrahamPeTTM(),
                        'pettm_ratio_status' : GrahamPeTTMRatio(),
                        'dividend_status': GrahamDividend(),
                        'cashflow_status': GrahamCashflow(),
                        'balance_status': GrahamBalance()
                        }

    def filter(self, codes):
        df_task_history = om.df_from_mongo(constant.table_name_task_order_graham_tendency)
        for code in codes:
            row = None
            if base.is_df_validate(df_task_history):
                row = df_task_history.loc[df_task_history['code'] == code]

            for key, value in self.grahams.items():
                if not self.excute_one(key, value, code, row):
                    break

    def excute_one(self, key, value, code, row):
        if self.is_excuted(row, key):
            return  self.is_supported(row,key)

        status = 0
        try:
            yesterday = datetime.date.today() - datetime.timedelta(days=1)
            if value.is_supported(code, yesterday):
                status = 1
        except Exception as e:
            logging.error(' is  supported  error :{}'.format(e.args))
        self.update_history(code, key, status)
        return  1 is status

    def update_history(self, code, key, status):
        value = om.get(constant.table_name_task_order_graham_tendency, {'code': code})
        if not base.is_df_validate(value):
            value = {'code': code}
        value[key] = status
        value['last_modify'] = constant.current_time_in_formate
        om.update(constant.table_name_task_order_graham_tendency, value, {'code': code})

    def is_excuted(self, df, status_key):
        if not base.is_df_validate(df) :
            return  False
        try:
            return not np.isnan(df[status_key].iloc[0])
        except Exception:
            return  False

    def is_supported(self, df, status_key):
        # 还没处理过，返回true 为了处理
        if not base.is_df_validate(df) :
            return  True

        if not status_key in df.keys():
            return  False

        return df[status_key].iloc[0] == 1



# 价值A  股票的盈利回报率（市盈率倒数）应大于美国 AAA 级债券收益的 2 倍
# 中国的 AAA级债券 为3.8, 我们以前一个工作日的peTTM作为一个衡量指标进行计算
class GrahamPeTTM(GrahamTendency):
    def __init__(self):
        super().__init__()

    def get_tmp_support_name(self):
        return constant.table_name_tmp_pettm_supported


    def is_supported(self, code, date):
        yesterday = date
        # 为了寻找工作日，往前推14进行排序
        df = self.historyManager.get_day_k_history(code, str(yesterday- datetime.timedelta(days=14)),str(yesterday))

        if df is None or len(df) <= 0:
            logging.error( 'get_day_k_history is None {}'.format(code))
            return  False

        df = df.sort_values(by=['date'], ascending=False).head(1)
        peTTM = df['peTTM'].astype(float).iloc[0]

        if (1/peTTM) > (CHINA_AAA*2/100):
            return  True
        else:
            return  False

#价值B: 股票的市盈率应小于其过去五年最高市盈率的 40%
class GrahamPeTTMRatio(GrahamTendency):
    def __init__(self):
        super().__init__()

    def get_tmp_support_name(self):
        return constant.table_name_tmp_pettm_ratio_supported

    def is_supported(self, code, date):
        yesterday = date
        year_five = yesterday - datetime.timedelta(days=356*5)
        df = self.historyManager.get_day_k_history(code, str(year_five), str(yesterday))
        if not base.is_df_validate(df):
            return  False

        df = df.sort_values(by=['date'], ascending = False)
        try:
            cur_peTTM = float(df.iloc[0].loc['peTTM'])
            if cur_peTTM < 0:
                return  False

            df['peTTM'] = df['peTTM'].astype(float, errors='ignore')
            df =  df.sort_values(by=['peTTM'], ascending = False)
            max_peTTM = df['peTTM'].iloc[0]

            return  cur_peTTM < max_peTTM*0.4
        except Exception:
            logging.error('peTTM ratio support exception {}'.format(code))
            return  False

##价值C: 股票派息率应大于美国 AAA 级债券收益率的 2/3
# 派息率=当年派息总数/同年每股总盈利。
# 年中和年末共发现金红利10股5元，每股总盈利是2元，派息率就是0.5/2=25%
class GrahamDividend(GrahamTendency):
    def __init__(self):
        super().__init__()

    def get_tmp_support_name(self):
        return constant.table_name_tmp_dividend_supported

    def is_supported(self,code, date):
        last_year = date.year - 1
        # 1.第一步先取每股派息数量
        bonus_ratio = self._get_bonus_ratio(code, last_year)
        if bonus_ratio is None:
            logging.info('drop stock dividend code:{} bonus_ratio: {}'.format(code, bonus_ratio))
            return  False
        bonus_ratio = float(bonus_ratio)/10

        # 2. 第二步 取每股盈利
        eps = self._get_eps(code, last_year)
        if eps is None:
            logging.info('drop stock dividend code:{} eps:{}'.format(code, eps))
            return False

        result = bonus_ratio / eps

        if result > CHINA_AAA*2/3:
            return False
        else :
            logging.info('drop stock dividend code:{} eps:{}'.format(code, eps))
            return True

    def _get_bonus_ratio(self, code, year):
        dividens =  self.bonusDataManager.get_data(code = code,
                                                   table_name = constant.table_name_dividend,
                                                   crawler = cj.DividendsCrawler())
        if base.is_df_validate(dividens):
            dividens = dividens.loc[(dividens['bonus_type'] == '年度分红')]
            if base.is_df_validate(dividens):
                dividens = dividens.sort_values(by=['report_date'], ascending=False).head(1)
                return dividens['bonus_ratio_rmb'].iloc[0]

    def _get_eps(self,code, year):
        incomes = self.bonusDataManager.get_data(code,
                                                 constant.talbe_name_incomes,
                                                 cj.IncomesCrawler())
        rtn = get_value_year(incomes, year, 'report_date', ['basic_eps'])
        if rtn is not None:
            return rtn.get('basic_eps')

 # D. 股价要低于每股有形资产净值的 2/3.
class GrahamBalance(GrahamTendency):
    def __init__(self):
        super().__init__()

    def get_tmp_support_name(self):
        return constant.table_name_tmp_balance_supported

    def is_supported(self, code, date):
        last_year = date - 1
        # 获取上一个报告日的有形资产
        totle_tangibles = self._get_totle_tangibles(code, last_year)
        if None is totle_tangibles:
            return  False

        capitals = self.bonusDataManager.get_data(code, constant.talbe_name_capital, cj.CapitalCrawler())
        rtn = get_value_year(capitals, last_year, 'pub_date', ['share_total'])
        if rtn is None:
            logging.info(' filter_cashflow drop， no last year capitals code {} '.format(code))
            return  False

        share_total = float(rtn.get('share_total')) * 10000

        close = self._get_close(code)
        if None is close:
            return False
        tmp =  (totle_tangibles / share_total) * 2 / 3
        rtn = close < tmp
        if not rtn:
            logging.info(' filter_balance drop code {} : , close : {} , result {}'.format(code, close, tmp))

        return  rtn


# 价值E．股价要低于每股净流动资产（流动资产 - 总负债）的2 / 3.
#  总负债 = total_liability， 流动资产 = total_current_assets
class GrahamCashflow(GrahamTendency):
    def __init__(self):
        super().__init__()
    def get_tmp_support_name(self):
        return constant.table_name_tmp_cashflow_supported

    def is_supported(self,code, date):
        last_year = date.year - 1

        cashflows = self.bonusDataManager.get_data(code, constant.talbe_name_cashflow, cj.CashflowCrawler())

        rtn = get_value_year(cashflows, last_year, 'pub_date', ['total_liability', 'total_current_assets'])
        if base.is_df_validate(rtn):
            # 总负债
            total_liability = float(rtn.get('total_liability'))
            # 流动资产
            total_current_assets = float(
                rtn.get('total_current_assets') if rtn.get('total_current_assets') is not None else 0.0)

            capitals = self.bonusDataManager.get_data(code, constant.talbe_name_capital, cj.CapitalCrawler())
            rtn = get_value_year(capitals, last_year, 'pub_date', ['share_total'])
            share_total = float(rtn.get('share_total')) * 10000

            # print("cashflow : close {} current {}".format(close, (total_current_assets - total_liability)/ share_total* 2 / 3))
            tmp = (total_current_assets - total_liability) / share_total
            if not tmp > 0:
                return  False

            close = self._get_close(code)
            if None is close:
                return  False

            rtn = close < tmp * 2 / 3
            if not rtn:
                logging.info('filter_cashflow drop code {} : {} '.format(code, tmp))

            return not rtn

        return False

# 安全A： 总负债要小于有形资产净值
# 安全 C. 总负债要要小于净流动资产的
class GrahamSafeTotalLiability(GrahamTendency):

    def __init__(self):
        super().__init__()

    def get_tmp_support_name(self):
        return constant.table_name_tmp_safe_total_liability_supported

    def is_supported(self, code, date):
        last_year = date.year - 1
        # 获取上一个报告日的有形资产
        totle_tangibles = self._get_totle_tangibles(code, last_year)
        if None is totle_tangibles:
            return False

        cashflows = self.bonusDataManager.get_data(code, constant.talbe_name_cashflow, cj.CashflowCrawler())
        rtn = get_value_year(cashflows, last_year, 'pub_date', ['total_liability', 'total_current_assets'])
        if base.is_df_validate(rtn):
            # 总负债
            total_liability = float(rtn.get('total_liability'))
            # 流动资产
            total_current_assets = float(
                rtn.get('total_current_assets') if rtn.get('total_current_assets') is not None else 0.0)
            if  not total_liability < totle_tangibles:
                logging.info('filter SafeTotalLiability drop code {} :, total_liability: {} , totle_tangibles: {} '
                             .format(code, total_liability, totle_tangibles))
                return  False
            if not total_current_assets < totle_tangibles:
                logging.info('filter SafeTotalLiability drop code {} :, total_current_assets: {} , totle_tangibles: {} '
                             .format(code, total_current_assets, totle_tangibles))

            return  True
        else:
            return  False

# 安全B. 流动比率（流动资产/流动负债） 要大于 2
class GrahamSafeCurrentRatio(GrahamTendency):

    def __init__(self):
        super().__init__()

    def get_tmp_support_name(self):
        return constant.table_name_tmp_safe_current_ratio_supported

    def is_supported(self, code, date):
        last_year = date.year - 1
        cashflows = self.bonusDataManager.get_data(code, constant.talbe_name_cashflow, cj.CashflowCrawler())
        rtn = get_value_year(cashflows, last_year, 'pub_date', ['total_liability', 'total_current_assets'])
        if not base.is_df_validate(rtn):
            return  False

            # 流动资产
        total_current_assets = float(
            rtn.get('total_current_assets') if rtn.get('total_current_assets') is not None else 0.0)

        balances = self.bonusDataManager.get_data(code, constant.table_name_balance, cj.BalanceCrawler())
        rtn = get_value_year(balances, last_year, 'pub_date', ['total_current_liability'])
        if rtn is None:
            logging.info(' filter_safecurrentratio drop， no last year capitals code {} '.format(code))
            return  False

        # 流动负债合计
        total_current_liability = float(rtn.get('total_current_liability'))

        rtn = (total_current_assets/total_current_liability) > 2
        if not rtn:
            logging.info(' filter_safecurrentratio drop，code: {}, total_current_assets: {},  total_current_liability: {}'
                         .format(code, total_current_assets, total_current_liability))

        return  rtn

# D． 过去 10 年的平均年化盈利增长率要大于 7%
class GrahamSafeProfitIncrement(GrahamTendency):

    def __init__(self):
        super().__init__()

    def get_tmp_support_name(self):
        return constant.table_name_tmp_safe_profit_increment_supported

    def is_supported(self, code, date):
        last_year = date.year - 1

        incomes = self.bonusDataManager.get_data(code,
                                                     constant.talbe_name_incomes,
                                                     cj.IncomesCrawler())
        last_year_total_profit = get_value_year(incomes, last_year, 'report_date', ['total_profit'])

        last_five_year_total_profit = get_value_year(incomes, last_year, 'report_date', ['total_profit'])

        if last_year_total_profit is None or  last_five_year_total_profit is None:
            return  False

        last_year_total_profit = last_year_total_profit.get('total_profit')
        last_five_year_total_profit = last_five_year_total_profit.get('total_profit')
        if last_year_total_profit is None or  last_five_year_total_profit is None:
            return  False


        return (  float(last_year_total_profit) - float(last_five_year_total_profit))\
               / float(last_five_year_total_profit) > (7/100)


# E 过去十年中不能超过 2 次的盈利增长率小于-5%。
class GrahamSafeEveryYearProfitIncrement(GrahamTendency):

    def __init__(self):
        super().__init__()

    def get_tmp_support_name(self):
        return constant.table_name_tmp_safe_profit_increment_supported

    def is_supported(self, code, date):
        last_year = date.year - 1

        incomes = self.bonusDataManager.get_data(code,
                                                constant.talbe_name_incomes,
                                                cj.IncomesCrawler())
        for i in range(5):
            if not self._is_year_support(last_year -1, incomes):
                return  False

        return  True


    def _is_year_support(self, year, incomes):
        last_year_total_profit = get_value_year(incomes, year, 'report_date', ['total_profit'])

        last_last_year_total_profit = get_value_year(incomes, year-1, 'report_date', ['total_profit'])

        if last_year_total_profit is None or last_last_year_total_profit is None:
            return False

        last_year_total_profit = last_year_total_profit.get('total_profit')
        last_last_year_total_profit = last_last_year_total_profit.get('total_profit')
        if last_year_total_profit is None or last_last_year_total_profit is None:
            return False

        return (float(last_year_total_profit) - float(last_last_year_total_profit)) \
               / float(last_last_year_total_profit) > (-5 / 100)



if __name__=='__main__':
    base.init_applicaiton()
    stockTendency = GrahamTendency()

    df_company = om.df_from_mongo(constant.table_name_company)
    codes = df_company['code'].to_list()
    #codes = ['sh.600015']
    orderGrahamTendency = OrderGrahamTendency()
    orderGrahamTendency.filter(codes)
    #GrahamBalance().filter(codes)
