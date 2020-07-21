import datetime
import logging
import  pandas as pd
from crawler.JQDataCrawlerManager import JQDataCrawlerManager
from orm import ORMManager
from crawler.CompanyCommonCrawler import CompanyCommonCrawler
import base


class StockTendency():

    def __init__(self, engine):
        self.engine = engine
        self.companyCommonCrawler = CompanyCommonCrawler()
        self.jqDataCrawlerManager = JQDataCrawlerManager()

    def excute(self):
        df = self.filter_peTTM_report()
        print('get_peTTM_report size() {}'.format(len(df)))
        df = self.filter_peTTM_ratio(df)
        print('get_peTTM_ratio size() {}'.format(len(df)))

        #df = self.filter_current_ratio(df)
        print('get_current_ratio size() {}'.format(len(df)))

       # df = self.filter_dividend(df)
        print('filter_dividend size() {}'.format(len(df)))

        #df = self.filter_cashflow(df)
        print('filter_cashflow size() {}'.format(len(df)))

        df = self.filter_assets_bigger_than_liability(df)
        print('filter_assets_bigger_than_liability size() {}'.format(len(df)))

        print(self.__append_company(df))
        pass

    #价值A  股票的盈利回报率（市盈率倒数）应大于美国 AAA 级债券收益的 2 倍
    def filter_peTTM_report(self):
        df = pd.read_sql_query(
            #  ORDER BY peTTM desc
            'SELECT code, peTTM, close FROM history_date where date = \'2020-04-28\' and  (100/ peTTM) > 7.6 limit 200', \
            self.engine)
        column_open = df['peTTM'].astype(float)
        print(len(df))
        return df

    #价值B: 股票的市盈率应小于其过去五年最高市盈率的 40%
    def filter_peTTM_ratio(self, df_input):
        df_rtn = df_input
        for index, row in df_input.iterrows():
            code = row['code']
            df = pd.read_sql_query('SELECT max(peTTM) FROM history_date where code = \'{}\''.format(code),
                                   self.engine)
            max_peTTM = float(df.iloc[0,0])
            cur_peTTM = float(row['peTTM'])
            if cur_peTTM > 0.4 * max_peTTM:
                df_rtn = df_rtn.drop(index)
                print('drop stock {}{}'.format(code,cur_peTTM))

        return df_rtn

    ##价值C: 股票派息率应大于美国 AAA 级债券收益率的 2/3
    # 派息率=当年派息总数/同年每股总盈利。
    #年中和年末共发现金红利10股5元，每股总盈利是2元，派息率就是0.5/2=25%
    def filter_dividend(self, df_input):
        df_rtn = df_input
        for index, row in df_input.iterrows():
            code = row['code']
            if self._is_filter_dividend(code):
                df_rtn = df_rtn.drop(index)

        return df_rtn

    def filter_cashflow(self, df_input):
        df_rtn = df_input
        for index, row in df_input.iterrows():
            code = row['code']
            if self._is_filter_cashflow(code, 2019, row['close']):
                df_rtn = df_rtn.drop(index)

        return  df_rtn

    def do_filter(self, df_input, is_filter_fun):
        df_rtn = df_input
        for index, row in df_input.iterrows():
            code = row['code']
            if is_filter_fun(code):
                df_rtn = df_rtn.drop(index)

        return df_rtn

    # 安全A. 总负债要小于有形资产净值
    def filter_assets_bigger_than_liability(self, df_input):
        return  self.do_filter(df_input, self._is_filter_assets_bigger_than_liability)

    def _is_filter_assets_bigger_than_liability(self, code):
        cashflows = self.jqDataCrawlerManager.get_cashflows(code)
        rtn = self.__get_value(cashflows, 2019, 'pub_date', ['total_liability', 'total_current_assets'])
        if rtn is not None:
            total_liability = float(rtn.get('total_liability'))
            rtn = self.__get_tangibles(code, 2019) - total_liability

            if rtn > 0:
                return  False
            else:
                logging.info('filter_assets_bigger_than_liability code: {}  result:{} '.format(code, rtn))
                return  True

        else:
            return  False

    # 有形资产 = 股东权益(total_owner_equities) - 无形资产（intangible_assets）
    def __get_tangibles(self, code, year):
        balances = self.jqDataCrawlerManager.get_balances(code)
        rtn = self.__get_value(balances, year, 'report_date', ['total_owner_equities', 'intangible_assets'])
        if rtn is not None:
            return  float(rtn['total_owner_equities']) - float(rtn['intangible_assets'])
        else:
            return  0.0

    #﻿﻿ 安全 B. 流动比率（流动资产/流动负债）要大于 2
    def filter_current_ratio(self, df_inupt):
        df_rtn = df_inupt

        for index, row in df_inupt.iterrows():
            code = row['code']
            companyBalance =  self.companyCommonCrawler.get_company_balance(code)

            if None is companyBalance:
                df_rtn = df_rtn.drop(index)
                print('get_current_ratio drop stock {}{}'.format(code))
            else:
                currentRatio = self.convert_to_float(companyBalance['currentRatio'][0])
                if currentRatio < 2:
                    df_rtn = df_rtn.drop(index)
                    print('get_current_ratio drop stock {}{}'.format(code, currentRatio))

        return  df_rtn

    def convert_to_float(self, value):
        try:
            return float(value)  # 此处更改想判断的类型
        except ValueError:
            print('xxxxxxx:'+value)
            return 0.0

    ##价值C: 股票派息率应大于美国 AAA 级债券收益率的 2/3
    # 派息率=当年派息总数/同年每股总盈利。
    # 年中和年末共发现金红利10股5元，每股总盈利是2元，派息率就是0.5/2=25%

    def _is_filter_dividend(self,code):
        # 1.第一步先取每股派息数量
        bonus_ratio = self.__get_bonus_ratio(code, 2019)
        if bonus_ratio is None:
            print('drop stock dividend code:{} bonus_ratio: {}'.format(code, bonus_ratio))
            return  False
        bonus_ratio = float(bonus_ratio)/10
        # 2. 第二步 取每股盈利
        eps = self.__get_eps(code, 2019)
        if eps is None:
            print('drop stock dividend code:{} eps:{}'.format(code, eps))
            return False

        result = bonus_ratio / eps

        if result > 3.8*2/3:
            return False
        else :
            print('drop stock dividend code:{} eps:{}'.format(code, eps))
            return True

    # 价值E．股价要低于每股净流动资产（流动资产 - 总负债）的2 / 3.
    #  总负债 = total_liability， 流动资产 = total_current_assets
    def _is_filter_cashflow(self, code, year, close):
        cashflows = self.jqDataCrawlerManager.get_cashflows(code)
        rtn = self.__get_value(cashflows, 2019, 'pub_date', ['total_liability','total_current_assets'])
        if rtn is not None:
            total_liability = float(rtn.get('total_liability'))
            total_current_assets = float(rtn.get('total_current_assets') if rtn.get('total_current_assets') is not  None else 0.0)

            capitals = self.jqDataCrawlerManager.get_capitals(code)
            rtn = self.__get_value(capitals, 2019, 'pub_date', ['share_total'])
            share_total = float(rtn.get('share_total'))*10000

            #print("cashflow : close {} current {}".format(close, (total_current_assets - total_liability)/ share_total* 2 / 3))
            tmp = (total_current_assets - total_liability)/ share_total
            rtn =  float(close) <  tmp* 2 / 3
            if not rtn:
                print('filter_cashflow drop {} '.format(tmp))

            return not rtn

        return  True



    def __get_bonus_ratio(self, code, year):
        dividens = self.jqDataCrawlerManager.get_dividends(code)
        if dividens is not None:
            dividens = dividens.loc[(dividens['bonus_type'] == '年度分红')]
            date = datetime.date(year, 1,1)
            dividens = dividens.loc[dividens['report_date'] > date]
            if dividens is not None:
                dividens = dividens.sort_values(by=['report_date'], ascending = False).head(1)
                return  dividens['bonus_ratio_rmb'].iloc[0]

    def __get_eps(self,code, year):
        incomes = self.jqDataCrawlerManager.get_incomes(code)
        rtn = self.__get_value(incomes,2019, 'report_date',['basic_eps'])
        if rtn is not None:
            return rtn.get('basic_eps')

    def __append_company(self, df):
        codes = '\''
        codes += '\',\''.join(df['code'])
        codes += '\''
        sql = 'select code, short_name, ipo_date, industry from company where code in ({})'.format(codes)
        df_company =  pd.read_sql_query(sql, self.engine)

        return  pd.merge(df, df_company)

    def __get_value(self, df, year, year_name,  names):
        date = datetime.date(year, 1, 1)
        df = df.loc[df[year_name] > date]
        if df is not None and len(df) > 0:
            df = df.sort_values(by=[year_name], ascending=False).head(1)
            rtn = {}
            for name in names:
                rtn.update({name:df[name].iloc[0]})
            return  rtn
if __name__=='__main__':
    base.init_applicaiton()
    stockTendency = StockTendency(ORMManager.ORMManager.getConnection())
    stockTendency.excute()
    #stockTendency._is_filter_dividend('sh.600001')
    logging.info("testing")
    logging.error("error")

