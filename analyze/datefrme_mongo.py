
import datetime
import logging

import base
import constant
from analyze.GrahamTendency import GrahamPeTTM, CHINA_AAA
import  orm.mongobase as om


def _is_support( peTTM):
    try :
        if (1/float(peTTM)) > (CHINA_AAA*2/100):
            return  True
        else:
            return  False
    except Exception:
        logging.error('petTM error :{}'.format(peTTM))
        return  False

def regress(date, trade):
    #sql = 'select * from k_data where code = \'{}\' '.format(code)
    resDf = om.df_from_mongo(constant.table_name_k_data, {'date': date}, translate_types={'date': datetime.date})

    if not base.is_df_validate(resDf):
        return None

    df = resDf.sort_values(by=['psTTM'], ascending=False)
    logging.info('regress :{}'.format(date))
    for index, row in df.iterrows():
        if _is_support(row['peTTM']):
            trade.buy(row)
        else:
            trade.sell(row)

class Trade():

    def __init__(self, total, per_total):
        self.total = total
        self.trade = []
        self.per_total = per_total

    def buy(self, row):
        if self.find(row['code']) >= 0 :
            return

        cur_price = float(row['close'])
        buy_count = 0
        if self.per_total < self.total:
            buy_count = self.per_total // cur_price
        else:
            buy_count = self.total // cur_price

        if buy_count > 0:
            self.total -= cur_price * buy_count
            self.trade.append({'code': row['code'], 'buy_price': cur_price, 'buy_count': buy_count})
            print('code: {} time :{} buy counts:{}, price:{}, amount: {}'.format(row['code'], row['date'], buy_count, cur_price, self.total))

    def sell(self, row):
        index = self.find(row['code'])
        if index < 0:
            return

        cur_price = float(row['close'])
        self.total += cur_price * self.trade[index]['buy_count']

        print('owner  code: {}, total: {}'.format(row['code'], self.trade[index]['buy_count'] *(cur_price - self.trade[index]['buy_price'])))
        print('time :{} sell counts:{}, price:{}, amount:{}'\
              .format(row['date'], self.trade[index]['buy_count'], float(row['close']), self.total))
        self.trade.pop(index)

    def getTotal(self, rows):
        tmp = 0
        for row in rows:
            code = row['code']

            index = self.find(code)
            if index >= 0:
                tmp += float(row['close']) * self.trade[index]['buy_count']

        return self.total + tmp

    def getKeptCodes(self):
        rtn = []
        for trade in self.trade:
            rtn.append(trade['code'])

        return  rtn


    def find(self, code):
        for  i in range(len(self.trade)):
            item = self.trade[i]
            if item['code'] == code:
                return  i

        return  -1

def getRows(df, codes):
    rtn = []

    for code in codes:
        df_tmp = df[df['code'] == code]
        if len(df_tmp) > 0:
            row = df_tmp.head(1).iloc[0]
            rtn.append(row)

    return  rtn



def getLastDf(date):
    date_str = date.strftime("%Y-%m-%d")
    resDf = om.df_from_mongo(constant.table_name_k_data, {'date': date_str}, translate_types={'date': datetime.date})
    if not  base.is_df_validate(resDf) or len(resDf) < 10:
        date -= datetime.timedelta(days=1)
        return getLastDf(date)

    return resDf


if __name__=='__main__':
    base.init_applicaiton()


    df_company = om.df_from_mongo(constant.table_name_company)
    codes = df_company['code'].to_list()
    sum = 0


    begin = datetime.date.today() - datetime.timedelta(days=365 * 5)

    trade = Trade(2000000, 100000)
    while begin <= datetime.date.today():
        regress(begin.strftime("%Y-%m-%d"), trade)
        begin += datetime.timedelta(days=1)

    last_df = getLastDf(datetime.date.today()- datetime.timedelta(days=3 * 6))
    rows = getRows(last_df, trade.getKeptCodes())
    print(trade.getTotal(rows))



