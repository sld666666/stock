import datetime
import logging

import base
import constant
from analyze.GrahamTendency import GrahamPeTTM, CHINA_AAA
from manager import HistoryDataManager
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

class GrahamRegression():
    def __init__(self):
        self.historyManager = HistoryDataManager.HistoryDataManager()

    def regress(self, code, amount):
        yesterday = datetime.date.today()- datetime.timedelta(days=356 * 5)
        year_five = datetime.date.today() - datetime.timedelta(days=356 * 10)
        df_history = self.historyManager.get_day_k_history(code, str(year_five),
                                                  str(yesterday))
        if not base.is_df_validate(df_history):
            return  amount

        df_history = df_history.sort_values(by=['date'], ascending=True)
        df_history = df_history.drop_duplicates(['code','date'])
        buy_count = 0
        buy_price = 0.0
        cur_price = 0.0
        for index, row in df_history.iterrows():
            date = row['date']
            cur_price = float(row['close'])

            if _is_support(row['peTTM']) and 0 is buy_count:
                buy_count = amount//cur_price
                buy_price = float(row['close'])
                logging.info('time :{} buy counts:{}, price:{}, amount: {}'.format(date, buy_count, buy_price, amount))
                amount -= buy_count* buy_price
            if not _is_support(row['peTTM']) and 0 is not buy_count:
                amount += cur_price * buy_count
                logging.info('time :{} sell counts:{}, price:{}, amount:{}'.format(date, buy_count, float(row['close']), amount))
                buy_count = 0


            #logging.info('time :{} pass, price:{} '.format(date, cur_price))

        return  amount+ float(cur_price)*float(buy_count)


if __name__=='__main__':
    base.init_applicaiton()
    regression = GrahamRegression()

    df_company = om.df_from_mongo(constant.table_name_company)
    codes = df_company['code'].to_list()
    sum = 0
    for code in codes:
        profit = regression.regress(code, 100000) - 100000
        print('{} profit : {}'.format(code, profit))
        sum += profit
        print('total profit {}'.format(sum))
    print(sum)


