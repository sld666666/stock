import datetime
import logging

import base
from analyze.GrahamTendency import GrahamPeTTM
from manager import HistoryDataManager


class GrahamRegression():
    def __init__(self):
        self.historyManager = HistoryDataManager.HistoryDataManager()

    def regress(self, code, amount):
        yesterday = datetime.date.today()
        year_five = datetime.date.today() - datetime.timedelta(days=356 * 5)
        df_history = self.historyManager.get_day_k_history(code, str(year_five),
                                                   str(yesterday))

        tendency =  GrahamPeTTM()
        buy_count = 0
        buy_price = 0.0
        cur_price = 0.0
        for index, row in df_history.iterrows():
            date = row['date']
            if tendency.is_supported(row['code'], date) and 0 is buy_count:
                buy_count = amount//float(row['close'])
                buy_price = float(row['close'])
                logging.info('time :{} buy counts:{}, price:{}, amount: {}'.format(date, buy_count, buy_price, amount))
                amount -= buy_count* buy_price
            if not tendency.is_supported(row['code'], date) and 0 is not buy_count:
                amount += float(row['close']) -  buy_price
                logging.info('time :{} sell counts:{}, price:{}, amount:{}'.format(date, buy_count, float(row['close']), amount))
                buy_count = 0
            cur_price = row['close']
            logging.info('time :{} pass, price:{} '.format(date, cur_price))

        return  amount+ float(cur_price)*float(buy_count)

if __name__=='__main__':
    base.init_applicaiton()
    regression = GrahamRegression()
    print(regression.regress('sh.600757', 100000))



