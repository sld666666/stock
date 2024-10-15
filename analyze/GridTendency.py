import datetime

import backtrader as bt
import pandas as pd

import orm.mongobase

## 策略，网格参数微调
class GridStategy:
    def __init__(self, max_price, min_price, grid_level = 10, size_stategy =1, cash_stategy = 1):
        #总共是几个网格
        self.grid_level = grid_level
        ## 网格大小
        self.grid_size = self.init_grid_size(size_stategy, max_price, min_price,grid_level)

        # 每次波动
        self.grid_per_cash = self.init_grid_per_cash(max_price, min_price, grid_level, cash_stategy)
    def init_grid_size(self, size_stategy, max_price, min_price, grid_level):
        # size_stategy 平均分配原则
        if size_stategy == 1:
            return (max_price - min_price) / grid_level

        return (max_price - min_price) / grid_level

    def init_grid_per_cash(self, max_price, min_price, grid_level, cash_stategy):
        if cash_stategy == 1:
            return (max_price - min_price) / grid_level

        return (max_price - min_price) / grid_level
    def get_grid_level(self):
        return self.grid_level

    def get_grid_size(self):
        return self.grid_size

    def get_grid_per_cash(self):
        return self.grid_per_cash


## 震荡行情可适当收小，尽量多的抓住每一个小的波动；趋势行情可适当放大，防止过早满仓或者空仓
class GridTradingStrategy(bt.Strategy):
    def __init__(self, grid_stategy):
        #self.grid_prices = []
        self.buy_orders = []
        self.grid_stategy = grid_stategy

    def log(self, txt):
        ''' Logging function for this strategy'''
        dt = self.datas[0].datetime.date(0).isoformat()
        print(f'{dt}, {txt}')

    def is_to_sell(self, buy_orders, close):
        if len(buy_orders) <= 0:
            return False

        return (close - buy_orders[len(buy_orders) -1].executed.price) > self.grid_stategy.get_grid_size()

    def is_to_buy(self, buy_orders, close):
        if len(buy_orders) <= 0:
            return True

        return (buy_orders[len(buy_orders) - 1].executed.price - close) > self.grid_stategy.get_grid_size()

    def sell_if_needed(self, close):
        if self.datas[0].datetime.date(0) < datetime.date(2010, 1, 1):
            return

        if self.is_to_sell(self.buy_orders, close):
            order = self.buy_orders.pop()
            self.sell(exectype=bt.Order.Limit, price=close, size=order.executed.size)
            self.log(f'Sell Created, Price: {close[0]}')
            print(f'current balance {self.broker.getvalue()}')
            self.sell_if_needed(close)


    def buy_if_needed(self, close):
        if self.is_to_buy(self.buy_orders, close):
            total_value = self.broker.getvalue()
            buy_size = int(total_value / self.grid_stategy.get_grid_level() / self.data.close)
            order = self.buy(size = buy_size)
            self.log(f'Buy Created, Price: {close[0]}')
            self.buy_orders.append(order)
            print(f'current balance {self.broker.getvalue()}')
            self.buy_if_needed(close)


    def next(self):
        if not self.position:
            # 如果没有持仓，则在当前价格建仓
            total_value = self.broker.getvalue()
            buy_size = total_value/self.grid_stategy.get_grid_level()/ self.data.close
            order = self.buy(size=buy_size)
            order.isbuy()
            self.log(f'Buy Created, Price: {self.data.close[0]}, size{buy_size}')
            self.buy_orders.append(order)
        else:
            self.sell_if_needed(self.data.close)
            self.buy_if_needed(self.data.close)

def convert_to_float(x):
    try:
        return pd.to_numeric(x, errors='coerce')
    except ValueError:
        return pd.NA
def max_and_min_price(code):
    df = orm.mongobase.df_from_mongo('history_data', {'code': code})
    df = df.sort_values(by=['date'], ascending=True)
    df.index = pd.to_datetime(df.date)
    selected_columns = ['open', 'high', 'low', 'close', 'volume', 'peTTM']
    df[selected_columns] = df[selected_columns].applymap(convert_to_float)
    df = df.sort_values(by=['close'], ascending=True)
    print('{}#{}'.format(df.iloc[0]['date'], df.iloc[0]['close']))
    print('{}#{}'.format(df.iloc[len(df)-1]['date'], df.iloc[len(df)-1]['close']))

if __name__ == '__main__':
    max_and_min_price('sh.000016')
    cerebro = bt.Cerebro()

    grid_stategy = GridStategy(4731.826, 700.434)
    # 添加策略
    cerebro.addstrategy(GridTradingStrategy, grid_stategy)

    # 加载数据
    df = orm.mongobase.df_from_mongo('history_data', {'code': 'sh.000016'})
    df = df.sort_values(by=['date'], ascending=True)
    df.index = pd.to_datetime(df.date)
    selected_columns = ['open', 'high', 'low', 'close', 'volume', 'peTTM']
    df[selected_columns] =df[selected_columns].applymap(convert_to_float)
    data = bt.feeds.PandasData(dataname=df)

    cerebro.adddata(data)

    # 设置初始资金
    cerebro.broker.setcash(1000000.0)

    # 设置交易手续费
    cerebro.broker.setcommission(commission=0.001)

    # 运行回测
    cerebro.run()

    # 打印最终资金
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())