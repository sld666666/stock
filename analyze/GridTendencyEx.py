import datetime

import backtrader as bt
import pandas as pd

import orm.mongobase


first_buy_date = datetime.date(2019, 1, 1)
sell_aciton = {
    0.3: 0.1,
    0.4: 0.2,
    0.5: 0.3,
    0.6: 0.4,
    1: 1
}
buy_aciton = {
    0.2: 0.1,
    0.3: 0.3,
    0.5: 0.5,
    1: 1
}
## 策略，网格参数微调
class GridStategy:
    def __init__(self, grid_size, totle_values):
        self.totle_values = totle_values
        self.grid_size = grid_size
        self.init_buy_aciton()
        self.init_sell_action()
    def init_buy_aciton(self):
        self.buy_action = self.get_actions(buy_aciton)
    def init_sell_action(self):
        self.sell_action = self.get_actions(sell_aciton)
    def get_actions(self, action_config):
        rtn = []
        for key in action_config:
            item = {
                'ratio': key,
                'grid': action_config[key],
                'excuted': False
            }
            rtn.append(item)
        return rtn
    def _get_item(self, items, ratio):

        if len(items) < 1:
            return None

        if ratio <= items[0]['ratio']:
            return  None

        if (len(items) < 2):
            return items[0]

        if ratio <= items[1]['ratio']:
            return items[0]
        else :
            return self._get_item(items[1:], ratio)
    def get_sell_action_and_set_excuted(self, ratio):
        item = self._get_item(self.sell_action, ratio)
        if item is not None and not item['excuted']:
            item['excuted'] = True
            return item['grid']
        else:
            return None

    def get_buy_action_and_set_excuted(self, ratio):
        item = self._get_item(self.buy_action, ratio)
        if item is not None and not item['excuted']:
            item['excuted'] = True
            return item['grid']
        else:
            return None


    def get_grid_size(self):
        return self.grid_size

    def get_totle_values(self):
        return self.totle_values

## 震荡行情可适当收小，尽量多的抓住每一个小的波动；趋势行情可适当放大，防止过早满仓或者空仓
class GridTradingStrategy(bt.Strategy):
    def __init__(self, grid_strategy):
        self.first_price = 0
        self.buy_orders = []
        self.grid_strategy = grid_strategy

    def get_grid_szie(self):
        return self.grid_strategy.get_grid_size()

    def log(self, txt):
        ''' Logging function for this strategy'''
        dt = self.datas[0].datetime.date(0).isoformat()
        print(f'{dt}, {txt}')
    def get_sell_size(self, close):
        ratio = (close - self.first_price) / self.first_price
        action = self.grid_strategy.get_sell_action_and_set_excuted(ratio)
        if action is None:
            return 0

        cur_sell_value = self.grid_strategy.get_totle_values() * action
        total_value = self.broker.getvalue()
        if cur_sell_value > total_value:
            cur_sell_value = self.broker.getvalue()

        sell_size = int(cur_sell_value / self.data.close)
        return sell_size
    def sell_if_needed(self, close):
        if self.datas[0].datetime.date(0) == datetime.date(2021, 5, 13):
            print('')

        sell_size = self.get_sell_size(close)
        if sell_size <= 0:
            return

        self.sell(exectype=bt.Order.Limit, price=close, size=sell_size)
        self.log(f'Sell Created, Price: {close[0]}, size： {sell_size}, '
                 f'cash: {self.broker.get_cash()}, current balance {self.broker.getvalue()}')
        ## 一旦有卖说明价格高过买入价，重新计算网格
        self.grid_strategy.init_buy_aciton()

    def get_buy_size(self, close):
        ratio = (self.first_price - close) / self.first_price
        action = self.grid_strategy.get_buy_action_and_set_excuted(ratio)
        if action is None:
            return 0

        cur_buy_value = self.grid_strategy.get_totle_values() * action

        if cur_buy_value > self.broker.get_cash():
            cur_buy_value = self.broker.get_cash()

        buy_size = int(cur_buy_value / self.data.close)
        return  buy_size

    def buy_if_needed(self, close):
        buy_size = self.get_buy_size(close)
        if buy_size <= 0:
            return

        order = self.buy(size = buy_size)
        self.buy_orders.append(order)
        self.log(f'Buy Created, Price: {close[0]}, size： {buy_size}, '
                 f'cash: {self.broker.get_cash()}, current balance {self.broker.getvalue()}')
        ## 一旦有买说明价格低于过买入价，重新计算网格
        self.grid_strategy.init_sell_action()


    def next(self):
        if self.datas[0].datetime.date(0) < first_buy_date:
            return

        if not self.position:
            # 如果没有持仓，则在当前价格建仓
            total_value = self.broker.getvalue()
            self.first_price = self.data.close[0]
            buy_size =int( total_value/self.get_grid_szie()/ self.data.close)
            order = self.buy(size=buy_size)
            order.isbuy()
            self.log(f'First Created, Price: {self.data.close[0]}, size： {buy_size}, '
                     f'cash: {self.broker.get_cash()}, current balance {self.broker.getvalue()}')
            self.buy_orders.append(order)
        else:
            self.sell_if_needed(self.data.close)
            self.buy_if_needed(self.data.close)

def convert_to_float(x):
    try:
        return pd.to_numeric(x, errors='coerce')
    except ValueError:
        return pd.NA

code = 'sh.601919'
totle_values = 50000
if __name__ == '__main__':
    cerebro = bt.Cerebro()

    grid_stategy = GridStategy(5, totle_values)
    # 添加策略
    cerebro.addstrategy(GridTradingStrategy, grid_stategy)

    # 加载数据
    df = orm.mongobase.df_from_mongo('history_data', {'code': code})
    df = df.sort_values(by=['date'], ascending=True)
    df.index = pd.to_datetime(df.date)
    selected_columns = ['open', 'high', 'low', 'close', 'volume', 'peTTM']
    df[selected_columns] =df[selected_columns].applymap(convert_to_float)
    data = bt.feeds.PandasData(dataname=df)

    cerebro.adddata(data)

    # 设置初始资金
    cerebro.broker.setcash(totle_values)

    # 设置交易手续费
    cerebro.broker.setcommission(commission=0.001)

    # 运行回测
    cerebro.run()

    # 打印最终资金
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())