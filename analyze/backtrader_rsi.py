import backtrader as bt
import backtrader.indicators as btind
import backtrader.feeds as btfeeds
import pandas as pd
import numpy as np
import tushare as ts
import matplotlib
from PyQt5 import QtCore
import matplotlib.pyplot as plt

#正常显示画图时出现的中文和负号
from pylab import mpl
from datetime import datetime

import base
import constant
from manager import HistoryDataManager
import  orm.mongobase as om

mpl.rcParams['font.sans-serif']=['SimHei']
mpl.rcParams['axes.unicode_minus']=False

class MyStrategy(bt.Strategy):
    params = (('short', 30),
              ('long', 70),
              ('printlog', False),
              ('maperiod', 15),
              ('code', None))

    def __init__(self):
        self.params.code = code
        self.rsi = bt.indicators.RSI_SMA(
            self.data.close, period=21)



    def next(self):
        #print(self.datas[0].datetime.date(0))
        if not self.position:
            if self.rsi < self.params.short:
                self.buy()
        else:
            if self.rsi > self.params.long:
                self.sell()


    def log(self, txt, dt=None, doprint=False):
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print(f'{dt.isoformat()},{txt}')

            # 记录交易执行情况（可省略，默认不输出结果）

    def notify_order(self, order):
        # 如果order为submitted/accepted,返回空
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 如果order为buy/sell executed,报告价格结果
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'买入:\n价格:{order.executed.price},\
                 成本:{order.executed.value},\
                 手续费:{order.executed.comm}')
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log(f'卖出:\n价格：{order.executed.price},\
                 成本: {order.executed.value},\
                 手续费{order.executed.comm}')
            self.bar_executed = len(self)

            # 如果指令取消/交易失败, 报告结果
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('交易失败')
        self.order = None


        # 记录交易收益情况（可省略，默认不输出结果）

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log(f'策略收益：\n毛收益 {trade.pnl:.2f}, 净收益 {trade.pnlcomm:.2f}')

        # 回测结束后输出结果（可省略，默认输出结果）

    def stop(self):
        self.log(' code: %s 期末总资金 %.2f' %
                 (self.params.code, self.broker.getvalue()), doprint=True)



def main(code, start, end='', startcash=10000, qts=500, com=0.001):
    print('###code:' + code)
    # 创建主控制器
    cerebro = bt.Cerebro()
    # 导入策略参数寻优
    cerebro.optstrategy(MyStrategy)
    # 获取数据
    historyManager = HistoryDataManager.HistoryDataManager()
    df = historyManager.get_day_k_history(code, '2016-01-01', '2020-07-07')
    if not base.is_df_validate(df):
        return startcash

    df = df.sort_values(by=['date'], ascending=True)

    df = df[df['volume'].astype(float) > 0.0]
    if len(df) < 100:
        return startcash

    df.index = pd.to_datetime(df.date)
    df = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    data = bt.feeds.PandasData(dataname=df)
    # 初始化cerebro回测系统设置
    cerebro = bt.Cerebro()
    # 加载数据
    cerebro.adddata(data)
    # 将交易策略加载到回测系统中
    cerebro.addstrategy(MyStrategy)
    # 设置初始资本为100,000
    cerebro.broker.setcash(100000.0)
    # 每次固定交易数量
    cerebro.addsizer(bt.sizers.FixedSize, stake=1000)
    # 手续费
    cerebro.broker.setcommission(commission=0.001)

    print(code + '初始资金: %.2f' % cerebro.broker.getvalue())
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='SharpeRatio')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')
    results = cerebro.run()
    return cerebro.broker.getvalue()
    #cerebro.plot()




if __name__=='__main__':
    base.init_applicaiton()
    # plot_stock('上证综指')
    code = 'sh.600015'
    df_company = om.df_from_mongo(constant.table_name_company)
    codes = df_company['code'].to_list()
    codes = ['sh.600760']
    sum = 0
    for code in codes:
        rtn = main(code, '', '', 1000000, 100)
        sum += rtn - 1000000
        print(sum)
    print(sum / (len(codes) * 1000000) * 100)