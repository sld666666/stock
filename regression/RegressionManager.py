
class RegressionMananger():

    def __init__(self):
        self.commission = 0.0


    def add_data(self, df):
        self.df = df

    def add_strategy(self, strategy):
        self.strategy = strategy

    def set_cash(self,cash):
        self.cash = cash

    def get_cash(self):
        return  self.cash

    def set_commission(self, commission):
        self.commission = commission

    def run(self):
        #self.strategy.start()
        #for index, row in self.df.iterrows():
           # self.next(row.to_dict())


       # self.strategy.stop()