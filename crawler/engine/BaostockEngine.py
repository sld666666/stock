
import baostock as bs
from concurrent.futures import ThreadPoolExecutor
class BasestockEngine():
    def __init__(self):
        # baostock 只支持单线程
        self.threadPool = ThreadPoolExecutor(1)
        self.do_login()

    def getEngine(self):
        return  bs

    def do_login(self):
        bs.logout()
        bs.login()
