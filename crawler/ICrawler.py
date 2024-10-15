import concurrent
from concurrent.futures import ThreadPoolExecutor

import baostock as bs
class ICrawler():

    def __init__(self, max_workers = 1):
        # baostock 只支持单线程
       # self.threadPool = ThreadPoolExecutor(max_workers)
        self._doLogin()

    def run(self):
        #tasks = []
        for item in self._preList():
            self._excute(item)
            #tasks.append(self.threadPool.submit(self._excute, item))

        #concurrent.futures.wait(tasks)
        pass
    def _doLogin(self):
        print('doLogin')
        bs.logout()
        bs.login()

    def _getClient(self):
        return bs
    def __exit__(self, exc_type, exc_val, exc_tb):
        print('logout')
        bs.logout()

    def _preList(self):
        print('ICrawler._preList')
        return []
    def _excute(self, item):
        print('ICrawler._excute')
        pass
