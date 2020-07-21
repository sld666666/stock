import datetime

import base
import constant
import orm.mongobase as om
import crawler.CompanyCrawler as cc
import logging

class CompanyaManager():

    def __init__(self):
        self.companyCrawler = cc.CompanyCrawler()
        pass

    def init_all_company(self):
        df = self.companyCrawler.excute()
        om.df_to_mongo(constant.table_name_company, df, ['code'])

if __name__=='__main__':
    base.init_applicaiton()
    companyManager = CompanyaManager()
    companyManager.init_all_company()
