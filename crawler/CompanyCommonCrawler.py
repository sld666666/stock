
import baostock as bs
import pandas as pd

class CompanyCommonCrawler():

    def __init__(self):
        pass

    def get_company_balance(self, code):
       # 登陆系统
       lg = bs.login()
       # 显示登陆返回信息
       print('login respond {} {}:',lg.error_code, lg.error_msg)

       # 偿债能力
       balance_list = []
       rs_balance = bs.query_balance_data(code, year=2019, quarter=2)
       while (rs_balance.error_code == '0') & rs_balance.next():
           balance_list.append(rs_balance.get_row_data())
       result_balance = pd.DataFrame(balance_list, columns=rs_balance.fields)

       # 登出系统
       bs.logout()

       return  result_balance