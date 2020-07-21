import mysql.connector as mc
import time

from orm.ORMManager import ORMManager
from orm.Industry import  Industry

def initConnector():
    connector = mc.connect(
        host="127.0.0.1",
        user="root",
        passwd="root",
        database = "stock"
    )

    cursor = connector.cursor()
    print('init connector {}', cursor)
    return cursor




if __name__=='__main__':
    cursor = initConnector()
    industry =  ORMManager.queryOne(Industry, (Industry.id == '5'))
    print('query one industry {}', industry.__dict__)

    #industries = ORMManager.queryAll(Industry)
    #print('query all industries {}', industries)


    names = ['医疗保健', '生物科技', '制药']
    parent_id = 14

    for name in names:
        industry = Industry(name, parent_id)
        industry.create_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        rtn = ORMManager.insert(industry)
        print(rtn)

    industries = ORMManager.queryAll(Industry, (Industry.parent_id == parent_id))
    print([ item.__dict__ for item in industries ])


