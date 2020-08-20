from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from analyze.datefrme_mongo import Trade

SPARK_HOME = '/Users/luodongshen/Documents/soft/spark-3.0.0-bin-hadoop3.2'
import findspark
findspark.add_packages('org.mongodb.spark:mongo-spark-connector_2.12:3.0.0')
findspark.init(SPARK_HOME)

from pyspark.sql import SparkSession

import datetime
import logging

import base
import constant
from analyze.GrahamTendency import GrahamPeTTM, CHINA_AAA
import  orm.mongobase as om


spark = SparkSession.builder.appName('MyApp')\
        .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1/stock.k_data') \
        .getOrCreate()

schema = StructType([
    StructField("code", StringType()),
    StructField("start_date", StringType()),
    StructField("end_date", StringType())
])


df = spark.read.format('mongo').load()
df.createOrReplaceTempView('k_data')

def _is_support( peTTM):
    try :
        if (1/float(peTTM)) > (CHINA_AAA*2/100):
            return  True
        else:
            return  False
    except Exception:
        logging.error('petTM error :{}'.format(peTTM))
        return  False

def regress(date, trade):
    #sql = 'select * from k_data where code = \'{}\' '.format(code)
    resDf = spark.sql('select * from k_data')
    df = resDf.filter(resDf.date == date)
    if df.count() <= 0:
        return

    df = df.sort('psTTM', ascending=False)
    logging.info('regress :{}'.format(date))
    for row in df.collect():
        if _is_support(row['peTTM']):
            trade.buy(row)
        else:
            trade.sell(row)


def getLastDf(date):
    resDf = spark.sql('select * from k_data')
    df = resDf.filter(resDf.date == date.strftime("%Y-%m-%d"))
    if df.count() <= 10 :
        date -= datetime.timedelta(days=1)
        return  getLastDf(date)

    return df

def getRows(df, codes):
    rtn = []
    for code in codes:
        df_tmp = df.filter(df['code'] == code)
        if df_tmp.count() > 0:
            row = df_tmp.collect()[0]
            rtn.append(row)

    return  rtn

if __name__=='__main__':
    base.init_applicaiton()


    df_company = om.df_from_mongo(constant.table_name_company)
    codes = df_company['code'].to_list()
    sum = 0
    begin = datetime.date.today() - datetime.timedelta(days=3 * 5)

    trade = Trade(2000000, 100000)
    while begin<=  datetime.date.today() - datetime.timedelta(days=3 * 4):
        regress(begin.strftime("%Y-%m-%d"), trade)
        begin += datetime.timedelta(days=1)

    last_df = getLastDf(datetime.date.today())
    rows = getRows(last_df, trade.getKeptCodes())
    print(trade.getTotal(rows))

    spark.stop()

