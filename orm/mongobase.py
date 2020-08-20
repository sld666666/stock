import datetime
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from dateutil import parser
plt.switch_backend('agg')


client = MongoClient('localhost', 27017)
db = client.stock

def df_to_mongo(table_name, df, query_keys):
    if df is None:
        return

    table = db[table_name]

    df = df_traslate_to_mongo(df)
    for index, row in df.iterrows():
        value = row.to_dict()
        query = {key:value[key] for key in (value.keys() & query_keys) }
        table.update(query, value, upsert=True)

def df_to_mongo_inserts(table_name, df):
    if df is None:
        return
    if len(df) <= 0:
        return

    table = db[table_name]
    values = []
    for index, row in df.iterrows():
        value = row.to_dict()
        values.append(value)

    table.insert_many(values)


def df_from_mongo(table_name, conditions=None, translate_types = None):
    table = db[table_name]
    values = table.find(conditions)
    df = pd.DataFrame(list(values))

    df = df_traslate_from_mongo(df, translate_types)
    return df

#遍历df, 对mongo 不知道的数据类型转换为str
def df_traslate_to_mongo(df_input):
    for index, clounms in df_input.iteritems():
        if len(clounms) and type(clounms[0]) == datetime.date:
            #df_input = df_input.drop([index],axis=1)
            items = []
            for item in clounms:
                items.append(str(item))
            df_input[index] = items

    return df_input

def df_traslate_from_mongo(df_input, translate_types):
    if translate_types is None:
        return  df_input

    for index, clounms in df_input.iteritems():
        if index in translate_types:
            items = []
            for item in clounms:
                date = datetime.datetime.strptime(item, '%Y-%m-%d').date()
                items.append(date)

            df_input[index] = items

    return df_input

def query(table_name, conditions = None):
    table = db[table_name]
    values = table.find(conditions)
    return  values

def get(table_name, conditions = None):
    table = db[table_name]
    value = table.find_one(conditions)
    return value


def update(table_name, value, query):
    table = db[table_name]
    return table.update(query, value, upsert = True)

def remove(table_name, conditions):
    table = db[table_name]
    return table.remove(conditions)