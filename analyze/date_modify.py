import time

import base
import constant
import  orm.mongobase as om

def modify_k_data_history():
    df_company = om.df_from_mongo(constant.table_name_company)
    codes = df_company['code'].to_list()

    for code in codes:
        start = time.perf_counter()
        df = om.df_from_mongo(constant.table_name_k_data, {'code': code})
        if base.is_df_validate(df):
            df = df.sort_values(by=['date'], ascending=True)
            date = df.head(1)['date'].iloc[0]
            om.update(constant.table_name_k_data_history, {'$set':{'code':code,'start_date': date}}, {'code':code})

        print((time.perf_counter() - start))

def remove_k_data(code):
    om.remove(constant.table_name_k_data, {'code':code})
    om.remove(constant.table_name_k_data_history, {'code': code})
if __name__=='__main__':
    remove_k_data('sh.600292')