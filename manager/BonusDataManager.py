import base
import constant
import orm.mongobase as om
import crawler.impl.JQDataCrawler as cij
import logging

KEY_START_DATE = 'start_date'
KEY_END_DATE = 'end_date'
class BonusDataManager():
    def __init__(self):
        self.jqDataCrawler = cij.JQDataCrawler()

    def get_data(self, code, table_name, crawler):
        values = om.df_from_mongo(table_name, {'code':code},
                                  translate_types=crawler.get_translate_types())
        if not base.is_df_validate(values):
            values = crawler.excute(code)
            values['code'] = code
            om.df_to_mongo(table_name, values, crawler.get_keys())

            values = om.df_from_mongo(table_name, {'code': code},
                                      translate_types=crawler.get_translate_types())

        return  values

    def get_data_condition(self, code, conditions, table_name, func_crawler):
        values = om.get(table_name, {'code': conditions})
        if values is None:
            func_crawler(code)

        values = om.get(table_name, {'code': conditions})

        return values
