import time

crawler_history_key_history_date = 'history_date'
crawler_history_key_bao_stock_params = 'date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST,peTTM,psTTM,pcfNcfTTM,pbMRQ'

current_time_in_formate = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))


time_since = '2000-01-01'


table_name_company =  'company'
talbe_name_incomes = 'incomes'
talbe_name_cashflow = 'cashflow'
talbe_name_capital = 'capital'
table_name_balance = 'balance'
table_name_k_data = 'k_data'
table_name_dividend = 'dividend'
table_name_tmp_pettm_supported = 'tmp_pettm_supported'
table_name_tmp_pettm_ratio_supported = 'tmp_pettm_ratio_supported'
table_name_tmp_dividend_supported = 'tmp_dividend_supported'
table_name_tmp_cashflow_supported = 'tmp_cashflow_supported'
table_name_task_order_graham_tendency = 'task_order_graham_tendency'
table_name_tmp_balance_supported = 'tmp_balance_supported'

table_name_tmp_safe_total_liability_supported= 'tmp_safe_total_liability_supported'
table_name_tmp_safe_profit_increment_supported= 'tmp_safe_profit_increment_supported'
table_name_tmp_safe_current_ratio_supported= 'tmp_safe_current_ratio_supported'
table_name_tmp_safe_every_year_profit_increment_supported= 'tmp_safe_every_year_profit_increment_supported'

table_name_k_data_history= 'k_data_history'


key_date_format = '%Y-%m-%d'
