import logging


base_log_dir = '/Users/luodongshen/Documents/stock_logs/'
def init_applicaiton():
    __init_config()
    __init_log()

def __init_log():
    logging.basicConfig(filename=base_log_dir+"stock_info.log", level=logging.INFO)
    logging.basicConfig(filename=base_log_dir+ "stock_error.log", level=logging.ERROR)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

def __init_config():
    pass

def is_df_validate(df):
    return  (df is not None) and ( len(df) > 0)


def isdigit(value):
    try :
        return value.isdigit()
    except Exception:
        print('isdigit exception {}'.format(value))
        return  False