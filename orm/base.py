from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

def initEngine():
    # ?charset=uft8
    engine = create_engine("mysql+pymysql://root:root@127.0.0.1/stock",
                           echo=False,
                           pool_size=8,
                           pool_recycle=60 * 30
                           )
    print('init engine {}',engine)
    return  engine

engine = initEngine()
DBSession = sessionmaker(bind=engine)