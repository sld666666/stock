import  orm.base
from sqlalchemy import Column, Integer, String, DateTime,Date
from enum import  Enum



class Valuation(orm.base.Base):
    __tablename__ = "valuation_daily"

    id = Column(Integer, primary_key=True)
    code = Column(String(100))
    update_time = Column(DateTime)
    create_time = Column(DateTime)
    pe = Column(String(100))
    pb = Column(String(100))
    average_price = Column(String(100))
    dividend_ratio = Column(String(100))
    date = Column(Date)


    def __init__(self):
        pass