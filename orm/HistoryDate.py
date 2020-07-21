import  orm.base
from sqlalchemy import Column, Integer, String, DateTime, Date, Float, SmallInteger


class HistoryDate(orm.base.Base):
    __tablename__ = "history_date"

    id = Column(Integer, primary_key=True)
    update_time = Column(DateTime)
    create_time = Column(DateTime)

    code = Column(String(100))
    open = Column(Float(6))
    high = Column(Float(6))
    low = Column(Float(6))
    close = Column(Float(6))
    preclose = Column(Float(6))
    amount = Column(Float(6))
    turn = Column(Float(6))
    pctChg = Column(Float(6))
    peTTM = Column(Float(6))
    psTTM = Column(Float(6))
    pcfNcfTTM = Column(Float(6))
    pbMRQ = Column(Float(6))
    volume = Column(Integer())
    adjustflag = Column(SmallInteger())
    tradestatus = Column(SmallInteger())
    isST = Column(SmallInteger())


    def __init__(self):
        pass