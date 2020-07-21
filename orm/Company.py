import  orm.base
from sqlalchemy import Column, Integer, String, DateTime,Date
from enum import  Enum

class CompanyType(Enum):
    MAJOR = 0
    MIDDLE = 1,
    TINY = 2


class Company(orm.base.Base):
    __tablename__ = "company"

    id = Column(Integer, primary_key=True)
    code = Column(String(100))
    name = Column(String(100), unique=True)
    update_time = Column(DateTime)
    create_time = Column(DateTime)
    type = Column(Integer)
    short_name = Column(String(100))
    ipo_date = Column(Date)
    industry = Column(String(100))


    def __init__(self):
        pass