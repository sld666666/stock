import  orm.base
from sqlalchemy import Column, Integer, String, DateTime,Date
from enum import  Enum


class CompanyDividend(orm.base.Base):
    __tablename__ = "company_dividend"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(100))
    update_time = Column(DateTime)
    create_time = Column(DateTime)
    bonus_type = Column(String(100))
    bonus_ratio_rmb = Column(String(100), nullable=False, server_default='')
    report_date = Column(Date)

    def __init__(self):
        self.update_time = ''
        pass