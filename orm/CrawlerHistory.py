import  orm.base
from sqlalchemy import Column, String


class CrawlerHistory(orm.base.Base):
    __tablename__ = "crawler_history"

    key = Column(String(100), primary_key=True)
    value = Column(String)

    def __init__(self, key, value):
        self.key = key
        self.value = value
        pass