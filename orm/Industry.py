import  orm.base
from sqlalchemy import Column, Integer, String, DateTime


class Industry(orm.base.Base):
    __tablename__ = "industry"

    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer)
    name = Column(String(100), unique=True)
    update_time = Column(DateTime)
    create_time = Column(DateTime)

    def __init__(self, name, parent_id):
        self.name = name
        self.parent_id = parent_id
