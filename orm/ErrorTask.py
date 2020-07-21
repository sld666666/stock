import  orm.base
from sqlalchemy import Column, Integer, String, DateTime,Date
from enum import  Enum



class ErrorTask(orm.base.Base):
    __tablename__ = "error_task"

    id = Column(Integer, primary_key=True)
    update_time = Column(DateTime)
    create_time = Column(DateTime)
    method = Column(String)
    params = Column(String)
    msg = Column(String)


    def __init__(self, method, params, msg, create_time):
        self.method = method
        self.params = params
        self.msg = msg
        self.create_time = create_time
        pass