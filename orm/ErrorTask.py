
from sqlalchemy import Column, Integer, String, DateTime,Date
from enum import  Enum



class ErrorTask():
    tablename = "error_task"



    def __init__(self, method, params, msg, create_time):
        self.method = method
        self.params = params
        self.msg = msg
        self.create_time = create_time
        pass