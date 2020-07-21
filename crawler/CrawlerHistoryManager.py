
from orm.ORMManager import  ORMManager
from orm.CrawlerHistory import CrawlerHistory

class CrawlerHisotryManager():
    def __init__(self):
        self.indexs = {}
        self.historys = []
        historys = ORMManager.queryAll(CrawlerHistory)

        index = 0

        for item in historys:
            tmpSub = set(item.value.split(';'))
            self.historys.append(tmpSub)
            self.indexs.update({item.key:index})
            index += 1



    def getValue(self, key):

        index = self.indexs.get(key)

        if None is not index:
            return  self.historys[index]

        return None

    def isItemContains(self, key, value):
        tmpSub = self.getValue(key)
        if None is tmpSub:
            return  False
        return  {value}.issubset(tmpSub)

    def addValue(self, key, value):

        index = self.indexs.get(key)

        if None is not index:
            tmpSub = self.historys[index]
            tmpSub.update({value})
            self.historys[index] = tmpSub
        else:
            tmpSub = set()
            tmpSub.update({value})
            self.indexs.update({key:len(self.indexs)})
            self.historys.append(tmpSub)

        self.update(key)

    def update(self, key):
        tmpSub = self.getValue(key)
        valueFormat = ''
        for item in tmpSub:
            if not len(valueFormat) == 0:
                valueFormat += ';'

            valueFormat += item

        ORMManager.updateOrInsert(CrawlerHistory(key, valueFormat))
