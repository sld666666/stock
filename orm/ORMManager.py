
import  orm.base

class ORMManager():

    @staticmethod
    def insert(obj):
        session = orm.base.DBSession()
        session.add(obj)
        session.commit()
        session.close()

    @staticmethod
    def inserts(objs):
        session = orm.base.DBSession()
        for obj in objs:
            session.add(obj)

        session.commit()
        session.close()

    @staticmethod
    def updateOrInsert(obj):
        session = orm.base.DBSession()
        session.merge(obj)
        session.commit()
        session.close()

    @staticmethod
    def queryOne(instance, conditions = None):
        session = orm.base.DBSession()
        try :
            if conditions is None:
                object = session.query(instance).one()
            else:
                object = session.query(instance).filter(conditions).one()
        except Exception  as e:
            object = None

        session.close()
        return  object

    @staticmethod
    def queryAll(instance, conditions = None):
        session = orm.base.DBSession()
        if conditions is  None:
            object = session.query(instance).all()
        else :
            object = session.query(instance).filter(conditions).all()
        session.close()
        return  object

    @staticmethod
    def delete(instance, conditions):
        session = orm.base.DBSession()
        object = session.delete(instance).filter(conditions).one()
        session.close()
        return  object

    @staticmethod
    def getConnection():
        return orm.base.engine