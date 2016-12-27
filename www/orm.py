#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'zhangcheng'

import asyncio,aiomysql,logging
logging.basicConfig(level=logging.INFO)

def log(sql,args=()):
    logging.info('SQL:%s' % sql)

#创建全局的数据库连接池
#为了简化并更好地标识异步IO，从Python 3.5开始引入了新的语法async和await，
#可以让coroutine的代码更简洁易读。
async def create_pool(loop,**kw):
    logging.info('create database connection pool..')
    global __pool
    __pool = await aiomysql.create_pool(
        host = kw.get('host','localhost'),
        port = kw.get('port',3306),
        user = kw['user'],
        password = kw['password'],
        db = kw['db'],
        charset = kw.get('charset','utf8'),
        autocommit = kw.get('autocommit',True),
        maxsize = kw.get('maxsize',10),
        minsize = kw.get('minsize',1),
        loop = loop
    )

async def select(sql,args,size=None):
    log(sql,args)
    global __pool
    async with __pool.get() as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?','%s'),args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs

async def execute(sql,args):
    log(sql)
    global __pool
    async with __pool.get() as conn:
        try:
            cur = await conn.cursor(aiomysql.DictCursor)
            await cur.execute(sql.replace('?','%s'),args or ())
            affected = cur.rowcount
            await cur.close()
        except BaseException as e:
           raise e
        return affected

class ModelMetaclass(type):
    def __new__(cls,name,bases,attrs):
        #排除掉对Model类本身的修改；
        if name=='Model':
            return type.__new__(cls,name,bases,attrs)
        #获取table名称
        tableName = attrs.get('__table__',None) or name
        logging.info('found model: %s (table: %s)' % (name,tableName))
        # 获取所有的Field和主键名
        mappings = dict()
        fields = []
        primaryKey = None
        
        for k,v in attrs.items():
            if isinstance(v,Field):
                logging.info('found mappings: %s ==> %s' % (k,v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('primary key is not found!')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))#生成['`name`','`age`',...]的序列
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey
        attrs['__fields__'] = fields
        attrs['__select__'] = 'select `%s` %s from `%s`' % (primaryKey,','.join(escaped_fields),tableName)
        attrs['__insert__'] = 'insert into `%s` (%s,`%s`) values (%s)' % (tableName,','.join(escaped_fields),primaryKey,create_args_string(len(fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s` = ?' % (tableName,','.join(map(lambda f : '`%s` =  ?' % (mappings.get(f).name or f),fields)),primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s` = ?' % (tableName,primaryKey)
        return type.__new__(cls, name, bases, attrs)

class Model(dict,metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self,key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)        

    def __setattr__(self,key,value):
        self[key] = value

    def getValue(self,key):
        return getattr(self,key,None)

    def getValueOrDefault(self,key):
        value = getattr(self,key,None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s : %s' % (key,str(value)))
                setattr(self,key,value)
        return value

    async def save(self):
        print(self)
        args = list(map(self.getValueOrDefault,self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__,args)
        if rows != 1:
            logging.warn('fialed to insert record: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValueOrDefault,self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__update__,args)
        if rows != 1:
            logging.warn('fialed to update record: affected rows: %s' % rows)

    @classmethod
    async def findAll(cls,where=None,args=None,**kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy',None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit',None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit,int):
                sql.append('?')
                args.append(limit)
            if isinstance(limit,tuple) and len(limit) == 2:
                sql.append('?,?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        print(sql)
        rs = await select(' '.join(sql),args)
        return [cls(**r) for r in rs]

    # 普通的方法，第一个参数需要是self，它表示一个具体的实例本身。
    # 如果用了staticmethod，那么就可以无视这个self，而将这个方法当成一个普通的函数使用。
    # 而对于classmethod，它的第一个参数不是self，是cls，它表示这个类本身。     
    @classmethod  
    async def find(cls,pk):
        rs = await select('%s where `%s` = ?' % (cls.__select__,cls.__primary_key__),pk,1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    @classmethod
    async def findNumber(cls,selectField,where=None,args=None):
        ' find number by select and where. '
        sql = ['select %s __num__ from `%s`' % (selectField,cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql),args,1)
        if len(rs) == 0:
            return None
        return rs[0]['__num__']

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__,args)
        if rows != 1:
            logging.warn('fialed to delete record: affected rows: %s' % rows)
        return rows

class Field(object):
    def __init__(self, name,primary_key,default,column_type):
        self.name = name
        self.primary_key = primary_key
        self.default = default
        self.column_type = column_type

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__,self.column_type,self.name)

class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name,primary_key, default,ddl)

class IntegerField(Field):
    def __init__(self, name=None,primary_key=False,default=0,ddl='bigint'):
        super().__init__(name,primary_key,default,ddl)

class BooleanField(Field):
    def __init__(self,name=None,default=False):
        super(BooleanField,self).__init__(name,False,default,'boolean')

class FloatField(Field):
    def __init__(self, name=None,primary_key=False,default=0.0):
        super(FloatField, self).__init__(name,primary_key,default,'real')
        
class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, False, default,'text')
        
        
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)

@asyncio.coroutine
def destory_pool():
    global __pool
    if __pool is not None :
        __pool.close()
        yield from __pool.wait_closed()
    
# class User(Model):
#     # 定义类的属性到列的映射：
#     id = IntegerField('id',True)
#     name = StringField('name')
#     email = StringField('email')
#     password = StringField('password')

#创建实例
# async def test():
#     datasource = {'user':'root','password':'root','db':'test'}
#     await create_pool(loop=loop,**datasource)
#     # await u.save()
#     # r = await User.find('12345')
#     # r = await User.findNumber('count(*)','id = ?',(12345,))
#     # r = await u.remove()
#     #await u.update()
#     r = await User.findAll('id = ?',[12345],orderBy='id',limit=(0,2))
#     print(r) 
#     await destory_pool()

# if __name__ == '__main__':
#     # 创建一个实例：
#     u = User(id=12345, name='zhangcheng', email='test@orm.org', password='my-pwd')
#     print('-------create finish-----------')
#     #创建异步事件的句柄
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(test())
#     loop.close()
#     # if loop.is_closed():
#     #     sys.exit(0)
