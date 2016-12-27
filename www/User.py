from orm import Model,StringField,IntegerField

class User(Model):
    __table__ = 'users'

    id = IntegerField(primary_key=true)
    name = StringField()
