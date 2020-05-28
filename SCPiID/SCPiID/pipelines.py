# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


from mongoengine import connect
from mongoengine import *



class User(Document):
    username = StringField(max_length=200, required=True , unique=True)
    meta = {'collection': 'users'}
    
class BaseDocument(Document):
    updatedDate = DateTimeField()
    createdDate = DateTimeField()
    createdBy=ReferenceField(User)
    updatedBy=ReferenceField(User)
    meta = {
            'abstract': True,
            }
    
    
class scpiConfig(BaseDocument):
    isinCode = StringField(required=True )
    name = StringField(required=True )
    url = StringField()
    meta = {'collection': 'scpiConfig'}   
    
    



class ScpiidPipeline(object):

    def __init__(self):
        connect(
                db='dataManager',
                username='nikomitrichev',
                password='vehcirtimokin',
                host='mongodb://ec2-18-194-121-142.eu-central-1.compute.amazonaws.com/',
                port=27017
                )


    def process_item(self, item, spider):
        data = scpiConfig()
        data.isinCode =  item['isinCode']
        data.name = item['name']
        data.url = item['url']
        scpiConfig.objects().insert(data)

        return item
