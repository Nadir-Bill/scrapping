# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.pipelines.files import FilesPipeline
import os
from scrapy import Request

from datetime import date 
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

class PdfrecoverPipeline(FilesPipeline):
    

    
    def get_media_requests(self, item, info):
        return [Request(x, meta={'fileName':item.get('file_name')}) for x in item.get(self.files_urls_field, [])]

    def file_path(self, request, response=None, info=None):
        connect(
            db='dataManager',
            username='nikomitrichev',
            password='vehcirtimokin',
            host='mongodb://ec2-18-194-121-142.eu-central-1.compute.amazonaws.com/',
            port=27017
            )        
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
            
        media_ext = os.path.splitext(request.url)[1]
        stri = ""+request.url.split('documentation/')[1].split(media_ext)[0].split('-')[0].replace(' ','').replace('_', ' ')

        i = scpiConfig.objects(name=stri)
        stri = i[0].isinCode   
        pdfName = request.meta['fileName']
        pathN = "other/"
        if (str(date.today().year-1) in pdfName) and '4T' in pdfName:
            pdfName = 'REPORTING'
            pathN = 'pdf_Files/'
        elif 'Statuts' == request.meta['fileName']:
            pdfName = 'PROSPECTUS'
            pathN = 'pdf_Files/'
        elif 'DIC' in request.meta['fileName']:
            pdfName = 'DICI'
            pathN = 'pdf_Files/'
        else:
            pathN = 'other/'
        
        return pathN+stri+'/%s%s' % (stri+'-'+pdfName, media_ext)

 
    