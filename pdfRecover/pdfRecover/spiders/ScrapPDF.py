# -*- coding: utf-8 -*-
"""
Created on Thu May 21 16:28:11 2020

@author: Mehri
"""

from scrapy import Spider   
from scrapy.loader import ItemLoader
from scrapy_splash import SplashRequest

from ..items import PdfrecoverItem




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

class Fill_Spider(Spider):
    name = 'scrapPdf'
    allowed_domains = ['www.primaliance.com']
    connect(
            db='dataManager',
            username='nikomitrichev',
            password='vehcirtimokin',
            host='mongodb://ec2-18-194-121-142.eu-central-1.compute.amazonaws.com/',
            port=27017
            )   
   
        
        
    start_urls = [i.url+'/documentation' for i in scpiConfig.objects()]  
    
# =============================================================================
#     def start_requests(self):
#         yield SplashRequest(
#                 url='https://www.primaliance.com/scpi-de-rendement/32-scpi-acces-valeur-pierre/documentation',
#                 callback=self.parse)
# =============================================================================
    
    def parse(self, response):
        item=PdfrecoverItem()
        for row in response.xpath('//*[@id="content"]/div[2]/div[5]/div/div/h2/a'):
            loader = ItemLoader(item=item, selector=row)
            relative_url = row.xpath('.//@href').extract_first().split('?')[0]
            #item['currUrl'] = 
            print(row.xpath('.//@href').extract_first())
            loader.add_value('file_urls', relative_url)
            loader.add_xpath('file_name','.//text()')
            yield loader.load_item()
        
               
            
            
            
     