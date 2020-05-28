# -*- coding: utf-8 -*-
"""
Created on Wed May 13 17:23:15 2020

@author: Mehri
"""

from scrapy import Spider
from ..items import AssetscpiItem

class SpiderSCPI(Spider):
    name = 'spiderSCPI'
    allowed_domains = ['www.primaliance.com']
    start_urls = ['https://www.primaliance.com/scpi-de-rendement']
    l={}
    
    def parse(self, response):
        #rows = response.css('tbody.tablesorterRight tr')
        rows = response.xpath('//*[@class ="w100 left mtm evo_performance en_desktop"]//tbody/tr')
        for row in rows:
            item = dict()
            #item['name'] = row.css('td a::text').get()
            #item['url'] = row.css('td a').xpath('@href').get()
            #item['date'] = row.css('b::text')
            item['date'] = row.xpath('td[2]').extract_first()
            yield item