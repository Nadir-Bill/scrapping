# -*- coding: utf-8 -*-
"""
Created on Wed May  6 16:18:27 2020

@author: Mehri
"""

#from __future__ import unicode_literals
from scrapy import Spider, Request
from ..items import AssetscpiItem
from mongoengine import connect
from mongoengine import *  
import scrapy
from datetime import datetime
import dateparser
from scrapy_splash import SplashRequest


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
    isinCode = StringField() 
    name = StringField(required=True )
    url = StringField()
    meta = {'collection': 'scpiConfig'} 
    
class stackSCPI(BaseDocument):
    isinCode = StringField()
    name = StringField()
    typeScpi = StringField()
    categoryScpi = StringField()
    CapitalScpi = StringField()
    creationDate = StringField()
    aumEur = StringField()
    manageSpaceScpi = StringField() 
    nbRentScpi  = StringField()
    priceMScpi = StringField()
    AveragePriceScpi = StringField()
    freqDivScpi = StringField()
    redemPriceScpi = StringField() 
    rateDistribScpi = StringField() 
    evolDiv5YScpi = StringField()
    evolPrice5YScpi = StringField()
    priceYTDScpi = StringField()
    tri10YScpi = StringField()
    admisFees = StringField()
    savingFees = StringField()
    ManagFees = StringField()
    companyName = StringField()
    meta = {'collection': 'stackSCPI'} 

       
    
class AssetScpiData(Spider):
    name = 'assetSCPI'
    allowed_domains = ['www.primaliance.com']
    
    connect(
        db='dataManager',
        username='nikomitrichev',
        password='vehcirtimokin',
        host='mongodb://ec2-18-194-121-142.eu-central-1.compute.amazonaws.com/',
        port=27017
        )   
    
    def start_requests(self):
        yield SplashRequest(
                url='https://www.primaliance.com/scpi-de-rendement/',
                callback=self.parse)  

    url_patrimoine = '/patrimoine'
    url_marche = '/marche-des-parts'
    url_ratio = '/ratios-avances'
    def parse(self, response):
        
        d = scpiConfig.objects()
        for scpi in d:
            item = AssetscpiItem()
            scpi_url = self.start_urls[0] + scpi.url
        

            req = scrapy.Request(scpi_url, callback = self.parseSynthese, meta={'url':scpi_url, 'name':scpi.name, 'isinCode':scpi.isinCode, 'itemd':item})

            yield req
            
            
            
    def parsePatrimoine(self, response):
        
        item= response.meta['itemd']
        item['manageSpaceScpi'] = response.xpath('//div[@class="bl left w45  mtm "]//div[@class="bl left w100 data_lines mtm txtblue txt13"]/div[b[contains(.,"Surface gérée")]]/span/text()').extract_first()
        item['nbRentScpi'] = response.xpath('//div[@class="bl left w45  mtm "]//div[@class="bl left w100 data_lines mtm txtblue txt13"]/div[b[contains(.,"Nombre de locataires")]]/span/text()').extract_first()
        item['priceMScpi'] = response.xpath('//div[@class="bl left w45  mtm "]//div[@class="bl left w100 data_lines mtm txtblue txt13"]/div[b[contains(.,"Prix moyen au m2")]]/span/text()').extract_first()
        item['AveragePriceScpi'] = response.xpath('//div[@class="bl left w45  mtm "]//div[@class="bl left w100 data_lines mtm txtblue txt13"]/div[b[contains(.,"Prix moyen par immeuble")]]/span/text()').extract_first()
        
        item['secteurNames'] = response.xpath('//*[@id="chart_scpi_repartition_sectorielle"]/div/div[1]/div/div/table/tbody/tr/td[1]/text()').extract()
        item['secteurVals'] = response.xpath('//*[@id="chart_scpi_repartition_sectorielle"]/div/div[1]/div/div/table/tbody/tr/td[2]/text()').extract()
        item['geoNames'] = response.xpath('//*[@id="chart_scpi_repartition_geographique"]/div/div[1]/div/div/table/tbody/tr/td[1]/text()').extract()
        item['geoVals'] = response.xpath('//*[@id="chart_scpi_repartition_geographique"]/div/div[1]/div/div/table/tbody/tr/td[2]/text()').extract()
         
         
        
        
        
        scpi_url = response.meta['url']
        isinCode = response.meta['isinCode']
        name = response.meta['name']
        item = response.meta['itemd']
        req = scrapy.Request(scpi_url+self.url_marche, callback = self.parseMarchParts, meta ={'url':scpi_url,'isinCode':isinCode, 'name':name, 'itemd':item })
                    
                    
        yield req

    def parseMarchParts(self, response):

        item = response.meta['itemd']
        item['freqDivScpi'] = response.xpath('//div[@class="fiche_box w100 bl clear left mb2 pam"]//div[@class="bl right  w45 data_lines  mtm txtblue txt13"]/div[b[contains(.,"Versement des dividendes")]]/span/text()').extract_first() 
        scpi_url = response.meta['url']
        isinCode = response.meta['isinCode']
        name = response.meta['name']
        
        req = scrapy.Request(scpi_url+self.url_ratio, callback = self.parseRatioAv, meta ={'url':scpi_url,'isinCode':isinCode, 'name':name, 'itemd':item })
                                    
        yield req
        
    def parseRatioAv(self, response):

        item = response.meta['itemd'] 
        item['rateDistribScpi'] = response.xpath('//div[@class="fiche_box w100 bl clear left mb2 pam"]//div[@class="bl left w50 data_lines mt4 pt4 txtblue txt13"]/div[b[contains(.,"Taux de distribution")]]/span/text()').extract_first()
        item['evolDiv5YScpi'] = response.xpath('//div[@class="fiche_box w100 bl clear left mb2 pam"]//div[@class="bl left w50 data_lines mt4 pt4 txtblue txt13"]/div[b[contains(.,"Evolution du dividende")]]/span/text()').extract_first()
        item['evolPrice5YScpi'] = response.xpath('//div[@class="fiche_box w100 bl clear left mb2 pam"]//div[@class="bl left w50 data_lines mt4 pt4 txtblue txt13"]/div[b[contains(.,"Evolution du prix sur 5 ans")]]/span/text()').extract_first()
        item['priceYTDScpi'] = response.xpath('//div[@class="fiche_box w100 bl clear left mb2 pam"]//div[@class="bl left w50 data_lines mt4 pt4 txtblue txt13"]/div[b[contains(.,"Evolution du prix depuis 01/01")]]/span/text()').extract_first()
        item['tri10YScpi'] = response.xpath('//div[@class="fiche_box w100 bl clear left mb2 pam"]//div[@class="bl left w50 data_lines mt4 pt4 txtblue txt13"]/div[b[contains(.,"TRI sur 10 ans")]]/span/text()').extract_first()
        item['name'] = response.meta['name']
        item['isinCode'] = response.meta['isinCode']
        item['assetType'] = "scpi"
        item['category'] = "Alternative"
        item['subCategory'] = "Real Estate"
        item['categoryCode'] = "Scpi"
        
        
        yield item
        
    def parseSynthese(self, response):
                       
        item = response.meta['itemd']
        item['typeScpi'] = response.xpath('//div[@class="new_blk_blue_bg"]//div[@class="bl left w45 data_lines mtm txtblue txt13 scpi_chiffre_cles"]/div[b[contains(.,"Type")]]/span/text()').extract_first()
        item['categoryScpi'] = response.xpath('//div[@class="new_blk_blue_bg"]//div[@class="bl left w45 data_lines mtm txtblue txt13 scpi_chiffre_cles"]/div[b[contains(.,"Catégorie")]]/span/text()').extract_first() 
        item['capitalScpi'] = response.xpath('//div[@class="new_blk_blue_bg"]//div[@class="bl left w45 data_lines mtm txtblue txt13 scpi_chiffre_cles"]/div[b[contains(.,"Capital")]]/span/text()').extract_first()
        item['creationDate'] = dateparser.parse('01'+response.xpath('//div[@class="new_blk_blue_bg"]//div[@class="bl left w45 data_lines mtm txtblue txt13 scpi_chiffre_cles"]/div[b[contains(.,"Création")]]/span/text()').extract_first()).date()
        item['aumEur'] = response.xpath('//div[@class="new_blk_blue_bg"]//div[@class="bl left w45 data_lines mtm txtblue txt13 scpi_chiffre_cles"]/div[b[contains(.,"Capitalisation")]]/span/text()').extract_first()
        item['rateOccupScpi'] = response.xpath('//div[@class="new_blk_blue_bg"]//div[@class="bl right  w45 data_lines  mtm txtblue txt13 scpi_chiffre_cles"]/div[b[contains(.,"Tx d\'occupation financier")]]/span/text()').extract_first()
        item['nbAssetScpi'] = response.xpath('//div[@class="new_blk_blue_bg"]//div[@class="bl right  w45 data_lines  mtm txtblue txt13 scpi_chiffre_cles"]/div[b[contains(.,"Nombre d\'immeubles")]]/span/text()').extract_first()
        item['admisFees'] = response.xpath('//div[@class="w50 left mt3 scpi_frais"]//div[@class="new_blk_grey_bg s-frais"]/div[b[contains(.,"Frais de souscription TTC")]]/span/text()').extract_first()
        item['savingFees'] = response.xpath('//div[@class="w50 left mt3 scpi_frais"]//div[@class="new_blk_grey_bg s-frais"]/div[b[contains(.,"Droits d\'enregistrement")]]/span/text()').extract_first()
        item['ManagFees'] = response.xpath('//div[@class="w50 left mt3 scpi_frais"]//div[@class="new_blk_grey_bg s-frais"]/div[b[contains(.,"Frais de gestion (% Capitalisation)")]]/span/text()').extract_first()
        item['companyName'] = response.xpath('//div[@class="w50 left mt3 scpi_gestion"]//div[@class="new_blk_grey_bg s-sg"]//a[@class="txt13 txtbold txtblue"]/span/text()').extract_first()
        item['tauxDistribution'] = response.xpath('//div[@class="bl left w50 mam fv_header_left"]//*[@class="bl left clear txtgrey txt36 txtlight" and contains(.,"%")]/text()').extract_first()
        item['annee'] = response.xpath('//div[@class="bl left w50 mam fv_header_left"]//*[contains(.,"Taux de distribution")]/text()').extract_first()
        item['prix'] = response.xpath('//div[@class="bl left w50 mam fv_header_left"]//span[@class="bl left clear txtgrey txt36 txtlight" and position() = 1]/text()').extract_first().split(',')[0]
        item['datePrix'] = response.xpath('//div[@class="bl left w50 mam fv_header_left"]//*[contains(.,"Prix acquéreur")]/text()').extract_first()
        scpi_url = response.meta['url']
        isinCode = response.meta['isinCode']
        name = response.meta['name']
        

        ###############################################
        

         
        """ for assetHistoric """
        item['laDate'] = response.xpath('//table[@class="w100 left mtm evo_performance en_desktop"]//tr[1]/td/b/text()').extract()
        item['value'] = response.xpath('//div[@class="new_blk_grey_bg"]//table[@class="w100 left mtm evo_performance en_desktop" and position() = 1]//tr[2]//td/text()').extract()
        """ for divHistoric """
        item['exDivDate'] = datetime.strptime('01/01/'+ str(response.xpath('//table[@class="w100 left mtm evo_performance en_desktop"]//tr[1]/td[position() = last()]/b/text()').extract_first()), '%m/%d/%Y')
        item['divUnadjGross'] = response.xpath('//div[@class="new_blk_grey_bg"]//table[@class="w100 left mtm evo_performance en_desktop"and position()=1]//tr[3]//td/text()').extract()
        
        req = scrapy.Request(scpi_url+self.url_patrimoine, callback = self.parsePatrimoine, meta ={'url':scpi_url,'isinCode':isinCode, 'name':name , 'itemd':item})
                                      
        yield req

        