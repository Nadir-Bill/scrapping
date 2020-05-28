# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from mongoengine import connect
from mongoengine import *
import json
import dateparser
import re

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
    assetType = StringField()
    category =StringField()
    subCategory = StringField()
    categoryCode = StringField()
    typeScpi = StringField()
    categoryScpi = StringField()
    capitalScpi = StringField()
    creationDate = DateTimeField()
    aumEur = DecimalField()
    manageSpaceScpi = StringField() 
    nbRentScpi  = DecimalField()
    priceMScpi = StringField()
    AveragePriceScpi = DecimalField()
    freqDivScpi = StringField()
    redemPriceScpi = StringField() 
    rateDistribScpi = DecimalField() 
    evolDiv5YScpi = DecimalField()
    evolPrice5YScpi = DecimalField()
    priceYTDScpi = DecimalField()
    tri10YScpi = DecimalField()
    admisFees = StringField()
    savingFees = DecimalField()
    ManagFees = DecimalField()
    companyName = StringField()
    meta = {'collection': 'stackSCPI'} 
    
class dataSCPI(BaseDocument):
    typeScpi = StringField()
    categoryScpi = StringField()
    capitalScpi = StringField()
    manageSpaceScpi = StringField()
    nbRentScpi = DecimalField()
    priceMScpi = StringField()
    AveragePriceScpi = DecimalField()
    freqDivScpi = StringField()
    rateDistribScpi = DecimalField()
    evolDiv5YScpi = DecimalField()
    evolPrice5YScpi = DecimalField()
    priceYTDScpi = DecimalField()
    tri10YScpi = DecimalField()   
    tauxDistribution = StringField()
    annee = DateField()
    prix = DecimalField()
    datePrix = DateField()
    
    meta = {'collection': 'dataSCPI'}
    
class valTab(EmbeddedDocument):
    date = DateTimeField()
    value = DecimalField()
    
class divHistoric(BaseDocument):
    isinCode = StringField()
    exDivDate = DateTimeField()
    divUnadjGross = DecimalField()
    currency = StringField()
    
    meta = {'collection': 'divHistoric'}
    
class assetHistoric(BaseDocument):
    isinCode = StringField()
    value = DecimalField()
    date = DateTimeField()
    
    meta = {'collection': 'assetHistoric'}
    



     
class SectorialEmb(EmbeddedDocument):
     name = StringField()
     value = StringField()
 
class GeographicEmb(EmbeddedDocument):
    name = StringField()
    value = StringField()
     
class assetDecomposition(BaseDocument):
     
    isinCode = StringField()
    sectorial = EmbeddedDocumentListField(SectorialEmb)
    geographic = EmbeddedDocumentListField(GeographicEmb) 
     
    meta = {'collection': 'assetDecomposition'}
     
     
    
    
    
    
    
    
    
    
###############################################################################    

class AssetscpiPipeline(object):
        
    def __init__(self):
        connect(
                db='dataManager',
                username='nikomitrichev',
                password='vehcirtimokin',
                host='mongodb://ec2-18-194-121-142.eu-central-1.compute.amazonaws.com/',
                port=27017
                )

    def process_item(self, item, spider):
        
        #######################################################################
        """Table StackSCPI"""
        data = stackSCPI()
        data.isinCode = item['isinCode']
        data.name = item['name']
        data.typeScpi = item['typeScpi']
        data.categoryScpi = item['categoryScpi']
        data.capitalScpi = item['capitalScpi']
        data.creationDate = item['creationDate']
        if item['aumEur'].split('%')[0].strip().replace(' ','').replace('M','').replace(',','.') == '-':
            data.aumEur = 0
        else:
            data.aumEur = float(item['aumEur'].split('%')[0].strip().replace(' ','').replace('M','').replace('€','').replace(',','.'))/ 100
        data.manageSpaceScpi = item['manageSpaceScpi'] 
        if item['nbRentScpi'].strip() == '-':
            data.nbRentScpi = 0
        else:
            data.nbRentScpi = int(item['nbRentScpi'].replace(' ',''))
        data.priceMScpi = item['priceMScpi']
        if item['AveragePriceScpi'].strip().split('€')[0].replace(' ','').replace(',','.') == '-':
            data.AveragePriceScpi = 0
        else: 
            data.AveragePriceScpi = float(item['AveragePriceScpi'].strip().split('€')[0].replace(' ','').replace(',','.'))
        data.freqDivScpi = item['freqDivScpi']
        if item['rateDistribScpi'].strip().split('%')[0].replace(',','.') == '-':
            data.rateDistribScpi = 0
        else:    
            data.rateDistribScpi = float(item['rateDistribScpi'].strip().split('%')[0].replace(',','.')) / 100
        if item['evolDiv5YScpi'].split('%')[0].replace(' ','').strip().replace(',','.') == '-':
            data.evolDiv5YScpi = 0
        else:    
            data.evolDiv5YScpi = float(item['evolDiv5YScpi'].strip().split('%')[0].replace(' ','').replace(',','.')) / 100
        if item['evolPrice5YScpi'].split('%')[0].replace(' ','').strip().replace(',','.') == '-':
            data.evolPrice5YScpi = 0
        else:
            data.evolPrice5YScpi = float(item['evolPrice5YScpi'].strip().split('%')[0].replace(' ','').replace(',','.')) / 100
        if item['priceYTDScpi'].strip().split('%')[0].replace(' ','').replace(',','.') == '-':
            data.priceYTDScpi = 0
        else:    
            data.priceYTDScpi = float(item['priceYTDScpi'].strip().split('%')[0].replace(' ','').replace(',','.'))/ 100
        if item['tri10YScpi'].split('%')[0].replace(' ','').strip().replace(',','.') == '-':
            data.tri10YScpi = 0
        else:
            data.tri10YScpi =  float(item['tri10YScpi'].split('%')[0].strip().replace(' ','').replace(',','.'))/ 100
        data.savingFees = float(item['savingFees'].split('%')[0].strip().replace(' ','').replace(',','.'))/ 100
        if item['ManagFees'].split('%')[0].strip().replace(' ','').replace(',','.') == '-':
            data.ManagFees = 0
        else:    
            data.ManagFees = float(item['ManagFees'].split('%')[0].strip().replace(' ','').replace(',','.'))/ 100
        data.companyName = item['companyName']
        data.assetType = item['assetType']
        data.category = item['category']
        data.subCategory = item['subCategory']
        data.categoryCode = item['categoryCode']

        #stackSCPI.objects().insert(data)
        #######################################################################
        
        """ Table dataSCPI """
        
        dataScpI = dataSCPI()
        
        dataScpI.isinCode = item['isinCode']
        dataScpI.typeScpi = item['typeScpi']
        dataScpI.categoryScpi = item['categoryScpi']
        dataScpI.capitalScpi = item['capitalScpi']
        dataScpI.manageSpaceScpi = item['manageSpaceScpi']
        if item['nbRentScpi'].strip().replace(' ','') == '-':
            dataScpI.nbRentScpi = 0
        else:
            dataScpI.nbRentScpi = int(item['nbRentScpi'].replace(' ',''))
        dataScpI.priceMScpi = item['priceMScpi']
        if item['AveragePriceScpi'].strip().split('€')[0].replace(' ','').replace(',','.') == '-':
            dataScpI.AveragePriceScpi = 0
        else: 
            dataScpI.AveragePriceScpi = float(item['AveragePriceScpi'].strip().split('€')[0].replace(' ','').replace(',','.'))
        dataScpI.freqDivScpi = item['freqDivScpi']
        if item['rateDistribScpi'].strip().split('%')[0].replace(',','.') == '-':
            dataScpI.rateDistribScpi = 0
        else:    
            dataScpI.rateDistribScpi = float(item['rateDistribScpi'].strip().split('%')[0].replace(',','.')) / 100
        if item['evolDiv5YScpi'].split('%')[0].replace(' ','').strip().replace(',','.') == '-':
            dataScpI.evolDiv5YScpi = 0
        else:    
            dataScpI.evolDiv5YScpi = float(item['evolDiv5YScpi'].strip().split('%')[0].replace(' ','').replace(',','.')) / 100
        if item['evolPrice5YScpi'].split('%')[0].replace(' ','').strip().replace(',','.') == '-':
            dataScpI.evolPrice5YScpi = 0
        else:
            dataScpI.evolPrice5YScpi = float(item['evolPrice5YScpi'].strip().split('%')[0].replace(' ','').replace(',','.')) / 100
        if item['priceYTDScpi'].strip().split('%')[0].replace(' ','').replace(',','.') == '-':
            dataScpI.priceYTDScpi = 0
        else:    
            dataScpI.priceYTDScpi = float(item['priceYTDScpi'].strip().split('%')[0].replace(' ','').replace(',','.'))/ 100
        if item['tri10YScpi'].split('%')[0].replace(' ','').strip().replace(',','.') == '-':
            dataScpI.tri10YScpi = 0
        else:
            dataScpI.tri10YScpi =  float(item['tri10YScpi'].split('%')[0].strip().replace(' ','').replace(',','.'))/ 100
        
        dataScpI.tauxDistribution = str(item['tauxDistribution']) 
        dataScpI.annee = dateparser.parse('01/01'+re.sub('Taux de distribution ',' ', item['annee']).replace(' ','').replace(':','')).date()
        dataScpI.prix = float(item['prix'])
        dataScpI.datePrix = dateparser.parse(re.sub('Prix acquéreur ',' :', item['datePrix']).split(' :')[1]).date()
        
        #dataSCPI.objects().insert(dataScpI)
        
         #######################################################################
        """ Table divHistoric """
         
       
        
        
        for i, j in zip(item['divUnadjGross'], item['laDate']):
            dataDivHistoric = divHistoric()
            dataDivHistoric.exDivDate = dateparser.parse('01/01/'+j).date()
            dataDivHistoric.isinCode = item['isinCode']
            if i.strip().split('€')[0].replace(',','.') == '':
                dataDivHistoric.divUnadjGross = 0 
            else:
                dataDivHistoric.divUnadjGross = float(i.strip().split('€')[0].replace(',','.'))
       
            dataDivHistoric.currency = 'EUR'
         
            #divHistoric.objects().insert(dataDivHistoric)
         #######################################################################
        """ Table assetHistoric """
         

        for i, j in zip(item['value'], item['laDate']): 
            dataAssetHistoric = assetHistoric()
         
            dataAssetHistoric.date = dateparser.parse('01/01/'+j).date()
            dataAssetHistoric.isinCode = item['isinCode']
            if i.strip().split('€')[0].replace(',','.').replace(' ','') == '':
                dataAssetHistoric.value = 0
            else:    
                dataAssetHistoric.value =float(i.strip().split('€')[0].replace(',','.').replace(' ',''))
            #assetHistoric.objects().insert(dataAssetHistoric)
         ######################################################################
        """ Table assetDecomposition """
         
        dataDecomposition = assetDecomposition()
        
        for i, j in zip(item['secteurNames'], item['secteurVals']):
            dataDecomposition.sectorial.append(SectorialEmb(name = i, value = j))    
       
        for i, j in zip(item['geoNames'], item['geoVals']):    
            dataDecomposition.geographic.append(GeographicEmb(name = i, value = j))
        
        assetDecomposition.objects().insert(dataDecomposition)
         
         
        
        return item