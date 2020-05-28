# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Item, Field
 

class AssetscpiItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    itemd = Field()
    laDate = Field()
    name = Field()
    isinCode = Field()
    assetType = Field()
    category = Field()
    subCategory = Field()
    categoryCode = Field()
    typeScpi = Field()
    categoryScpi = Field()
    capitalScpi = Field()
    creationDate = Field()
    aumEur = Field()
    manageSpaceScpi = Field()  
    nbRentScpi  = Field()
    priceMScpi = Field()
    AveragePriceScpi = Field()
    rateOccupScpi = Field()
    nbAssetScpi = Field()
    freqDivScpi = Field()
    redemPriceScpi = Field() 
    rateDistribScpi = Field() 
    evolDiv5YScpi = Field()
    evolPrice5YScpi = Field()
    priceYTDScpi = Field()
    tri10YScpi = Field()
    admisFees = Field()
    savingFees = Field()
    ManagFees = Field()
    companyName = Field()
    divUnadjGross = Field()
    currency = Field()
    exDivDate = Field()
    value = Field()
    tauxDistribution = Field()
    annee = Field()
    prix = Field()
    datePrix = Field()
    secteurNames = Field()
    secteurVals = Field()
    geoNames = Field()
    geoVals = Field()
    
    
    

