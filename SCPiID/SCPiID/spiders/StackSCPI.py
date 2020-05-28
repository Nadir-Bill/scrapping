


import scrapy
from ..items import ScpiidItem

class scrapSCPI(scrapy.Spider):
    name ="SCPI"
    allowed_domains = ['www.primaliance.com']
    start_urls = ['https://www.primaliance.com/scpi-de-rendement/']
    
    
    
    def parse(self, response):
        rows = response.css('tbody.tablesorterRight tr') 
        nbr = 1
        isin = "CRISTSCPI"
        for row in rows:            
            item = ScpiidItem()
            item['name'] = row.css('td a::text').get()
            item['url'] = row.css('td a').xpath('@href').get()#.split('https://www.primaliance.com/scpi-de-rendement/')[1]
            if(len(isin+str(nbr)) < 11 ):
                item['isinCode'] = isin+"00"+str(nbr)
            elif(len(isin+str(nbr)) < 12):
                item['isinCode'] = isin+"0"+str(nbr)
            else:
                item['isinCode'] = isin + str(nbr)
            nbr = nbr + 1
            yield item    
            