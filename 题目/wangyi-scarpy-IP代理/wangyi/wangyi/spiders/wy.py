import scrapy
from scrapy import cmdline
from scrapy.http import HtmlResponse,JsonRequest

class WySpider(scrapy.Spider):
    name = 'wy'
    allowed_domains = ['hr.163.com']
    start_urls = ['http://hr.163.com/']

    def start_requests(self):
        url = "https://hr.163.com/api/hr163/position/queryPage"


        for page in range(1,214):
            payload = {
                'currentPage': page,
                'pageSize': 10,
                'workType': "0"
            }

            yield JsonRequest(url=url,data=payload,dont_filter=False)




    def parse(self, response:HtmlResponse,**kwargs):
        data_list = response.json()["data"]["list"]

        for data in data_list:
            name = data["name"]
            postTypeFullName = data["postTypeFullName"]
            reqEducationName = data["reqEducationName"]
            workPlaceNameList = data["workPlaceNameList"][0]

            yield {
                "name":name,
                "postTypeFullName":postTypeFullName,
                "reqEducationName":reqEducationName,
                "workPlaceNameList":workPlaceNameList
            }





if __name__ == '__main__':
    cmdline.execute('scrapy crawl wy --loglevel=ERROR'.split())