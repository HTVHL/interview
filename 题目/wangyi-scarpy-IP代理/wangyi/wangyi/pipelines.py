# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
import csv

class WangyiPipeline:
    def open_spider(self, spider):
        if spider.name == "wy":
            with open("file.csv", "a", encoding="utf-8", newline="") as f:
                csv_writer = csv.writer(f)
                name = ['name', 'postTypeFullName','reqEducationName', 'workPlaceNameList']
                csv_writer.writerow(name)
            # self.mongo_obj = pymongo.MongoClient()
            # self.mongo_client = self.mongo_obj["py_spider"]["wyzp_data"]

    def process_item(self, item, spider):
        if spider.name == "wy":
            with open("file.csv", "a", encoding="utf-8", newline="") as self.f:
                csv_writer = csv.writer(self.f)
                z = [item["name"],item["postTypeFullName"],item["reqEducationName"],item["workPlaceNameList"]]
                csv_writer.writerow(z)
                print("数据保存成功")

    def close_spider(self, spider):
        if spider.name == "wy":
            self.f.close()

