# -*- coding: utf-8 -*-
import requests
from queue import Queue
import threading
import pymongo
from lxml import etree
from fake_useragent import UserAgent
import hashlib
import redis
import time
import csv


class DDW:
    # mongo_client = pymongo.MongoClient()
    # collection = mongo_client['py_spider']['dangdang_shop_multithreading']
    def __init__(self):
        with open("file.csv", "a", encoding="utf-8", newline="") as f:
            csv_writer = csv.writer(f)
            name = ['text', 'title', 'href']
            csv_writer.writerow(name)
        self.redis_client = redis.Redis()
        self.url = "https://search.dangdang.com/?key=python&act=input&page_index={}"
        self.headers = {
            "Referer":"https://search.dangdang.com/?key=python&act=input&page_index=10",
            "User-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
        }
        self.url_queue = Queue()#网址
        self.data_text = Queue()#未处理数据
        self.data_disposes_accomplish = Queue()#处理完成数据
        self.data_repetition_inspect = Queue()#重复检查


    #设置要爬取网址
    def hl_url_info(self):
        for page in range(1,101):
            url_new = self.url.format(page)
            self.url_queue.put(url_new)

    # ip代理
    def hl_proxy_pool(self):
        api_url = "https://dps.kdlapi.com/api/getdps/?secret_id=oaclda9tzg2d2hbk4e20&signature=pzh8sha2zjdndamoqw2hprdchc1sqb41&num=1&pt=1&format=text&sep=1"
        # 获取API接口返回的代理IP
        proxy_ip = requests.get(api_url).text
        # 用户名密码认证(私密代理/独享代理)
        self.headers["User-agent"] = UserAgent().random
        username = "d2791151583"
        password = "skfg1ac6"
        proxies = {
            "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": proxy_ip},
            "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": proxy_ip}
        }
        return proxies


    #请求网址
    def hl_get_work_info(self):
        proxies = self.hl_proxy_pool()
        while True:
            url = self.url_queue.get()
            response = requests.get(url, headers=self.headers, proxies=proxies,timeout=5)
            #判断请求是否正常
            if response.status_code == 200:
                self.data_text.put(response.text)
                self.url_queue.task_done()
            else:
                print("不是200是："+str(response.status_code))
                self.url_queue.put(url)
                proxies = self.hl_proxy_pool()

    #处理获取数据
    def hl_data_disposes(self):
        while True:
            data = self.data_text.get()
            html = etree.HTML(data)
            title = html.xpath("//ul[@class='bigimg']/li")
            for i in title:
                item = dict()
                text = i.xpath("./p[2]/text()")
                item["text"] = text[0]if text else '空'
                item["title"] = i.xpath("./p[1]/a/@title")[0]
                item["href"] = i.xpath("./p[1]/a/@href")[0]
                self.data_disposes_accomplish.put(item)
            self.data_text.task_done()

    #去重
    def hl_repetition_data_disposes(self):
        while True:
            data = self.data_disposes_accomplish.get()
            data_md5 = hashlib.md5(str(data).encode('utf-8')).hexdigest()
            restle = self.redis_client.sadd('movie:filter', data_md5)
            if restle:
                self.data_repetition_inspect.put(data)
            else:
                print("数据重复以剔除")
            self.data_disposes_accomplish.task_done()

    #保存数据
    def hl_mongo_save(self):
        # mongodb保存
        # while True:
        #     try:
        #         detail = self.data_repetition_inspect.get()
        #         print("保存数据成功")
        #         self.collection.insert_one(detail)
        #         self.data_repetition_inspect.task_done()
        #     except Exception as e:
        #         print("保存数据错误",e)
        while True:
            with open("file.csv", "a", encoding="utf-8", newline="") as f:
                csv_writer = csv.writer(f)
                detail = self.data_repetition_inspect.get()
                z = [detail["title"],detail["text"],detail["href"]]
                csv_writer.writerow(z)
                print("写入数据成功")
            self.data_repetition_inspect.task_done()






    #多线程启动
    def hl_main(self):
        threading_list = list()
        self.hl_url_info()

        for i in range(5):
            t_get = threading.Thread(target=self.hl_get_work_info)
            threading_list.append(t_get)

        for i in range(5):
            t_disposes = threading.Thread(target=self.hl_data_disposes)
            threading_list.append(t_disposes)

        for i in range(2):
            t_repetition = threading.Thread(target=self.hl_repetition_data_disposes)
            threading_list.append(t_repetition)

        t_save = threading.Thread(target=self.hl_mongo_save)
        threading_list.append(t_save)

        for i in threading_list:
            i.daemon = True
            i.start()

        time.sleep(1)
        for q in [self.url_queue,self.data_repetition_inspect,self.data_disposes_accomplish,self.data_text]:
            q.join()


if __name__ == '__main__':
    ddw_start = DDW()
    ddw_start.hl_main()







