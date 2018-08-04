import time
import json
import redis
import requests


class RedisQueue(object):
    """Simple Queue with Redis Backend"""

    def __init__(self, name, namespace, redis_conn):
        self.__db = redis_conn
        self.key = '%s:%s' % (namespace, name)

    def qsize(self):
        return self.__db.llen(self.key)

    def empty(self):
        return self.qsize() == 0

    def put(self, item):
        self.__db.rpush(self.key, item)

    def get(self, block=True, timeout=None):
        if block:
            item = self.__db.blpop(self.key, timeout=timeout)
        else:
            item = self.__db.lpop(self.key)
        if item:
            item = item[1]
        return item

    def get_nowait(self):
        return self.get(False)


class SpiderConfig:
    ProxyURL = 'http://http.tiqu.qingjuhe.cn/getip?num=1&type=2&pro=&city=0&yys=0&port=1&pack=14621&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=0&regions='
    RedisAddress = '192.168.100.94'
    RedisPort = '7001'


class Spider(object):
    def __init__(self):
        super(Spider, self).__init__()
        self.redis_conn = redis.Redis(host=SpiderConfig.RedisAddress, port=SpiderConfig.RedisPort, db=7)
        self.Qr = RedisQueue(name='new_task', namespace='myQueue', redis_conn=self.redis_conn)  # 保存队列
        self.Qi = RedisQueue(name='result_shop_c', namespace='myQueue', redis_conn=self.redis_conn)  # 保存队列
        self.ero_list = RedisQueue(name='ero', namespace='myQueue', redis_conn=self.redis_conn)  # 错误队列

        self.base_url = 'https://www.amap.com/detail/get/detail'
        self.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'amapuuid': 'c51c0b45-599b-457d-bbff-15d7e747fa34',
            'Connection': 'keep-alive',
            'Host': 'www.amap.com',
            'Referer': 'https://www.amap.com/search?query=%E5%9C%B0%E9%93%81&city=110000&geoobj=116.211726%7C39.700019%7C116.82284%7C40.118217&zoom=11',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
        }

    def run(self):
        while not self.Qr.empty():
            task = str(self.Qr.get(), encoding='utf-8')
            payload = {'id': task}
            detail_json = requests.get(self.base_url, payload, headers=self.headers)

            try:
                if detail_json.json()['data']['base']['poiid'] == task:
                    self.Qi.put(detail_json.text)
                else:
                    self.ero_list.put(task)
            except:
                self.ero_list.put(task)

        time.sleep(2)


if __name__ == '__main__':
    Spider().run()
    {"query_type": "TQUERY", "pagesize": "20", "pagenum": "1", "qii": "true", "cluster_state": "5", "need_utd": "true",
     "utd_sceneid": "1000", "div": "PC1000", "addr_poi_merge": "true", "is_classify": "true", "zoom": "12",
     "city": "810000", "geoobj": "113.970426|22.296128|114.165777|22.548118", "keywords": "地铁"}