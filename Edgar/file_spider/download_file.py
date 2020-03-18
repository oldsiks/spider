
"""
    功能
"""
import json
import logging
import time
import redis
import requests
from fake_useragent import UserAgent

logging.captureWarnings(True)
location = r'C:\Users\EricI\Envs\spider\Lib\site-packages\AGpath.json'
ua = UserAgent(path=location)


class Download(object):
    """
    从redis当中获取消息，而后对文件进行下载！
    """

    def __init__(self):
        self.rc = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True, db=0)

    def download_to_file(self, year, qtr, _url, _fname, unique_key, loaded):
        """
        本函数用于下载文件！
        :param year: 年
        :param qtr: 季度
        :param _url: 地址
        :param _fname: 文件名称
        :param unique_key: 唯一标识
        :param loaded: 已下载的文件
        :return:
        """
        number_of_tries = 3
        sleep_time = 2  # Note sleep time accumulates according to err

        for i in range(1, number_of_tries + 1):
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                # 'Connection': 'keep-alive',
                'Host': 'www.sec.gov',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': ua.random
            }

            try:
                """
                被注释掉的这部分，原本是添加代理的，后来因为免费代理不好使，于是取消了！
                我这里写的方法是根据时间来换ip的，更好的方法应该是当请求失败之后立刻换ip！
                """
                # if time.time() - t1 <= 1800:
                #     response = requests.get(url=_url, headers=headers, verify=False)
                # elif (time.time() - t1 > 1800) & (time.time() - t1 <= 3600):
                #     print("代理ip开始抓...")
                #     response = requests.get(url=_url, headers=headers, proxies=proxies, verify=False)
                # else:
                #     t1 = time.time()
                #     print("1小时后开始切换...")
                #     response = requests.get(url=_url, headers=headers, verify=False)
                time.sleep(1)
                response = requests.get(url=_url, headers=headers, timeout=120).text
                # print(response)
                with open(_fname, 'w', encoding='utf8') as f:
                    f.write(response)
                    f.flush()
                print(f'已下载：{unique_key}，{time.strftime("%c")}')
                return
            except Exception as exc:
                if i == 1:
                    print('\n==>urlretrieve error in download_to_file.py')
                print('  {0}. _url:  {1}'.format(i, _url))
                print('     _fname:  {0}'.format(_fname))
                print('     Warning: {0}  [{1}]'.format(str(exc), time.strftime('%c')))
                if '404' in str(exc):
                    print('被服务器拒绝！')
                    break
                print('     Retry in {0} seconds'.format(sleep_time))
                time.sleep(sleep_time)
                if i == 3:  # 当文件下载失败三次后， 将该消息重新放入redis当中。过一会再抓取！
                    self.rc.rpushx('loading', (year, qtr, _url, _fname, unique_key, loaded))

    def empty(self):
        """
        判断redis当中是否还有消息！
        :return:  当有文件时返回 False， 没有时返回True！
        """
        return self.rc.llen('loading') == 0

    def run(self):
        """
        用于调度各种方法。
        """
        while not self.empty():
            task = eval(self.rc.lpop('loading'))
            year, qtr, _url, _fname, unique_key, loaded = task
            print(_url)
            self.download_to_file(year, qtr, _url, _fname, unique_key, loaded)

