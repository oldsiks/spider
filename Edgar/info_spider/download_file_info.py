
"""
    功能：从redis中获取参数后对文件信息进行下载！
"""

import re
import time
import redis
import requests
from lxml import etree
import logging
from setting.db_init import InitSql
from fake_useragent import UserAgent

logging.captureWarnings(True)
location = r'C:\Users\EricI\Envs\spider\Lib\site-packages\AGpath.json'
ua = UserAgent(path=location)
PARM_EDGARPREFIX = 'https://www.sec.gov'
t1 = time.time()


class FileInfo(object):
    def __init__(self, form):
        self.rc = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True, db=1)
        self.form = form
        self.cursor = InitSql('edgar')

    def request_info(self, CIK, UNI):
        # company_info = grab_company(CIK, UNI)
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            # 'Connection': 'keep-alive',
            'Host': 'www.sec.gov',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': ua.random
        }

        url = f'https://www.sec.gov/Archives/edgar/data/{CIK}/{UNI}-index.htm'
        if (time.time() - t1) % 1800 < 900:
            resp = requests.get(url=url, headers=headers, verify=False, timeout=20)
        else:
            resp = requests.get(url=url, headers=headers, verify=False, timeout=20)
        return resp, CIK, url

    def analysis(self, resp, CIK, url):
        """
        对请求回来的网页进行解析！
        :param resp:  请求回来的网页。
        :param CIK:  有些元素定位需要cik，所以说要将cik传进来！
        :param url: 将网页的url传入进来用于保存。
        :return:  返回的是当前网页中解析出的想要信息！
        """
        text_list = resp.text.split('\n')
        index = 0
        while index < len(text_list):
            con_line = text_list[index]
            if '<div id="filerDiv">' in con_line:
                for i in range(index + 1, len(text_list)):
                    endline = text_list[i]
                    if '<div id="filerDiv">' in endline or i == len(text_list) - 1:
                        info = text_list[index:i + 1]
                        for line in info:
                            if CIK in line:
                                INFO = ' '.join(info)
            index += 1
        html = etree.HTML(resp.text)
        FILE_TYPE = html.xpath('//div[@id="formName"]/strong/text()')[0][5:]
        EDATE = html.xpath('//div[@class="formContent"]/div[@class="formGrouping"]/div[@class="info"]/text()')[0]
        if html.xpath(r'//div[@class="formGrouping"]/div[contains(text(),"Period of Report")]/text()') != []:
            PERIOD = html.xpath(r'//div[contains(text(),"Period of Report")]/../div[2]/text()')[0]
        else:
            PERIOD = ''
        SEC_LINK = url
        DOCUMENTS = html.xpath('//div[@class="formContent"]/div[@class="formGrouping"]/div[@class="info"]/text()')[2]
        ACCEPTED = html.xpath('//div[@class="formContent"]/div[@class="formGrouping"]/div[@class="info"]/text()')[1]
        if ' | Fiscal Year End: ' in html.xpath(f'//div[@id="filerDiv"]/div[@class="companyInfo"]/span/a[contains(text(), "{CIK}")]/../../p/text()'):
            FISCAL_YEAR_END = re.findall(r'Fiscal Year End: <strong>(.*?)</strong>', INFO)[0]
        else:
            FISCAL_YEAR_END = ''
        if ' | Film No.: ' in html.xpath(f'//div[@id="filerDiv"]/div[@class="companyInfo"]/span/a[contains(text(), "{CIK}")]/../../p/text()'):
            FILM_NO = re.findall(r'Film No.: <strong>(.*?)</strong>', INFO)[0]
        else:
            FILM_NO = ''
        if ' | File No.: ' in html.xpath(f'//div[@id="filerDiv"]/div[@class="companyInfo"]/span/a[contains(text(), "{CIK}")]/../../p/text()'):
            FILE_NO = re.findall(r'File No.: <a href=.*?<strong>(.*?)</strong>', INFO)[0]
        else:
            FILE_NO = ''
        n = 1
        value_list = []
        while True:
            if html.xpath('//table[@class="tableFile"]/@summary')[0] == 'Document Format Files':
                n += 1
                if html.xpath(f'//table[@summary="Document Format Files"]/tr[{n}]/td[3]/a/text()') != []:
                    SEC_LINK_HASH = PARM_EDGARPREFIX + \
                                    html.xpath(f'//table[@summary="Document Format Files"]/tr[{n}]/td[3]/a/@href')[0]
                    DOCUMENT = html.xpath(f'//table[@summary="Document Format Files"]/tr[{n}]/td[3]/a/text()')[0]
                else:
                    SEC_LINK_HASH = ''
                    DOCUMENT = ''

                if html.xpath(f'//table[@summary="Document Format Files"]/tr[{n}]/td[2]/text()') != []:
                    DESCRIPTION = str(
                        html.xpath(f'//table[@summary="Document Format Files"]/tr[{n}]/td[2]/text()')[0]).lower()
                else:
                    DESCRIPTION = ''

                if html.xpath(f'//table[@summary="Document Format Files"]/tr[{n}]/td[4]/text()') != []:
                    ETYPE = html.xpath(f'//table[@summary="Document Format Files"]/tr[{n}]/td[4]/text()')[0]
                else:
                    ETYPE = ''
                if html.xpath(f'//table[@summary="Document Format Files"]/tr[{n}]/td[5]/text()') != []:
                    ESIZE = html.xpath(f'//table[@summary="Document Format Files"]/tr[{n}]/td[5]/text()')[0]
                else:
                    ESIZE = ''
                if DESCRIPTION == 'Complete submission text file'.lower():
                    UNIQUE_KEY = CIK + '_' + DOCUMENT[0: -4]
                    TICKER = '1'
                else:
                    UNIQUE_KEY = ''
                    TICKER = '0'
                if PERIOD != '':
                    FISCAL_PERIOD = PERIOD.split('-')[0] + '-' + FISCAL_YEAR_END[0:2] + '-' + FISCAL_YEAR_END[2:]
                else:
                    FISCAL_PERIOD = ''
                if html.xpath(f'//table[@summary="Document Format Files"]/tr[{n}]/td[3]/a/@href') == [] and html.xpath(
                        f'//table[@summary="Document Format Files"]/tr[{n}]/td[2]/text()') == []:
                    break
                file_info = (
                CIK, SEC_LINK, EDATE, PERIOD, DOCUMENTS, ACCEPTED, FILE_TYPE, FISCAL_YEAR_END, FISCAL_PERIOD, FILE_NO,
                FILM_NO, DOCUMENT, SEC_LINK_HASH, DESCRIPTION, ETYPE, ESIZE, TICKER, UNIQUE_KEY)
                value_list.append(file_info)
        return value_list

    def run(self):
        while not self.form_empty(self.form):
            task = self.rc.lpop(self.form)  # 从redis中获取参数。
            print("task:", task, time.asctime(time.localtime(time.time())))
            CIK, UNI = eval(task)
            try_times = 3
            for i in range(1, try_times+1):  # 请求失败三次后，将消息重新传入redis
                try:
                    t1 = time.time()
                    resp, CIK, url = self.request_info(CIK, UNI)
                    time.sleep(1)
                    values = self.analysis(resp, CIK, url)
                    self.cursor.save_values(values, f'insert into `{self.form}`(CIK, SEC_LINK, FILINGDATE, PERIOD, DOCUMENTS, ACCEPTED, FILE_TYPE, FISCAL_YEAR_END, FISCAL_PERIOD, FILE_NO, FILM_NO, DOCUMENT, SEC_LINK_HASH, DESCRIPTION, ETYPE, ESIZE, TICKER, UNIQUE_KEY)values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)')
                    print(time.time()-t1)

                    break
                except BaseException as e:
                    time.sleep(3)
                    if i == 3:
                        self.rc.rpush(CIK, UNI)
                    print(f'放回消息队列，改时间再战：{e}')

    def message_empty(self):  # 检查redis中消息的长度！
        # print(self.rc.llen(self.key))
        return self.rc.llen(self.form)


    def form_empty(self, form):  # 如果该表不存在，那么自动创建一个表！
        self.cursor.common(f"CREATE TABLE if not exists {form}( `id`INT NOT NULL AUTO_INCREMENT,`CIK` varchar(15) default NULL,`SEC_LINK` char(200) DEFAULT NULL,`FILINGDATE` varchar(200) DEFAULT NULL,`PERIOD` char(200) DEFAULT NULL,`DOCUMENTS` varchar(100) DEFAULT NULL,`ACCEPTED` varchar(100) DEFAULT NULL,`FILE_TYPE` varchar(100) DEFAULT NULL,`FISCAL_YEAR_END` varchar(100) DEFAULT NULL, `FISCAL_PERIOD` varchar(100) DEFAULT NULL, `FILE_NO` varchar(100) DEFAULT NULL,`FILM_NO` varchar(100) DEFAULT NULL,`DOCUMENT` varchar(100) DEFAULT NULL,`SEC_LINK_HASH` varchar(200) DEFAULT NULL,`DESCRIPTION` varchar(100) DEFAULT NULL,`ETYPE` varchar(50) DEFAULT NULL,`ESIZE` varchar(50) DEFAULT NULL,`TICKER` varchar(2) DEFAULT NULL, `UNIQUE_KEY` varchar(200) DEFAULT NULL,PRIMARY KEY ( id )) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
