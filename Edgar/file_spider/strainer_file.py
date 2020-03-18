
"""
    功能：根据已下载文件， 以及筛选条件form（文件类型）cik（公司代码）筛选出需要下载的文件，然后将参数传入redis。
    方法：用redis进行消息传递，从而实现多线程。
    @Author: Eric Liu
    @Date  : 2019.6.25
"""
import json
import os
import redis
from file_spider.grab_master import get_masterindex

PARM_EDGARPREFIX = 'https://www.sec.gov/Archives/'
rc = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True, db=0)
# rc.delete('loading')

def initialize(year, qtr, form_range, cik_range, PARM_PATH):
    """
    :param year: 所要爬取的年份。
    :param qtr:  所要爬取的季度。
    :param form_range:  要爬去的文件类型范围！
    :param cik_range:  爬取文件的cik范围!
    :param PARM_PATH:  所下载文件最终要保存的路径。
    :return: 将所有根据cik，类型，时间所过滤出的文件信息传入到redis中！
    """
    if os.path.exists(f'../loaded_json/{year}_{qtr}.json'):  # 将已下载的记录读取进来，为已下载文件过滤做准备！
        with open(r'../loaded_json/{year}_{qtr}.json'.format(year=str(year), qtr=str(qtr)), 'r') as f:
            loaded = json.loads(f.read())
    else:
        loaded = []
    file_count = {}
    path = '{0}{1}\\QTR{2}\\'.format(PARM_PATH, str(year), str(qtr))
    if not os.path.exists(path):
        os.makedirs(path)
        print('Path: {0} created'.format(path))
    masterindex = get_masterindex(year, qtr, True)  # 根据年，季度获取已master中的文件信息！
    if masterindex:
        for item in masterindex:
            if item.form in form_range and str(item.cik) in cik_range:  # 根据cik，文件类型，筛选出要文件。
                if item.unique_key not in loaded and item.filingdate<=20190531:  # 20190531为过滤日期！ 再根据已下载文件和日期进行二次过滤！
                    fid = str(item.cik) + str(item.filingdate) + item.form  # 因为有的文件会有cik，type，发布日期会重复，在此添加一个标记！
                    if fid in file_count:
                        file_count[fid] += 1
                    else:
                        file_count[fid] = 1
                    url = os.path.join(PARM_EDGARPREFIX, item.path)
                    fname = (path + str(item.filingdate) + '_' + item.form.replace('/', '-').replace(' ', '-') + '_' +
                             item.path.replace('/', '_'))
                    fname = fname.replace('.txt', '_' + str(file_count[fid]) + '.txt').strip('\n')
                    rc.rpush('loading', (year, qtr, url, fname, item.unique_key, loaded))  # 将下载文件所需信息存入redis中！

