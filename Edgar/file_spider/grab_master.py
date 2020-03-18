
"""
本模块用于master文件的获取。
"""
import logging
import os
import time
import requests
from fake_useragent import UserAgent
logging.captureWarnings(True)  # 忽略文件在运行时的警告！
location = r'C:\Users\EricI\Envs\spider\Lib\site-packages\AGpath.json'  # 将所有的user_agent都保存到本地，以便使用！
ua = UserAgent(path=location)  # 配置随机UserAgent，再请求时随机自动切换。
PARM_ROOT_PATH = 'https://www.sec.gov/Archives/edgar/full-index/'  # 获取master的地址。历年的所有公司的所有文件都在这里保存。

def get_masterindex(year, qtr, flag=False):

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        # 'Connection': 'keep-alive',
        'Host': 'www.sec.gov',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': ua.random
    }

    masterindex = []
    number_of_tries = 3
    sleep_time = 5
    start = time.clock()
    url = PARM_ROOT_PATH + str(year) + f'/QTR' + str(qtr) + '/master.idx'  # 拼接出想要的master地址。
    master_name = '-'.join([str(year), f'QTR{str(qtr)}']) + '.idx'  # master的命名，例如：“2019-QTR1.idx”
    local_path = os.path.join(r'D:\work\files\New_Edgar\master', master_name)  # master的保存路径。

    if os.path.exists(local_path):
        """
        再次做判断，在本地是否有已经下载好的master文件，如果没有则下载，如果有则用本地的！ 但是要注意最新一季度的master文件，因为最新季度的
        master文件在本地保存时并没有完全更新完毕。例如：现在时9月份，第四季度的master已经有了，并且在本地已经下载，等明年再用现在下载的第四
        季度master文件就不行了。需要下载最新版本的！此时需要手动删除之前下载的第四季度master。
        """
        with open(local_path, 'r') as f:
            records = f.readlines()
        for line in records[11:]:
            mir = MasterIndexRecord(line)
            if not mir.err:
                masterindex.append(mir)
        print('masterindex:', len(masterindex))
        return masterindex

    else:
        for i in range(1, number_of_tries + 1):
            try:
                master = requests.get(url=url, headers=headers, stream=False).text
                with open(local_path, 'w', encoding='utf8') as f:
                    f.write(master)
                    f.flush()
                print(local_path, '已保存至本地')
                records = master.splitlines()[10:]
                break
            except BaseException as e:
                if i == 1:
                    print('\nError in download_masterindex')
                print('  {0}. _url:  {1}'.format(i, url))
                print('  Warning: {0}  [{1}]'.format(str(e), time.strftime('%c')))
                if '404' in str(e):
                    print('访问被拒绝，需改变反爬措施。')
                    break
                if i == number_of_tries:
                    return False
                print('     Retry in {0} seconds'.format(sleep_time))
                time.sleep(sleep_time)
                sleep_time += sleep_time
        for line in records:
            mir = MasterIndexRecord(line)
            if not mir.err:
                masterindex.append(mir)

        if flag:
            print('download_masterindex:  ' + str(year) + ':' + str(qtr) + ' | ' +
                  'len() = {:,}'.format(len(masterindex)) + ' | Time = {0:.4f}'.format(time.clock() - start) +
                  ' seconds')
        return masterindex


class MasterIndexRecord:
    """
    将master文件中的文件信息转换为一个对象，将cik，文件类型，文件地址，文件名字，发布日期，唯一标识转换为对象属性，以便调用！
    """
    def __init__(self, line):
        self.err = False
        parts = line.split('|')
        if len(parts) == 5:
            self.cik = int(parts[0])
            self.name = parts[1]
            self.form = parts[2]
            self.filingdate = int(parts[3].replace('-', ''))
            self.path = parts[4]
            self.unique_key = '_'.join([self.path.split('/')[2], self.path.split('/')[3].strip('.txt\n')])
        else:
            self.err = True
        return
