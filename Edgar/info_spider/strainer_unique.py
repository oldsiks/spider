
"""
用于消息生产，将准备下载文件信息所需参数传入redis。
"""

import os
import redis
from setting.db_init import InitSql

cursor = InitSql('edgar')
rc = redis.Redis(host='127.0.0.1', port=6379, db=1)

def producer(flag='a', form=''):
    loaded = [i[0] for i in cursor.get_data(f'select distinct unique_key from {form}')]  # 获取已经下载的文件信息！
    if flag == 'w':
        rc.delete(form)
        uni_list = []
        for root, dir, files in os.walk('../loaded_json'):  # 从loaded_json中将要下载的文件uni——key获取！
            for file in files:
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf8') as f:
                    uni_list.extend(eval(f.read()))
        for uni_key in uni_list:
            if uni_key not in loaded:  # 根据已下载的文件信息将uni——key过滤一遍！
                cik = uni_key.split('_')[0]
                uni = uni_key.split('_')[1]
                rc.rpush(form, (cik, uni))
    else:
        print('redis已保存，无需重复~~~')



