
"""
    注意事项：运行程序之前要确认是否有手动新增文件，如果有就先将此文件运行，以保证获取的已下载文件与文件夹中一致！
    功能获取已下载文件列表的loaded.json文件
"""
import os
import re
import json
unique = re.compile(r'data_(.*?)_\d{1,3}.txt')
def get_loaded_json(path=None):


    files_in = {}
    for root, dirs, files in os.walk(path):
        if len(files) == 0:
            continue
        year, qtr = root.split('\\')[-2], root.split('\\')[-1][-1]
        # print(year, qtr)
        if year not in files_in.keys():
            files_in[year] = {}
        if qtr not in files_in[year].keys():
            files_in[year][qtr] = []
        # for file in files:
        #     try:
        #         uni_key = unique.findall(file)[0]
        #         files_in[year][qtr].append(uni_key)
        #     except:
        #         print(file)
        files_in[year][qtr].extend([unique.findall(file)[0] for file in files])


    for year, qtr_values in files_in.items():
        for qtr, values in qtr_values.items():
            loaded = json.dumps(values)
            with open(r'..\loaded_json\{year}_{qtr}.json'.format(year=year, qtr=qtr), 'w', encoding='utf8') as f:
                f.write(loaded)

if __name__ == '__main__':
    get_loaded_json(r'\\fintechdata\SecOriData\IPO\2nd')




