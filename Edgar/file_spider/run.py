
from threading import Thread

import redis

from setting.db_init import InitSql
from file_spider.download_file import Download
from file_spider import edgar_form
from file_spider.strainer_file import initialize
rc = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True, db=0)
rc.delete('loading')


def main():
    PARM_PATH = r'\\fintechdata\SecOriData\IPO\2nd\\'
    form_range = edgar_form.all_424B
    cursor = InitSql('edgar')
    cik_range = [i[0] for i in cursor.get_data('select cik from cik_sheet')]
    print('cik_range:', len(cik_range))
    year = 2019
    for qtr in range(1, 3):
        initialize(year, qtr, form_range, cik_range, PARM_PATH)
    for i in range(5):
        Thread(target=Download().run).start()


if __name__ == '__main__':
    main()