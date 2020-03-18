import time
from threading import Thread
from info_spider.strainer_unique import producer
from info_spider.download_file_info import FileInfo


if __name__ == '__main__':
    """
    在这里将保存信息的表名作为redis中的key！
    """
    form = 'thread_info_424b_20190531'
    Thread(target=producer('w', form)).start()
    time.sleep(5)
    for i in range(5):
        Thread(target=FileInfo(form).run).start()

