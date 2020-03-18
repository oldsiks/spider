#!/usr/bin/env file_spider
# -*- coding: utf-8 -*-
# @File  : db_init.py
# @Author: Eric
# @Date  : 2019.4.11
# @Desc  : 初始化数据库连接
import pymysql


class InitSql:
    def __init__(self, dbase, host='127.0.0.1', port=3306, user='root', pwd='你猜'):
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.dbase = dbase
        self.charset = 'utf8'
        self.db = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.pwd, charset=self.charset,
                                  db=self.dbase)
        self.cursor = self.db.cursor()

    def get_data(self, sql):
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        return data

    def common(self, sql):
        self.cursor.execute(sql)

    def save(self, data, sql):
            try:
                self.cursor.execute(sql, data)
                self.db.commit()
            except BaseException as e:
                self.db.rollback()
                print(f"error!{e} not stored~ {data}")

    def save_values(self, data, sql):
        try:
            self.cursor.executemany(sql, data)
            self.db.commit()
        except BaseException as e:
            self.db.rollback()
            print(f"error!{e} not stored~ ")

    def delete(self, sql, data=None):
        try:
            if data == None:
                self.cursor.execute(sql)
            else:
                self.cursor.execute(sql, data)
            self.db.commit()
        except BaseException as e:
            self.db.rollback()
            print(f"error!{e} 删除失败~ ")

    def update(self, sql, data=None):

        try:
            if data == None:
                self.cursor.execute(sql)
            else:
                self.cursor.execute(sql, data)
            self.db.commit()
        except BaseException as e:
            self.db.rollback()
            print(f"error!{e} 更新失败~")


    def close(self):
        self.db.close()


