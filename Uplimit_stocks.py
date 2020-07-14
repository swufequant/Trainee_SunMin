# -*- coding: utf-8 -*-
"""
Created on Tue Jul 14 16:46:57 2020

@author: sywgqh
"""

from pymongo import MongoClient as mc
import json
import os
import datetime
import time
import pandas as pd

class MongoDBReader(object):
    def __init__(self):
        self.client = None
        config_filename = "config.json"
        with open(config_filename, "rb") as fp:
            self.conf = json.load(fp)

    def login(self, server="localhost", port=27017, user="", pwd=""):
        if server == "":
            server = self.conf["server"]
            port = int(self.conf["port"])
            user = self.conf["user"]
            pwd = self.conf["pwd"]
        if user == "":
            uri = 'mongodb://{}:{}'.format(server, port)
        else:
            uri = 'mongodb://{}:{}@{}:{}'.format(user, pwd, server, port)
        self.client = mc(uri)

    def logoff(self):
        if self.client is not None:
            self.client.close()
            self.client = None

    @classmethod
    def SeqConditionGenerator(self, seq_st=0, seq_ed=0):
        '''
        序列号参数生成器
        :param seq_st:
        :param seq_ed:
        :return:
        '''
        seq_condition = {}
        if seq_st is not None:
            seq_condition["$gte"] = seq_st
            # 结束日期参数检查
        if seq_ed is not None:
            seq_condition["$lte"] = seq_ed
            # 序列号参数检查
        if len(seq_condition) > 0:
            if seq_ed is not None and seq_ed <= 0:
                return None
            elif seq_st == seq_ed:
                return seq_st
            else:
                return seq_condition

    @classmethod
    def TimenumConditionGenerator(self, timenum_st, timenum_ed):
        timenum_condition = {}
        if timenum_st is not None:
            timenum_condition["$gte"] = timenum_st
            # 结束日期参数检查
        if timenum_ed is not None:
            timenum_condition["$lte"] = timenum_ed
            # 序列号参数检查
        if len(timenum_condition) > 0:
            if timenum_st is not None and timenum_st == timenum_ed:
                return timenum_st
            else:
                return timenum_condition

    @classmethod
    def CodeConditionGenerator(self, code=None):
        '''
        证券代码参数生成器
        :param seq_st:
        :param seq_ed:
        :return:
        '''
        code_condition = {}
        if code is None or code == "":
            return None
        else:
            if isinstance(code, str):
                if len(code) != 6 and len(code) != 8:
                    print("error code:{}".format(code))
                    return None
                return code
            else:
                print("error code type:{}".format(type(code)))
                return None

    def QueryStockDayLine(self, date_st=None, date_ed=None, code=None, time_stat=False):
        '''
        查询指定日期[date_st, date_ed] 之间
        :param date_st:
        :param date_ed:
        :param code:
        :return:
        '''
        basename = "admin"
        tablename = 'StockDayLine'
        time_st = 0.0
        if time_stat:
            time_st = time.time()
        db = self.client.get_database(basename)  # 创建base
        table = db.get_collection(tablename)  # 获取表
        condition = {}
        date_condition = {}
        # 证券代码参数检查
        code_condition = self.CodeConditionGenerator(code)
        if code_condition is not None:
            condition["code"] = code_condition
        # 起始日期参数检查
        if date_st is not None:
            date_condition["$gte"] = date_st
        # 结束日期参数检查
        if date_ed is not None:
            date_condition["$lte"] = date_ed
        # 日期参数检查
        if len(date_condition) > 0:
            if date_st == date_ed:
                condition["date"] = date_st
            else:
                condition["date"] = date_condition
        # 查询
        cursor = table.find(condition, {"_id": 0})
        df = pd.DataFrame(list(cursor))
        if time_stat:
            print("QueryStockDayLine data:{} used time:{:.3f}s".format(len(df), time.time() - time_st))
        return df

   

    def QueryStockInfo(self, code=None, time_stat=False):
        '''
        查询指定日期[date_st, date_ed] 之间
        :param date_st:
        :param date_ed:
        :param code:
        :return:
        '''
        basename = "admin"
        tablename = 'StockInfo'
        time_st = 0.0
        if time_stat:
            time_st = time.time()
        db = self.client.get_database(basename)  # 创建base
        table = db.get_collection(tablename)  # 获取表
        condition = {}
        # 证券代码参数检查
        code_condition = self.CodeConditionGenerator(code)
        if code_condition is not None:
            condition["code"] = code_condition
        # 查询
        cursor = table.find(condition, {"_id": 0})
        df = pd.DataFrame(list(cursor))
        if time_stat:
            print("QueryStockInfo data:{} used time:{:.3f}s".format(len(df), time.time() - time_st))
        return df

    def QueryUplimitInfo(self, date, code="", time_stat=False):
        '''
        查询指定日期 指定代码的股票的涨停/破板信息
        :param date: 指定日期
        :param code: 指定代码
        :param time_stat: 是否统计程序运行时间
        :return: pd.DataFrame()
        '''
        basename = "admin"
        tablename = "UplimitInfo"
        time_st = 0.0
        if time_stat:
            time_st = time.time()
        db = self.client.get_database(basename)  # 创建base
        table = db.get_collection(tablename)  # 获取表
        condition = {"date": date}
        if code != "":
            condition["code"] = code
        # 查询数据
        cursor = table.find(condition, {"_id": 0})
        df = pd.DataFrame(list(cursor))
        if time_stat:
            print("QueryStockTickLevel data:{} used time:{:.3f}s".format(len(df), time.time() - time_st))
        return df


reader = MongoDBReader()
reader.login("")

def getTradingdays(startdate, enddate):
    df = reader.QueryStockDayLine(startdate, enddate, "SZ000001",time_stat=False)
    Tradingdays = df["date"]
    return Tradingdays

days = getTradingdays(20180101,20200710)

Uplimit=pd.DataFrame()

for i in range(0,len(days)):
    Tradingday=int(days[i])
    df=reader.QueryUplimitInfo(Tradingday,time_stat = True)
    Uplimit = Uplimit.append(df)
    
Uplimit.to_csv('./Uplimit_stocks.csv',sep=',',header=True)
    
reader.logoff()   
    




