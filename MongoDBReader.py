# author='lwz'
# coding:utf-8

from pymongo import MongoClient as mc
import json
import os
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

    def QueryStockTickOrder(self, date, code="", seq_st=None, seq_ed=None, time_stat=False):
        basename = "TickOrder"
        tablename = str(date)
        time_st = 0.0
        if time_stat:
            time_st = time.time()
        db = self.client.get_database(basename)  # 创建base
        table = db.get_collection(tablename)  # 获取表
        condition = {}
        # 序列号参数检查
        seq_condition = self.SeqConditionGenerator(seq_st, seq_ed)
        if seq_condition is not None:
            condition["seq"] = seq_condition
        # 证券代码参数检查
        code_condition = self.CodeConditionGenerator(code)
        if code_condition is not None:
            condition["code"] = code_condition
        # 查询数据
        cursor = table.find(condition, {"_id": 0})
        df = pd.DataFrame(list(cursor))
        if time_stat:
            print("QueryStockTickOrder data:{} used time:{:.3f}s".format(len(df), time.time() - time_st))
        return df

    def QueryStockTickTrade(self, date, code="", seq_st=None, seq_ed=None, time_stat=False):
        basename = "TickTrade"
        tablename = str(date)
        time_st = 0.0
        if time_stat:
            time_st = time.time()
        db = self.client.get_database(basename)  # 创建base
        table = db.get_collection(tablename)  # 获取表
        condition = {}
        # 序列号参数检查
        seq_condition = self.SeqConditionGenerator(seq_st, seq_ed)
        if seq_condition is not None:
            condition["seq"] = seq_condition
        # 证券代码参数检查
        code_condition = self.CodeConditionGenerator(code)
        if code_condition is not None:
            condition["code"] = code_condition
        # 查询数据
        cursor = table.find(condition, {"_id": 0})
        df = pd.DataFrame(list(cursor))
        if time_stat:
            print("QueryStockTickTrade data:{} used time:{:.3f}s".format(len(df), time.time() - time_st))
        return df

    def QueryStockTickLevel(self, date, code="", timenum_st=None, timenum_ed=None, time_stat=False):
        basename = "TickLevel"
        tablename = str(date)
        time_st = 0.0
        if time_stat:
            time_st = time.time()
        db = self.client.get_database(basename)  # 创建base
        table = db.get_collection(tablename)  # 获取表
        condition = {}
        # 序列号参数检查
        timenum_condition = self.TimenumConditionGenerator(timenum_st, timenum_ed)
        if timenum_condition is not None:
            condition["timenum"] = timenum_condition
        # 证券代码参数检查
        code_condition = self.CodeConditionGenerator(code)
        if code_condition is not None:
            condition["code"] = code_condition
        # 查询数据
        cursor = table.find(condition, {"_id": 0})
        df = pd.DataFrame(list(cursor))
        if time_stat:
            print("QueryStockTickLevel data:{} used time:{:.3f}s".format(len(df), time.time() - time_st))
        return df

    def QueryStockTickSnap(self, date, code="", timenum_st=None, timenum_ed=None, time_stat=False):
        basename = "TickSnap"
        tablename = str(date)
        time_st = 0.0
        if time_stat:
            time_st = time.time()
        db = self.client.get_database(basename)  # 创建base
        table = db.get_collection(tablename)  # 获取表
        condition = {}
        # 序列号参数检查
        timenum_condition = self.TimenumConditionGenerator(timenum_st, timenum_ed)
        if timenum_condition is not None:
            condition["timenum"] = timenum_condition
        # 证券代码参数检查
        code_condition = self.CodeConditionGenerator(code)
        if code_condition is not None:
            condition["code"] = code_condition
        # 查询数据
        cursor = table.find(condition, {"_id": 0})
        df = pd.DataFrame(list(cursor))
        if time_stat:
            print("QueryStockTickLevel data:{} used time:{:.3f}s".format(len(df), time.time() - time_st))
        return df

    def QueryStockTickSeq(self, date, channel=None, time_st=None, time_ed=None, time_stat=False):
        '''
        查询指定日期 指定频道，指定时间范围内的股票的订单序列号
        :param date: 指定日期 20200218
        :param channel: 指定频道代码 002012
        :param time_st: 指定起始时间
        :param time_ed: 指定结束时间
        :param time_stat: 是否统计程序运行时间
        :return: pd.DataFrame()
        '''
        basename = "TickSeq"
        tablename = str(date)
        time_st = 0.0
        if time_stat:
            time_st = time.time()
        db = self.client.get_database(basename)  # 创建base
        table = db.get_collection(tablename)  # 获取表
        condition = {}
        # 时间段参数检查
        timenum_condition = self.TimenumConditionGenerator(time_st, time_ed)
        if timenum_condition is not None:
            condition["timenum"] = timenum_condition
        # 频道代码参数检查
        if channel is not None:
            condition["channel"] = channel
        # 查询数据
        cursor = table.find(condition, {"_id": 0})
        df = pd.DataFrame(list(cursor))
        if time_stat:
            print("QueryStockTickLevel data:{} used time:{:.3f}s".format(len(df), time.time() - time_st))
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


def QueryStockDayLine_Test():
    reader = MongoDBReader()
    reader.login("")
    df = reader.QueryStockDayLine(20200102, time_stat=True)
    print(df.head())
    df = reader.QueryStockDayLine(20200102, code="SZ000001", time_stat=True)
    print(df.head())
    df = reader.QueryStockDayLine(20190102, 20190102, "SZ000001", time_stat=True)
    print(df.head())
    reader.logoff()


def QueryStockInfo_Test():
    reader = MongoDBReader()
    reader.login("")
    df = reader.QueryStockInfo(time_stat=True)
    print(df.head())
    df = reader.QueryStockInfo(code="SZ000001", time_stat=True)
    print(df.head())


def QueryStockTickTrade_Test():
    reader = MongoDBReader()
    reader.login("")
    df = reader.QueryStockTickTrade(20200102)
    print(df.head())
    df = reader.QueryStockTickTrade(20200102, code="000955", time_stat=True)
    print(df.head())
    df = reader.QueryStockTickTrade(20200102, "000955", 500, time_stat=True)
    print(df.head())
    df = reader.QueryStockTickTrade(20200102, "000955", seq_ed=45000, time_stat=True)
    print(df.tail())
    df = reader.QueryStockTickTrade(20200102, "000955", 2000, 50000, time_stat=True)
    print(df.head())
    reader.logoff()


def QueryStockTickOrder_Test():
    reader = MongoDBReader()
    reader.login("")
    df = reader.QueryStockTickOrder(20200102)
    print(df.head())
    df = reader.QueryStockTickOrder(20200102, code="000955", time_stat=True)
    print(df.head())
    df = reader.QueryStockTickOrder(20200102, "000955", 500, time_stat=True)
    print(df.head())
    df = reader.QueryStockTickOrder(20200102, "000955", seq_ed=45000, time_stat=True)
    print(df.tail())
    df = reader.QueryStockTickOrder(20200102, "000955", 2000, 50000, time_stat=True)
    print(df.head())
    reader.logoff()


def QueryStockTickLevel_Test():
    reader = MongoDBReader()
    reader.login("")
    df = reader.QueryStockTickLevel(20190819)
    print(df.head())
    df = reader.QueryStockTickLevel(20190819, code="300127", time_stat=True)
    print(df.head())
    df = reader.QueryStockTickLevel(20190819, "300127", 93500, time_stat=True)
    print(df.head())
    df = reader.QueryStockTickLevel(20190819, "300127", timenum_ed=94000, time_stat=True)
    print(df.tail())
    df = reader.QueryStockTickLevel(20190819, "300127", 93500, 94000, time_stat=True)
    print(df.head())
    reader.logoff()


def QueryStockTickSnap_Test():
    reader = MongoDBReader()
    reader.login("")
    df = reader.QueryStockTickSnap(20190819)
    print(df.head())
    df = reader.QueryStockTickSnap(20190819, code="300127", time_stat=True)
    print(df.head())
    df = reader.QueryStockTickSnap(20190819, "300127", 93500, time_stat=True)
    print(df.head())
    df = reader.QueryStockTickSnap(20190819, "300127", timenum_ed=94000, time_stat=True)
    print(df.tail())
    df = reader.QueryStockTickSnap(20190819, "300127", 93500, 94000, time_stat=True)
    print(df.head())
    reader.logoff()


def QueryStockTickSeq_Test():
    from datetime import datetime as dt
    reader = MongoDBReader()
    reader.login("")
    df = reader.QueryStockTickSeq(20200218, time_stat=True)
    print(df.head())
    df = reader.QueryStockTickSeq(20200218, channel=2012, time_stat=True)
    print(df.head())
    df = reader.QueryStockTickSeq(20200218, channel=2011, time_st=dt(2020, 2, 18, 9, 45, 3), time_stat=True)
    print(df.head())
    df = reader.QueryStockTickSeq(20200218, channel=2013, time_st=dt(2020, 2, 18, 9, 45, 3),
                                  time_ed=dt(2020, 2, 18, 9, 45, 4), time_stat=True)
    print(df.tail())
    df = reader.QueryStockTickSeq(20200218, time_st=dt(2020, 2, 18, 13, 45, 3),
                                  time_ed=dt(2020, 2, 18, 13, 45, 4), time_stat=True)
    print(df.head())
    reader.logoff()


def QueryUplimitInfo_Test():
    reader = MongoDBReader()
    reader.login("")
    df = reader.QueryUplimitInfo(20200218, time_stat=True)
    print(df.head())
    df = reader.QueryUplimitInfo(20200218, code="300691", time_stat=True)
    print(df.head())
    reader.logoff()


if __name__ == "__main__":
    QueryStockDayLine_Test()
    QueryStockInfo_Test()
    QueryStockTickTrade_Test()
    QueryStockTickOrder_Test()
    QueryStockTickLevel_Test()
    QueryStockTickSnap_Test()
    QueryStockTickSeq_Test()
    QueryUplimitInfo_Test()
