# -*- coding: utf-8 -*-
"""
Created on Thu Jul 16 00:57:11 2020

@author: sywgqh
"""

from pymongo import MongoClient as mc
import json
import os
import datetime
import time
import pandas as pd
import numpy as np

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

def Correlation_STG(date):
    '''
   查询某天的涨停股票及相关性最高的
    '''
    #提取当日所有涨停板股票
    df = reader.QueryUplimitInfo(date,time_stat = True)
    Uplimit=df[["code","date"]]
    Uplimit=Uplimit.drop_duplicates()
    print(Uplimit.head())
    
    #导入股票信息数据表
    df2 = pd.read_csv('./Stockinfo.csv', sep=',')
    df2['newcode'] =df2['code'].str[2:8]
    block = df2[["newcode","code","fullname","market"]]
    block = block.rename(columns={"newcode":"code","code":"oldcode"})

    #合并涨停股票板块信息
    Uplimit=pd.merge(Uplimit,block,how='inner',on='code')
    Uplimit=Uplimit[['date','oldcode','fullname','market']]
    Uplimit=Uplimit.rename(columns={"oldcode":"code"})
    list = Uplimit['code']
    print('循环准备开始')

    
    Result = pd.DataFrame() #汇总结果的空表
    
    for i in range(0,len(list)):
        a = Uplimit.at[i,'fullname']
        upstock = Uplimit.at[i,'code']
        upstock_df = reader.QueryStockDayLine((date-10000),(date-1),upstock,time_stat=True)
        days = upstock_df["date"]
        up_date = reader.QueryStockDayLine(date,date,upstock)
        up_Price = up_date["close"]
        sameBlock = df2[df2['fullname']==a] #同板块所有股票
        sameBlock = sameBlock.reset_index(drop=True)
        samelist = sameBlock["code"]
        print('new round')
        print(i)
        for j in range(0,len(samelist)):
            cor_df = reader.QueryStockDayLine((date-10000),(date-1),samelist[j],time_stat=False)
            cor_df = cor_df[-len(days):] #上市不足一年的涨停股票的处理
            if len(cor_df)==0:
                continue
            cor_df = cor_df.reset_index(drop=True)
            corr = round(upstock_df["close"].corr(cor_df["close"]),4)
            price_diff = upstock_df["close"]-cor_df["close"]
            mean_diff = round(np.mean(price_diff),4)
            std_diff = round(np.std(price_diff),4)
            cor_date= reader.QueryStockDayLine(date,date,samelist[j])
            if len(cor_date==0):
                continue
            cor_Price = cor_date["close"]
            zScore=((up_Price-cor_Price)-mean_diff)/std_diff
            idx = i*1000+j
            line=pd.DataFrame({"breakStock":upstock, "corr_stock":samelist[j], "block":a, "correlation":corr, "PriceDiffMean":mean_diff, "PriceDiffStd":std_diff,"breakClose":up_Price, "corClose":cor_Price, "zScore":zScore},index=[0])
            Result = Result.append(line)
            Result = Result.reset_index(drop=True)
            print(j)
    
    return Result

            
            
            
        

    
    