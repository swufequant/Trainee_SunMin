# -*- coding: utf-8 -*-
"""
Created on Wed Jul 22 16:22:30 2020

@author: Min Sun
"""


from pymongo import MongoClient as mc
import json
import os
import datetime
import time
import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import coint 
import datetime as dt



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

def Correlation(date):
    '''
   查询某天的涨停股票及相关性
    '''
    #提取当日所有涨停板股票
    df = reader.QueryUplimitInfo(date,time_stat = True)
    Uplimit=df[["code","date"]]
    Uplimit=Uplimit.drop_duplicates() #去掉反复破板重复的股票代码
    del df
    print(Uplimit.head())
    
    #导入股票信息数据表
    df2 = pd.read_csv('./Stockinfo.csv', sep=',')
    df2['newcode'] =df2['code'].str[2:8]
    block = df2[["newcode","code","name","fullname","market","list_date"]]
    block = block.rename(columns={"newcode":"code","code":"oldcode"})
    
    #合并涨停股票板块信息
    Uplimit=pd.merge(Uplimit,block,how='inner',on='code')
    Uplimit=Uplimit[['date','oldcode',"name",'fullname','market','list_date']]
    Uplimit=Uplimit.rename(columns={"oldcode":"code"})
    Uplimit['judge']=date-Uplimit['list_date']
    Uplimit = Uplimit[~(Uplimit['judge']<10000)==True] #去掉上市不满一年的股票
    Uplimit = Uplimit.reset_index(drop=True)
    list = Uplimit['code'] #所有符合条件的涨停股票代码
    print('循环准备开始')

    
    Result = pd.DataFrame() #汇总结果的空表
    
    for i in range(0,(len(list)-1)):
        print('i round start')
        print(i)
        
        a = Uplimit.at[i,'fullname'] #涨停股票所属板块
        upstock = Uplimit.at[i,'code'] #单只涨停股票的代码
        upstkName = Uplimit.at[i,'name'] #单只涨停股票名称
        upstock_df = reader.QueryStockDayLine((date-10000),(date-1),upstock,time_stat=True) #涨停股票过去一年的交易信息
        
        up_date = reader.QueryStockDayLine(date,date,upstock) #涨停当日涨停股票交易信息
        up_Price = up_date["close"] #涨停股票当日收盘价
        
        sameBlk = df2[df2['fullname']==a] #同板块所有股票
        sameBlk["judge"] = -(sameBlk['list_date']-date)
        sameBlk = sameBlk[~(sameBlk['judge']<10000)==True]
        sameBlk = sameBlk[~(sameBlk['code']==upstock)==True]
        sameBlk = sameBlk.reset_index(drop=True)
        samelist = sameBlk["code"]
        print('new round end')
        print(i)
        
        for j in range(0,(len(samelist)-1)):

            cor_df = reader.QueryStockDayLine((date-10000),(date-1),samelist[j],time_stat=False) #同板块第j只股票前一年的交易信息
            cor_df = cor_df.reset_index(drop=True)
           
            cor_name = sameBlk.at[j,'name'] #同板块第j只股票名称
            corr = round(upstock_df["close"].corr(cor_df["close"]),4) #涨停股票与同板块第j只股票的相关系数
           
            up_close =upstock_df[["date","close"]]
            cor_close = cor_df[["date","close"]]
            mergedata = pd.merge(up_close,cor_close,how="inner",on="date")
            del up_close
            del cor_close
            price_diff = mergedata["close_x"]-mergedata["close_y"] #价差序列
            mean_diff = round(np.mean(price_diff),4) #价差序列均值
            std_diff = round(np.std(price_diff),4) #价差序列标准差
            cointtest = coint(mergedata["close_x"],mergedata["close_y"])
            
            cor_date= reader.QueryStockDayLine(date,date,samelist[j]) #同板块第j只股票当日交易信息
            if len(cor_date)==0:
               continue #如果第j只股票当日停牌，跳过
           
            cor_Price = cor_date.at[0,"close"] #第j只股票当日股价
            zScore=((up_Price-cor_Price)-mean_diff)/std_diff
            
            line=pd.DataFrame({"breakStock":upstock,"break_name":upstkName, "corr_stock":samelist[j],"cor_name":cor_name, "block":a, "correlation":corr, "PriceDiffMean":mean_diff, "PriceDiffStd":std_diff,"breakClose":up_Price, "corClose":cor_Price, "zScore":zScore,"coint_p":cointtest[1]},index=[0])
           
            Result = Result.append(line)
            
    return Result
    

def StockSelect(date,corr,coint_p,topN,zscore):
    
    '''
    提取某交易日所有符合交易标准的股票

    Parameters
    ----------
    date : int
        提取日期，eg：20200327
    corr : float
        相关系数要求，取值（0，1）
    coint_p : float
        协整检验p值，通常可选0.1，0.05，0.01
    topN : float
        相关系数top N的股票
    zscore : float
        价差z-Score表准

    Returns
    -------
    stock_Select : DataFrame
        筛选后的股票列表

    '''
    
    df=Correlation(date) #提取date日的涨停股票及其相关性信息数据
    
    stock_Select = df[(df['coint_p']<coint_p)==True] #只保留协整检验pp值小于标准的股票
    stock_Select= stock_Select[(stock_Select['correlation']>corr)==True] #保留相关系数大于标准的股票
    stock_Select= stock_Select[(stock_Select['zScore']>zscore)==True] #保留z-Score大于标准的股票
   
    #对每只涨停股票的相关股票的相关系数排序
    stock_Select['rank']=stock_Select.groupby(['breakStock']).rank(method='first',ascending=0)['correlation']
    stock_Select = stock_Select[(stock_Select['rank']<=topN)==True]
    stock_Select = stock_Select.reset_index(drop=True)
    stock_Select['date']=date
    return stock_Select



#------------------------------------------------------------------------------
#update: 获取涨停相关股票的未来5个交易日的收益表现
def StockPerformance(date):
    data = Correlation(date)
    data = data.reset_index(drop = True)
    stks = data['corr_stock'] #当日所有涨停股票的相关股票代码
    trddate = dt.datetime.strptime(str(date),'%Y%m%d') #当天日期转化为日期格式
    
    #导入交易日日历
    TrdCal=pd.read_csv("./TrdCal.csv") 
    
    
    #查询日向后五个交易日都转化为int型
    def date_to_int(t):
        intdate = t.year*10000+t.month*100+t.day
        return intdate
    
    #输入日期格式日期，返回int格式交易日
    def Trdday(t):
        t = date_to_int(t)
        df_t = TrdCal.loc[TrdCal['cal_date']==t]
        idx=TrdCal.loc[TrdCal['cal_date']==t].index.tolist()
        index = idx[0]
        while (int(df_t['is_open'])==0):
            index = index+1
            df_t = TrdCal.loc[index]
    
        t = int(df_t['cal_date'])
        return t #返回的是int型日期
    
    t1 = trddate+dt.timedelta(days=1) #t1日期型
    t1 = Trdday(t1)
    dt1 = dt.datetime.strptime(str(t1),'%Y%m%d')
    
    t2 = dt1+dt.timedelta(days=1)
    t2 = Trdday(t2)
    dt2 = dt.datetime.strptime(str(t2),'%Y%m%d')
    
    t3 = dt2+dt.timedelta(days = 1)
    t3 = Trdday(t3)
    dt3 = dt.datetime.strptime(str(t3),'%Y%m%d')
    
    t4 = dt3+dt.timedelta(days=1)
    t4 = Trdday(t4)
    dt4 = dt.datetime.strptime(str(t4),'%Y%m%d')
    
    t5 = dt4+dt.timedelta(days=1)
    t5 = Trdday(t5)
        
    returns = pd.DataFrame()
    
    for i in range(0,(len(stks))):
        
        print(i)
        #提取股票i的5日数据
        df = pd.DataFrame() #股票i的五日数据表
        for j in {t1,t2,t3,t4,t5}:
            dfi = reader.QueryStockDayLine(j,j,code=stks[i])
            #对于股票停牌,保留日期,但只保留空值
            if len(dfi)==0:
                df_line=pd.DataFrame({"code":stks[i] },index=[0])
            else:
                df_line= dfi[['code','close','pre_close']]
            df = df.append(df_line)
            df = df.reset_index(drop=True)
        

        ret1 = round((df.at[0,'close']-df.at[0,'pre_close'])*100/df.at[0,'pre_close'],4)
        ret2 = round((df.at[1,'close']-df.at[0,'pre_close'])*100/df.at[0,'pre_close'],4)
        ret3 = round((df.at[2,'close']-df.at[0,'pre_close'])*100/df.at[0,'pre_close'],4)
        ret4 = round((df.at[3,'close']-df.at[0,'pre_close'])*100/df.at[0,'pre_close'],4)
        ret5 = round((df.at[4,'close']-df.at[0,'pre_close'])*100/df.at[0,'pre_close'],4)
        
        p1 = df.at[0,'close']
        p2 = df.at[1,'close']
        p3 = df.at[2,'close']
        p4 = df.at[3,'close']
        p5 = df.at[4,'close']
        
        line = pd.DataFrame({"corr_stock":stks[i],"ret1":ret1,"ret2":ret2,"ret3":ret3,\
                             "ret4":ret4,"ret5":ret5,"close1":p1,"close2":p2,"close3":p3,\
                                 "close4":p4,"close5":p5},index=[i])
        returns = returns.append(line)
        print("one round end")
     
    
    data = pd.merge(data,returns,how='left',on='corr_stock')      
    data = data.sort_values(['correlation'],ascending=False).groupby(['breakStock'])