# -*- coding: utf-8 -*-
"""
Created on Wed Jul 29 23:29:20 2020

@author: min
"""


from WindPy import *
import datetime,time
import numpy as np
import pandas as pd

w.start()

error,data_df=w.wsd("000001.SZ",  "2016-11-01", "2017-01-03", "",usedf=True)
assert error == 0, "API数据提取错误，ErrorCode={}，具体含义请至帮助文档附件《常见API错误码》中查询。".format(error)
data_df.head() #查看前几行数据
 

def getAStockCodesWind(end_date=time.strftime('%Y%m%d',time.localtime(time.time()))):
    '''
    通过wset数据集获取所有A股股票代码，深市代码为股票代码+SZ后缀，沪市代码为股票代码+SH后缀。
    如设定日期参数，则获取参数指定日期所有A股代码，不指定日期参数则默认为当前日期
    :return: 指定日期所有A股代码，不指定日期默认为最新日期
    '''
    #加日期参数取最指定日期股票代码
    #stockCodes=w.wset("sectorconstituent","date="+end_date+";sectorid=a001010100000000;field=wind_code")
    #不加日期参数取最新股票代码
    stockCodes=w.wset("sectorconstituent","sectorid=a001010100000000;field=wind_code")
    return stockCodes.Data[0]
    #return stockCodes

codes = getAStockCodesWind() #获取当前日期的所有A股股票代码list

def getConceptsTS(start_date,end_date):
    '''
    输入开始时间和结束时间，查询此时间段所有股票的所属概念序列
    输入格式为str
    例如：“2019-01-01”
    '''
    concepts = pd.DataFrame()
    for i in range(0,len(codes)):
        df = w.wsd(codes[i],"trade_code,concept",start_date,end_date,"Fill=Previous",usedf=True)
        concepts = concepts.append(df)
    return concepts

concepts_TS = getConceptsTS("2018-01-01","2020-07-15")









