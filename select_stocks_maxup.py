# -*- coding: utf-8 -*-
"""
Spyder 编辑器
这是一个临时脚本文件。
"""

from WindPy import*
import pandas as pd
from datetime import*
import time
import numpy as np

w.start()
print(w.isconnected())

#get trading days' date series
startdate = "2018-01-01"
enddate = "2020-06-30"
error,getdate=w.wsd("000001.SZ", "open", startdate, enddate, "",usedf=True)  
assert error == 0, "API数据提取错误，ErrorCode={}，具体含义请至帮助文档附件《常见API错误码》中查询。".format(error)
getdate.head()
getdate = getdate.reset_index()
stocks_select=pd.DataFrame(columns=['股票代码','股票名称','日期'])
date_select = getdate["index"]

for i in range(0,len(getdate)):
    a=str(date_select[i])
    def verifyResult(errCode):
        assert errCode == 0, "API数据提取错误，ErrorCode={}，具体含义请至帮助文档附件《常见API错误码》中查询。".format(errCode)
    date = str(date_select)
    error1,code=w.wset("sectorconstituent","date="+a+";sectorid=a001010100000000",usedf=True) #提取股票代码
    verifyResult(error1) 
    
    error2,change=w.wss(list(code['wind_code']), "sec_name,maxupordown","tradeDate="+a+";cycle=D",usedf=True) #提取股票对应的涨跌情况
    verifyResult(error2) 
    
    choosed=change[change['MAXUPORDOWN']==1] 
    result=pd.DataFrame() 
    result['股票代码']=choosed.index.tolist()
    result['股票名称']=choosed["SEC_NAME"].values
    result['日期']=a
    stocks_select=stocks_select.append(result)
    
stocks_select.to_csv('./maxupstock.csv', sep=',', header=True, index=True)
    
    




