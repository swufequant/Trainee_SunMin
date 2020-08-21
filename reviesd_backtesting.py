# -*- coding: utf-8 -*-
"""
Created on Mon Aug 6 13:18:59 2020
思路:
    1. 所需数据: 涨停股票信息/涨停相关股票日线（过去一年+未来N天）/交易日历
    2. 三级板块列表：可以从stock Info里面查询建立三级板块的列表
    3. 对于板块i，查询板块内所有股票的日线, 存储dataframe待用
    4. 查询回测期内板块i所有涨停股票（已经建好的Uplimit表中筛选）
    5. 查询板块i的回测期index日线
    6. 计算回测期内第j天，股票s与其它股票的相关性以及与板块指数的相关性
    7. 并找到该股票未来5个交易日的收益率
    8. 板块内循环，放入结果表中
    9. 再对不同的股票循环
    
@author:Min Sun
"""
from pymongo import MongoClient as mc
import json
import os
import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import coint 
import datetime as dt
from MongoDBReader import*

os.chdir('C:\\Users\\sywgqh\\Desktop\\Trainee_SunMin-master\\correlation_revised')

# 回测2014年以来的数据
reader = MongoDBReader()
reader.login("")

#===========================================================================
# Step 1: 读取交易日历，获取涨停股票列表
TrdCalendar = pd.read_csv('TrdCal.csv')
TrdCalendar.sort_values(['cal_date'],inplace=True)
calendar = TrdCalendar[TrdCalendar['is_open']==1]
calendar = calendar[calendar['cal_date']>20140101]
calendar = calendar[calendar['cal_date']<20200810]
calendar = calendar.reset_index(drop=True)
calendar = calendar['cal_date'] #交易日历色series 
calendar = list(calendar)

#获取回测区间内每天的涨停股票信息列表
uplimit = pd.DataFrame()
for i in range(0,len(calendar)-7):
    date = int(calendar[i])
    print(str(i)+'/'+str(len(calendar)))
    df = reader.QueryUplimitInfo(date, time_stat=False)
    if len(df) !=0: #如果有数据就只保留涨停股票的代码和日期，并去除重复项
        df = df[['code','date']]
        df.drop_duplicates(['code','date'],inplace=True)
    else: #如果没有数据就跳过
        continue
    uplimit = uplimit.append(df)
uplimit.reset_index(drop=True,inplace=True)
uplimit.to_csv('uplimit.csv')

#==============================================================================
# step 2:获取对应交易日历下的股票信息和板块数据（防止行业板块成分股发生变动）
uplimit = pd.read_csv('uplimit.csv')
uplimit = uplimit[['code','date']]

maxdt = np.max(uplimit['date'])
mindt = np.min(uplimit['date'])
calendar = TrdCalendar[(TrdCalendar['cal_date']>=mindt)&(TrdCalendar['cal_date']<=maxdt)]
calendar = calendar[calendar['is_open']==1]
calendar.reset_index(drop=True,inplace=True)
calendar = calendar['cal_date']

# 股票信息列表
stockInfo = reader.QueryStockInfo()
stockInfo['listdate'] = stockInfo['list_date'].apply(lambda x: int(x))
stockInfo = stockInfo[stockInfo['listdate']<(maxdt)-10000-300] #取掉上市不足一年+3个月的股票
stockInfo = stockInfo[stockInfo['L3Name']!='']
stockInfo.rename(columns = {"code":"newcode","symbol":"code"},inplace=True)
stockInfo['code'] = stockInfo['code'].apply(lambda x: int(x))

# 三级行业板块列表
industry = stockInfo[['L3','L3Name']]
industry = industry.drop_duplicates(['L3','L3Name'])
industry.reset_index(drop=True,inplace=True) 

#===============================================================================
# step 3：按照板块循环提取数据计算相关性及未来收益等
uplimit = pd.merge(uplimit,stockInfo,how='left',on='code')
calendar = TrdCalendar.sort_values('cal_date')
calendar = calendar[calendar['is_open']==1]
calendar = calendar.reset_index(drop=True)
calendar = list(calendar['cal_date'])

# 对三级板块循环,对于每一个i,对应一个三级行业板块
All_result = pd.DataFrame()
for i in range(0,len(industry)-1):
    
    #提取同一三级板块内的所有股票信息列表，历史涨停和涨停日历和回测期日线
    up = uplimit[uplimit['L3']==industry.at[i,'L3']] #三级板块内历史涨停信息
    up = up.sort_values(['date','code'])
    up = up.reset_index(drop=True)
    
    stock = stockInfo[stockInfo['L3'] == industry.at[i,'L3']] #同板块股票列表
    stock = stock.reset_index(drop=True)
    
    
    #提取同板块的回测期日线数据
    Df = pd.DataFrame() #存储板块内日线数据
    for j in range(0,len(stock)-1):
        end_dt = int(maxdt)
        if stock.at[j,'listdate']>mindt-10000:
            start_dt = int(stock.at[j,'listdate'])
        else:
            start_dt = int(mindt-10000)
        df = reader.QueryStockDayLine(start_dt,end_dt,stock.at[j,'newcode'])
        Df = Df.append(df)
    
    #提取该三级板块指数的回测期历史收益率数据
    start_dt = int(mindt-10000)
    end_dt = int(maxdt)
    index = reader.QueryIndexDayLine(start_dt,end_dt,industry.at[i,'L3'])
    index['index_ret'] = np.log(index['close']/index['pre_close'])*100
    index = index[['date','code','index_ret','count']]
    
    #计算板块内股票的收益率
    Df['ret'] = np.log(Df['close']/Df['pre_close'])*100
    Df = Df[['code','date','ret','close']]
    
    print('data prepare finished '+str(i))
    #对每一条涨停信息循环，再对板块内每只股票循环，计算各项指标，写入空表中
    result = pd.DataFrame()
    for j in range(0,len(up)-1):
        
        print(str(j)+' round start')
        
        upcode = up.at[j,'newcode'] #这个循环中涨停的股票
        update = up.at[j,'date'] #涨停日期
        upname = up.at[j,'name'] #涨停股票名称
        Industry = up.at[j,'L3Name'] #涨停所属三级行业名称
        
        updf = Df[Df['code']==upcode] #从同板块股票日线数据中取出涨停股票日线
        
        #一些日期参数：
        a = calendar.index(update)
        back_dt = calendar[a-240] #回溯涨停日之前240个交易日的起始日期
        future_dt1 = calendar[a+1]
        future_dt2 = calendar[a+2]
        future_dt3 = calendar[a+3]
        future_dt4 = calendar[a+4]
        future_dt5 = calendar[a+5]  #涨停后5个交易日的交易日期
        
        if min(updf['date'])<back_dt:
            up_df = updf[(updf['date']>=back_dt)&(updf['date']<=update)]
            up_df.rename(columns={'code':'upcode','ret':'up_ret','close':'up_close'},inplace=True)
            
            for k in range(0,len(stock)-1):
                stkdata = Df[Df['code']==stock.at[k,'newcode']]
                stkdata.reset_index(drop=True,inplace=True)
                
                if stkdata.at[0,'code'] != upcode:
                    
                    if min(stkdata['date'])<back_dt:
                        calendar_used = TrdCalendar[(TrdCalendar['cal_date']>=back_dt)&(TrdCalendar['cal_date']<=update)]
                        calendar_used = calendar_used[calendar_used['is_open']==1]
                        calendar_used = calendar_used[['cal_date','is_open']]
                        calendar_used = calendar_used.rename(columns={'cal_date':'date'})
                        
                        cordf = pd.merge(calendar_used,stkdata,how='left',on='date')
                        del cordf['is_open']
                        
                        df_use = pd.merge(up_df,cordf,how='right',on='date')
                        df_use = df_use.sort_values('date').reset_index(drop=True)
                        df_use['price_diff'] = df_use['close']-df_use['up_close']
                        df_use.fillna(0)
                        
                        cor_close = df_use.at[240,'close'] #同板块股票涨停当日收盘价
                        cor_ret = df_use.at[240,'ret'] #同板块股票涨停当日的日收益
                        upprice = df_use.at[240,'up_close']
                        
                        corr_price = round(df_use['up_close'].corr(df_use['close']),4) #收盘价的相关性
                        corr_ret = round(df_use['up_ret'].corr(df_use['ret']),4) #收益率的相关性
                        
                        pricediff_mean = round(np.mean(df_use['price_diff']),4)
                        pricediff_std = round(np.std(df_use['price_diff']),4)
                        zscore = (df_use.at[240,'price_diff']-pricediff_mean)/pricediff_std
                        zscore = round(zscore,4) #两股价差的Z-Score
                        
                        coint_price = coint(df_use['close'],df_use['up_close'])
                        coint_p = round(coint_price[1],4) #股价的协整检验t值
                        
                        #涨停日后五天的日期，在股票k日线数据中的index
                        cor_datelist = list(stkdata['date'])
                        iid1 = cor_datelist.index(future_dt1)
                        iid2 = cor_datelist.index(future_dt2)
                        iid3 = cor_datelist.index(future_dt3)
                        iid4 = cor_datelist.index(future_dt4)
                        iid5 = cor_datelist.index(future_dt5)
                        #同板块相关股票k的未来五天日收益率
                        ret1 = stkdata.at[iid1,'ret']
                        ret2 = stkdata.at[iid2,'ret']
                        ret3 = stkdata.at[iid3,'ret']
                        ret4 = stkdata.at[iid4,'ret']
                        ret5 = stkdata.at[iid5,'ret']
                        #同板块相关股票的未来五天收盘价
                        p1 = stkdata.at[iid1,'close']
                        p2 = stkdata.at[iid2,'close']
                        p3 = stkdata.at[iid3,'close']
                        p4 = stkdata.at[iid4,'close']
                        p5 = stkdata.at[iid5,'close']
                        
                        #汇总结果，写出一行，然后粘贴在result空表
                        line = pd.DataFrame({"BreakCode":upcode,"BreakName":upname,"BreakDate":update,"Industry":Industry,\
                                             "BreakClose":upprice,"cor_stockcode":stock.at[k,'newcode'],"cor_Close":cor_close,\
                                                 "cor_Return":cor_ret,"correlation_price":corr_price,"correlation_return":corr_ret,\
                                                     "z_score":zscore,"coint_p_value":coint_p,\
                                                         "return1":ret1,"return2":ret2,"return3":ret3,"return4":ret4,"return5":ret5,\
                                                             "close1":p1,"close2":p2,"close3":p3,"close4":p4,"close5":p5},index=[k])
                        result = result.append(line)
                        result = result.reset_index(drop=True)
                        
                    else:
                        continue
                else:
                    continue
        else:
            continue
        print(str(j)+' round end')
    aaa='C:\\Users\\sywgqh\\Desktop\\Trainee_SunMin-master\\correlation_revised\\result\\'+'industry'+str(i)+'.csv'
    result.to_csv(aaa)
    All_result = All_result.append(result)         

reader.logoff()
All_result.to_csv('all_result.csv')      
                
            
        
        
        
    
    
    
    
    
        
    




    




