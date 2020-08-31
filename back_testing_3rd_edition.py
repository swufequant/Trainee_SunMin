'''
Rewriting backtesting 3rd edtion:
    
    1.读取概念变动/交易日历/已经准备好的涨停信息数据
    2. 提取单个三级板块的各种数据
    3.计算每一条涨停信息下的各项数据
      
'''

import pandas as pd
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
# Step 1: 准备通用可以直接调取的数据
# 1.读取交易日历，获取涨停股票列表
TrdCalendar = pd.read_csv('TrdCal.csv')
TrdCalendar.sort_values(['cal_date'],inplace=True) #原始的交易日历，备用
calendar = TrdCalendar[TrdCalendar['is_open']==1]
calendar = calendar[calendar['cal_date']>20140101]
calendar = calendar[calendar['cal_date']<20200810]
calendar = calendar.reset_index(drop=True)
calendar = calendar['cal_date'] #交易日历色series 
calendar = list(calendar)

# 2. 提取回测期所有的涨停股票信息，保留在本地，提升效率
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

uplimit = pd.read_csv('uplimit.csv') #之前已经download好的数据，读进来
uplimit = uplimit[['code','date']] #总的涨停信息

# 3.股票信息列表，主要用到行业信息，成立日期
stockInfo = reader.QueryStockInfo()
stockInfo['listdate'] = stockInfo['list_date'].apply(lambda x: int(x))
stockInfo = stockInfo[stockInfo['listdate']<(max(uplimit['date']))-10000-300] #取掉上市不足一年+3个月的股票
stockInfo = stockInfo[stockInfo['L3Name']!='']
stockInfo.rename(columns = {"code":"newcode","symbol":"code"},inplace=True)
stockInfo['code'] = stockInfo['code'].apply(lambda x: int(x))
stockInfo = stockInfo[['newcode','code','name','listdate','L3','L3Name']]
stockInfo = stockInfo.sort_values(['L3','listdate'])
#把涨停信息和行业信息合并：
uplimit = pd.merge(uplimit,stockInfo,how='left',on=['code'])
uplimit = uplimit[pd.isna(uplimit['L3'])==False]

# 4. 三级行业板块列表
industry = list(set(uplimit['L3']))
# 共227个三级行业，逐一取每个行业的同板块股票作为数据集，计算相关数据
# 涨停数据里面只有217个行业

#==============================================================================
# Step 2：提取同一个板块内进行回测所需要的数据————同板块的涨停数据，历史日线数据,板块指数数据
def getData(start_dt,end_dt,L3): #填入三级板块代码L3 

    uplimit_df = uplimit[uplimit['L3']==L3] #同行业的所有历史涨停信息
    uplimit_df = uplimit_df[(uplimit_df['date']>=start_dt)&(uplimit_df['date']<=end_dt)]
    uplimit_df = uplimit_df.reset_index(drop=True)
    
    stockinfo_df = stockInfo[stockInfo['L3']==L3] #同行业的所有股票
    stockinfo_df = stockinfo_df[stockinfo_df['listdate']<=start_dt-10000]
    stockinfo_df = stockinfo_df.reset_index(drop=True)
    
    index_df = reader.QueryIndexDayLine(start_dt-10000,end_dt,L3) #提取指数数据
    index_df = index_df[['date','code','close','pre_close','count']]
    
    dayline_df = pd.DataFrame() #存储板块内日线数据
    for i in range(0,len(stockinfo_df)-1):
        dfi = reader.QueryStockDayLine(start_dt-10000,end_dt,stockinfo_df.at[i,'newcode'])
        dayline_df = dayline_df.append(dfi)
    dayline_df['ret'] = np.log(dayline_df['close']/dayline_df['pre_close'])*100
    dayline_df = dayline_df[['code','date','close','ret']]
    
    return uplimit_df,stockinfo_df,index_df,dayline_df

#==============================================================================
# Step 3：计算每条涨停信息下的股票过去一年的相关性和协整检验
start_dt=20140101
end_dt = 20200825

df = getData(start_dt,end_dt,industry[4])
def character(df):
    result = pd.DataFrame()
    if pd.isna(df)==False: #先判断上一步函数提取的数据是不是为空，不为空再继续
        # 1. 拆解数据
        uplimit_df = df[0]
        stockinfo_df = df[1]
        index_df = df[2]
        dayline_df = df[3]
       
        # 2. 对uplimit_df中每一条涨停数据循环
        try:
            for i in range(0,len(uplimit_df)-1):
                #(1) 确定涨停股票的一些相关信息
                upcode = uplimit_df.at[i,'newcode']
                update = uplimit_df.at[i,'date']
                upname = uplimit_df.at[i,'name']
                upindustry = uplimit_df.at[i,'L3Name'] 
                indexcode = uplimit_df.at[i,'L3']
                
                #(2)确定一些需要用到的日期
                a = calendar.index(update)
                back_dt = calendar[a-240]
                d5 = calendar[a+5]
                
                if uplimit_df.at[i,'listdate']<=back_dt:    #要保证涨停股票已经成立一年  ###(3)
                
                    #3. 对板块内每只股票循环
                    try:
                    for j in range(0,len(stockinfo_df)-1):
                        if stockinfo_df.at[j,'newcode']!=upcode: #要保证计算的股票不是涨停股票  ###(2)
                    
                            if stockinfo_df.at[j,'listdate']<=back_dt: ###(1)
                                try:
                                    
                                    #(1)准备需要用的数据子集
                                    stockdata = dayline_df[dayline_df['code']==stockinfo_df.at[j,'newcode']]
                                    updata = dayline_df[dayline_df['code']==upcode]
                                    indexdata = index_df[index_df['code']==indexcode]
                                    indexdata = indexdata.rename(columns={'close':'index'})
                                    indexdata = indexdata[['date','index']]
                                    
                                    cal1 = TrdCalendar[(TrdCalendar['cal_date']>=back_dt)&(TrdCalendar['cal_date']<=d5)]
                                    cal1 = cal1[cal1['is_open']==1]
                                    cal1 = cal1[['cal_date','is_open']]
                                    cal1 = cal1.rename(columns={'cal_date':'date'})
                                    
                                    dfuse = pd.merge(cal1,updata,how='left',on='date')
                                    dfuse = dfuse[['date','code','close']]
                                    dfuse = dfuse.rename(columns={'code':'upcode','close':'upclose'})
                                    dfuse = pd.merge(dfuse,stockdata,how='left',on='date')
                                    dfuse = pd.merge(dfuse,indexdata,how='left',on='date')
                                    
                                    set1 = dfuse[dfuse['date']<update]
                                    set1 = set1[~pd.isna(set1['upclose'])==True]
                                    set1 = set1[~pd.isna(set1['close'])==True] #set1用来算相关性，zscore，协整性（不可以有空值）
                                    set2 = dfuse[dfuse['date']>=update] #set2用来填入未来几天的持有期收益率
                                    set2 = set2.sort_values('date').reset_index(drop=True)
                                    
                                    #(2)计算相关性
                                    corr = round(set1['upclose'].corr(set1['close']),4) #涨停股票和其他股票的相关性
                                    corr_idx = round(set1['upclose'].corr(set1['index']),4) #涨停股票与指数的相关性
                                    corr_idxstk = round(set1['close'].corr(set1['index']),4) #其他股票和指数的相关性
                                    
                                    #(3)计算价差/价差均值方差/zscore
                                    set1['diff'] = set1['close']-set1['upclose']
                                    mean = np.mean(set1['diff'])
                                    std = np.std(set1['diff'])
                                    zscore = round((set2.at[0,'close']-set2.at[0,'upclose']-mean)/std,4)
                                    
                                    #(4) 计算协整性的p值
                                    coint_diff = coint(set1['close'],set1['upclose'])
                                    coint_p = round(coint_diff[1],4)
                            
                                    #(5) 将结果写入一行
                                    line = pd.DataFrame({"date":update,"breakcode":upcode,"breakname":upname,"industry":upindustry,\
                                                         "stockcode":stockinfo_df.at[j,'newcode'],"stockret":set2.at[0,'ret'],\
                                                             "index_price":set2.at[0,'index'],\
                                                                 "corr":corr,"corr_upidx":corr_idx,"corr_stkidx":corr_idxstk,\
                                                                     "zscore":zscore,"coint_p":coint_p,\
                                                                         "ret1":set2.at[1,'ret'],"ret2":set2.at[2,'ret'],"ret3":set2.at[3,'ret'],\
                                                                             "ret4":set2.at[4,'ret'],"ret5":set2.at[5,'ret']},index=[j])
                                    result = result.append(line)
                                except:
                                    pass
                            else:
                                continue ###(1)
                        else:
                            continue ###(2)
                else:
                    continue ###(3)
        except:
            pass
    return result

df = getData(start_dt,end_dt,industry[6])
resulttry = character(df)    










