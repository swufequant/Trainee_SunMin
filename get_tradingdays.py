# -*- coding: utf-8 -*-
"""
Created on Thu Jul 23 18:29:25 2020

@author: sywgqh
"""
import pandas as pd
import tushare as ts
ts.set_token('76ee399780bf632e49e25058355ee686ca0b88521981086f2afe9f89')   
pro = ts.pro_api()

TrdCal=pro.trade_cal()
TrdCal.to_csv("./TrdCal.csv")


