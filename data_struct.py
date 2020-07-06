# author='lwz'
# coding:utf-8
# 抽取数据
'''
MongoDB数据库数据类型说明
'''

from enum import IntEnum


# 订单内容
class OrderType(IntEnum):
    cancel = 0          # 撤单
    deal = 1            # 成交
    limit = 2           # 限价
    ourbest = 3         # 本方最优
    market = 4          # 市价


# 订单方向
class OrderSide(IntEnum):
    cancel = 0          # 撤单
    buy = 1             # 买入
    sell = 2            # 卖出


# 订单最终状态
class OrderFinalStatus(IntEnum):
    sended = 0          # 已报
    dealed = 1          # 已成
    canceled = 2        # 已撤




