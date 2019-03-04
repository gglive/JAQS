# encoding: UTF-8


import datetime, time
from collections import defaultdict 

from jaqs.trade import model
from jaqs.trade.tradegateway import RealTimeTradeApi
from jaqs.data.dataservice import RemoteDataService

import pymysql
import sqlalchemy

from openpyxl import Workbook
from openpyxl import load_workbook
from openpyxl.writer.excel import ExcelWriter

from WindPy import w

w.start ()

today = datetime.datetime.today().strftime("%Y-%m-%d")
print (today)

prev_tday = w.tdaysoffset (-1, today, "").Times[0]
print (prev_tday)

# Connect to the database
connection = pymysql.connect(
    host='1.85.40.235',
    port=28888,
    user='kods',
    password='Abc1234#',
    db='strategy_kaiyuan',
    cursorclass=pymysql.cursors.DictCursor
)

portfolios = {}

holdings = {}
positions = defaultdict (dict)

try:
    with connection.cursor() as cursor:
        sql = "SELECT `trade_day`, `cap_val`, `pct_val`, `tag` FROM `_legacy_pnl_2` WHERE `trade_day`=%s"
        cursor.execute(sql, (prev_tday,))
        result = cursor.fetchall ()
        for record in result:
            portfolios[ record['tag']] = record

    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT `trade_day`, `security_id`, `bsflag`, `held_avgpx`, `held_qty`, `tag` FROM `_legacy_position` WHERE `trade_day`=%s AND `tag` IS NOT NULL"
        cursor.execute(sql, (prev_tday,))
        result = cursor.fetchall ()
        for record in result:
            holdings[record['security_id']] = { "security_id": record['security_id'] } 
            positions[record['tag']][record['security_id']] = record
finally:
    connection.close()





orders = {}

#import sys
#if argv[1].upper() == '-R':
    
workbook = load_workbook ( filename=u"C://Users/admin/Documents/alpha/_Trading.xlsx", data_only=True )

###
worksheet = workbook.get_sheet_by_name ('weights')
values = worksheet.values
cols = next ( values)
rows = list ( values) 

for row in rows:
    p = portfolios [row[0]]
    p['c'] = row[1]
    p['w'] = row[2]
    p['s'] = row[3]
    p['op'] = row[4]


worksheet = workbook.get_sheet_by_name ("orders")
print ( worksheet.min_row, worksheet.max_row)
print ( worksheet.min_column, worksheet.max_column)
values = worksheet.values
cols = next ( values)
rows = list ( values) 

for row in rows:
    cmd = {}
    for n, col in enumerate(cols):
        cmd[col] = row[n]
    
    holdings[cmd['security_id']] = { "security_id": cmd['security_id'] } 
    orders[ cmd['tag'] + '-' + cmd['security_id'] ] = cmd


wData = w.wss(",".join(holdings.keys()), "sec_name,pre_close",f"tradeDate={today};priceAdj=U;cycle=D")
for n, code in enumerate(wData.Codes):
    hold = holdings[code]
    hold['security_name'] = wData.Data[0][n]
    hold['prev_close'] = wData.Data[1][n]


for cmd in orders.values():
    ###
    # 准备订单
    #
    if cmd ['order_side'] == 'S':
        position = positions[cmd['tag']].pop (cmd['security_id'])
        cmd['order_qty'] = position['held_qty']
        cmd['order_side_impl'] = 'Sell'
    else:
        portfolio = portfolios[ cmd['tag'] ]
        hold = holdings[ cmd['security_id'] ]
        cmd['order_qty'] = int(round( portfolio['op']/ hold['prev_close']/100,0 ) * 100) # position['price_settlement']
        cmd['order_side_impl'] = 'Buy'
    
    ###
    print (f"Cmd: {cmd['order_side_impl']} - Security:{cmd['security_id']}, Qty:{cmd['order_qty']}, {cmd['tag']}")
    ###
    orders[ cmd['tag'] + '-' + cmd['security_id'] ] = cmd


# 仓位调整
for portfolio in portfolios:
    if not 'c' in portfolio:
        continue

    v0 = portfolio['cap_val'] * portfolio['pct_val']
    v1 = portfolio['c'] * portfolio['w']
    
    delta = v0 - v1
    diff = abs ( delta )
    if diff < 10000:
        continue
    
    pd = positions[ portfolio['tag'] ] 
    if delta < 0:
        for position in pd.values():
            hold = holdings[ position['security_id'] ]
            cmd = {
                'security_id': position['security_id'],
                'order_side': 'B',
                'order_side_impl': 'Buy',
                'order_qty': int(round( diff / portfolio['s'] / hold['prev_close'] / 100, 0)* 100) 
            }
    else:
        for position in pd.values():
            cmd = {
                'security_id': position['security_id'],
                'order_side': 'S',
                'order_side_impl': 'Sell',
                'order_qty': int(round( diff / portfolio['s'] / hold['prev_close'] / 100, 0)* 100) 
            }
    ###
    print (f"Cmd: {cmd['order_side_impl']} - Security:{cmd['security_id']}, Qty:{cmd['order_qty']}, {cmd['tag']}")
    ###
    orders[ cmd['tag'] + '-' + cmd['security_id'] ] = cmd

# import signal

yesOrNo = input("确认下单（Y/N）：")
if yesOrNo.upper() == 'Y':

    trade_api = RealTimeTradeApi({
        "remote.trade.address": "tcp://1.85.40.235:58086",
        "remote.trade.username": 'diryox',
        #"remote.trade.address": "tcp://127.0.0.1:58086",
        #"remote.trade.username": "rqalpha",
        "remote.trade.password": "10086"
    })
    trade_api.init_from_config({
        "remote.trade.address": "tcp://1.85.40.235:58086",
        "remote.trade.username": "diryox",
        # "remote.trade.address": "tcp://127.0.0.1:58086",
        # "remote.trade.username": "rqalpha",
        "remote.trade.password": "10086"
    })
    context = model.Context(data_api=None, trade_api=trade_api, instance=None, strategy=None, pm=None)
    
    #
    for order in orders.values():
        order_id, msg = trade_api.place_order( 
            order['security_id'], order['order_side_impl'], 0, order['order_qty'],
            algo='TWAP_KY_01', 
            algo_param={ 
                'algo.style': 2,
                'algo.order_position': 'OP1',
                'algo.order_tick': 99 ,
                'algo.append_position': 'OP1',
                'algo.append_tick': 99,
                'algo.cancel_cycle': 60,
                'start_time': '14:30:00',
                'stop_time':  '14:57:00'
            },
            userdata=order['tag']
        )
        print (f"Order: Id:{order_id} - Security:{order['security_id']}, Qty:{order['order_qty']}, {order['tag']}")


