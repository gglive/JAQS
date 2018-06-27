# encoding: UTF-8


import time


from jaqs.trade.tradegateway import RealTimeTradeApi
from jaqs.data.dataservice import RemoteDataService
            
# data_api = RemoteDataService()
# data_api.init_from_config ({
#     "remote.data.address": "tcp://127.0.0.1:58088",
#     "remote.data.username": "experimental",
#     "remote.data.password": "10086"
# })

trade_api = RealTimeTradeApi({
    #"remote.trade.address": "tcp://1.85.40.235:58086",
    "remote.trade.address": "tcp://127.0.0.1:58086",
    "remote.trade.username": "alpha01",
    "remote.trade.password": "10086"
})
trade_api.init_from_config({
    #"remote.trade.address": "tcp://1.85.40.235:58086",
    "remote.trade.address": "tcp://127.0.0.1:58086",
    "remote.trade.username": "alpha01",
    "remote.trade.password": "10086"
})


while True:

    print (time.time(), "----------------------------------")
    print ("Positions:")
    df_pos, msg = trade_api.query_position()
    print (df_pos)
# 
    print (time.time(), "----------------------------------")
    print ("Orders:")
    df_order, msg = trade_api.query_order()
    print (df_order)
    
    #print (time.time(), "----------------------------------")
    # task_id, msg = trade_api.place_order(
    #     "300036.SZ", "Buy", 0, 10000,
    #     algo='TWAP_KY_01', 
    #     algo_param={ 
    #         'algo.style': 2,
    #         'algo.order_position': 'OP1',
    #         'algo.order_tick': 99 ,
    #         'algo.append_position': 'OP1',
    #         'algo.append_tick': 99,
    #         'algo.cancel_cycle': 60,
    #         'start_time': '09:30:00',
    #         'stop_time': '14:30:00'
    #     }
    # )
    # print ('PlaceOrder: task_id ', task_id, msg)

    # print (time.time(), "----------------------------------")
    # result, msg = trade_api.cancel_order('#00000002')
    # 
    
    time.sleep(5)
