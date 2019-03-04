# encoding: UTF-8


import json
from collections import OrderedDict
import threading
from threading import Thread
from time import sleep
from datetime import datetime
from copy import copy


import functools
import time
import uuid
from collections import defaultdict
try:
    import queue
except ImportError:
    import Queue as queue

# import copy

import zmq #, zmq.asyncio
import msgpack #, snappy

class Client: # (IEndpoint):
    
    def __init__(self, ):
        # new context of client session
        self.context        = None # zmq.Context.instance()
        
        # push calls by logic threads into and 
        # pull out by the zmq thread  
        self.pull_socket    = None
        self.push_socket    = None

        # listen to remote zmq messages
        self.zmq_socket  = None 

        # identity of current endpoint, used as routing id for server side
        self.identity       =  uuid.uuid4().__str__()
        # address of the physical endpoint 
        self.endpoint       = "0.0.0.0"

        # max time allowed to send, recv of 0MQ
        self.timeout        = 3 # timeout

        # if closed to handle messages
        self._closed         = False

        # heartbeat settings
        self._heartbeat_interval    = 1
        self._heartbeat_timeout     = 3

        
        # Threads'  settings
        self._zmq_thread    = None

        # locks
        self._send_lock = threading.Lock()
        self._wait_lock = threading.Lock()
     
        # Waitable-Queues one for each thread
        self._zrpc_waitables      = {} # threading.local ()
        # waits for rpc call return
        self._zrpc_waitees    = {}

        # jsonrpc notifications &N connection events
        # self._zrpc_events = queue.Queue ()


    def start (self, endpoint):
        self.context        = zmq.Context.instance()

        self.zmq_socket = self.context.socket(zmq.DEALER)
        self.zmq_socket.setsockopt_string(zmq.IDENTITY, self.identity)
        #self.zmq_socket.setsockopt(zmq.SNDTIMEO, self.timeout*1000)
        #self.zmq_socket.setsockopt(zmq.RCVTIMEO, self.timeout*1000)
        self.zmq_socket.setsockopt(zmq.LINGER, 0)
        self.zmq_socket.setsockopt(zmq.PROBE_ROUTER, True)
        self.zmq_socket.setsockopt(zmq.RECONNECT_IVL_MAX, 1000)
        
        self.endpoint = endpoint
        # connect to the endpoint
        self.zmq_socket.connect (self.endpoint)

        # bridging frontend and backend  
        self.pull_socket = self.context.socket (zmq.PULL)
        self.pull_socket.bind ("inproc://rhino")
        self.push_socket = self.context.socket(zmq.PUSH)
        self.push_socket.connect("inproc://rhino")

        self._closed = False
        self._zmq_thread    = threading.Thread (target=self._zmq_loop,)
        self._zmq_thread.start()
        # 

    def close ( self, ):
        self._closed     = True
        self._zmq_thread.join()

    
    def call ( self, method, params, ):
        clientRequest = {
            "session": self.identity,
            "id": uuid.uuid4().__str__(),
            "method": method, 
            "params": params
        }
        print (clientRequest)
        
        call_id = clientRequest['id']
        
        tid = threading.current_thread().ident
        # no waitable queue on current thread, create a new one.
        with self._wait_lock:
            if tid not in self._zrpc_waitables:
                self._zrpc_waitables[ tid ]= queue.Queue()
        waitable = self._zrpc_waitables[tid]
        self._zrpc_waitees[call_id] = waitable
        
        self.send ( clientRequest)
        try:
            # wait to get result that zmq thread put into the waitable queue 
            resp = waitable.get( timeout=self.timeout)
            waitable.task_done()
        except queue.Empty:
            resp = None
        
        with self._wait_lock:
            del self._zrpc_waitees [call_id]

        if resp:
            return ( resp.get('code'), resp.get('payload'))
        else:
            return ( -1, "jsonrpc, timeout")

    def send ( self, payload):
        try:
            jsonData = msgpack.dumps(payload, encoding='utf-8')
            with self._send_lock:
                self.push_socket.send ( jsonData)
        except zmq.error.ZMQError as zmqerror:
            print ("0MQ: ", zmqerror)
    
    def heartbeat (self, ):
        clientRequest = {
            "session": self.identity,
            'jsonrpc' : '2.0',
            'method'  : 'rhino.heartbeat',
            'params'  : { 'time': time.time() },
        }
        self.send ( clientRequest)

        
    def _zmq_loop (self, ):

        heartbeat_ping = 0
        heartbeat_pong = 0
        poller = zmq.Poller()
        poller.register (self.pull_socket, zmq.POLLIN)
        poller.register ( self.zmq_socket, zmq.POLLIN)

        while not self._closed:
            try:
                if time.time() - heartbeat_pong > self._heartbeat_timeout:
                    # TODO: heartbeat timeout, retry to build connection 
                    print ("RPC: Heartbeat timeout")
                if time.time() - heartbeat_ping > self._heartbeat_interval:
                    self.heartbeat()
                    heartbeat_ping = time.time()

                ss = dict ( poller.poll(500) )
                if ss.get ( self.pull_socket) == zmq.POLLIN:
                    msgData = self.pull_socket.recv()
                    # if cmd.startswith (b"#"):
                    # print ("SEND:", msgData)
                    self.zmq_socket.send ( msgData )

                if ss.get(self.zmq_socket) == zmq.POLLIN:
                    data = self.zmq_socket.recv()
                    try:
                        msgData = msgpack.loads (data, encoding='utf-8')
                        # print ("RECV: ", msgData)
                        if not msgData:
                            print("RPC: Can't parse message data")

                        if msgData.get( 'method') == 'rhino.heartbeat':
                            heartbeat_pong = time.time()
                            continue

                        # print (msgData)
                        self.dispatch ( msgData)
                    except Exception as e:
                        print("RPC: handle msg failed, ", e)
                        pass
            except zmq.error.Again as zmqerror:
                print ("0MQ: RECV TIMEOUT: ", zmqerror)
            except Exception as e:
                print("RPC: recv data failed, ", e)

    
    def dispatch (self, responseData):

        call_id = responseData.get('id')
        if call_id is None: # rpc call return
            # TODO: raise it ??
            pass

        with self._wait_lock:
            waitable = self._zrpc_waitees.get (call_id )
            waitable.put ( responseData)




########################################################################
class RhinoClient:

    #----------------------------------------------------------------------
    def __init__(self, ):
        """Constructor"""
        super(RhinoClient, self).__init__()
        
        self.rpc_client = Client()
   
        self.orderDict = {}
        self.tradeSet = set()       # 保存成交编号的集合，防止重复推送
        
        # self.qryThread = Thread(target=self.qryData)
        
    #----------------------------------------------------------------------
    def connect(self, endpoint):
        """连接"""
        # 载入配置
        self.rpc_client.start ( endpoint=endpoint)
        # self.qryThread.start()
  
    #----------------------------------------------------------------------
    def sendOrder(self, symbol, side, qty, offset, start_time, stop_time, tag):
        """发单"""
        # order_delta_time = datetime.datetime.now() + datetime.timedelta( minutes=1)
        code, exchange = symbol.split(".")
        if exchange in ["SZ", "SH"]:
            acct_tag = 'CS'
        else:
            acct_tag = 'FUT'
        orderRequest = { 
            "exchange_id": exchange,
            "security_id": code,
            "security_code": symbol,
            "order_side": side,
            "order_offsetFlag": offset,
            "order_tag": tag,
            "order_qty" : int(qty),
            "order_price_limit": 0,
            'offer_start_time': start_time, # '09:30:00',
            'offer_stop_time': stop_time, # "COMMON_ORDER" # order_delta_time.strftime("%H:%M:%S") # 
            "order_place_method": "TWAP_KY_01",
            "order_place_settings": {
                'algo.style': 2,
                'algo.order_position': 'OP1',
                'algo.order_tick': 99 ,
                'algo.append_position': 'OP1',
                'algo.append_tick': 99,
                'algo.cancel_cycle': 30,
            },
        }

        code, payload = self.rpc_client.call ("broker-create-instruction", orderRequest)
        if code != 0:
            print (code, u'委托失败：%s' %payload)
            return ''
        
        orderId = payload
        print ("OrderID:", orderId)
        return orderId
 
    #----------------------------------------------------------------------
    def cancelOrder(self, orderId):
        """撤单"""
        code, payload = self.rpc_client.call("broker-cancel-instruction", { "order_id": orderId } )
        if code != 0 or not payload:
            print (code, u'撤单失败：%s' %payload)
            return

        return payload
    
    def getPositions (self, tag):
        code, payload = self.rpc_client.call("portfolio-realtime-positions", {"tag": tag})
        if code != 0:
            print (code, u'撤单失败：%s' %payload)
            return []

        return payload

    #----------------------------------------------------------------------
    def close(self):
        """关闭"""
        self.rpc_client.close()
     


my_client = RhinoClient()
my_client.connect( endpoint= "tcp://1.85.40.235:58086")

orders= [
    ( "600866.SH", "B" ,   74200 ),
    ( "000697.SZ", "S" ,   20100 ),
]

if True:
    for order in orders:
        p = my_client.sendOrder(order[0], order[1], order[2], "", "09:30:00", "14:57:00", "alpha01")
        print (p)

