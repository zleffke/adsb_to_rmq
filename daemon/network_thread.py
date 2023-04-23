#!/usr/bin/env python
################################################################################
#   Title: KISS Network
# Project: VTGS
# Version: 1.0.0
#    Date: Jan, 2020
#  Author: Zach Leffke, KJ4QLP
# Comments:
#   -This is the user interface thread
################################################################################

import threading
import time
import datetime
import socket
import errno
import json

from queue import Queue
from logger import *

class Thread_TCP_Client(threading.Thread):
    """
    Title: TCP Client Thread
    Project: Multiple
    Version: 0.0.1
    Date: Jan 2020
    Author: Zach Leffke, KJ4QLP

    Purpose:
        Handles TCP Interface to a server

    Args:
        cfg - Configurations for thread, dictionary format.
        parent - parent thread, used for callbacks

    """
    def __init__ (self, cfg, log_name, parent):
        # threading.Thread.__init__(self)
        threading.Thread.__init__(self, name='net_thread')
        self._stop  = threading.Event()
        self.cfg    = cfg
        self.parent = parent #Parent is Tracker Class
        #self.setName(self.cfg['thread_name'])
        # self.logger = logging.getLogger(self.cfg['main_log'])
        self.logger = logging.getLogger(log_name) #main logger
        # self.logger = logger

        self.rx_q   = Queue() # Data from device, rx over socket
        self.tx_q   = Queue() # Data to Device, tx over socket
        #self.tlm_q  = Queue() # for thread monitoring, status, etc

        self.connected = False
        self.logger.info("Initializing {}".format(self.name))

        self.data_logger = None
        self.tlm = { #Thread control Telemetry message
            "type":"TLM", #Message Type: Thread Telemetry
            "connected":False, #Socket Connection Status
            "rx_count":0, #Number of received messages from socket
            "tx_count":0, #Number of messages sent to socket
            "ip":self.cfg['ip'], #Socket IP
            "port":self.cfg['port'], #Socket Port
        }
        self.rx_data = {  #Raw Data Message received from Socket
            'type': 'RX', #Message Type: RX (received message)
            'ts':None,
            'msg': None
        }

    def run(self):
        self.logger.info('Launched {:s}'.format(self.name))
        self._init_socket()
        while (not self._stop.isSet()):
            if not self.connected:
                try:
                    self._attempt_connect()
                except socket.error as err:
                    if err.args[0] == errno.ECONNREFUSED:
                        self.connected = False
                        time.sleep(self.cfg['retry_time'])
                except Exception as e:
                    self._handle_socket_exception(e)
            else:
                try:
                    # data = self.sock.recvfrom(1024) #Blocking/Timeout
                    data = self._handle_socket_buffer()

                    self._handle_recv_data(data)

                except socket.timeout as e: #Expected after connection
                    self._handle_socket_timeout()
                except Exception as e:
                    self._handle_socket_exception(e)
            time.sleep(0.000001)

        self.sock.close()
        self.logger.warning('{:s} Terminated'.format(self.name))
        #sys.exit()

    def _handle_socket_buffer(self):
        while 1:
            data = self.sock.recv(1)[0]
            if data == 0x06:
                self.logger.debug("Received QPT-500 ACK: {:02x}".format(data))
            elif data == 0x03:
                self.sock_buff.append(data)
                break
            else:
                self.sock_buff.append(data)
        self.rx_data['datetime'] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        buff = self.sock_buff
        self.sock_buff = bytearray()
        return buff


    def _start_logging(self):
        self.cfg['log']['startup_ts'] = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        print (self.cfg['log'])
        setup_logger(self.cfg['log'])
        self.data_logger = logging.getLogger(self.cfg['log']['name']) #main logger
        for handler in self.data_logger.handlers:
            if isinstance(handler, logging.FileHandler):
                self.logger.info("Started {:s} Data Logger: {:s}".format(self.name, handler.baseFilename))

    def _stop_logging(self):
        if self.data_logger != None:
            handlers = self.data_logger.handlers[:]
            print (handlers)
            for handler in handlers:
                if isinstance(handler, logging.FileHandler):
                    self.logger.info("Stopped Logging: {:s}".format(handler.baseFilename))
                handler.close()
                self.data_logger.removeHandler(handler)
            self.data_logger = None

    #### Socket and Connection Handlers ###########
    def _handle_recv_data(self, data):
        # print(data.hex())
        try:
            if self.data_logger != None:
                self.data_logger.info("RX: {:s}".format(data.hex()))
            self.tlm['rx_count'] += 1
            self.rx_data['msg'] = data.hex()
            self.rx_q.put(self.rx_data)
        except Exception as e:
            self.logger.debug("Unhandled Receive Data Error")
            self.logger.debug(sys.exc_info())

    def _handle_socket_timeout(self):
        if not self.tx_q.empty():
            msg = self.tx_q.get()
            if ((msg['type']=="CMD") and (msg['cmd']=="SEND")):
                self._send_msg(msg['msg'])
            elif msg['type']=="CTL":
                if msg['cmd']=='RESET': self._reset()

    def _send_msg(self, msg):
        buff = bytearray.fromhex(msg['hex'])
        self.logger.info("Sending: {:s}: 0x{:s}".format(msg['name'], buff.hex()))
        self.sock.sendall(buff)
        self.tlm['tx_count'] += 1
        time.sleep(0.1)

    def _reset(self):
        self.logger.info("Resetting Packet Counters")
        self.tlm['tx_count'] = 0
        self.tlm['rx_count'] = 0

    def get_tlm(self):
        self.tlm['connected'] = self.connected
        # self.tlm_q.put(self.tlm)
        self.rx_q.put(self.tlm)

    def _attempt_connect(self):
        self.logger.info("Attempting to connect to: [{:s}, {:d}]".format(self.cfg['ip'],
                                                                         self.cfg['port']))
        self.sock.connect((self.cfg['ip'], self.cfg['port']))
        self.logger.info("Connected to: [{:s}, {:d}]".format(self.cfg['ip'],
                                                             self.cfg['port']))

        time.sleep(0.01)
        self.sock.settimeout(self.cfg['timeout'])   #set socket timeout
        self.connected = True
        self.tlm['connected']=self.connected
        self.tx_q.queue.clear()
        self.rx_q.queue.clear()
        #self.parent.set_connected_status(self.connected)
        # self.tlm_q.put(self.tlm)
        self.rx_q.put(self.tlm)
        # self._start_logging()


    def _handle_socket_exception(self, e):
        self.logger.debug("Unhandled Socket error")
        self.logger.debug(sys.exc_info())
        self._reset_socket()

    def _reset_socket(self):
        self.logger.debug('resetting socket...')
        self.sock.close()
        self.connected = False
        #self.parent.set_connected_status(self.connected)
        self.tlm['connected']=self.connected
        # self.tlm_q.put(self.tlm)
        self.rx_q.put(self.tlm)
        # self._stop_logging()
        self._init_socket()

    def _init_socket(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #TCP Socket, initialize
        self.sock_buff = bytearray()
        self.logger.debug("Setup socket")

    #### END Socket and Connection Handlers ###########


    def stop(self):
        #self.conn.close()
        self.logger.info('{:s} Terminating...'.format(self.name))
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()
