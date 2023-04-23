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
import copy
import pika
import uuid

from queue import Queue
from logger import *
from rmq_comms import BrokerProducer

class Producers(BrokerProducer):
    def __init__(self, cfg, loggername=None):
        super(Producers, self).__init__(cfg, loggername)
        self.logger = logging.getLogger(loggername) #logger
        self.cfg = cfg
        self.queue = self.cfg['exchange_key']

    def send(self, msg):
        properties = pika.BasicProperties(app_id=self.app_id,
                                          content_type='application/json',
                                          correlation_id=uuid.uuid4().hex,
                                          message_id=None)

        # if self.loggername is not None:
        #     self.logger.debug('Publishing "{:s}": {:s}'.format(msg['rmq']['routing_key'],
        #                                                        json.dumps(msg)))
        self.channel.basic_publish(exchange=self.exchange,
                                   routing_key = msg['rmq']['routing_key'],
                                   body = json.dumps(msg),
                                   properties = properties)

    def get_connection_state(self):
        return self.connected



class RabbitMQ_Thread(threading.Thread):
    """
    Title: RabbitMQ Thread
    Project: Multiple (HW Control Daemons)
    Version: 1.0.0
    Date: Oct 2022
    Author: Zach Leffke, KJ4QLP

    Purpose:
        Handles RabbitMQ Interface

    Design Logic:
        Thread creates a Consumer Class with one connection to the broker
        Within the Connection, One Channel is Created
        Within the Channel, Two Queues are created, one per consumer key

    """
    def __init__ (self, cfg, log_name, parent):
        # threading.Thread.__init__(self)
        threading.Thread.__init__(self, name='RMQ_Thread')
        self._stop  = threading.Event()
        self.cfg    = cfg
        self.parent = parent #Parent is Tracker Class
        self.type   = self.cfg['type'] #Daemon or Client
        self.log_name = log_name
        self.logger = logging.getLogger(self.log_name) #main logger
        self.logger.info("Initializing {:s}".format(self.name))

        self.tx_q  = Queue() # Messages to broadcast
        self.tlm_q = Queue() # Thread control Queue

        self._init_producers()
        self.connected = False
        self.tlm = { #Thread control Telemetry message
            "type":"TLM", #Message Type: Thread Telemetry
            "connected":False, #Socket Connection Status
            "tx_count":0, #Number of received messages from socket
        }

    def run(self):
        self.logger.info('Launched {:s}'.format(self.name))
        # start producer
        self.produce_thread.start()
        self.logger.info('Producer Started')

        while (not self._stop.isSet()):
            # if (self.consumer.get_connection_state() and self.producer.get_connection_state()):
            if self.connected != self.producer.get_connection_state():
                self.connected = self.producer.get_connection_state()
                self.tlm['connected']=self.connected
                self.tlm_q.put(self.tlm)

            if self.connected:
                if not self.tx_q.empty():#Message to send
                    tx_msg = self.tx_q.get()
                    if self.cfg['type']=="daemon":
                        self.producer.send(tx_msg)
                    elif self.cfg['type']=="client":
                        self._send_message_cli(tx_msg)

            time.sleep(0.000001)#needed to throttle


        self.producer.stop_producing()
        time.sleep(1)

        self.producer.stop()
        self.logger.warning('{:s} Terminated'.format(self.name))
        sys.exit()
        #sys.exit()

    def update_session_id(self, id):
        self.session_id = id

    def _send_message(self, msg):
        # self.producer.send(msg)
        rmq_msg.update({
            "rmq":{
                "exchange":         self.cfg['exchange'],
                "app_id":           self.cfg['app_id'],
                "content_type":     "application/json",
                "message_id":       None
            },
            "msg":msg
        })
        pass

    def _init_producers(self):
        self.logger.info("Initializing Producer")
        self.producer = Producers(self.cfg, loggername=self.log_name)
        self.produce_thread = threading.Thread(target=self.producer.run, name = 'Producer')
        self.produce_thread.daemon = True

    #### END Socket and Connection Handlers ###########
    def get_tlm(self):
        self.tlm['connected'] = self.connected
        # self.tlm_q.put(self.tlm)
        return self.tlm

    def stop(self):
        #self.conn.close()
        self.logger.info('{:s} Terminating...'.format(self.name))
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()
