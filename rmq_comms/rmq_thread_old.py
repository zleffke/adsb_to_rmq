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
from rmq_comms import BrokerConsumer
from rmq_comms import BrokerProducer

class Consumers(BrokerConsumer):
    def __init__(self, cfg, msgs, loggername=None):
        super(Consumers, self).__init__(cfg, loggername)
        self.logger = logging.getLogger(loggername) #logger
        self.msgs = msgs
        self.cfg = cfg
        self.type = cfg['type']
        self.q  = Queue() #place received messages here.

        self.queue = self.cfg['consume_key']
        self.routing_key = "{:s}.*.{:s}.#".format(self.cfg['exchange_key'],
                                                  self.cfg['consume_key'])
        self.rmq_msg={}

    def start_consuming(self):
        """Begin receiving messages.  We'll register the on_message function
        as a callback for message receipt.
        """
        if self.loggername is not None:
            self.logger.debug('Registering as consumer with broker')
        self.add_on_cancel_callback()
        # self.consumer_tag = self.channel.basic_consume(on_message_callback = self.on_message,
        #                                                queue = self.queue)
        self.channel.basic_consume(queue = self.queue,
                                   on_message_callback = self.on_message,
                                   consumer_tag = self.routing_key)

    def process_message(self, channel, method, properties, body):
        rmq_msg={
            "rmq":{
                "routing_key": method.routing_key,
                "exchange": method.exchange,
                "delivery_tag": method.delivery_tag,
                "app_id": properties.app_id,
                "content_type": properties.content_type,
                "correlation_id": properties.correlation_id,
                "message_id":properties.message_id
            },
            "msg":json.loads(body)
        }
        if self.loggername is not None:
            self.logger.debug('Received message {:s}'.format(json.dumps(rmq_msg)))
        self.q.put(rmq_msg)


    def get_connection_state(self):
        return self.connected

class Producers(BrokerProducer):
    def __init__(self, cfg, msgs, loggername=None):
        super(Producers, self).__init__(cfg, loggername)
        self.logger = logging.getLogger(loggername) #logger
        self.msgs = msgs
        self.cfg = cfg
        self.q  = Queue() #place received messages here.

        self.queue = self.cfg['produce_key']
        self.routing_key = "{:s}.*.{:s}.#".format(self.cfg['exchange_key'],
                                                  self.cfg['produce_key'])

    def send(self, msg):
        # print(json.dumps(msg,indent=2))
        key = msg['rmq']['routing_key']


        # corr_id = msg['rmq']['correlation_id']
        if "tc" in key:
            msg_id = uuid.uuid4().hex
        else:
            msg_id = msg['rmq']['message_id']
        properties = pika.BasicProperties(app_id=self.app_id,
                                          content_type='application/json',
                                          correlation_id=msg['rmq']['correlation_id'],
                                          message_id=msg_id)
        # print(self.msgs.keys())
        if key in self.msgs.keys():
            new_msg = self.msgs[key]
            new_msg['header']['session_id'] = msg['rmq']['correlation_id']
            new_msg['parameters'] = msg['params']

        # print(new_msg)
        if self.loggername is not None:
            self.logger.debug('Publishing message bound for "{:s}": {:s}'.format(key, json.dumps(new_msg)))
        self.channel.basic_publish(exchange=self.exchange,
                                   routing_key = msg['rmq']['routing_key'],
                                   body = json.dumps(new_msg),
                                   properties = properties)

    def send_old(self, msg):
        print(msg)
        if self.loggername is not None:
            self.logger.debug('Publishing message bound for "{}"'.format(key))

        corr_id = json.loads(msg)['header']['session_id']
        msg_id = uuid.uuid4().hex
        properties = pika.BasicProperties(app_id=self.app_id,
                                          content_type='application/json',
                                          correlation_id=corr_id,
                                          message_id=msg_id)
        self.channel.basic_publish(exchange=self.exchange,
                                   routing_key = key,
                                   body = msg,
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
    def __init__ (self, cfg, msgs, log_name, parent):
        # threading.Thread.__init__(self)
        threading.Thread.__init__(self, name='RMQ_Thread')
        self._stop  = threading.Event()
        self.cfg    = cfg
        self.parent = parent #Parent is Tracker Class
        self.type   = self.cfg['type'] #Daemon or Client

        self.log_name = log_name
        self.logger = logging.getLogger(self.log_name) #main logger
        self.logger.info("Initializing {:s}; Mode: {:s}".format(self.name, self.type))

        self.rx_q   = Queue() # Track_Service <- rx_q
        self.tx_q   = Queue() # Track_Service -> tx_q

        self.header = msgs['header']
        self.msgs   = msgs['msgs']
        self.rmq    = {} #Rabbit MQ Key/Value Messages
        self.rmq_c  = {} #RabbitMQ Key/Value Consumer Messages ('tc')
        self.rmq_p  = {} #RabbitMQ Key/Value Consumer Messages ('tc')
        self.x_key  = self.cfg['exchange_key']
        self._init_rmq_messages() #extract necessary messages
        self._init_consumers()
        self._init_producers()

        self.connected = False
        self.corr_id = None
        self.last_msg_id = None

    def run(self):
        self.logger.info('Launched {:s}'.format(self.name))

        #Start consumer
        self.consume_thread.start()
        self.logger.info('Consumer Started')
        # start producer
        self.produce_thread.start()
        self.logger.info('Producer Started')

        while (not self._stop.isSet()):
            # if (self.consumer.get_connection_state() and self.producer.get_connection_state()):
            if (self.consumer.get_connection_state() and self.producer.get_connection_state()):
                self.connected = True
            else:
                self.connected = False

            if self.connected:
                if (not self.consumer.q.empty()): #received a message on command q
                    rx_msg = self.consumer.q.get()
                    self.rx_q.put(rx_msg)
                elif (not self.tx_q.empty()):#essage to send
                    tx_msg = self.tx_q.get()
                    if self.cfg['type']=="daemon":
                        self._send_message(tx_msg)
                    elif self.cfg['type']=="client":
                        self._send_message_cli(tx_msg)

            time.sleep(0.01)#needed to throttle

        self.consumer.stop_consuming()
        self.producer.stop_producing()
        time.sleep(1)
        self.consumer.stop()
        self.producer.stop()
        self.logger.warning('{:s} Terminated'.format(self.name))
        sys.exit()
        #sys.exit()

    def update_session_id(self, id):
        self.session_id = id

    def _send_message(self, msg):
        # key = msg['rmq']['routing_key']
        self.producer.send(msg)

    def _send_message_cli(self, msg):
        # print(msg)
        # if "tc" in msg['key']:
        #     msg_id = uuid.uuid4().hex
        #     print(type(msg_id))
        #     self.last_msg_id = msg_id

        rmq_msg={
            "rmq":{
                "routing_key":      msg['key'],
                "exchange":         self.cfg['exchange'],
                "app_id":           self.cfg['app_id'],
                "content_type":     "application/json",
                "correlation_id":   msg['msg']['header']['session_id'],
                "message_id":       None
            },
            "params":msg['msg']['parameters']
        }
        # print(msg)
        #msg['msg']['header']['session_id'] = corr_id
        # key = msg['key']
        # body = json.dumps(msg['msg'])
        self.producer.send(rmq_msg)


    #---MESSAGE Import and Parsing---------
    def _init_rmq_messages(self):
        self.c_keys = [self.x_key] #initialize key list with exchange name
        self.iterdict(self.msgs)
        #print(json.dumps(self.rmq, indent=4))

        for k,v in self.rmq.items():
            if self.type == "daemon":
                if   'tc' in k: self.rmq_c[k] = v #We Consume Telecommands
                elif 'tm' in k: self.rmq_p[k] = v #We Produce Telemetry
            elif self.type == "client":
                if   'tm' in k: self.rmq_c[k] = v #We Consume Telemetry
                elif 'tc' in k: self.rmq_p[k] = v #We Produce Telecommands
            else: self.logger.warning('Invalid Key/Value pair in RMQ messages detected: {:s}:{:s}'.format(k,v))

        for k,v in self.rmq_c.items():
            if   self.type == "daemon": self.logger.debug("Imported Consumer (tc) Message: {:s}".format(k))
            elif self.type == "client": self.logger.debug("Imported Consumer (tm) Message: {:s}".format(k))
        for k,v in self.rmq_p.items():
            if   self.type == "daemon": self.logger.debug("Imported Producer (tm) Message: {:s}".format(k))
            elif self.type == "client": self.logger.debug("Imported Producer (tc) Message: {:s}".format(k))
        self.logger.info("RMQ Message conversion complete")

    def iterdict(self, d):
        for k,v in d.items():
            if isinstance(v, dict):
                if k == "parameters":
                    rmq_k = ".".join(self.c_keys)
                    rmq_m = {}
                    rmq_m['header'] = self.header
                    rmq_m[k] = v
                    self.rmq[rmq_k]=rmq_m
                    # print (rmq_k, ":", rmq_m)
                else:
                    self.c_keys.append(k)
                    self.iterdict(v)
        self.c_keys = self.c_keys[:-1]

    def get_rmq_messages(self, key=None):
        if key == "producers": return self.rmq_p
        if key == "consumers": return self.rmq_c
        else:                  return self.rmq

    def _init_consumers(self):
        self.logger.info("Initializing Consumers")
        self.consumer = Consumers(self.cfg, self.rmq_c, loggername=self.log_name)
        self.consume_thread = threading.Thread(target=self.consumer.run, name = 'Serv_Consumer')
        self.consume_thread.daemon = True

    def _init_producers(self):
        self.logger.info("Initializing Producers")
        self.producer = Producers(self.cfg, self.rmq_p, loggername=self.log_name)
        self.produce_thread = threading.Thread(target=self.producer.run, name = 'Serv_Producer')
        self.produce_thread.daemon = True

    #### END Socket and Connection Handlers ###########
    def get_connection_state(self):
        return self.connected

    def stop(self):
        #self.conn.close()
        self.logger.info('{:s} Terminating...'.format(self.name))
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()
