#!/usr/bin/env python3
###############################################################
#   Title: Generic Tracking Daemon Main Thread
# Project: VTGS, Telescope
# Version: 1.0.0
#    Date: Dec 24, 2021
#  Author: Zach Leffke, KJ4QLP
# Comment:
#  - Main Thread for tracking daemon
#  - Handles State Machine
#  - Flows communication between threads via python queues
###############################################################

import threading
import os
import math
import sys
import string
import time
import socket
import json
import binascii
import datetime
import uuid
import copy
from queue import Queue

from daemon.logger import *
from rmq_comms import BrokerConsumer

class Consumer(BrokerConsumer):
    def __init__(self, cfg, loggername=None):
        super(Consumer, self).__init__(cfg, loggername)
        self.logger = logging.getLogger(loggername) #logger
        self.cfg = cfg
        self.type = cfg['type']
        self.q  = Queue() #place received messages here.

        self.queue = self.cfg['exchange_key']
        # self.routing_key = self.cfg['key']
        # self.consumer_tag = self.routing_key
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
        # if self.loggername is not None:
        #     self.logger.debug('Received message {:s}'.format(json.dumps(rmq_msg)))
        self.q.put(rmq_msg)

    def set_icao(self, icao):
        self.icao=icao
        self.routing_key = "{:s}.{:s}.#".format(self.cfg['exchange_key'],
                                                self.icao)

    def get_connection_state(self):
        return self.connected


class Main_Thread(threading.Thread):
    """ docstring """
    def __init__ (self, cfg):
        threading.Thread.__init__(self, name = 'Main   ')
        self._stop      = threading.Event()
        self.cfg = cfg
        self.thread_enable = self.cfg['thread_enable']

        self.log_name = self.cfg['main']['log']['name']
        self.main_log_fh = setup_logger(self.cfg['main']['log'])
        self.logger = logging.getLogger(self.log_name) #main logger
        self.logger.info("configs: {:s}".format(json.dumps(self.cfg)))

        self.state  = 'BOOT' #BOOT, IDLE, STANDBY, ACTIVE, FAULT, CALIBRATE
        self.state_map = {
            'BOOT':0x00,    #bootup
            'IDLE':0x01,    #threads launched, no connections, attempt md01 connect
            'RUN':0x02,     #user connected, md01 connected
            'FAULT':0x04    #Fault State
        }

        # self.sbs1_tlm = None #SBS1 Thread Status
        # self.mlat_tlm = None #MLAT Thread Status
        # self.rmq_tlm  = None #RMQ Thread Status

    def run(self):
        print("Main Thread Started...")
        self.logger.info('Launched main thread')
        try:
            while (not self._stop.isSet()):
                if self.state == 'BOOT':
                    self._do_boot()
                elif self.state == 'FAULT':
                    print("in FAULT state, exiting")
                    sys.exit()
                else:# NOT IN BOOT State
                    #Always check for service message
                    # if (self.thread_enable['sbs1'] and (not self.sbs1_thread.tlm_q.empty())):
                    #     self.sbs1_tlm = self.sbs1_thread.tlm_q.get()
                    # if (self.thread_enable['rmq'] and (not self.rmq_thread.tlm_q.empty())):
                    #     self.rmq_tlm = self.rmq_thread.tlm_q.get()
                    # self._check_con_status()

                    if self.state == 'IDLE':
                        self._do_idle() #wait for user conn AND mdo1 conn

                    elif self.state == 'RUN':
                        self._do_run()

                    elif self.state == 'CALIBRATE':
                        self._do_calibrate()

                time.sleep(0.0001)

        except (KeyboardInterrupt): #when you press ctrl+c
            #print "\n"+self.utc_ts() + "Caught CTRL-C, Killing Threads..."
            self.logger.warning('Caught CTRL-C, Terminating Threads...')
            self._stop_threads()
            self.logger.critical('Terminating Main Thread...')
            sys.exit()
        except SystemExit:
            self.logger.critical('Terminating Main Thread...')
        sys.exit()

    def _do_boot(self):
        '''
        BOOT State - initialize threads
        '''
        if self._init_consumer():#if all threads activate succesfully
            self.logger.info('Successfully Launched RabbitMQ Consumer')
            self._set_state('RUN')
            time.sleep(1)
            # self._check_con_status()

    def _do_idle(self):
        '''
        waiting for connection to adsb receiver and rabbitmq broker
        '''
        # self._check_con_status()
        pass

    def _do_run(self):
        '''
        connected to SBS1 port and RMQ broker
        broadcast messages over RabbitMQ
        '''
        self.connected = self.consumer.get_connection_state()
        if (not self.consumer.q.empty()): #Received a message from user
            msg = self.consumer.q.get()
            self._process_message(msg)

    def _do_calibrate(self):
        pass

    def _process_message(self, msg):
        print(json.dumps(msg, indent = 2))

    def _init_consumer(self):
        self.logger.info("Initializing Consumer")
        self.consumer = Consumer(self.cfg['rmq']['connection'], loggername=self.log_name)
        self.consume_thread = threading.Thread(target=self.consumer.run, name = 'Consumer')
        self.consume_thread.daemon = True
        self.consumer.set_icao(self.cfg['icao'])
        self.consume_thread.start()
        self.logger.info('Consumer Started')
        # time.sleep(1)
        return True

    def _stop_threads(self):
        self.consumer.stop_consuming()
        time.sleep(0.5)

    #---STATE FUNCTIONS----
    def _set_state(self, state):
        self.state = state
        self.logger.info('Changed STATE to: {:s}'.format(self.state))

    def get_state(self):
        return self.state
    #---END STATE FUNCTIONS----

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()
