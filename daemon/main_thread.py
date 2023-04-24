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

from daemon.logger import *
from daemon.sbs1_thread import *
from daemon.mlat_thread import *
# from daemon.Focus_Service import *
# from rmq_comms import rmq_thread
from rmq_comms import BrokerProducer

#Transmission Types for 'MSG' messages
# used for RMQ key generation
tt_key=['es_ident', 'es_sur_pos', 'es_air_pos', 'es_air_vel',
        'surv_alt', 'surv_id', 'air_to_air', 'all_call_rep']

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

        self.sbs1_tlm = None #SBS1 Thread Status
        self.mlat_tlm = None #MLAT Thread Status
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
                    self._check_con_status()

                    if self.state == 'IDLE':
                        self._do_idle() #wait for user conn AND mdo1 conn

                    elif self.state == 'RUN':
                        self._do_run()

                    elif self.state == 'CALIBRATE':
                        self._do_calibrate()

                time.sleep(0.000001)

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
        if self._init_threads():#if all threads activate succesfully
            self.logger.info('Successfully Launched ADSB Threads, Switching to IDLE State')
            self._set_state('IDLE')
            time.sleep(1)
            self._check_con_status()

    def _do_idle(self):
        '''
        waiting for connection to adsb receiver and rabbitmq broker
        '''
        self._check_con_status()
        pass

    def _do_run(self):
        '''
        connected to SBS1 port and RMQ broker
        broadcast messages over RabbitMQ
        '''
        if (self.thread_enable['sbs1'] and (not self.sbs1_thread.rx_q.empty())): #Received a message from user
            msg = self.sbs1_thread.rx_q.get()
            self._process_sbs1_message(msg, base_key='adsb')

        if (self.thread_enable['mlat'] and (not self.mlat_thread.rx_q.empty())): #Received a message from user
            msg = self.mlat_thread.rx_q.get()
            self._process_sbs1_message(msg, base_key='mlat')
        pass

    def _do_calibrate(self):
        pass

    def _process_sbs1_message(self, msg, base_key="adsb"):
        routing_key = ".".join([base_key, msg['hex_ident'], tt_key[msg['tx_type']-1]])
        self.producer.send(routing_key, json.dumps(msg))

    def _check_con_status(self):
        '''
        Checks ADSB Receiver and RabbitMQ broker connection status
        sets Daemon state accordingly
        '''
        if (self.thread_enable['sbs1']): self.sbs1_tlm = self.sbs1_thread.get_tlm()
        if (self.thread_enable['mlat']): self.mlat_tlm = self.mlat_thread.get_tlm()
        # if (self.thread_enable['rmq']):  self.rmq_tlm  = self.rmq_thread.get_tlm()

        if ((self.thread_enable['sbs1'] == True) and (self.sbs1_tlm['connected']==True)): #Connected to dump1090 SBS1 receiver
            # if ((self.thread_enable['rmq']==True) and (self.rmq_tlm['connected']==True)): #Connected to RabbitMQ Broker
            if self.state == 'IDLE': #Daemon is in IDLE
                self._set_state('RUN')
            # if ((self.thread_enable['rmq']==True) and (self.rmq_tlm['connected']==False)):
                # if self.state == 'RUN':
                    # self._set_state('IDLE')
        elif ((self.thread_enable['sbs1'] == True) and (self.sbs1_tlm['connected']==False)): #Connected to dump1090 SBS1 receiver
            if (self.state == 'RUN'):
                self._set_state('IDLE')

    def _init_producer(self):
        self.logger.info("Initializing Producer")
        self.producer = BrokerProducer(self.cfg['rmq']['connection'], loggername=self.log_name)
        self.produce_thread = threading.Thread(target=self.producer.run, name = 'Producer')
        self.produce_thread.daemon = True
        self.produce_thread.start()
        self.logger.info('Producer Started')
        # time.sleep(1)

    def _init_threads(self):
        try:
            #Initialize Threads
            #print 'thread_enable', self.thread_enable
            self.logger.info("Thread enable: {:s}".format(json.dumps(self.thread_enable)))
            for key in self.thread_enable.keys():
                if self.thread_enable[key]:
                    if key == 'sbs1': #Initialize ADSB SBS1 Thread
                        self.logger.info('Setting up ADSB SBS1 Thread')
                        self.sbs1_thread = SBS1_Thread(self.cfg['sbs1'], self) #ADSB SBS1 Thread
                        self.sbs1_thread.daemon = True
                    elif key == 'mlat': #Initialize ADSB SBS1 Thread
                        self.logger.info('Setting up MLAT SBS1 Thread')
                        self.mlat_thread = MLAT_Thread(self.cfg['mlat'], self) #MLAT SBS1 Thread
                        self.mlat_thread.daemon = True

            #Launch threads
            for key in self.thread_enable.keys():
                if self.thread_enable[key]:
                    if key == 'sbs1': #Start Service Thread
                        self.logger.info('Launching ADSB SBS1 Thread')
                        self.sbs1_thread.start() #non-blocking
                    elif key == 'mlat': #Start Service Thread
                        self.logger.info('Launching MLAT SBS1 Thread')
                        self.mlat_thread.start() #non-blocking

            self._init_producer()
            return True
        except Exception as e:
            self.logger.error('Error Launching Threads:', exc_info=True)
            self.logger.warning('Setting STATE --> FAULT')
            self._set_state('FAULT')
            return False

    def _stop_threads(self):
        for key in self.thread_enable.keys():
            if self.thread_enable[key]:
                if key == 'sbs1':
                    self.sbs1_thread.stop()
                    self.logger.warning("Terminated ADSB SBS1 Thread.")
                elif key == 'mlat':
                    self.mlat_thread.stop()
                    self.logger.warning("Terminated MLAT SBS1 Thread.")

                self.producer.stop_producing()
                time.sleep(0.5)

    #---STATE FUNCTIONS----
    def _send_session_start(self):
        pass

    def set_state_fault(self):
        self._set_state('FAULT')

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
