#!/usr/bin/env python3
########################################################
#   Title: High Altitude Balloon ADSB Payload
#  Thread: SBS1 Thread
# Project: HAB Flights
# Version: 0.0.1
#    Date: July 2021
#  Author: Zach Leffke, KJ4QLP
# Comment:
########################################################

import threading
import os
import math
import sys
import string
import time
import socket
import errno
import json
import binascii as ba
import numpy
import datetime
from queue import Queue
from logger import *
from daemon.watchdog_timer import *

class MLAT_Thread(threading.Thread):
    """
    Title: SBS1 Client Thread
    Project: ADSB to RabbitMQ Broadcaster
    Version: 0.0.1
    Date: Dec 2022
    Author: Zach Leffke, KJ4QLP

    Purpose:
        Handles TCP Interface to ADSB Receiver for SBS1 Format
        Expcted ADSB receiver developed against: dump1090

    Args:
        cfg - Configurations for thread, dictionary format.
        parent - parent thread, used for callbacks

    """
    def __init__ (self, cfg, parent):
        threading.Thread.__init__(self)
        self._stop  = threading.Event()
        self.cfg    = cfg
        print(cfg)
        self.parent = parent
        self.thread_name = ".".join([self.cfg['name'],self.cfg['type']])
        self.setName(self.thread_name)
        self.logger = logging.getLogger(self.cfg['main_log'])

        self.rx_q  = Queue()
        self.tlm_q = Queue()
        self.buffer = ''

        self.connected = False
        self.rx_count = 0
        self.logger.info("Initializing {}".format(self.name))

        self.tlm = { #Thread control Telemetry message
            "type":"TLM", #Message Type: Thread Telemetry
            "connected":False, #Socket Connection Status
            "rx_count":0, #Number of received messages from socket
            "ip":self.cfg['connection']['ip'], #Socket IP
            "port":self.cfg['connection']['port'], #Socket Port
        }


    def run(self):
        self.logger.info('Launched {:s}'.format(self.name))
        self._init_socket()
        while (not self._stop.isSet()):
            if not self.connected:
                try:
                    time.sleep(0.05)
                    self._attempt_connect()
                except Exception as e:
                    self.logger.debug(e)
                    self.connected = False
                    self.tlm['connected'] = self.connected
                    time.sleep(self.cfg['connection']['retry_time'])
            else:
                try:
                    for l in self._readlines():
                        self._handle_recv_data(l)
                except socket.timeout as e: #Expected after connection
                    self._handle_socket_timeout()
                except Exception as e:
                    self._handle_socket_exception(e)
            time.sleep(0.000001)

        self.sock.close()
        self.logger.warning('{:s} Terminated'.format(self.name))

    def _reset_socket(self):
        self.logger.debug('resetting socket...')
        self.sock.close()
        self.connected = False
        # self._update_main_thread()
        self._init_socket()

    def _init_socket(self):
        self.buffer = ''
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #TCP Socket, initialize
        self.logger.debug("Setup socket")
        self._socket_watchdog = Watchdog(timeout=self.cfg['connection']['watchdog_time'],
                                         name = self.thread_name+".wd",
                                         userHandler=self._socket_watchdog_expired)
        self.logger.debug("Setup socket Watchdog")

    def _attempt_connect(self):
        self.logger.info("Attempting to connect to: [{:s}, {:d}]".format(self.cfg['connection']['ip'],
                                                                         self.cfg['connection']['port']))
        self.sock.connect((self.cfg['connection']['ip'], self.cfg['connection']['port']))
        self.logger.info("Connected to: [{:s}, {:d}]".format(self.cfg['connection']['ip'],
                                                             self.cfg['connection']['port']))

        # time.sleep(0.01)
        self.sock.settimeout(self.cfg['connection']['timeout'])   #set socket timeout
        self.connected = True
        self.tlm['connected'] = self.connected
        self.tlm['rx_count']  = 0
        self._socket_watchdog.start()

    def _socket_watchdog_expired(self):
        self.logger.debug("Socket Watchdog Expired")
        self._reset_socket()

    def _readlines(self, recv_buffer=8192, delim='\n'):
        #self.lock.acquire()
        data = True
        while data:
            data = self.sock.recv(recv_buffer)
            if len(data) == 1:
                self.logger.debug('Received MLAT socket heartbeat')
                self._socket_watchdog.reset(timeout=self.cfg['connection']['watchdog_time'])
            else:
                self.buffer += data.decode('utf-8')
                while self.buffer.find(delim) != -1:
                    line, self.buffer = self.buffer.split('\n', 1)
                    yield line.strip('\r')

        return

    def _handle_recv_data(self, data):
        try:
            self._socket_watchdog.reset(timeout=self.cfg['connection']['watchdog_time'])
            self.tlm['rx_count'] += 1
            self.rx_count += 1
            msg = self._encode_sbs1_json(data.strip('\r').split(','))
            self.rx_q.put(msg)
            # self._socket_watchdog.reset(timeout=5)
        except Exception as e:
            self.logger.debug("Unhandled Receive Data Error")
            self.logger.debug(sys.exc_info())

    def _encode_sbs1_json(self,data):
        #REF: http://woodair.net/sbs/Article/Barebones42_Socket_Data.htm
        try:
            msg = {
                "msg_type":data[0],      # MLAT, MSG, CLK, STA, AIR, ID, SEL
                "tx_type":int(data[1]),       # transmission type
                "session_id":int(data[2]),    # String. Database session record number.
                "aircraft_id":int(data[3]),   # String. Database aircraft record number.
                "hex_ident":data[4],     # String. 24-bit ICACO ID, in hex.
                "flight_id":int(data[5]),     # String. Database flight record number.
                "generated_date":data[6],# String. Date the message was generated.
                "generated_time":data[7],# String. Time the message was generated.
                "logged_date":data[8],   # String. Date the message was logged.
                "logged_time":data[9]    # String. Time the message was logged.
            }
            if len(data[10]) != 0: msg.update({"callsign":data[10]}) # String. Eight character flight ID or callsign.
            if len(data[11]) != 0: msg.update({"altitude":float(data[11])})# Integer. Mode C Altitude relative to 1013 mb (29.92" Hg).
            if len(data[12]) != 0: msg.update({"ground_speed":float(data[12])}) # Integer. Speed over ground.
            if len(data[13]) != 0: msg.update({"track":float(data[13])}) # Integer. Ground track angle.
            if len(data[14]) != 0: msg.update({"latitude":float(data[14])}) # Float. Latitude.
            if len(data[15]) != 0: msg.update({"longitude":float(data[15])}) # Float. Longitude
            if len(data[16]) != 0: msg.update({"vertical_rate":float(data[16])}) # Integer. Climb rate.
            if len(data[17]) != 0: msg.update({"squawk": data[17]}) # String. Assigned Mode A squawk code.
            if len(data[18]) != 0: msg.update({"alert":float(data[18])}) # Boolean. Flag to indicate that squawk has changed.
            if len(data[19]) != 0: msg.update({"emergency":float(data[19])}) # Boolean. Flag to indicate emergency code has been set.
            if len(data[20]) != 0: msg.update({"spi":float(data[20])}) # Boolean. Flag to indicate Special Position Indicator has been set.
            if len(data[21]) != 0: msg.update({"is_on_ground":float(data[21])}) # Boolean. Flag to indicate ground squat switch is active.
        except Exception as e:
            print(e)
            return None
        return msg

    #### Socket and Connection Handlers ###########


    def _handle_socket_timeout(self):
        pass

    def _reset(self):
        self.logger.info("Resetting Packet Counters")

    def get_tlm(self):
        self.tlm['connected'] = self.connected
        # self.tlm_q.put(self.tlm)
        return self.tlm






    def _update_main_thread(self):
        self.tlm['connected'] = self.connected
        self.tlm['rx_count']  = 0
        self.tlm_q.queue.clear()
        self.tlm_q.put(self.tlm)

    def _handle_socket_exception(self, e):
        self.logger.debug("Unhandled Socket error")
        self.logger.debug(sys.exc_info())
        self._reset_socket()



    #### END Socket and Connection Handlers ###########
    def stop(self):
        #self.conn.close()
        self.logger.info('{:s} Terminating...'.format(self.name))
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()
