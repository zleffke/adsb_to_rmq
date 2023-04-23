#!/usr/bin/env python
from math import *
import string
import time
import sys
import csv
import os
from datetime import datetime as date
import numpy as np

deg2rad = pi / 180.0
rad2deg = 180.0 / pi
c       = float(299792458)  #[m/s], speed of light
R_e     = 6378.137 				#Earth Radius, in kilometers
e_e     = 0.081819221456	    #Eccentricity of Earth

def Print_ADSB_Data(cur, exp):
        os.system('clear')
        print "Current Aircraft Count: %i" % len(cur)
        print "#\tICAO\tRange [km/mi]\tmsgs"
        for i in range(len(cur)):
            cur[i].since = (date.utcnow() - cur[i].last_seen).total_seconds()
            if cur[i].range != None:
                print "%i\t%s\t%3.1f / %3.1f\t%i\t%3.1f" % (i+1, cur[i].icao, cur[i].range, cur[i].range*0.621371, len(cur[i].msgs), cur[i].since)
            else:
                print "%i\t%s\t%3.1f\t\t%i\t%3.1f" % (i+1, cur[i].icao, 0, len(cur[i].msgs), cur[i].since)

        print "\n"
        print "Expired Count:", len(exp)

class gs(object):
    def __init__(self, lat, lon, alt):
        self.lat  = lat
        self.lon  = lon
        self.alt  = alt

class aircraft(object):
    def __init__(self, icao):
        self.icao       = icao
        self.lat        = None
        self.lon        = None
        self.alt        = None
        self.range      = None
        self.az         = None
        self.el         = None
        self.speed      = None
        self.track      = None
        self.first_seen = date.utcnow()
        self.last_seen  = self.first_seen
        self.since      = 0
        self.msg_count  = None
        self.msgs       = []    #list of messages
        self.pos_valid  = False # Boolean. Flag to indicate valid position

    def add_msg(self, msg, gs):
        self.since = (date.utcnow() - self.last_seen).total_seconds()
        self.last_seen = date.utcnow()
        if msg.tx_type == 3:
            self.lat = msg.lat
            self.lon = msg.lon
            self.alt = msg.alt
            [msg.range, msg.az, msg.el] = RAZEL(gs.lat, gs.lon, gs.alt, msg.lat, msg.lon, msg.alt*0.0003048)
            [self.range, self.az, self.el] = [msg.range, msg.az, msg.el]
            self.pos_valid = True
        if msg.ground_speed != None: self.speed = msg.ground_speed
        if msg.track        != None: self.track = msg.track
        if msg.alt          != None: self.alt = msg.alt
        self.msgs.append(msg)

class sbs1_msg(object):
    def __init__(self, msg):
        self.range          = None # Range from GS to Target, km
        self.az             = None # Azimuth from GS to Target, deg
        self.el             = None # Elevation from GS to target, deg

        self.msg_type       = None # MSG, CLK, STA, AIR, ID, SEL
        self.tx_type        = None # 111, always the same
        self.session_id     = None # String. Database session record number.
        self.aircraft_id    = None # String. Database aircraft record number.
        self.hex_ident      = None # String. 24-bit ICACO ID, in hex.
        self.flight_id      = None # String. Database flight record number.
        self.generated_date = None # String. Date the message was generated.
        self.generated_time = None # String. Time the message was generated.
        self.logged_date    = None # String. Date the message was logged.
        self.logged_time    = None # String. Time the message was logged.
        self.callsign       = None # String. Eight character flight ID or callsign.
        self.alt            = None # Integer. Mode C Altitude relative to 1013 mb (29.92" Hg).
        self.ground_speed   = None # Integer. Speed over ground.
        self.track          = None # Integer. Ground track angle.
        self.lat            = None # Float. Latitude.
        self.lon            = None # Float. Longitude
        self.vertical_rate  = None # Integer. Climb rate.
        self.squawk         = None # String. Assigned Mode A squawk code.
        self.alert          = None # Boolean. Flag to indicate that squawk has changed.
        self.emergency      = None # Boolean. Flag to indicate emergency code has been set.
        self.spi            = None # Boolean. Flag to indicate Special Position Indicator has been set.
        self.is_on_ground   = None # Boolean. Flag to indicate ground squat switch is active.
        self.init_msg(msg)
        #Transmission Type:
            #ES_IDENT_AND_CATEGORY	1	ES identification and category	DF17 BDS 0,8
            #ES_SURFACE_POS	        2	ES surface position message	    DF17 BDS 0,6
            #ES_AIRBORNE_POS	    3	ES airborne position message	DF17 BDS 0,5
            #ES_AIRBORNE_VEL	    4	ES airborne velocity message	DF17 BDS 0,9
            #SURVEILLANCE_ALT	    5	Surveillance alt message	    DF4, DF20
            #SURVEILLANCE_ID	    6	Surveillance ID message	        DF5, DF21
            #AIR_TO_AIR	            7	Air-to-air message	            DF16
            #ALL_CALL_REPLY	        8	All call reply	                DF11

        #self.razel(msg)

    def init_msg(self, msg):
        self.msg_type       = msg[0] # MSG, CLK, STA, AIR, ID, SEL
        self.tx_type        = int(msg[1]) # transmission type
        self.session_id     = msg[2] # String. Database session record number.
        self.aircraft_id    = msg[3] # String. Database aircraft record number.
        self.hex_ident      = msg[4] # String. 24-bit ICACO ID, in hex.
        self.flight_id      = msg[5] # String. Database flight record number.
        self.generated_date = msg[6] # String. Date the message was generated.
        self.generated_time = msg[7] # String. Time the message was generated.
        self.logged_date    = msg[8] # String. Date the message was logged.
        self.logged_time    = msg[9] # String. Time the message was logged.
        self.callsign       = msg[10] # String. Eight character flight ID or callsign.
        if len(msg[11]) != 0: self.alt          = float(msg[11]) # Integer. Mode C Altitude relative to 1013 mb (29.92" Hg).
        if len(msg[12]) != 0: self.ground_speed = float(msg[12]) # Integer. Speed over ground.
        if len(msg[13]) != 0: self.track        = float(msg[13]) # Integer. Ground track angle.
        if len(msg[14]) != 0: self.lat          = float(msg[14]) # Float. Latitude.
        if len(msg[15]) != 0: self.lon          = float(msg[15]) # Float. Longitude
        if len(msg[16]) != 0: self.vertical_rate= float(msg[16]) # Integer. Climb rate.
        self.squawk = msg[17] # String. Assigned Mode A squawk code.
        if len(msg[18]) != 0: self.alert        = float(msg[18]) # Boolean. Flag to indicate that squawk has changed.
        if len(msg[19]) != 0: self.emergency    = float(msg[19]) # Boolean. Flag to indicate emergency code has been set.
        if len(msg[20]) != 0: self.spi          = float(msg[20]) # Boolean. Flag to indicate Special Position Indicator has been set.
        if len(msg[21]) != 0: self.is_on_ground = float(msg[21]) # Boolean. Flag to indicate ground squat switch is active.

    def get_json(self):
        self.msg = {
            "msg_type":self.msg_type,      # MLAT, MSG, CLK, STA, AIR, ID, SEL
            "tx_type":self.tx_type,       # transmission type
            "session_id":self.session_id,    # String. Database session record number.
            "aircraft_id":self.aircraft_id,   # String. Database aircraft record number.
            "hex_ident":self.hex_ident,     # String. 24-bit ICACO ID, in hex.
            "flight_id":self.flight_id,     # String. Database flight record number.
            "generated_date":self.generated_date,# String. Date the message was generated.
            "generated_time":self.generated_time,# String. Time the message was generated.
            "logged_date":self.logged_date,   # String. Date the message was logged.
            "logged_time":self.logged_time    # String. Time the message was logged.
        }
        if len(self.callsign) != 0:
            self.msg.update("callsign":self.callsign) # String. Eight character flight ID or callsign.
            self.msg.update("altitude":self.alt) # Integer. Mode C Altitude relative to 1013 mb (29.92" Hg).
            self.msg.update(self.ground_speed) # Integer. Speed over ground.
            self.msg.update(self.track) # Integer. Ground track angle.
            self.msg.update(self.lat) # Float. Latitude.
            self.msg.update(self.lon) # Float. Longitude
            self.msg.update(self.vertical_rate) # Integer. Climb rate.
            self.msg.update(self.squawk) # String. Assigned Mode A squawk code.
            self.msg.update(self.alert) # Boolean. Flag to indicate that squawk has changed.
            self.msg.update(self.emergency      = None # Boolean. Flag to indicate emergency code has been set.
            self.msg.update(self.spi            = None # Boolean. Flag to indicate Special Position Indicator has been set.
            self.msg.update(self.is_on_ground   = None # Boolean. Flag to indicate ground squat switch is active.
        }

    #def razel(self, msg):
    #    if (self.tx_type == 3):
    #        [self.range, self.az, self.el] = RAZEL(self.gs_lat, self.gs_lon, self.gs_alt, self.lat, self.lon, self.alt* 0.0003048)
    #        print "%s\t%2.6f\t%2.6f\t%5i\t%3.1f\t%3.1f\t%2.1f" % (self.hex_ident ,self.lat, self.lon, self.alt, self.range, self.az, self.el)



#--Range Calculations Functions------
def LLH_To_ECEF(lat, lon, h):
	#INPUT:
	#	h   - height above ellipsoid (MSL), km
	#	lat - geodetic latitude, in radians
	#	lon - longitude, in radians
    C_e = R_e / sqrt(1 - pow(e_e, 2) * pow(sin(lat),2))
    S_e = C_e * (1 - pow(e_e, 2))
    r_i = (C_e + h) * cos(lat) * cos(lon)
    r_j = (C_e + h) * cos(lat) * sin(lon)
    r_k = (S_e + h) * sin(lat)
    return r_i, r_j, r_k

def RAZEL(lat1, lon1, h1, lat2, lon2, h2):
	#Calculates Range, Azimuth, Elevation in SEZ coordinate frame from SITE to UAV
	#INPUT:
	# lat1, lon1, h1 - Site Location
	# lat2, lon2, h2 - UAV location
    # lats and lons in degrees
    # h in km
	#OUTPUT:
	# Slant Range, Azimuth, Elevation

    lat1 = lat1 * deg2rad
    lon1 = lon1 * deg2rad
    lat2 = lat2 * deg2rad
    lon2 = lon2 * deg2rad

    r_site   = np.array(LLH_To_ECEF(lat1, lon1, h1))
    r_uav    = np.array(LLH_To_ECEF(lat2, lon2, h2))
    rho_ecef = r_uav - r_site

    ECEF_2_SEZ_ROT = np.array([[sin(lat1) * cos(lon1), sin(lat1) * sin(lon1), -1 * cos(lat1)],
                               [-1 * sin(lon1)       , cos(lon1)            , 0             ],
                               [cos(lat1) * cos(lon1), cos(lat1) * sin(lon1), sin(lat1)     ]])

    rho_sez = np.dot(ECEF_2_SEZ_ROT ,rho_ecef)
    rho_mag = np.linalg.norm(rho_sez)
    el = asin(rho_sez[2]/rho_mag) * rad2deg
    az_asin = asin(rho_sez[1]/sqrt(pow(rho_sez[0],2)+pow(rho_sez[1], 2))) * rad2deg
    az_acos = acos(-1 * rho_sez[0]/sqrt(pow(rho_sez[0],2)+pow(rho_sez[1], 2))) * rad2deg
    #print az_asin, az_acos
    #Perform Quadrant Check:
    if (az_asin >= 0) and (az_acos >= 0): az = az_acos# First or Fourth Quadrant
    else: az = 360 - az_acos# Second or Third Quadrant
    #This is the Azimuth From the TARGET to the UAV
    #Must convert to Back Azimuth:
    back_az = az + 180
    if back_az >= 360:  back_az = back_az - 360
    #print az, back_az
    # rho_mag in kilometers, range to target
    # back_az in degrees, 0 to 360
    # el in degrees, negative = down tilt, positive = up tilt
    return rho_mag, az, el
