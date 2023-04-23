#!/usr/bin/env python3

import socket
import os
import string
import sys
import time
import argparse
import datetime
import json, csv, yaml
import subprocess
from binascii import *


def import_configs_yaml(args):
    ''' setup configuration data '''
    fp_cfg = '/'.join([args.cfg_path,args.cfg_file])
    print (fp_cfg)
    if not os.path.isfile(fp_cfg) == True:
        print('ERROR: Invalid Configuration File: {:s}'.format(fp_cfg))
        sys.exit()
    print('Importing configuration File: {:s}'.format(fp_cfg))
    with open(fp_cfg, 'r') as yaml_file:
        cfg = yaml.safe_load(yaml_file)
        yaml_file.close()

    if cfg['main']['base_path'] == 'cwd':
        cfg['main']['base_path'] = os.getcwd()
    return cfg

def configure_logs(cfg):
    #configure main log configs
    log_name = '.'.join([cfg['main']['name'],cfg['main']['log']['name']])
    log_path = '/'.join([cfg['main']['log']['path'],startup_ts])
    cfg['main']['log'].update({
        'name':log_name,
        'startup_ts':startup_ts,
        'path':log_path
    })
    if not os.path.exists(cfg['main']['log']['path']):
        os.makedirs(cfg['main']['log']['path'])

    #configure thread specific logs
    for key in cfg['thread_enable'].keys():
        #first tell the sub thread where to find main log
        cfg[key].update({
            'main_log':cfg['main']['log']['name']
        })
    return cfg

def main(cfg):
    main_thread = Main_Thread(cfg)
    main_thread.daemon = True
    main_thread.run()
    sys.exit()

if __name__ == '__main__':
    """ Main entry point to start the service. """
    startup_ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    #--------START Command Line argument parser------------------------------------------------------
    parser = argparse.ArgumentParser(description="RabbitMQ ADSB Client",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    cwd = os.getcwd()
    cfg_fp_default = '/'.join([cwd, 'config'])
    cfg = parser.add_argument_group('Configuration File')
    cfg.add_argument('--cfg_path',
                       dest='cfg_path',
                       type=str,
                       default='/'.join([os.getcwd(), 'config']),
                       help="Daemon Configuration File Path",
                       action="store")
    cfg.add_argument('--cfg_file',
                       dest='cfg_file',
                       type=str,
                       default="adsb2rmq_config.yaml",
                       help="Daemon Configuration File",
                       action="store")
    args = parser.parse_args()
#--------END Command Line argument parser------------------------------------------------------
    #print(chr(27) + "[2J")
    subprocess.run(["reset"])
    #print(sys.path)
    cfg = import_configs_yaml(args)
    #print(cfg)

    cfg = configure_logs(cfg)
    # print(json.dumps(cfg, indent=2))
    # sys.exit()

    main(cfg)
    sys.exit()
