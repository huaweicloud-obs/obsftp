#!/usr/bin/python
# -*- coding:utf-8 -*-
# Copyright (C) 2018 charles    nahra@163.com
# Use of this source code is governed by MIT license

import os, sys

current_path = os.path.dirname(os.path.abspath(__file__))
root_path = os.path.abspath( os.path.join(current_path, os.pardir))

if sys.platform.startswith("linux"):
    python_lib_path = os.path.abspath( os.path.join(root_path, "python27", "unix", "lib"))
    sys.path.append(python_lib_path)
elif sys.platform == "darwin":
    python_lib_path = os.path.abspath( os.path.join(root_path, "python27", "unix", "lib"))
    sys.path.append(python_lib_path)
    extra_lib = "/System/Library/Frameworks/Python.framework/Versions/2.7/Extras/lib/python/PyObjc"
    sys.path.append(extra_lib)
elif sys.platform == "win32":
    if root_path not in sys.path:
        sys.path.append(root_path)
    pass
else:
    raise RuntimeError("detect platform fail:%s" % sys.platform)

import logging
from logging.handlers import RotatingFileHandler
import errno
from optparse import OptionParser

from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import ThreadedFTPServer

from FTPAuthCallback import ObsAuthorizer
from AbstrFileToObject import AbstrFileToObs

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: 
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def set_logger(level):
    #log related
    # work_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

    log_dir = current_path + '/log/obsftp/'
    mkdir_p(log_dir)
    LOGFILE = log_dir + "obsftp.log"
    MAXLOGSIZE = 10*1024*1024 #Bytes
    BACKUPCOUNT = 30
    FORMAT =  "%(asctime)s %(levelname)-8s[%(filename)s:%(lineno)d(%(funcName)s)] %(message)s"
    handler = RotatingFileHandler(LOGFILE, mode='w', maxBytes=MAXLOGSIZE, backupCount=BACKUPCOUNT)
    formatter = logging.Formatter(FORMAT)
    handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.setLevel(level)
    logger.addHandler(handler)

def start_ftp(masquerade_address, listen_address, port, log_level, internal, passive_ports):

    LogLevel=["DEBUG","INFO", "WARNING","ERROR","CRITICAL"]
    if log_level in LogLevel:
        for lev in LogLevel:
            if log_level  == lev:
                level = lev
    else:
        print "wrong loglevel parameter: %s" % log_level
        exit(1)

    authorizer = ObsAuthorizer()
    authorizer.internal = internal
    handler = FTPHandler
    handler.passive_ports = passive_ports
    handler.permit_foreign_addresses = True
    if handler.masquerade_address != "":
        handler.masquerade_address = masquerade_address 
    handler.authorizer = authorizer
    handler.abstracted_fs = AbstrFileToObs
    handler.banner = 'obs ftpd ready.'
    address = (listen_address, port)
    set_logger(level)
    server_muti = ThreadedFTPServer(address, handler)
    server_muti.serve_forever()

def main(args, opts):
    masquerade_address = ""
    listen_address = "127.0.0.1"
    port = 10020
    log_level = "DEBUG"
    internal = None
    passive_ports_start = None
    passive_ports_end = None
    passive_ports = None
    if opts.masquerade_address:
        masquerade_address = opts.masquerade_address
    if opts.listen_address:
        listen_address = opts.listen_address
    if opts.port:
        try:
            port = int(opts.port)
        except ValueError:
            print "invalid FTP port, please input a valid port like --port=10020"
            exit(1)

    if opts.loglevel:
        log_level = opts.loglevel

    if opts.passive_ports_end and opts.passive_ports_start:
        if VaildPassivePortNum(opts.passive_ports_start,opts.passive_ports_end):
            passive_ports_start = int(opts.passive_ports_start)
            passive_ports_end = int(opts.passive_ports_end)
            passive_ports = range(passive_ports_start, passive_ports_end)
        else:
            exit(1)
    start_ftp(masquerade_address, listen_address, port, log_level, internal, passive_ports)

def VaildPassivePortNum(StartNum,EndNum):
    try:
        PassiveStartNum = int(StartNum)
        PassiveEndNum = int(EndNum)
    except ValueError:
        print "invalid FTP passive_ports_start/end, please input a valid port like --passive_ports_end=20000/--passive_ports_end=60000"
        return False
    if  not isinstance(PassiveStartNum,int) or not isinstance(PassiveEndNum,int):
        print "invalid FTP passive_ports_start/end, please input a valid port like --passive_ports_end=20000/--passive_ports_end=60000"
        return False
    else:
        if PassiveStartNum <= 1024 or PassiveEndNum >=65535:
            print "passive_ports_start should >1024 and passive_ports_end should < 65535"
            return False
        elif PassiveStartNum > PassiveEndNum:
            print "passive_ports_start should <= passive_ports_end"
            return False
        else:
            return True

if __name__ == '__main__':
    print " Now start the ftp server,the default server ip is 127.0.0.1,port is 10020;\n Suggest use filezilla access this server;\n \
access account is username: AcccessKeyID/bucketname,password:SecretyKeyID .\n \
More selfdefine parameter can get use --help/-h; Terminated this proces can use Ctrl + c"

    parser = OptionParser()
    parser.add_option("", "--masquerade_address", dest="masquerade_address", help="the ip that will reply to FTP Client, then client will send data request to this address.")
    parser.add_option("", "--listen_address", dest="listen_address", help="the address which ftpserver will listen, default is 127.0.0.1")
    parser.add_option("", "--port", dest="port", help="the local port which ftpserver will listen, default is 10020")
    parser.add_option("", "--loglevel", dest="loglevel", help="DEBUG/INFO/")
    parser.add_option("", "--passive_ports_start=", dest="passive_ports_start", help="the start port of passive ports when transefer data, >=1")
    parser.add_option("", "--passive_ports_end=", dest="passive_ports_end", help="the end port of passive ports when transefer data, <=65535")
    (opts, args) = parser.parse_args()
    main(args, opts)
