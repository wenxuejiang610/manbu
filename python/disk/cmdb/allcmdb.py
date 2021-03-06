#!/usr/bin/python26
# -*- coding: UTF-8 -*-
# Copyright 2012 sina.com
import os
import time
import commands
import sys
import MySQLdb
import urllib
import urllib2
import json
import datetime
import logging
import logging.handlers
import re

log_level = logging.DEBUG

log_filename = "log_for_cmdb.log"
logger = logging.getLogger("cmdb")
logger.setLevel(log_level)
handler = logging.handlers.RotatingFileHandler(log_filename, maxBytes=1000000000, backupCount=0)
formatter = logging.Formatter("%(asctime)s - [%(levelname)s] - [%(name)s/%(filename)s: %(lineno)d] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def get_all_machine_list(qurey):
    params = {}
    headers = {}
    url = "http://newcmdb.intra.sina.com.cn/api"
    headers['accept'] = 'application/json'
    params['username'] = 'api_db'
    params['auth'] = '8kl9E0Xh1eovrb0HdjR9h2p6'
    params['num'] = 0
    params['return_total'] = 1
    params['q'] = qurey
    params = urllib.urlencode(params)
    url = url + '?' + params

    req = urllib2.Request(url, headers=headers)
    res = urllib2.urlopen(req, timeout=300).read()
    d = json.loads(res)
    return d.get('result')

def generate_dict_cmdb_ip(i):
    if i.get('asset_number') and i.get('ips'):
        try:
            cmdb = i.get('asset_number').encode('utf8')
            ip_in = []
            for iplist in i.get('ips').split(';'):
                if re.search(re_words,iplist):
                    ip_in = iplist.split('-')[1].encode('utf8')
        except Exception as e:
            logger.debug("%s,%s" % (e,str(i)))
        else:
            result = {'cmdb':cmdb,'ip_in':ip_in}
            result_all.append(result)    

def get_ip(i):
    if i.get('ip_in'):
        ip_list.append(i.get('ip_in'))
    

if __name__ == "__main__":
    result_all = []
    ip_list = []
    re_words = '内网'.decode('utf8')
    qurey = "administrator==chencheng5"
    all_cmdb = get_all_machine_list(qurey)
    map(generate_dict_cmdb_ip,all_cmdb)
    map(get_ip,result_all)

    m = '\n'.join(ip_list)
    with open('./cmdb.hadoop','w') as f:
        f.writelines(m)
