#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import time
import re
import multiprocessing
import hadoop_nodeall
import diskmail
from subprocess import Popen,PIPE

def run_cmd(host,cmd):
    rcmd = "ssh -q -p 26387 -i ~/.ssh/id_rsa2 -o StrictHostKeyChecking=no -o ConnectTimeout=4 root@%s %s" %(host,cmd)
    p = Popen(rcmd,stdout = PIPE,shell=True,)
    p.wait()
    if p.returncode != 0:
        res = ''
    else:
        res = p.stdout.readlines()
    return res

def tcollector_chk(node,cmd):
    result = node
    res = run_cmd(node[0],cmd)
    if not res:
        chkres = 'ssherror'
    for row in res:
        if re.search('y7gNCF03tGkAJBfVwWbUB13T0STfI2',row):
            chkres = 'ok'
            break
        else:
            chkres = 'close'
    result.append(chkres)
    #print result
    return result       

def get_regionserver(exclude):
    nodes = hadoop_nodeall.get_nodes()
    chknodes = []
    for node in nodes:
        if 'regionserver' in node[1] and node[2] not in exclude:
            chknodes.append(node)
    return chknodes

def run(cmd,exclude):
    res = []
    errserver = []
    chknodes = get_regionserver(exclude)
    p = multiprocessing.Pool(processes=20)
    for node in chknodes:
        res.append(p.apply_async(tcollector_chk,(node,cmd)))
    p.close()
    time.sleep(3)
    
    #display for err
    for i in res:
        try:
            server = i.get(timeout=30)
        except multiprocessing.TimeoutError as ex:
            print 'get timeout' + str(ex)
        cluster = {}
        if 'ok' not in server[3]:
            print server
            errserver.append(server)
    return errserver
    
def main():
    exclude = []
    if len(sys.argv) >= 2:
        exclude = sys.argv
        exclude.pop(0)
    
    chkcmd = "'ps -ef|grep collector|grep kid'"
    errserver = run(chkcmd,exclude)

    #send mail
    msg_title = "Hbase tcollector check:"
    columns = ['IP', '角色', '集群', '状态']
    if errserver:
        diskmail.send(msg_title,columns,errserver)

if __name__ == '__main__':
    main()
