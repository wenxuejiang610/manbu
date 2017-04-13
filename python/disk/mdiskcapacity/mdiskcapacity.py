#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import multiprocessing
import hadoop_role
import diskmail
from subprocess import Popen,PIPE
from copy import copy

def run_cmd(host,cmd):
    rcmd = "ssh -q -p 26387 -i ~/.ssh/id_rsa2 -o StrictHostKeyChecking=no -o ConnectTimeout=4 root@%s %s" %(host,cmd)
    p = Popen(rcmd,stdout = PIPE,shell=True,)
    p.wait()
    if p.returncode != 0:
        res = ''
    else:
        res = p.stdout.readlines()
    return res

def disk_cap(host,cmd,max=80):
    result = [host]
    res = run_cmd(host,cmd)
    if not res:
        result.append('SSH ERROR!')
    else:
        for disk in res:
            r = disk.strip('\n').split(' ')
            if int(r[1]) > max:
                result.append(r)
    return result
 
def datasort(datalist):
    rows = []
    for m in datalist:
        host = []
        for e in m:
            if type(e) != list:
                host.append(e)
            else:
                row = copy(host)
                row.append(e[0])
                row.append(e[1])
                '''
                check for int failed
                '''
                try:
                    p=int(row[4])
                    rows.append(row)
                except:
                    print "parse failed:",row
    s_datalist = sorted(rows, key=lambda rows:int(rows[4]), reverse=True)
    return s_datalist

def run(ips,cmd):
    res = []
    errserver = []
    p = multiprocessing.Pool(processes=20)
    for i in ips:
        i = i.strip('\n')
        res.append(p.apply_async(disk_cap,(i,cmd)))
    p.close()
    time.sleep(3)
    
    #display for err
    for i in res:
        try:
            server = i.get(timeout=30)
        except multiprocessing.TimeoutError as ex:
            print 'get timeout' + str(ex)
        
        cluster = {}
        if len(server) == 1 or server[0].isalpha():
            pass
        else:
            #host = server[0]
            print server

            cluster = hadoop_role.get_cluster(server[0])
            if not cluster.keys():
                cluster['cluster'] = 'unknow'
                cluster['role'] = 'unknow'
            server.insert(0,cluster['cluster']) 
            server.insert(1,cluster['role']) 
            errserver.append(server)
    errserver = datasort(errserver)
    return errserver

if __name__ == '__main__':
    ipfile = '../cmdb/cmdb.hadoop'
    chkcmd = "df |egrep -v 'File|tmpfs|flashcache|^1'|tr -d '%'|awk '{print $NF,$(NF-1)}'"
    
    with open(ipfile) as f:
        ips = f.readlines()

    errserver = run(ips,chkcmd)
    
    #send mail
    msg_title = "Hadoop磁盘容量:"
    columns = ['集群', '角色', 'IP', '检测目录', '检测值(%)']
    diskmail.send(msg_title,columns,errserver)
