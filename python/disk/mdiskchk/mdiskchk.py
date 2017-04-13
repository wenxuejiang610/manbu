#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import multiprocessing
import re
import hadoop_role
import diskmail
from os import path
from subprocess import Popen,PIPE

#ips = [
#'10.75.21.45',
#'10.75.21.44',
#'10.75.21.47',
#'10.75.21.49']

def dell_chk(result,errcount=150,perrcount=50):
    ids = []
    err = []
    perr = []
    state = []
    for line in result:
        line = line.strip('\n')
        if re.search('^Slot',line):
            ids.append(line.split(': ')[1])
        elif re.search('Media|Other',line):
            err.append(int(line.split(': ')[1]))
        elif re.search('Predictive',line):
            perr.append(int(line.split(': ')[1]))
        elif re.search('Firmware',line):
            state.append(line.split(': ')[1].split(',')[0])
    res = ''        
    if ids:
        if int(ids[-1]) + 1 != len(ids):
            res = res + 'Disk-Lose,'
    if err:
        for e in err:
            if int(e) > errcount:
                res = res + 'Disk-Err,'
                break
    if perr:
        for e in perr:
            if int(e) > perrcount:
                res = res + 'Disk-PErr,'
                break
    if state:
        for s in state:
            if s != 'Online' and s != 'Hotspare':
                res = res + 'Disk-Offline,'
    return res

def hp_chk(result):
    ids = []
    state = []
    for line in result:
        line = line.strip('\n')
        id = line.split('bay')[1].split(',')[0].strip(' ')
        s = line.split(':')[-1].strip(' ')
        ids.append(int(id))
        state.append(s)
    res = ''
    if ids:
        ids.sort()
        if ids[-1] != len(ids):
            res = res + 'Disk-Lose,'
    if state:
        for s in state:
            if s != 'OK':
                res = res + 'Disk-Offline,'
    return res

def run_cmd(host,cmd):
    rcmd = "ssh -q -p 26387 -i ~/.ssh/id_rsa2 -o StrictHostKeyChecking=no -o ConnectTimeout=3 root@%s %s" %(host,cmd)
    p = Popen(rcmd,stdout = PIPE,shell=True,)
    p.wait()
    if p.returncode != 0:
        res = ''
    else:
        res = p.stdout.readlines()
    return res

def disk_chk(host,cmd):
    result = [host]
    res = run_cmd(host,cmd)
    if not res:
        result.append('SSH or No Checksoft!')
    else:
        if re.search('physicaldrive',res[0]):
            chkres = hp_chk(res)
        else:
            chkres = dell_chk(res)
        
        if not chkres:
            result.append('OK')
        else:
            result.append(chkres)
    return result

def get_errlog(host,cmd,dir,url):
    chktime = time.strftime('%m-%d')
    filename = host + '_' + chktime + '.log'
    res = run_cmd(host,cmd)
    if not res:
        err_log = 'SSH or No Checksoft!'
    else:
        err_log = res
    with open(dir + filename,'w') as f:
        f.writelines(err_log)
    logurl = ''
    if path.isfile(dir + filename):
        logurl = url + filename
    return logurl    
 
def run(IPs,chkcmd,logcmd,dir,url):
    res = []
    errserver = []
    p = multiprocessing.Pool(processes=20)
    for i in IPs:
        i = i.strip('\n')
        res.append(p.apply_async(disk_chk,(i,chkcmd)))
    p.close()
    time.sleep(4)
    
    #display for err
    for i in res:
        try:
            server = i.get(timeout=30)
        except multiprocessing.TimeoutError as ex:
            print ex
        
        cluster = {}
        if 'OK' not in server:
            #host = server[0]
            print server

            cluster = hadoop_role.get_cluster(server[0])
            if not cluster.keys():
                cluster['cluster'] = 'unknow'
                cluster['role'] = 'unknow'
            server.append(cluster['cluster']) 
            server.append(cluster['role']) 
            errserver.append(server)
            logurl = get_errlog(server[0],logcmd,dir,url)
            errserver.append(logurl)
            
    return errserver
if __name__ == '__main__':

    chkcmd = "'curl -s ftp://10.73.11.200/diskchk.sh|bash'"
    logcmd = "'curl -s ftp://10.73.11.200/showdisk.sh|bash'"
    ipfile = "../cmdb/cmdb.hadoop"
    dir = "/data2/ftp/diskchk/"
    url = "ftp://10.73.11.152/"
    with open(ipfile) as f:
        ips = f.readlines()
    
    errserver = run(ips,chkcmd,logcmd,dir,url)

    msg_title = "Hadoop磁盘检查:"
    columns = ['IP', '错误信息', '集群', '角色', '更多信息']
    diskmail.send(msg_title,columns,errserver)
