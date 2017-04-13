#!/usr/bin/python
import urllib
import urllib2
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

def get_send_url(url, data, headers={}):
    try:
        ret_data = None

        url = url + '?' + urllib.urlencode(data)

        req = urllib2.Request(url, headers=headers)
        res = urllib2.urlopen(req, timeout=300)
        ret_data = res.read()
        res.close()
    except Exception as ex:
        print ex
    finally:
        return ret_data

def get_nodes():
    URL = "http://api.dsp.cluster.sina.com.cn/dp_clusternode/scan/"
    DATA = {}
    nodes = []
    re = json.loads(get_send_url(URL,DATA))
    for row in re:
        node = []
        node.append(row['ip'])
        node.append(row['role'])
        node.append(row['cluster'])
        nodes.append(node)
    return nodes

if __name__ == '__main__':
    for i in get_nodes():
        print i
