#!/usr/bin/python
import urllib
import urllib2
import json

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
URL = "http://api.dsp.cluster.sina.com.cn/dp_clusterinfo/scan"
DATA = {}
re = json.loads(get_send_url(URL,DATA))
for cluster in re:
    print cluster['clusterName'] + ': ' + cluster['adminUrl']
