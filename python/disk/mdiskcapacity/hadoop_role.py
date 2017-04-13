import urllib
import urllib2
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

def post_send_url(url, data, headers={}):
    try:
        ret_data = None

        data = urllib.urlencode(data)

        req = urllib2.Request(url, data, headers=headers)
        res = urllib2.urlopen(req, timeout=300)
        ret_data = res.read()
        res.close()
    except Exception as ex:
        print 'no cluster in ' + str(ex)
    finally:
        return ret_data

def get_cluster(ip):
    URL = 'http://api.dsp.cluster.sina.com.cn/dp_clusternode/find/'
    data = {'searchtype':'ip','typevalue':ip}
    cluster={}
    respond = json.loads(post_send_url(URL, data))
    try:
        cluster['cluster'] = respond[0]['cluster']
        cluster['role'] = respond[0]['role']
    except Exception as ex:
        print ex
    return cluster

if __name__ == '__main__':
    print get_cluster('10.77.112.94')

