#coding: utf-8
import smtplib
from email.mime.text import MIMEText

HOST = "mail.staff.sina.com.cn"
PORT = 587
SUBJECT = u"Hadoop_DiskCapacity"
TO = "hbase-eng@staff.sina.com.cn"
#TO = "chencheng5@staff.sina.com.cn"
FROM = "dspmail@staff.sina.com.cn"

def gen_title(columns):
    title = '<tr bgcolor="#B0C4DE" height="20">'
    for column in columns:
        title += '<th>' + column + '</th>'
    title += '</tr>'
    return title

def gen_rows(dataSet):
    rows = ""
    colors = ['#FFFFFF','#F0E68C']
    flag = ['/','/var','/usr','/tmp']
    for row in dataSet:
        host = '<tr>'
        for m in row:
            if m in flag:
                color = colors[1]
            else:
                color = colors[0]
            host += '<td bgcolor="' + color + '">' + m + '</td>'
        rows += host + '</tr>'
    return rows

def send(msg_title,columns,dataSet):
    message = '<h3>%s</h3>' %(msg_title)
    message += '<table width="800" border="1" cellspacing="0" cellpadding="0" style="font-size:13px">'
    title = gen_title(columns)
    rows = gen_rows(dataSet)
    message += title
    message += rows
    message += '</table>'
    msg = MIMEText(message,"html","utf-8")
    msg['Subject'] = SUBJECT
    msg['From'] = FROM
    msg['To'] = TO
    try:
        server = smtplib.SMTP()
        server.connect(HOST,PORT)
        server.starttls()
        server.login("dspmail@staff.sina.com.cn","sina-Dsp")
        server.sendmail(FROM, TO, msg.as_string())
        server.quit()
        print "邮件发送成功！"
    except Exception, e:  
        print "失败："+str(e)
if __name__ == '__main__':
    msg_title = 'Test Mail'
    columns = ['IP', '集群', '角色', '检测目录', '检测值(%)']
    dataSet = [['10.13.0.162', u'\u5fae\u535a\u65e0\u7ebf', u'datanode,tasktracker', ['/data2', '83'], ['/data3', '83'], ['/data4', '84'], ['/data5', '83'], ['/data6', '83'], ['/data8', '82'], ['/data9', '83'], ['/data10', '84'], ['/data11', '83']],['10.13.0.163', u'\u5fae\u535a\u65e0\u7ebf', u'datanode,tasktracker', ['/data2', '83'], ['/data3', '83'], ['/data4', '84'], ['/data5', '83'], ['/data6', '83'], ['/data8', '82'], ['/data9', '83'], ['/data10', '84'], ['/data11', '83']]]
    send(msg_title,columns,dataSet)
