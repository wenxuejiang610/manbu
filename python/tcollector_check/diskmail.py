#coding: utf-8
import smtplib
from email.mime.text import MIMEText

HOST = "mail.staff.sina.com.cn"
PORT = 587
SUBJECT = u"Hbase tcollector_check"
TO = "hbase-eng@staff.sina.com.cn"
#TO = "chencheng5@staff.sina.com.cn"
FROM = "dspmail@staff.sina.com.cn"

def gen_title(columns):
    title = '<tr bgcolor="#F0E68C" height="20">'
    for column in columns:
        title += '<th>' + column + '</th>'
    title += '</tr>'
    return title

def gen_rows(dataSet):
    rows = ""
    for row in dataSet:
        host = '<tr>'
        for m in row:
            host += '<td>' + m + '</td>'
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
    othermsg = '<hr>修复地址:<a href="http://10.73.11.152:8080/view/Tools/job/tcollectorAdd/build?delay=0sec">  >>To Jenkins</a>'
    message += othermsg
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
    columns = ['IP', '角色', '集群', '状态']
    dataSet = [['10.13.0.162', u'datanode,tasktracker', u'\u5fae\u535a\u65e0\u7ebf', 'close']]
    send(msg_title,columns,dataSet)
