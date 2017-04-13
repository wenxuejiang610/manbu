#coding: utf-8
import smtplib
from email.mime.text import MIMEText

HOST = "mail.staff.sina.com.cn"
PORT = 587
SUBJECT = u"Hadoop_DiskCheck"
TO = "hbase-eng@staff.sina.com.cn"
#TO = "chencheng5@staff.sina.com.cn"
FROM = "dspmail@staff.sina.com.cn"

def gen_title(columns):
    title = '<tr bgcolor="#CECFAD" height="20">'
    for column in columns:
        title += '<th>' + column + '</th>'
    title += '</tr>'
    return title

def gen_rows(dataSet):
    rows = ""
    for row in dataSet:
        if type(row) == list:
            rows += '<tr>'
            for e in row:
                if type(e) == unicode:
                    se = e.encode('utf8')
                    rows += "<td>" + se + "</td>"
                else:
                    rows += "<td>" + str(e) + "</td>"
        else:
            rows += '<td><a href="%s">查看详细>></a></td>' %(row)
            rows += '</tr>'
    return rows

def send(msg_title,columns,dataSet):
    message = '<h3>%s</h3>' %(msg_title)
    message += '<table width="900" border="1" cellspacing="0" cellpadding="0" style="font-size:13px">'
    title = gen_title(columns)
    rows = gen_rows(dataSet)
    message += title
    message += rows
    message += '</table>'
    msg = MIMEText(message,"html","utf-8")
    msg['Subject'] = SUBJECT
    msg['From']=FROM
    msg['To']=TO
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
    columns = ['host', 'cluster', 'role', 'err', 'more']
    dataSet = [['10.10.10.1','test','hadoop','diskerr'],'www.sina.com.cn',['10.10.10.1','test','hadoop','diskerr'],'www.sina.com.cn']
    send(msg_title,columns,dataSet)
