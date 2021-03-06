import datetime
import pymysql
import sqlalchemy

from openpyxl import Workbook
from openpyxl import load_workbook
from openpyxl.writer.excel import ExcelWriter

import csv


# Connect to the database
connection = pymysql.connect(
    host='1.85.40.235',
    port=28888,
    user='kods',
    password='Abc1234#',
    db='strategy_kaiyuan',
    cursorclass=pymysql.cursors.DictCursor
)


today = datetime.datetime.today().strftime("%Y-%m-%d")


with connection.cursor() as cursor:
    sql = "SELECT `symbol`, `heldqty` FROM `positions` WHERE `date`=%s AND `tag`='alpha01' AND `heldqty`>0"
    cursor.execute(sql, (today,))
    result = cursor.fetchall ()

    with open ("C:/Users/admin/Documents/alpha/position-20.csv", "w", newline="") as file:
        w = csv.writer( file, delimiter=",")
        for record in result:
            print (record, "alpha01")
            security_id, _ = record['symbol'].split(".")
            w.writerow ([ 
                int(security_id),
                int( record['heldqty'])
            ])

with connection.cursor() as cursor:
    sql = "SELECT `symbol`, `heldqty` FROM `positions` WHERE `date`=%s AND `tag`='alpha06' AND `div`=\'\' AND `heldqty`>0"
    cursor.execute(sql, (today,))
    result = cursor.fetchall ()

    with open ("C:/Users/admin/Documents/alpha/position-100.csv", "w", newline="") as file:
        w = csv.writer( file, delimiter=",")
        for record in result:
            print (record, "alpha06")
            security_id, _ = record['symbol'].split(".")
            w.writerow ([ 
                int(security_id),
                int( record['heldqty'])
            ])


from email.mime.multipart import MIMEMultipart
from email.utils import parseaddr, formataddr
import smtplib

# msg = MIMEMultipart()
# msg['From'] = 'diryox@outlook.com'
# msg['To'] = '249163213@qq.com'
# msg['Cc'] = ['tomjames822@163.com','option.service@kysec.cn']
# msg['Subject'] = "paper trading, " + today