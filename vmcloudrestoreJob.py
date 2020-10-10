#!/usr/bin/python
#Maintenance Database
DBM_HOST = '172.25.20.17'
DBM_USER = 'GenBackupUser'
DBM_USER_PASSWORD = 'DBB@ckuPU53r*'
DBM_NAME = 'db_legacy_maintenance'
SERVER_NAME = 'SUSWEYAK03'
SSL_PATH='.\\ssl-certs\\'

# Import required python libraries
import time
import datetime
import pipes
import mysql.connector
import sys

import sourceModules
from sourceModules import *

from mysql.connector.constants import ClientFlag
from mysql.connector import Error

GCP_ACCOUNT = 'p836514923182-091vci@gcp-sa-cloud-sql.iam.gserviceaccount.com'

#DATA FOR GSTORAGE
BUCKET_GCS = 'ti-sql-02'
BUCKET = 'ti-sql-02/Backups/Current/'
BUCKET_PATH="/root/cloudstorage/Backups/Current/"

#DATETIME = time.strftime('%Y-%m-%d')
DATETIME = '2020-10-08'


config = {
    'user': DBM_USER,
    'password': DBM_USER_PASSWORD,
    'host': DBM_HOST,
    'database': DBM_NAME,
    'client_flags': [ClientFlag.SSL],
    'ssl_ca': SSL_PATH + '\server-ca.pem' ,
    'ssl_cert': SSL_PATH + '\client-cert.pem',
    'ssl_key': SSL_PATH + '\client-key.pem',
}

try:
    cloudsql = build('sqladmin','v1beta4')
    sqlinstance_name = 'loveit'
    project = "ti-is-devenv-01"
    zone = "us-west1-a"
    wait = True

    print("""
    ********************************BEGIN PROCESS*******************************
    Date Start Run: {}
    Performing step 1.
    Project: {}
    Zone: {}
    CREATING CloudSQL Instance...
    """.format(DATETIME,project,zone))

    operation1 = create_sqlinstance(cloudsql,project,zone,\
    generate_random_name(sqlinstance_name,5),\
    "db-custom-4-15360",100,'SQLSERVER_2017_WEB',"Pass12345")
    wait_for_operation(cloudsql, project, operation1['name'])

    print("""
    CloudSQL Instance {} created successful.
    """.format(operation1['targetId']))

    instances = list_sql_instances(cloudsql, project, operation1['targetId'])

    print('CloudSQL Instance details:')
    print('Name: ' + instances[0]['name'])
    print('serviceAccount: ' + instances[0]['email'])
    print(instances[0]['ip'])
    print('IP: ' + instances[0]['ip'][0])

    print('Adding service account to the roles/storage.admin for GStorage.')

    add_bucket_iam_member(BUCKET_GCS,"roles/storage.admin","serviceAccount:" \
    + instances[0]['email'])

    print('Permissions completed.')

    cnx = mysql.connector.connect(**config)
    cur = cnx.cursor()
    cur.execute("SELECT srv_name, srv_ip, srv_user, srv_pwd, srv_directory, \
    srv_os, srv_frecuency, srv_domain FROM lgm_servers where srv_active = 1 \
    and srv_type = 'MSSQL'")
    dbs = cur.fetchall()
    for rowdb in dbs:
        logcmd = "select lbl_id, lbl_date, lbl_server, lbl_filename, "
        logcmd += "SUBSTRING(lbl_filename, INSTR(lbl_filename,'_')+1, \
        (LENGTH(SUBSTRING(lbl_filename, INSTR(lbl_filename,'_')+1))-\
        LENGTH(RIGHT(SUBSTRING(lbl_filename, \
        INSTR(lbl_filename,'_')+1),25)))) dbname "
        logcmd += "from lgm_backups_log where lbl_filename not like '%master%'\
        and lbl_filename not like '%model%'\
        and lbl_filename not like '%msdb%'\
        and lbl_filename like '%FULL%' and lbl_server = 'SUSWEYAK08'"
        logcmd += " and lbl_date = '"
        logcmd += DATETIME +  "' and lbl_server = '"
        logcmd += rowdb[0]  + "' order by RAND() limit 1 "
        cur2 = cnx.cursor()
        cur2.execute(logcmd)
        dbs2 = cur2.fetchall()
        for rowdb2 in dbs2:
            uri = 'gs://' + BUCKET + rowdb2[2] + '/' +  rowdb2[3]

            print("""IMPORTING the database {} from: {}""".format(rowdb2[4],uri))
            operation2 = import_sqlinstance(cloudsql, project, \
            instances[0]['name'], uri, rowdb2[4], 'BAK')
            wait_for_operation(cloudsql, project, operation2['name'])

            print("""
            Database {} imported successfully.
            """.format(rowdb2[4]))

            print('Registering it into the control database')

            icmd = "INSERT INTO db_legacy_maintenance.lgm_daily_restore\
            (lwr_date,lwr_server,lwr_database,lwr_result) VALUES(%s,%s,%s,%s);"
            args = (DATETIME, rowdb2[2], rowdb2[4], 1)

            try:
                cnx3 = mysql.connector.connect(**config)
                cur3 = cnx3.cursor()
                cur3.execute(icmd,args)
                cnx3.commit()
                print('Record added to the control database')
            except Error as error:
                print(error)

            finally:
                cur3.close()
                cnx3.close()
        cur2.close()
    cur.close()
except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print ("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print ("Database does not exist")
        else:
                print ("Unknow Error")
finally:
    cnx.close()

    print("""Destroying any related CloudSQL instance {}""".format(instances[0]['name']))

    instances = list_sql_instances(cloudsql, project, sqlinstance_name)

    if instances:
        for instance in instances:
            if instance['state']=='RUNNABLE':
                print("""Destroying CloudSQL instance {}""".format(instance['name']))
                operation = destroy_sqlinstance(cloudsql, project, instance['name'])
                wait_for_operation(cloudsql, project, operation['name'])
                print("""CloudSQL instance {} doesn't exist anymore.""".format(operation['targetId']))

    print("""
    Date End Run: {}
    """.format(DATETIME))
    print("""
    ********************************END PROCESS*******************************
    """)
