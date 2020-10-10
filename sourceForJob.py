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

import RestoreTestAuto
from RestoreTestAuto import *


#'gsutil acl ch -u' + GCP_ACCOUNT + ':R gs://ti-sql-02/Backups/Current/SUSWEYAK11/2019-06-06_db_thecore_ph.sql.gz
GCP_ACCOUNT = 'p836514923182-091vci@gcp-sa-cloud-sql.iam.gserviceaccount.com'

# MySQL database details to which backup to be done.
# Make sure below user having enough privileges to take databases backup.
#DB_HOST = '35.233.240.223'
#DB_HOST = 'localhost'
#DB_USER = 'sys-backup'
#DB_USER_PASSWORD = 'T3lu*U*erBackuP'

#DATA FOR GSTORAGE
BUCKET_GCS = 'ti-sql-02'
BUCKET = 'ti-sql-02/Backups/Current/'
BUCKET_PATH="/root/cloudstorage/Backups/Current/"

#DATETIME = time.strftime('%Y-%m-%d')
DATETIME = '2020-10-07'

from mysql.connector.constants import ClientFlag
from mysql.connector import Error

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

    print('Creating the CloudSQL Instance...')
    operation = create_sqlinstance(cloudsql,project,zone,generate_random_name(sqlinstance_name,5),"db-custom-4-15360",100,'SQLSERVER_2017_WEB',"Pass12345")
    wait_for_operation(cloudsql, project, operation['name'])

    print("""
    CloudSQL Instance created.
    It will take a minute or two for the instance to complete work.
    Once the CloudSQL Instance is created press enter to IMPORT a database.
    """.format(project))

    instances = list_sql_instances(cloudsql, project, operation['targetId'])

    print('CloudSQL Instances in project %s and zone %s:' % (project, zone))
    print(instances['name'])
    print(instances['email'])
    print(instances['ip'])

    add_bucket_iam_member(BUCKET_GCS,"roles/storage.admin","serviceAccount:" + instances['email'])

    if wait:
        input()

    cnx = mysql.connector.connect(**config)
    cur = cnx.cursor()
    cur.execute("SELECT srv_name, srv_ip, srv_user, srv_pwd, srv_directory, srv_os, srv_frecuency, srv_domain FROM lgm_servers where srv_active = 1 and srv_type = 'MSSQL'")
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
            print (uri)

            print('Importing a Database.')
            operation = import_sqlinstance(cloudsql, project, instances['name'], uri, rowdb2[4], 'BAK')
            wait_for_operation(cloudsql, project, operation['name'])

            print("""
            Database imported.
            Once the Database is imported press enter to DELETE the instance.
            """.format(project))

            if wait:
                input()

            print('Deleting CloudSQL instance.')

            operation = destroy_sqlinstance(cloudsql, project, instances['name'])
            wait_for_operation(cloudsql, project, operation['name'])

            icmd = "INSERT INTO db_legacy_maintenance.lgm_daily_restore(lwr_date,lwr_server,lwr_database,lwr_result) VALUES(%s,%s,%s,%s);"
            args = (DATETIME, rowdb2[2], rowdb2[4], 1)

            try:
                cnx3 = mysql.connector.connect(**config)
                cur3 = cnx3.cursor()
                cur3.execute(icmd,args)
                cnx3.commit()
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
                print ("Error ")
finally:
    cnx.close()
