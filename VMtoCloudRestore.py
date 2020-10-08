#!/usr/bin/python
#Maintenance Database
DBM_HOST = '172.25.20.17'
DBM_USER = 'GenBackupUser'
DBM_USER_PASSWORD = 'DBB@ckuPU53r*'
DBM_NAME = 'db_legacy_maintenance'
SERVER_NAME = 'SUSWEYAK03'
SSL_PATH="/ssl-certs/"

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

DATETIME = time.strftime('%Y-%m-%d')

from mysql.connector.constants import ClientFlag
from mysql.connector import Error

config = {
    'user': DBM_USER,
    'password': DBM_USER_PASSWORD,
    'host': DBM_HOST,
    'database': DBM_NAME,
    'client_flags': [ClientFlag.SSL],
    'ssl_ca': SSL_PATH + SERVER_NAME + '/server-ca.pem' ,
    'ssl_cert': SSL_PATH + SERVER_NAME + '/client-cert.pem',
    'ssl_key': SSL_PATH + SERVER_NAME + '/client-key.pem',
}

#config2 = {
#    'user': DB_USER,
#    'password': DB_USER_PASSWORD,
#    'host': DB_HOST,
#}

try:
    #cnx0 = mysql.connector.connect(**config2,
    #                          auth_plugin='mysql_native_password')
    #cur0 = cnx0.cursor()
    #cur0.execute("select SCHEMA_NAME from information_schema.SCHEMATA l where l.SCHEMA_NAME not in ('sys','mysql','information_schema','performance_schema');")
    #db0 = cur0.fetchall()
    #for row2del in db0:
    #    delcmd = "DROP SCHEMA " + row2del[0] + ";"
    #    curd = cnx0.cursor()
    #    curd.execute(delcmd)
    #    cnx0.commit()
    #cur0.close()
    #cnx0.close()

    cnx = mysql.connector.connect(**config)
    cur = cnx.cursor()
    cur.execute("SELECT srv_name, srv_ip, srv_user, srv_pwd, srv_directory, srv_os, srv_frecuency, srv_domain FROM lgm_servers where srv_active = 1 and srv_type = 'MYSQL'")
    dbs = cur.fetchall()
    for rowdb in dbs:
        #print rowdb[0]
        logcmd = "select lbl_id, lbl_date, lbl_server, lbl_filename, "
        logcmd += "SUBSTRING(lbl_filename, INSTR(lbl_filename,'_')+1, "
        logcmd += "INSTR(lbl_filename,'.') - INSTR(lbl_filename,'_')-1) dbname "
        logcmd += "from lgm_backups_log where lbl_filename not like '%mysql%' "
        logcmd += " and lbl_date = '"
        logcmd += DATETIME +  "' and lbl_server = '"
        logcmd += rowdb[0]  + "' order by RAND() limit 1 "
        cur2 = cnx.cursor()
        cur2.execute(logcmd)
        dbs2 = cur2.fetchall()
        for rowdb2 in dbs2:
            bucket = 'gs://' + BUCKET + rowdb2[2] + '/' +  rowdb2[3]
            print (bucket)
            #createcmd = 'mysql -h' + DB_HOST + ' -u' + DB_USER + ' -p' + DB_USER_PASSWORD + ' -e "CREATE DATABASE IF NOT EXISTS ' +  rowdb2[4] +'";'
            #restorecmd= 'gunzip < ' + '/backup/dumps/' +  rowdb2[4] + '.sql.gz | mysql  --default-character-set=utf8 -h' + DB_HOST + ' -u' + DB_USER + ' -p' + DB_USER_PASSWORD + ' ' + rowdb2[4]
            #delcmd = 'rm -rf ' + '/backup/dumps/' +  rowdb2[4] + '.sql.gz'

            #os.system(delcmd)
            #os.system(bucketcmd)
            #os.system(createcmd)
            #os.system(restorecmd)

            #icmd = "INSERT INTO db_legacy_maintenance.lgm_daily_restore(lwr_date,lwr_server,lwr_database,lwr_result) VALUES(%s,%s,%s,%s);"
            #args = (DATETIME, rowdb2[2], rowdb2[4], 1)

            #try:
            #    cnx3 = mysql.connector.connect(**config)
            #    cur3 = cnx3.cursor()
            #    cur3.execute(icmd,args)
            #    cnx3.commit()
            #except Error as error:
            #    print(error)

            #finally:
            #    cur3.close()
            #    cnx3.close()

            #os.system(delcmd)

            #pcmd = 'gsutil acl ch -u' + GCP_ACCOUNT + ':R gs://' + BUCKET + rowdb2[2] + '/' +  rowdb2[3]
            #impcmd = 'gcloud sql import sql isdba-mysql-restore gs://' + BUCKET + rowdb2[2] + '/' +  rowdb2[3] + ' --quiet --database=' + rowdb2[4]
            #os.system(pcmd)
            #os.system(impcmd)
            #print (pcmd)
            #print ("Database " + rowdb2[4] + " restored")
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
