# -*- coding: utf-8 -*-
"""
Python 3.8
Created on Mon Oct  5 10:57:06 2020

@author: juan.cruz2

"""

import mysql.connector

def error_handler(err, title):
    tkinter.messagebox.showerror("lgmdb - Conn error: "+ title , err)

def success_handler(title,message):
    tkinter.messagebox.showinfo("lgmdb - " + title,message)

def mysqlconnect(mysqlserver,mysqlusername,mysqlpsw):
    #mysqlserver = '172.25.20.17'
    mysqldatabase = 'db_legacy_maintenance'
    #mysqlusername = 'ISJCruz'
    #mysqlpsw = 'T3lu52018!'
    #mysqlinstancename = 'SUSWEYAK03'
    #sslpath="./ssl-certs/"

    config = {
    'user': mysqlusername,
    'database': mysqldatabase,
    'password': mysqlpsw,
    'host': mysqlserver,
    #'client_flags': [ClientFlag.SSL],
    #'ssl_ca': sslpath + '/server-ca.pem' ,
    #'ssl_cert': sslpath + '/client-cert.pem',
    #        'ssl_key': sslpath + '/client-key.pem',
            }
    try:
        mysqlconn = mysql.connector.connect(**config)
        return mysqlconn
    except mysql.connector.Error as err:
        error_handler(err,"Inventory Database")
        #messagebox.showinfo ("Connection Error", err)
        #return err

def restoreCreate(queryexec,parameters,mysqlserver,mysqlusername,mysqlpsw):
    conn=mysqlconnect(mysqlserver,mysqlusername,mysqlpsw)
    cur=conn.cursor()
    cur.execute(queryexec,parameters)
    conn.commit()
    conn.close()
