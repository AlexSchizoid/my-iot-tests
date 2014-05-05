#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import pycurl
import sys
import json
from StringIO import StringIO 
import thread
import time
import random

devices = 0

def create_devices_table():

    try:
        con = mdb.connect('localhost', 'root', 'root', 'waste');

        with con:
            cur = con.cursor()
            cur.execute("DROP TABLE IF EXISTS Readings")
            cur.execute("DROP TABLE IF EXISTS Devices")
            cur.execute("CREATE TABLE Devices(Id INT PRIMARY KEY AUTO_INCREMENT, \
                	 IPv6 VARCHAR(128))")
            cur.execute("CREATE TABLE Readings(Id INT PRIMARY KEY AUTO_INCREMENT, \
                     Date DATETIME, \
                     Value INT, \
                     device_id INT, \
                     FOREIGN KEY(device_id) REFERENCES Devices(Id))")

    except mdb.Error, e:
  
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)
    
    finally:    
        
        if con:    
            con.close()

def add_devices_to_db(my_list):
    try:
        con = mdb.connect('localhost', 'root', 'root', 'waste');

        with con:
            cur = con.cursor()
            if my_list:
                for i in my_list:
                    cur.execute("INSERT INTO Devices(IPv6) VALUES('%s')"% i)

    except mdb.Error, e:
        
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)

    finally:
        if con:
            con.close()


def add_sample_to_db(value, dev_id):
    try:
        con = mdb.connect('localhost', 'root', 'root', 'waste');

        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO Readings(Date, Value, device_id) VALUES(sysdate(), %d, %d)"% ( value, dev_id))

    except mdb.Error, e:
        
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)

    finally:
        if con:
            con.close()

def get_device_table():
    try:
        con = mdb.connect('localhost', 'root', 'root', 'waste');

        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM Devices")
            rows = cur.fetchall()
 
    except mdb.Error, e:
 
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)

    finally:
        if con:
            con.close()
    
def get_device_id_from_db(ipv6_addr):
    try:
        con = mdb.connect('localhost', 'root', 'root', 'waste');

        with con:
            cur = con.cursor()
            cur.execute("SELECT Id FROM Devices where IPv6 = \"%s\""% ipv6_addr)
            rows = cur.fetchall()
            #print rows[0][0]
 
    except mdb.Error, e:
 
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)

    finally:
        if con:
            con.close()

    return rows[0][0]

def body(buf):
    response = json.loads(buf)
    global devices
    devices = response["devices"]
    if devices:
        #for iterator in devices:
            #print iterator
        add_devices_to_db(devices)
        
def get_devices():
    c = pycurl.Curl()
    c.setopt(c.URL, 'http://./device-list')
    c.setopt(c.PROXY, 'http://localhost:1880')
    c.setopt(pycurl.WRITEFUNCTION, body)
    c.perform()

def body_device_response(buf):
    response = json.loads(buf)
    #print response["value"]
    return response["value"]

def get_device_response(device):
    #print "Getting response for device %s"% device
    storage = StringIO()
    c = pycurl.Curl()
    c.setopt(c.URL, 'http://./sensors/battery?device=%s'% device)
    c.setopt(c.PROXY, 'http://localhost:1880')
    c.setopt(pycurl.WRITEFUNCTION, storage.write)
    c.perform()
    return body_device_response(storage.getvalue())

def thread_func(device, delay):
    #print "Starting thread for device %s"% device
    while 1:
        time.sleep(delay)
        id_dev = int(get_device_id_from_db(device))
        value = int(get_device_response(device))
        #print "id=%d ipv6=%s value=%d"% (int(id_dev), device, int(value))
        add_sample_to_db(value,id_dev)
        

def main():
    print "starting"
    create_devices_table()
    get_devices()
    #if devices:
    #    for device in devices:
    #        get_device_response(device.encode("ascii"))
    #for device in devices:
    #    get_device_id_from_db(device)
    #get_device_table()
    for device in devices:
        thread.start_new_thread( thread_func, (device.encode("ascii"), random.randrange(10,30),) )
    
    while 1:
        pass
        time.sleep(1)
    
    

if __name__ == "__main__":
    main()
