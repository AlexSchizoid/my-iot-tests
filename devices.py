#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import pycurl
import sys
import json
from StringIO import StringIO 

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

def body(buf):
    response = json.loads(buf)
    global devices
    devices = response["devices"]
    if devices:
        for iterator in devices:
            print iterator
        add_devices_to_db(devices)
        
def get_devices():
    c = pycurl.Curl()
    c.setopt(c.URL, 'http://./device-list')
    c.setopt(c.PROXY, 'http://localhost:1880')
    c.setopt(pycurl.WRITEFUNCTION, body)
    c.perform()

def body_device_response(buf):
    response = json.loads(buf)
    print response["value"]

def get_device_response(device):
    print "Getting response for device %s"% device
    storage = StringIO()
    c = pycurl.Curl()
    c.setopt(c.URL, 'http://./sensors/battery?device=%s'% device)
    c.setopt(c.PROXY, 'http://localhost:1880')
    c.setopt(pycurl.WRITEFUNCTION, storage.write)
    c.perform()
    body_device_response(storage.getvalue())

def main():
    print "starting"
    create_devices_table()
    get_devices()
    if devices:
        for device in devices:
            get_device_response(device.encode("ascii"))

if __name__ == "__main__":
    main()
