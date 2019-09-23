#!/usr/bin/env python
from __future__ import print_function
import cx_Oracle
import json
import sys
import time
import datetime
import requests

URL = ""
DOWNLOAD = False
VERBOSE = False
SCHED_FILE = "/tmp/schedconfig.json"
TABLE = "TEST_SCHEDCONFIG"

HOST = "localhost"
PORT = ""
SERVICE_NAME = ""
DB_USER = ""
DB_PASSWD = ""

def retrieve_json(src, output=None):
    r = requests.get(src)
    
    if r.status_code == 200:
        if output:
            with open(output, "w") as outf:
                outf.write(r.text)
        else:
            return r.json()
            
    else:
        print(r.status_code)
        sys.exit()

def _prof_time(t0, message, verbose=True):
    timestamp = time.time()
    if t0:
        duration = timestamp - t0
    else:
        duration = 0
    if verbose:
        print("* %s (%.1f s)" % (message, duration))

    return timestamp

t = None
t = _prof_time(t, "Start", verbose=VERBOSE)

def utcnow_string():
    utcnow = datetime.datetime.utcnow()
    return utcnow.strftime('%Y-%m-%d %H:%M:%S')

def create_table(conn, cursor, tbl_name):
    sqlstat = """ CREATE TABLE %s (SITE_NAME VARCHAR2(50), SCHEDCONFIG NCLOB CONSTRAINT ensure_json CHECK (SCHEDCONFIG IS JSON), LAST_UPDATE DATE) """ % tbl_name
    cursor.execute(sqlstat)
    conn.commit()

def insert_schedconfig(conn, cursor, tbl_name, site, schedconfig, last_update):
    sqlstat = """ INSERT INTO %s VALUES ('%s', :schedconf, TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss')) """ % (tbl_name, site, last_update)
    cursor.execute(sqlstat, schedconf=dict2json(schedconfig))

def update_schedconfig(conn, cursor, tbl_name, site, schedconfig, last_update):
    sqlstat = """ UPDATE %s SET SITE_NAME='%s', SCHEDCONFIG=:schedconf, LAST_UPDATE=TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss') WHERE SITE_NAME='%s' """ % (tbl_name, site, last_update, site)
    cursor.execute(sqlstat, schedconf=dict2json(schedconfig))

def dict2json(inp_dict):
    return inp_dict.__str__().replace("True", "true").replace("False", "false").replace("None", "null")

if DOWNLOAD:
    js = retrieve_json(URL, output=SCHED_FILE)
    with open(SCHED_FILE, "r") as jsf:
        js = json.load(jsf)
else:
    js = retrieve_json(URL, output=None)

t = _prof_time(t, "Retrieve Json from AGIS", verbose=VERBOSE)

dsn_tns = cx_Oracle.makedsn(HOST, PORT, service_name=SERVICE_NAME)
conn = cx_Oracle.connect(user=DB_USER, password=DB_PASSWD, dsn=dsn_tns)
cursor = conn.cursor()

create_table(conn, cursor, TABLE)
t = _prof_time(t, "Create Table", verbose=VERBOSE)

last_update = utcnow_string()
for site in js:
    insert_schedconfig(conn, cursor, TABLE, site, js[site], last_update)
conn.commit()

t = _prof_time(t, "Insert Data", verbose=VERBOSE)
 
# last_update = utcnow_string()
# for site in js:
#     update_schedconfig(conn, cursor, TABLE, site, js[site], last_update)
# conn.commit()
# 
# # Test for data update
# # -----------------------------------------------------
# t = _prof_time(t, "Update Data", verbose=VERBOSE)
# 
# last_update = utcnow_string()
# for site in js:
#     update_schedconfig(conn, cursor, TABLE, site, js[site], last_update)
# conn.commit()
# 
# t = _prof_time(t, "Update Data", verbose=VERBOSE)
# 
# last_update = utcnow_string()
# for site in js:
#     update_schedconfig(conn, cursor, TABLE, site, js[site], last_update)
# conn.commit()

t = _prof_time(t, "Update Data", verbose=VERBOSE)

