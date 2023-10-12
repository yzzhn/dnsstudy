import os
import pandas as pd
import numpy as np
import asyncio
import sys
import pickle
import asyncio
import time
import datetime
import psutil
import sqlite3

# local import 
from utils import Domain, CNameLoopsTooLong
from config import MAXCONCURRENCY, RESOLVER_LIST, HIST_DIR, HIST_DB, PARSE_DIR
from asyncquery import *

from itertools import islice
import multiprocessing



def singlecore_apexquerying(df_dict):
    today = datetime.datetime.now()
    todaystr = today.strftime("%Y-%m-%d")
    
    print("Datetime:", todaystr)
    
    ### Create data path
    OUT_DIR = os.path.join(HIST_DIR, todaystr)
    print("Output Dir:", OUT_DIR)

    if not os.path.exists(OUT_DIR):
        os.mkdir(OUT_DIR)
        print("Create folder:", OUT_DIR)

    APEX_DIR = os.path.join(OUT_DIR, "apex")
    if not os.path.exists(APEX_DIR):
        os.mkdir(APEX_DIR)
        print("Create folder:", APEX_DIR)

    apex_file_fmt = os.path.join(APEX_DIR, "output_{:03d}.pickle")

    sem = asyncio.Semaphore(MAXCONCURRENCY)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    print("Query APEX History Domains")

    for key, df in df_dict.items():
        print("DataFrame Shape:", df.shape)
        dom_l = init_domain_list(df, "apex")
        print(dom_l[0].name)
        outfpath = apex_file_fmt.format(key)
        print("working on file:", key)
        print("OUTPUT FILE PATH:", outfpath)
        
        resolver = get_resolver(addresses=RESOLVER_LIST)
        RESULTS = dict()

        print("start query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        s = time.perf_counter()

        loop.run_until_complete(query_all_dns_ns_soa(domains=dom_l, resolver=resolver, sem=sem, data_dict=RESULTS))

        elapsed = time.perf_counter() - s
        print(f"{__file__} executed in {elapsed:0.6f} seconds.")

        print("end query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        with open(outfpath, 'wb') as outfile:
            pickle.dump(RESULTS, outfile)
    
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
    return 0

def singlecore_wwwquerying(df_dict):
    today = datetime.datetime.now()
    todaystr = today.strftime("%Y-%m-%d")

    print("Datetime:", todaystr)
    
    ### Create data path
    OUT_DIR = os.path.join(HIST_DIR, todaystr)
    print("Output Dir:", OUT_DIR)

    if not os.path.exists(OUT_DIR):
        os.mkdir(OUT_DIR)
        print("Create folder:", OUT_DIR)

    WWW_DIR = os.path.join(OUT_DIR, "www")
    if not os.path.exists(WWW_DIR):
        os.mkdir(WWW_DIR)
        print("Create folder:", WWW_DIR)

    www_file_fmt = os.path.join(WWW_DIR, "output_{:03d}.pickle")

    sem = asyncio.Semaphore(MAXCONCURRENCY)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    print("Query APEX History Domains")

    for key, df in df_dict.items():
        print("DataFrame Shape:", df.shape)
        dom_l = init_domain_list(df, "www")
        print(dom_l[0].name)
        outfpath = www_file_fmt.format(key)
        print("working on file:", key)
        print("OUTPUT FILE PATH:", outfpath)
        
        resolver = get_resolver(addresses=RESOLVER_LIST)
        RESULTS = dict()

        print("start query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        s = time.perf_counter()

        loop.run_until_complete(query_all_dns_ns_soa(domains=dom_l, resolver=resolver, sem=sem, data_dict=RESULTS))

        elapsed = time.perf_counter() - s
        print(f"{__file__} executed in {elapsed:0.6f} seconds.")

        print("end query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        with open(outfpath, 'wb') as outfile:
            pickle.dump(RESULTS, outfile)
    
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
    return 0


def mltproc_histquerying(apexhist, wwwhist, maxproc = psutil.cpu_count(logical = False), mute = False):

    pool = multiprocessing.Pool(processes = maxproc)

    # split into smaller df 
    MOD = 20
    MAXSPLIT = MOD * maxproc
    
    # query apex
    df_list = np.array_split(apexhist, MAXSPLIT)

    data_dict = {}
    proc_list = []
    for i in range(MAXSPLIT):
        data_dict[i]= df_list[i]
        if len(data_dict) == MOD:
            proc_list.append(data_dict)
            data_dict = {}            
    
    res = pool.map(singlecore_apexquerying, proc_list)
    
    # query www 
    df_list = np.array_split(wwwhist, MAXSPLIT)
    data_dict = {}
    proc_list = []
    for i in range(MAXSPLIT):
        data_dict[i]= df_list[i]
        if len(data_dict) == MOD:
            proc_list.append(data_dict)
            data_dict = {}            
    
    res = pool.map(singlecore_wwwquerying, proc_list)
    print ("finished query")
    return 0 

def upsert_entry(dom, logday, dbcursor, dtype="apex",):
    
    if dtype == "apex":    
        dbcursor.execute("""SELECT EXISTS(SELECT 1 FROM apexhistory WHERE apex=?);""", (dom,))
        hasrecord = dbcursor.fetchone()[0]
    
        if hasrecord == 0:  # insert new entry
            dbcursor.execute("""INSERT INTO apexhistory VALUES (?,?,?)""", (dom, logday, logday))
        elif hasrecord == 1:
            #print("hasrecord")
            dbcursor.execute("""UPDATE apexhistory SET last_seen=? WHERE apex=?""", (logday, dom))
        else:
            print("HAS RECORD ERROR...")
            return -1
        return 0

    if dtype == "www":    
        dbcursor.execute("""SELECT EXISTS(SELECT 1 FROM wwwhistory WHERE www=?);""", (dom,))
        hasrecord = dbcursor.fetchone()[0]
    
        if hasrecord == 0:  # insert new entry
            dbcursor.execute("""INSERT INTO wwwhistory VALUES (?,?,?)""", (dom, logday, logday))
        elif hasrecord == 1:
            #print("hasrecord")
            dbcursor.execute("""UPDATE wwwhistory SET last_seen=? WHERE www=?""", (logday, dom))
        else:
            print("HAS RECORD ERROR...")
            return -1
        return 0

    
def update_database(logday, dbfpath=HIST_DB):
    ## Update history database using latest data
    ## Connecting to database
    conn = sqlite3.connect(dbfpath)
    dbcursor = conn.cursor()
    
    ## loading parsed data
    apexfpath = os.path.join(PARSE_DIR, logday, "apex_https.csv")
    apexdf = pd.read_csv(apexfpath)
    apexdf = apexdf.loc[apexdf["error"]=="{}"]
    print("Loaded Data From:", apexfpath)

    cnt = 0
    for idx, item in apexdf.iterrows():
        if cnt % 1000 == 0:
            print(idx, item["domain"], logday)
        upsert_entry(item["domain"], logday, dbcursor, dtype="apex")
        cnt += 1
    
    wwwfpath = os.path.join(PARSE_DIR, logday, "www_https.csv")
    wwwdf = pd.read_csv(wwwfpath)
    wwwdf = wwwdf.loc[wwwdf["error"]=="{}"]
    print("Loaded Data From:", wwwfpath)
    
    cnt = 0
    for idx, item in wwwdf.iterrows():
        if cnt % 1000 == 0:
            print(idx, item["domain"], logday)
        upsert_entry(item["domain"], logday, dbcursor, dtype="www")
        cnt += 1

    conn.commit()
    conn.close()
    return 0

def get_hist_table(dtype, dbfpath=HIST_DB):
    conn = sqlite3.connect(dbfpath)
    if dtype == "apex":
        resdf = pd.read_sql_query("SELECT * from apexhistory", conn)
    
    if dtype == "www":
        resdf = pd.read_sql_query("SELECT * from wwwhistory", conn)

    conn.commit()
    conn.close()
    return resdf

if __name__ == "__main__":

    CPUCOUNT = psutil.cpu_count(logical = False)
    print("Total physical CPUs:", CPUCOUNT)

    today = datetime.datetime.now()
    todaystr = today.strftime("%Y-%m-%d")

    print("Datetime:", todaystr)
    
    ### Update History Database
    update_database(todaystr)
    wwwhist = get_hist_table("www")
    wwwhist["rank"] = -1
    print("www history table shape:", wwwhist.shape)
    print(wwwhist.head(1))
    apexhist = get_hist_table("apex")
    apexhist["rank"] = -1
    print("apex history table shape:", apexhist.shape)
    print(apexhist.head(1))
   

    mltproc_histquerying(apexhist=apexhist, wwwhist=wwwhist)
