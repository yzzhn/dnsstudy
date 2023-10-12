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
import json

# local import 
from utils import Domain, CNameLoopsTooLong
from config import MAXCONCURRENCY, RESOLVER_LIST, PARSE_DIR, NSIP_DB, NSIP_DIR
from asyncquery import init_domain_list, query_all_nameserver_ip, get_resolver

from itertools import islice
import multiprocessing



def singlecore_nsquerying(df_dict):
    today = datetime.datetime.now()
    todaystr = today.strftime("%Y-%m-%d")
    
    print("Datetime:", todaystr)
    
    ### Create data path
    OUT_DIR = os.path.join(NSIP_DIR, todaystr)
    print("Output Dir:", OUT_DIR)

    if not os.path.exists(OUT_DIR):
        os.mkdir(OUT_DIR)
        print("Create folder:", OUT_DIR)

    file_fmt = os.path.join(OUT_DIR, "output_{:03d}.pickle")

    sem = asyncio.Semaphore(MAXCONCURRENCY)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    print("Query Name Server Domains")

    for key, df in df_dict.items():
        print("DataFrame Shape:", df.shape)
        dom_l = init_domain_list(df, "nameserver")
        print("Printing the first domain name:", dom_l[0].name)
        outfpath = file_fmt.format(key)
        print("working on file:", key)
        print("OUTPUT FILE PATH:", outfpath)
        
        resolver = get_resolver(addresses=RESOLVER_LIST)
        RESULTS = dict()
        
        print("start query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        s = time.perf_counter()

        loop.run_until_complete(query_all_nameserver_ip(domains=dom_l, resolver=resolver, sem=sem, data_dict=RESULTS))

        elapsed = time.perf_counter() - s
        print(f"{__file__} executed in {elapsed:0.6f} seconds.")

        print("end query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        with open(outfpath, 'wb') as outfile:
            pickle.dump(RESULTS, outfile)
    
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
    return 0


def mltproc_nsquerying(nshist, maxproc = psutil.cpu_count(logical = False), mute = False):

    pool = multiprocessing.Pool(processes = maxproc)

    # split into smaller df 
    MOD = 2
    MAXSPLIT = MOD * maxproc
    
    # query apex
    df_list = np.array_split(nshist, MAXSPLIT)

    data_dict = {}
    proc_list = []
    for i in range(MAXSPLIT):
        data_dict[i]= df_list[i]
        if len(data_dict) == MOD:
            proc_list.append(data_dict)
            data_dict = {}            
    
    res = pool.map(singlecore_nsquerying, proc_list)
    
    print ("finished query")
    return 0 


def get_ns_df(datadf: pd.DataFrame):
    ## get non dup nameservers from daily dataframes
    datadf = datadf.loc[datadf["error"]=="{}"]
    datadf = datadf.loc[~datadf["ns"].isna()][["ns"]]
    datadf["nameserver"] = datadf["ns"].apply(lambda x: json.loads(x)["NS"])
    tmpdf = datadf.explode("nameserver")[["nameserver"]]
    tmpdf = tmpdf.drop_duplicates()
    
    return tmpdf


def upsert_nameserver(ns, logday, dbcursor):
    
    if not type(ns)==str:
        return -1
    
    dbcursor.execute("""SELECT EXISTS(SELECT 1 FROM nameservers WHERE nameserver=?);""", (ns,))
    hasrecord = dbcursor.fetchone()[0]

    if hasrecord == 0:  # insert new entry
        dbcursor.execute("""INSERT INTO nameservers VALUES (?,?,?)""", (ns, logday, logday))
    elif hasrecord == 1:
        #print("hasrecord")
        dbcursor.execute("""UPDATE nameservers SET last_seen=? WHERE nameserver=?""", (logday, ns))
    else:
        print("HAS RECORD ERROR...")
        return -1
    return 0


def update_database(logday, dbfpath=NSIP_DB, datadir=PARSE_DIR):
    ## Update nameserver database using the data on logday
    ## Connecting to database
    conn = sqlite3.connect(dbfpath)
    dbcursor = conn.cursor()
    
    ## loading parsed data
    apexfpath = os.path.join(datadir, logday, "apex_https.csv")
    apexdf = pd.read_csv(apexfpath)
    print("Loaded Data From:", apexfpath)
    
    ## preprocess
    nsdf = get_ns_df(apexdf)
    print("Processed Data From:", apexfpath)

    ## insert entry
    cnt = 0
    for idx, item in nsdf.iterrows():
        if cnt % 1000 == 0:
            print(idx, item["nameserver"], logday)
        upsert_nameserver(item["nameserver"], logday, dbcursor)
        cnt += 1
        
    ## update nameserver db using www df
    wwwfpath = os.path.join(datadir, logday, "www_https.csv")
    wwwdf = pd.read_csv(wwwfpath)
    print("Loaded Data From:", wwwfpath)
    
    ## preprocess
    nsdf = get_ns_df(wwwdf)
    print("Processed Data From:", wwwfpath)
    print("Processed data shape:", nsdf.shape)
    
    cnt = 0
    for idx, item in nsdf.iterrows():
        if cnt % 1000 == 0:
            print(idx, item["nameserver"], logday)
        upsert_nameserver(item["nameserver"], logday, dbcursor)
        cnt += 1

    conn.commit()
    conn.close()
    return 0

def get_ns_table(dtype="ns", dbfpath=NSIP_DB):
    conn = sqlite3.connect(dbfpath)
    if dtype == "ns":
        resdf = pd.read_sql_query("SELECT * from nameservers", conn)

    conn.commit()
    conn.close()
    return resdf



if __name__ == "__main__":

    CPUCOUNT = psutil.cpu_count(logical = False)
    print("Total physical CPUs:", CPUCOUNT)

    today = datetime.datetime.now()
    todaystr = today.strftime("%Y-%m-%d")

    print("Datetime:", todaystr)
    
    ### Update NameServerIP Database
    update_database(todaystr)
    
    nshist = get_ns_table()
    nshist["rank"] = -1
    print("ns table shape:", nshist.shape)
    print(nshist.head(1))

    mltproc_nsquerying(nshist=nshist)
