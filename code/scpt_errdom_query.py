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
from config import MAXCONCURRENCY, RESOLVER_LIST, PARSE_DIR, ERRDF_DIR
from asyncquery import *

from itertools import islice
import multiprocessing



def singlecore_apexquerying(df_dict):
    today = datetime.datetime.now()
    todaystr = today.strftime("%Y-%m-%d")
    
    print("Datetime:", todaystr)
    
    ### Create data path
    OUT_DIR = os.path.join(ERRDF_DIR, todaystr)
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
    #todaystr = "2023-08-17"


    print("Datetime:", todaystr)
    
    ### Create data path
    OUT_DIR = os.path.join(ERRDF_DIR, todaystr)
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


def mltproc_errquerying(apexdf, wwwdf, maxproc = psutil.cpu_count(logical = False), mute = False):

    pool = multiprocessing.Pool(processes = maxproc)

    # split into smaller df 
    MOD = 1
    MAXSPLIT = MOD * maxproc
    
    # query apex
    df_list = np.array_split(apexdf, MAXSPLIT)

    data_dict = {}
    proc_list = []
    for i in range(MAXSPLIT):
        data_dict[i]= df_list[i]
        if len(data_dict) == MOD:
            proc_list.append(data_dict)
            data_dict = {}            
    
    res = pool.map(singlecore_apexquerying, proc_list)
    
    # query www 
    df_list = np.array_split(wwwdf, MAXSPLIT)
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


if __name__ == "__main__":

    CPUCOUNT = psutil.cpu_count(logical = False)
    print("Total physical CPUs:", CPUCOUNT)

    today = datetime.datetime.now()
    todaystr = today.strftime("%Y-%m-%d")

    print("Datetime:", todaystr)
    
    ### Update History Database
    apexdf = pd.read_csv(os.path.join(PARSE_DIR, todaystr, "apex_https.csv"))
    wwwdf = pd.read_csv(os.path.join(PARSE_DIR, todaystr, "www_https.csv"))

    apexdf_err = apexdf.loc[apexdf["error"]!="{}"][["domain", "rank"]]
    apexdf_err = apexdf_err.rename(columns={"domain":"apex"})
    wwwdf_err = wwwdf.loc[wwwdf["error"]!="{}"][["domain", "rank"]]
    wwwdf_err = wwwdf_err.rename(columns={"domain":"www"})

    print("Apex Error DF:", apexdf_err.shape)
    print("WWW Error DF:", wwwdf_err.shape)

    mltproc_errquerying(apexdf=apexdf_err, wwwdf=wwwdf_err)
