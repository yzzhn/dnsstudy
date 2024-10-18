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
import click
# local import 
from utils import Domain, CNameLoopsTooLong
from config import MAXCONCURRENCY, RESOLVER_LIST, DATAROOT_DIR, TRANCO_DIR, TLS_DIR
from asyncquery import *

from itertools import islice
import multiprocessing


def singlecore_querying(df_dict):
    apex_file_fmt = os.path.join(APEX_DIR, "output_{:03d}.pickle")
    www_file_fmt = os.path.join(WWW_DIR, "output_{:03d}.pickle")

    sem = asyncio.Semaphore(MAXCONCURRENCY)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    print("Query APEX Domains")

    for key, df in df_dict.items():
        dom_l = init_domain_list(df, "apex")
        print(dom_l[0].name)
        outfpath = apex_file_fmt.format(key)
        print("working on file:", key)
        print("OUTPUT FILE PATH:", outfpath)
        
        resolver = get_resolver(addresses=RESOLVER_LIST)
        RESULTS = dict()

        print("start query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        s = time.perf_counter()

        loop.run_until_complete(query_all_dns_https(domains=dom_l, resolver=resolver, sem=sem, 
                                                    data_dict=RESULTS, dtype="apex", logday=todaystr))

        elapsed = time.perf_counter() - s
        print(f"{__file__} executed in {elapsed:0.6f} seconds.")

        print("end query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        with open(outfpath, 'wb') as outfile:
            pickle.dump(RESULTS, outfile)
    
    # start query www domains
    print("\nQeury WWW Domains.......\n")

    for key, df in df_dict.items():
        dom_l = init_domain_list(df, "www")
        print(dom_l[0].name)
        outfpath = www_file_fmt.format(key)
        print("working on file:", key)
        print("OUTPUT FILE PATH:", outfpath)
        
        resolver = get_resolver(addresses=RESOLVER_LIST)
        RESULTS = dict()

        print("start query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        s = time.perf_counter()

        loop.run_until_complete(query_all_dns_https(domains=dom_l, resolver=resolver, sem=sem, 
                                                    data_dict=RESULTS, dtype="www", logday=todaystr))
        
        elapsed = time.perf_counter() - s
        print(f"{__file__} executed in {elapsed:0.6f} seconds.")

        print("end query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        with open(outfpath, 'wb') as outfile:
            pickle.dump(RESULTS, outfile)


    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
    return 0


def mltproc_querying(data, maxproc = psutil.cpu_count(logical = False), mute = False):

    pool = multiprocessing.Pool(processes = maxproc)

    # split into smaller df 
    MAXSPLIT = 100 * maxproc
    df_list = np.array_split(data, MAXSPLIT)

    data_dict = {}
    proc_list = []
    for i in range(MAXSPLIT):
        data_dict[i]= df_list[i]
        if len(data_dict) == 100:
            proc_list.append(data_dict)
            data_dict = {}            
    
    res = pool.map(singlecore_querying, proc_list)
    print ("finished query")
    return 0 



@click.command()
@click.option("--date", default=None, help="Specify Date to Parse. Default None. If None, parse current date.")
def cmd(date):
    ### Check Time
    global todaystr
    if date is None:
        today = datetime.datetime.now()
        todaystr = today.strftime("%Y-%m-%d")
    else:
        try:
            ## test if argument date is correct format:
            if len(date)==10:
                today = datetime.datetime.strptime(date, "%Y-%m-%d")
                todaystr = date                
        except ValueError as ve:
            print(f'You entered {date}, which is not a valid date format(%Y-%m-%d).')


    #print("TRANCO LIST DIR:", TRANCO_DIR)
    CPUCOUNT = psutil.cpu_count(logical = False)
    print("Total physical CPUs:", CPUCOUNT)
   
    trancodir = os.path.join(TRANCO_DIR, todaystr)
    print("Reading Tranco list from: ", trancodir)

    # read top 1m list
    trancofpath = os.path.join(trancodir, "top-1m.csv.zip")
    print("Reading Tranco csv......", trancofpath)
    tranco = pd.read_csv(trancofpath, names=["rank", "name"])

    tranco["apex"] = tranco["name"].apply(lambda x: get_apexName(x))
    tranco["www"] = tranco["apex"].apply(lambda x: get_wwwName(x))

    print("Parsing domain name finished.")
    
    # split into smaller df 
    MAXSPLIT = 100 * CPUCOUNT
    df_lst = np.array_split(tranco, MAXSPLIT)

    OUT_DIR = os.path.join(DATAROOT_DIR, todaystr)

    if not os.path.exists(OUT_DIR):
        os.mkdir(OUT_DIR)
        print("Create folder:", OUT_DIR)

    global TLSCONN_DIR 
    TLSCONN_DIR = os.path.join(TLS_DIR, todaystr)
    if not os.path.exists(TLSCONN_DIR):
        os.mkdir(TLSCONN_DIR)
        print("Create folder:", TLSCONN_DIR)

    global APEX_DIR 
    APEX_DIR = os.path.join(OUT_DIR, "apex")
    global WWW_DIR
    WWW_DIR = os.path.join(OUT_DIR, "www")
    
    if not os.path.exists(APEX_DIR):
        os.mkdir(APEX_DIR)
        print("Create folder:", APEX_DIR)

    if not os.path.exists(WWW_DIR):
        os.mkdir(WWW_DIR)
        print("Create folder:", WWW_DIR)
    print("Start query")
    mltproc_querying(tranco)
    return 0

if __name__ == "__main__":
    cmd()