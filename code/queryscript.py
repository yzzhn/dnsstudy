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

# local import 
from utils import Domain, CNameLoopsTooLong
from config import MAXCONCURRENCY, RESOLVER_LIST, DATAROOT_DIR, TRANCO_DIR
from asyncquery import *


if __name__ == "__main__":

    today = datetime.datetime.now()
    todaystr = today.strftime("%Y-%m-%d")

    trancodir = os.path.join(TRANCO_DIR, todaystr)
    print("Reading Tranco list from: ", trancodir)

    """ read daily tranco top 1m list """
    trancofpath = os.path.join(trancodir, "top-1m.csv.zip")
    print("Reading Tranco csv......", trancofpath)
    tranco = pd.read_csv(trancofpath, names=["rank", "name"])

    tranco["apex"] = tranco["name"].apply(lambda x: get_apexName(x))
    tranco["www"] = tranco["apex"].apply(lambda x: get_wwwName(x))

    print("Parsing domain name finished.")
    
    """ split into smaller dfs"""
    MAXSPLIT = 200
    df_lst = np.array_split(tranco, MAXSPLIT)

    today = datetime.datetime.now().strftime("%Y%m%d")
    OUT_DIR = os.path.join(DATAROOT_DIR, todaystr)

    if not os.path.exists(OUT_DIR):
        os.mkdir(OUT_DIR)
        print("Create folder:", OUT_DIR)

    apex_dir = os.path.join(OUT_DIR, "apex")
    www_dir = os.path.join(OUT_DIR, "www")

    if not os.path.exists(apex_dir):
        os.mkdir(apex_dir)
        print("Create folder:", apex_dir)

    if not os.path.exists(www_dir):
        os.mkdir(www_dir)
        print("Create folder:", www_dir)


    apex_file_fmt = os.path.join(apex_dir, "output_{:03d}.pickle")
    www_file_fmt = os.path.join(www_dir, "output_{:03d}.pickle")

    """ set up maximum concurrency """
    sem = asyncio.Semaphore(MAXCONCURRENCY)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    print("Query APEX Domains")

    for i in range(MAXSPLIT):
        df = df_lst[i]
        dom_l = init_domain_list(df, "apex")
        print(dom_l[0].name)
        outfpath = apex_file_fmt.format(i)
        print("working on file:", i)
        print("output file path:", outfpath)
        
        resolver = get_resolver(addresses=RESOLVER_LIST)
        RESULTS = dict()

        print("start query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        s = time.perf_counter()

        loop.run_until_complete(query_all_dns_https(domains=dom_l, resolver=resolver, sem=sem, data_dict=RESULTS))
        

        elapsed = time.perf_counter() - s
        print(f"{__file__} executed in {elapsed:0.6f} seconds.")

        print("end query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))


        with open(outfpath, 'wb') as outfile:
            pickle.dump(RESULTS, outfile)
   
    
    # start query www domains
    print("\nQeury WWW Domains.......\n")

    for i in range(MAXSPLIT):
        df = df_lst[i]
        dom_l = init_domain_list(df, "www")
        print(dom_l[0].name)
        outfpath = www_file_fmt.format(i)
        print("output file path:", outfpath)
        print("working on file:", i)
        
        resolver = get_resolver(addresses=RESOLVER_LIST)
        RESULTS = dict()

        print("start query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        s = time.perf_counter()

        loop.run_until_complete(query_all_dns_https(domains=dom_l, resolver=resolver, sem=sem, data_dict=RESULTS))
        
        elapsed = time.perf_counter() - s
        print(f"{__file__} executed in {elapsed:0.6f} seconds.")

        print("end query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        with open(outfpath, 'wb') as outfile:
            pickle.dump(RESULTS, outfile)


    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()

    