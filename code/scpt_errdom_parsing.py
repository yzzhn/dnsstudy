import pandas as pd
import pickle
import dns
import dns.message
from typing import Literal
from utils import Domain
import glob
import sys
import psutil
import datetime
import os
import click

import multiprocessing
import numpy as np

from dnsrecords import *


def unpack_domain_obj(domain:Domain) -> pd.Series:
    version = "errdom-0.1"
    rank = domain.rank
    name = domain.name
    error = domain.error
    NS = None
    SOA = None

    res = None
            
    if "NS" in domain.message.keys():
        try:
            msg = domain.message["NS"]
            tmp = NSAnswer(msg)
            NS = tmp.get_nameservers(msg)
        except Exception as err:
            raise err
            
    
    if "SOA" in domain.message.keys():
        try:
            msg = domain.message["SOA"]
            tmp = SOAAnswer(msg)
            SOA = tmp.get_soainfo(msg)
        except Exception as err:
            raise err
            
    
    res = pd.Series({"version": version, "rank": rank, "domain": name, 
           "ns": NS, "soa": SOA, "error": error})
    
    return res


def process_data(flist: list) -> pd.DataFrame:
    df = pd.DataFrame(columns = ["version", "rank", "domain", "ns", "soa", "error"])
    errdf = pd.DataFrame(columns = ["fname", "domain", "runtimeErrorType", "runtimeErrorInfo"])

    for f in flist:
        print(f)
        with open (f, 'rb') as infile:
            domain_dict = pickle.load(infile)

        for key, value in domain_dict.items():
            try:
                #print(key, value)
                res = unpack_domain_obj(value)
                if res is None:
                    print(f, key, "EMPTY")
                    continue
                
                df = pd.concat([df, res.to_frame().T], ignore_index=True)
                #df = df.append(res, ignore_index=True)
            except Exception as err:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                errdf = pd.concat([errdf, pd.Series({"fname": f, "domain":key, "runtimeError": exc_type.__name__, "runtimeErrorInfo": exc_value}).to_frame().T],
                                  ignore_index=True)
                print("ERROR LOG:", key, exc_type.__name__)

        print(f, ":", df.shape)
        print(f, "ERROR:", errdf.shape)
    return df, errdf   


def mltproc_detection(flist, maxproc = psutil.cpu_count(logical = False), mute = False):

    pool = multiprocessing.Pool(processes = maxproc)
    
    fl_list = np.array_split(flist, maxproc)
    print("split...")
    df_tuples = pool.map(process_data, fl_list)
    
    resdf = [df_tuples[idx][0] for idx in range(len(df_tuples))]
    errdf = [df_tuples[idx][1] for idx in range(len(df_tuples))]
    resdf_total = pd.concat(resdf)
    errdf_total = pd.concat(errdf)
    return resdf_total, errdf_total



@click.command()
@click.option("--dtime", default=None, help="Specify Date to Parse. Default None. If None, parse current date.")
@click.option("--dtype", default="apex", help="Specify Data Type to Parse. Default apex. Choose from [apex, www]. ")
def cmd(dtime, dtype):
    """Query HTTPS DNS Records Data every Hour"""

    if dtime is None:
        today = datetime.datetime.now()
        todaystr = today.strftime("%Y-%m-%d")
    else:
        try:
            ## test if argument date is correct format:
            if len(dtime)==10:
                today = datetime.datetime.strptime(dtime, "%Y-%m-%d")
                todaystr = dtime
            elif len(dtime) == 13:
                todaystr = dtime
                
        except ValueError as ve:
            print(f'You entered {dtime}, which is not a valid date format(%Y-%m-%d).')
            
    if dtype not in ["apex", "www"]:
        raise ValueError('Choose from [apex, www]')
    
    CPUCOUNT = psutil.cpu_count(logical = False)
    print("Total physical CPUs:", CPUCOUNT)
    
    fl = glob.glob(f"/data/errordom/raw/{todaystr}/{dtype}/*.pickle")
    fl.sort()
    print("Total length:", len(fl))

    if len(fl) == 0:
        print("NO Files")

    parsing_dir = os.path.join("/data/errordom/parsed", todaystr) 
    if not os.path.exists(parsing_dir):
        os.mkdir(parsing_dir)
        print("Create folder:", parsing_dir)

    datadf, errtotal = mltproc_detection(fl)
    datadf.to_csv(os.path.join(parsing_dir, f"{dtype}_https.csv"), index=False)
    errtotal.to_csv(os.path.join(parsing_dir, f"{dtype}_error.csv"), index=False)
        

if __name__ == "__main__":
    cmd()
    