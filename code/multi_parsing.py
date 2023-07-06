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
    rank = domain.rank
    name = domain.name
    cname = domain.cname
    error = domain.error
    https = None
    A = None
    AAAA = None
    
    #if len(domain.message) == 0:
    #    #print("No data....Continue")
    #    return None
    
    res = None
    
    if "HTTPS" in domain.message.keys():
        try:
            msg = domain.message["HTTPS"]
            tmp = HTTPSAnswer(msg)
            https = tmp.parse_https_answer(msg)
            if https is None:
                return None
        except Exception as err:
            raise err
    
    if "A" in domain.message.keys():
        try:
            msg = domain.message["A"]
            tmp = IPAnswer(msg)
            A = tmp.get_ip_addresses(msg)
        except Exception as err:
            raise err
                
    if "AAAA" in domain.message.keys():
        try:
            msg = domain.message["AAAA"]
            tmp = IPAnswer(msg)
            AAAA = tmp.get_ip_addresses(msg)
        except Exception as err:
            raise err
    
    res = pd.Series({"rank": rank, "domain": name, "cname": cname,
           "https": https, "a": A, "aaaa": AAAA, "error": error}).to_frame().T
    return res


def process_data(flist: list) -> pd.DataFrame:
    df = pd.DataFrame(columns = ["rank", "domain", "cname", "https", "a", "aaaa", "error"])
    errdf = pd.DataFrame(columns = ["domain", "runtimeErrorType", "runtimeErrorInfo"])

    for f in flist:
        print(f)
        with open (f, 'rb') as infile:
            domain_dict = pickle.load(infile)

        for key, value in domain_dict.items():
            try:
                res = unpack_domain_obj(value)
                if res is None:
                    print(f, key, "EMPTY")
                    continue
                df = pd.concat([df, pd.DataFrame(res)], ignore_index=True)
            except Exception as err:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                errdf = pd.concat([errdf, pd.Series({"fname": f, "domain":key, "runtimeError": exc_type.__name__, 
                                                "runtimeErrorInfo": exc_value}).to_frame().T],
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
@click.option("--date", default=None, help="Specify Date to Parse. Default None. If None, parse current date.")
@click.option("--dtype", default="apex", help="Specify Data Type to Parse. Default apex. Choose from [apex, www]. ")
def cmd(date, dtype):
    """Parsing HTTPS DNS Records Data"""

    if date is None:
        today = datetime.datetime.now()
        todaystr = today.strftime("%Y-%m-%d")
    else:
        try:
            ## test if argument date is correct format:
            today = datetime.datetime.strptime(date, "%Y-%m-%d")
            todaystr = date
        except ValueError as ve:
            print(f'You entered {date}, which is not a valid date format(%Y-%m-%d).')
            
    if dtype not in ["apex", "www"]:
        raise ValueError('Choose from [apex, www]')
    
    CPUCOUNT = psutil.cpu_count(logical = False)
    print("Total physical CPUs:", CPUCOUNT)
    
    fl = glob.glob(f"/data/raw/{todaystr}/{dtype}/*.pickle")
    fl.sort()
    print("Total length:", len(fl))

    if len(fl) == 0:
        print("NO Files")

    parsing_dir = os.path.join("/data/parsed", todaystr) 
    if not os.path.exists(parsing_dir):
        os.mkdir(parsing_dir)
        print("Create folder:", parsing_dir)

    datadf, errtotal = mltproc_detection(fl)
    datadf.to_csv(os.path.join(parsing_dir, f"{dtype}_https.csv"), index=False)
    errtotal.to_csv(os.path.join(parsing_dir, f"{dtype}_error.csv"), index=False)
        

if __name__ == "__main__":
    cmd()
    