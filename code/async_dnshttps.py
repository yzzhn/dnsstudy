import dns
import pandas as pd
import os

import dns.name
import dns.asyncresolver
import dns.dnssectypes

import sys
import pickle
import asyncio
import time
import datetime
import numpy as np
from typing import Literal


# local import 
from utils import Domain, CNameLoopsTooLong
from config import MAXCONCURRENCY, RESOLVER_LIST, DATAROOT_DIR


def get_resolver(addresses=None, lifetime=5, payload=1420):
    """
    Return asyncresolver object configured to use given list of addresses, and
    that sets DO=1, RD=1, AD=1, and EDNS payload for queries to the resolver.
    """

    resolver = dns.asyncresolver.Resolver()
    resolver.set_flags(dns.flags.RD | dns.flags.AD)
    resolver.use_edns(edns=0, ednsflags=dns.flags.DO, payload=payload)
    resolver.lifetime = lifetime
    if addresses is not None:
        resolver.nameservers = addresses
    return resolver


def ifCNAME(dnsrr: dns.rrset.RRset) -> bool:
    """
    Return true if the DNS record is a CNAME record.
    ref: https://dnspython.readthedocs.io/en/latest/rdatatype-list.html  
    """
    if dnsrr.rdtype == 5:
        return True
    return False

async def query_dns_rec(domain: Domain, resolver: dns.asyncresolver, query_type: str) -> dns.message.QueryMessage:
    """
    query dns records given the domain object and the dns asynchronous resolver, 
    return the corresponding dns response or raise error

    Parameters
    ----------
    domain : str
    resolver : dns.asyncresolver
    query_type : str
    """
    try:
        if domain.cname is None:
            qname = dns.name.from_text(domain.name)
        else:
            qname = dns.name.from_text(domain.cname)
    
        res = await resolver.resolve(qname, query_type, raise_on_no_answer=False)
        return res.response

    except Exception as err:
        raise err


async def query_https_rec(domain: Domain, resolver: dns.asyncresolver) -> Domain:
    """
    query dns https records given the domain object and the dns asynchronous resolver.
    if cname was returned, resolving the correct cname and query the corresponding https record.
    return the corresponding dns response or raise error

    Parameters
    ----------
    domain : str
    resolver : dns.asyncresolver    
    """
    try:
        msg = await query_dns_rec(domain, resolver, "HTTPS")
        
        # if no CNAME records, return domain https records
        if not any([ifCNAME(rr) for rr in msg.answer]):
            return msg
    
        # resolve cname and query the corresponding HTTPS records       
        try:
            cname = msg.resolve_chaining().canonical_name.to_text()
            print("resolving cname:", cname)
            
            domain.set_cname(cname)
            
            # just to make sure there's no loops.
            domain.__cnameloop__ += 1
            if domain.__cnameloop__ > 20:
                domain.set_error("CNAME", "CNameLoopsTooLong")
                raise CNameLoopsTooLong
            return query_https_rec(domain, resolver) # requery till resolved        
        except Exception as err: # capture cname resolving error 
            raise err
    except Exception as err:
        raise err

async def set_dns_records(domain: Domain, resolver: dns.asyncresolver, 
                          query_type: Literal["HTTPS", "A", "AAAA"]) -> dns.message.QueryMessage:
    """
   
    """
    try:
        if query_type == "HTTPS":
            print("before set https")
            msg = await query_https_rec(domain, resolver)
            print("after set https")

        msg = await query_dns_rec(domain, resolver, query_type)
        domain.set_message(query_type, msg)
    except Exception as err:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        domain.set_error(query_type, exc_type.__name__)
        if query_type == "HTTPS":
            raise err
            
    return domain


async def query_domain(domain: Domain, resolver: dns.asyncresolver) -> Domain:
    try:
        domain = await set_dns_records(domain, resolver, "HTTPS")
        
        https_answer = domain.message["HTTPS"].answer
        
        # raise NoAnswer error if there's no answer in https records
        if len(https_answer) == 0:
            raise dns.resolver.NoAnswer
        
        # if ipv4 or ipv6 hint in https record, query corresponding dns records
        for rr in https_answer:
            rr_str = rr.to_text()
            if "ipv4hint" in rr_str:
                domain = await set_dns_records(domain, resolver, "A")
            
            if "ipv6hint" in rr_str:
                domain = await set_dns_records(domain, resolver, "AAAA")

    except Exception as err:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("ERROR LOG:", domain.name, exc_type.__name__)
        if exc_type == dns.resolver.NoAnswer:
            raise err
    return domain


async def safe_async_query(domain: Domain, resolver: dns.asyncresolver, sem: asyncio.Semaphore):
    """
    Restrict the concurrency of query with asyncio.Semaphore.
    """
    async with sem:  # semaphore limits num of simultaneous downloads
        try:
            res = await query_domain(domain, resolver)
            RESULTS[domain.name] = res
            return res
        except:
            pass


async def query_all_dns_https(domains: [Domain], resolver: dns.asyncresolver, sem: asyncio.Semaphore):
    """
    Wrap function to query all domains
    """
    tasks = [asyncio.ensure_future(safe_async_query(domain, resolver, sem)) for domain in domains]
    await asyncio.gather(*tasks)
    
    
def init_domain_list(df: pd.DataFrame) -> [Domain]:
    dom_l = []
    for idx, item in df.iterrows():
        dom = Domain(item["name"], item["rank"])
        dom_l.append(dom)
    return dom_l

"""
async def query_https(domain: Domain, resolver: dns.asyncresolver, query_type: str = "HTTPS") -> Domain:
    
    #Asynchronous query dns records given the domain and resolver, 
    #Set the domain.message and domain.error
    #Return domain object. 

    #Parameters
    #----------
    #domain : Domain
    #resolver : dns.asyncresolver
    #query_type : str, optional (default HTTPS)
    
    try:
        qname = dns.name.from_text(domain.name)
        res = await resolver.resolve(qname, query_type, raise_on_no_answer=False)
        domain.set_message(res.response) 
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        domain.set_error(exc_type.__name__)
    return domain



"""

if __name__ == "__main__":

    # read top 1m list
    trancofpath = os.path.join(DATAROOT_DIR, "tranco_LYVK4.csv")
    print("reading csv:", trancofpath)
    tranco = pd.read_csv(trancofpath, names=["rank", "name"])

    # split into smaller df
    MAXSPLIT = 200
    df_lst = np.array_split(tranco, MAXSPLIT)

    today = datetime.datetime.now().strftime("%Y%m%d")
    OUT_FILE = "../data/2023-03-20/output_{}_{:03d}.pickle"

    sem = asyncio.Semaphore(MAXCONCURRENCY)

    loop = asyncio.get_event_loop()

    for i in range(MAXSPLIT):
        df = df_lst[i]
        dom_l = init_domain_list(df)
        print(dom_l[0].name)
        outfpath = OUT_FILE.format(today, i)
        print("output file path:", outfpath)
        print("working on file:", i)
        
        resolver = get_resolver(addresses=RESOLVER_LIST)
        RESULTS = dict()

        print("start query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        s = time.perf_counter()

        loop.run_until_complete(query_all_dns_https(domains=dom_l, resolver=resolver, sem=sem))
        

        elapsed = time.perf_counter() - s
        print(f"{__file__} executed in {elapsed:0.6f} seconds.")

        print("end query", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        with open(outfpath, 'wb') as outfile:
            pickle.dump(RESULTS, outfile)
    
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()