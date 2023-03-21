import dns
import pandas as pd
import os
import dns.name
import dns.asyncresolver
import sys
import pickle
import asyncio
import time
import datetime
import numpy as np

# local import 
from domain import Domain
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


async def query_https(domain: Domain, resolver: dns.asyncresolver, query_type: str = "HTTPS") -> Domain:
    """
    Asynchronous query dns records given the domain and resolver, 
    Set the domain.message and domain.error
    Return domain object. 

    Parameters
    ----------
    domain : Domain
    resolver : dns.asyncresolver
    query_type : str, optional (default HTTPS)
    """
    try:
        qname = dns.name.from_text(domain.name)
        res = await resolver.resolve(qname, query_type, raise_on_no_answer=False)
        domain.set_message(res.response) 
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        domain.set_error(exc_type.__name__)
    return domain


async def safe_async_query(domain, resolver: dns.asyncresolver, sem: asyncio.Semaphore):
    """
    Restrict the concurrency of query with asyncio.Semaphore.
    """
    async with sem:  # semaphore limits num of simultaneous downloads
        res = await query_https(domain, resolver)
        RESULTS[domain.name] = res
        return res


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