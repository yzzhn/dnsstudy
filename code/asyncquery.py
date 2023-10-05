import dns
import pandas as pd
import os
import traceback

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


def get_resolver(addresses=None, lifetime=5, payload=1420, AuthenticData=False):
    """
    Return asyncresolver object configured to use given list of addresses, and
    that sets DO=1, RD=1, AD=1, and EDNS payload for queries to the resolver.
    """
    resolver = dns.asyncresolver.Resolver()

    if AuthenticData == True:
        resolver.set_flags(dns.flags.RD | dns.flags.AD)
    else:
        resolver.set_flags(dns.flags.RD)
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
    query dns records given the domain object, the dns asynchronous resolver, and query type
    return the corresponding dns response or raise error

    Parameters
    ----------
    domain : Domain
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
    domain : Domain
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

            domain.set_cname(cname)
            
            # just to make sure there's no loop.
            domain.__cnameloop__ += 1
            if domain.__cnameloop__ > 20:
                domain.set_error("CNAME", "CNameLoopsTooLong")
                raise CNameLoopsTooLong
                
            print("REQUERYING CNAME:", domain.cname)
            coro = query_https_rec(domain, resolver)
            task = await asyncio.create_task(coro)
            return task # requery till resolved
        except Exception as err: # capture cname resolving error 
            raise err
            
    except Exception as err: # capture dns https query error
        raise err

        
async def set_dns_records(domain: Domain, resolver: dns.asyncresolver, 
                          query_type: Literal["HTTPS", "A", "AAAA", "NS", "SOA"]) -> dns.message.QueryMessage:
    """
    query dns https records given the domain object and the dns asynchronous resolver.
    if cname was returned, resolving the correct cname and query the corresponding https record.
    return the corresponding dns response or raise error

    Parameters
    ----------
    domain : Domain
    resolver : dns.asyncresolver,
    query_type : Literal["HTTPS", "A", "AAAA"]
    """
    
    try:
        if query_type == "HTTPS":
            msg = await query_https_rec(domain, resolver)
        else:
            msg = await query_dns_rec(domain, resolver, query_type)
        domain.set_message(query_type, msg) # store message
    except Exception as err:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        domain.set_error(query_type, exc_type.__name__) # store error
        
        if query_type == "HTTPS":
            raise err
        
    return domain


async def query_domain(domain: Domain, resolver: dns.asyncresolver) -> Domain:
    try:
        domain = await set_dns_records(domain, resolver, "HTTPS")
        
        https_answer = domain.message["HTTPS"].answer
        
        # raise NoAnswer error if the returned answer is empty https records
        if len(https_answer) == 0:
            raise dns.resolver.NoAnswer
        
        """ deprecated 2023-07-06
        # if ipv4 or ipv6 hint in https record, query corresponding dns records
        for rr in https_answer:
            rr_str = rr.to_text()
            if "ipv4hint" in rr_str:
                domain = await set_dns_records(domain, resolver, "A")
            
            if "ipv6hint" in rr_str:
                domain = await set_dns_records(domain, resolver, "AAAA")
        """
        
        """ starting from 2023-07-06, we query A, AAAA, NS for any domain that has HTTPS rr """
        domain = await set_dns_records(domain, resolver, "NS")
        domain = await set_dns_records(domain, resolver, "SOA")
        domain = await set_dns_records(domain, resolver, "A")
        domain = await set_dns_records(domain, resolver, "AAAA")

    except Exception as err:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # raise noanswer error so we can capture this error and skip the request during safe_async_query()
        if exc_type == dns.resolver.NoAnswer: 
            #pass
            raise err 
        if exc_type != dns.resolver.NoAnswer:
            print("ERROR LOG:", domain.name, ",ERROR TYPE:", exc_type.__name__, ",ERROR VALUE:", exc_value)
        #msg = "".join(traceback.format_exception(type(err), err, err.__traceback__))
        #print("ERROR LOG:",msg)
    return domain


async def safe_async_query(domain: Domain, resolver: dns.asyncresolver, sem: asyncio.Semaphore, data_dict:dict):
    """
    Restrict the concurrency of query with asyncio.Semaphore.
    """
    async with sem:  # semaphore limits num of simultaneous queries
        try:
            res = await query_domain(domain, resolver)
            data_dict[domain.name] = res #store data in dictionary
            return res
        except:
            # if the request domain does not have HTTPS answer, we do not store the data in data_dict.
            pass


async def query_all_dns_https(domains: [Domain], resolver: dns.asyncresolver, sem: asyncio.Semaphore, data_dict:dict):
    """
    Wrap function to query all domains
    """
    tasks = [asyncio.ensure_future(safe_async_query(domain, resolver, sem, data_dict)) for domain in domains]
    await asyncio.gather(*tasks)
    
    
def init_domain_list(df: pd.DataFrame, columnname: str) -> [Domain]:
    """
    initiate all domain into a list
    """
    dom_l = []
    for idx, item in df.iterrows():
        dom = Domain(item[columnname], item["rank"])
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


async def query_domain_ns_soa(domain: Domain, resolver: dns.asyncresolver) -> Domain:
    try:
        """query NS and SOA for any given domain"""
        domain = await set_dns_records(domain, resolver, "NS")
        domain = await set_dns_records(domain, resolver, "SOA")
    except Exception as err:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # raise noanswer error so we can capture this error and skip the request during safe_async_query()
        if exc_type == dns.resolver.NoAnswer: 
            #pass
            raise err 
        if exc_type != dns.resolver.NoAnswer:
            print("ERROR LOG:", domain.name, ",ERROR TYPE:", exc_type.__name__, ",ERROR VALUE:", exc_value)
        #msg = "".join(traceback.format_exception(type(err), err, err.__traceback__))
        #print("ERROR LOG:",msg)
    return domain


async def safe_async_query_ns_soa(domain: Domain, resolver: dns.asyncresolver, sem: asyncio.Semaphore, data_dict:dict):
    """
    Restrict the concurrency of query with asyncio.Semaphore.
    """
    async with sem:  # semaphore limits num of simultaneous queries
        try:
            res = await query_domain_ns_soa(domain, resolver)
            data_dict[domain.name] = res #store data in dictionary
            return res
        except:
            # if the request domain does not have HTTPS answer, we do not store the data in data_dict.
            pass


async def query_all_dns_ns_soa(domains: [Domain], resolver: dns.asyncresolver, sem: asyncio.Semaphore, data_dict:dict):
    """
    Wrap function to query NS and SOA records for all given domains
    """
    tasks = [asyncio.ensure_future(safe_async_query_ns_soa(domain, resolver, sem, data_dict)) for domain in domains]
    await asyncio.gather(*tasks)


    
def get_apexName(name):
    """
    parse domain apex name, assume the name in tranco list is in decent format
    todo? tldextract to parse the domain.
    """
    subs = name.split(".")
    
    if subs[0] == "www":
        return ".".join(subs[1:])
    return name

def get_wwwName(name):
    """add `www` before domain name"""
    subs = name.split(".")
    
    if subs[0] != "www":
        return ".".join(["www", name])
    return name

