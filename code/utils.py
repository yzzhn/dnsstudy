import dns
from typing import Literal

class Domain:
    
    """Class to hold Domain related information"""
    def __init__(self, name, rank):
        self.name = name
        self.rank = rank
        self.cname = None
        self.message = {}
        self.error = {}
        self.__cnameloop = 0

    def set_cname(self, cname:str):
        """set cname, if any, from query """
        print("Set CNAME:", cname, "for ORIG NAME:", self.name)
        self.cname = cname
        
    def set_message(self, qtype: Literal["HTTPS", "A", "AAAA"], msg: dns.message.QueryMessage):
        """store DNS answer message for a specific query type"""
        print("Set Message for Query Type:", qtype)
        self.message[qtype] = msg

    def set_error(self, qtype: Literal["HTTPS", "A", "AAAA", "CNAME"], error: str):
        """set error message from query attempt for a specific query type"""
        print("Log Error for Query Type:", qtype)
        self.error[qtype] = error

class CNameLoopsTooLong(dns.exception.DNSException):
    """ CNAME chain seems too long """
    
    
class Domain_v1:    
    """Class to hold Domain related information"""
    def __init__(self, name, rank):
        self.name = name
        self.rank = rank
        self.message = None
        self.error = None

    def set_message(self, msg):
        """store DNS answer message"""
        self.message = msg

    def set_error(self, error):
        """set error message from query attempt"""
        self.error = error