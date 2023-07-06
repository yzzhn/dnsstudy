import dns
import dns.message
import dns.rdtypes.IN.HTTPS
import dns.rdtypes.ANY.RRSIG
import base64
import json

class SVCBRecord:
    """
    Class to parse SVCB records
    reference: https://dnspython.readthedocs.io/en/stable/_modules/dns/rdtypes/svcbbase.html
    reference : https://datatracker.ietf.org/doc/draft-ietf-dnsop-svcb-https/
    """
    params_map = {0: "mandatory", 
                  1: "alph", 
                  2: "no-default-alph", 
                  3: "port",
                  4: "ipv4hint", 
                  5: "ech",
                  6: "ipv6hint"}
    
    def __init__(self, svcbrr):
        self.parsed = self.parse_rec(svcbrr)

    def parse_rec(self, svcbrr: dns.immutable.Dict) -> dict:
        try:
            svcb_dict = svcbrr._odict
            res = {}
            for key, value in svcb_dict.items():
                mapped_key = self.params_map[key]
                parsed_value = self.__get_svcb_attr__(key, value)
                res[mapped_key] = parsed_value
            return res
        except Exception as err:
            raise err
    
    def __get_svcb_attr__(self, svcb_key, svcb_value):
        try:
            match svcb_key:
                case 0: # mandatory
                    return svcb_value.keys
                case 1: # alph
                    return svcb_value.to_text().strip('"')
                case 2: # no-default-alph
                    return None
                case 3: # port
                    return svcb_value.port
                case 4: # ipv4hint
                    return svcb_value.to_text().strip('"')
                case 5: # ech
                    return svcb_value.to_text().strip('"')
                case 6: # ipv6hint
                    return svcb_value.to_text().strip('"')
                case _:
                    raise ValueError("SVCB Key Not Correct.")
        except Exception as err:
            raise err
    """
    def __to_str__(self, item):
        if item.__class__ == bytes:
            #return item.decode()
            return base64.b64encode(item).decode('utf-8')
        if item.__class__ in [str, int, float]:
            return item
 
    def __tuple_check__(self, tp):
        if tp.__class__ == tuple:
            tmp = []
            for i in tp:
                tmp.append(self.__to_str__(i))
            return tuple(tmp)
        
        if tp.__class__ == bytes:
            return self.__to_str__(tp)
        
        return tp
    """  
            
class RRSIGRecords:
    """
    Class to parse RRSIG records
    reference: https://dnspython.readthedocs.io/en/stable/_modules/dns/rdtypes/ANY/RRSIG.html#RRSIG
    """
    
    def __init__(self, rrsig):
        self.parsed = self.parse_rec(rrsig)

    def parse_rec(self, rrsig: dns.rdtypes.ANY.RRSIG.RRSIG) -> dict:
        try:
            ## get dns.rdtypes.ANY.RRSIG.RRSIG class variables 
            attributes = [v for v in dir(rrsig) if not callable(getattr(rrsig, v)) and v[0] != '_']
            
            res = {}
            for attr in attributes:
                parsed_value = self.__get_rrsig_attr__(attr, getattr(rrsig, attr))
                res[attr] = parsed_value
            return res
        except Exception as err:
            raise err

    def __get_rrsig_attr__(self, rrsig_attr, rrsig_value):
        try:
            if rrsig_attr in ['algorithm', 'rdclass', 'rdtype', 'type_covered']:
                return rrsig_value.name
            elif rrsig_attr == 'signer':
                return rrsig_value.to_text()
            elif rrsig_attr == 'signature':
                return base64.b64encode(rrsig_value).decode('utf-8')
            else:
                return rrsig_value
        except Exception as err:
            raise err
        
class HTTPSRecord:
    """
    class to hold parsed https records
    reference: https://dnspython.readthedocs.io/en/stable/rdata-subclasses.html?highlight=HTTPS#dns.rdtypes.IN.HTTPS.HTTPS
    reference: https://datatracker.ietf.org/doc/draft-ietf-dnsop-svcb-https/
    """
    def __init__(self, httpsrr: dns.rdtypes.IN.HTTPS.HTTPS):
        self.parsed = self.parse_rec(httpsrr)
        
    def parse_rec(self, httpsrr) -> dict:
        try:
            res = {}
            target = self.__get_target__(httpsrr)
            priority = self.__get_priority__(httpsrr)
            svcbParams = self.__get_svcbParams__(httpsrr)
            
            res['target'] = target
            res['priority'] = priority
            for key, value in svcbParams.items():
                res["svcb.{}".format(key)] = value
            return res
        except Exception as err:
            raise err
        
        
    def __get_target__(self, httpsrr) -> str:
        try:
            return httpsrr.target.to_text()
        except Exception as err:
            raise err
    
    def __get_priority__(self, httpsrr) -> int:
        try:
            return httpsrr.priority
        except Exception as err:
            raise err
            
    def __get_svcbParams__(self, httpsrr) -> dict:
        try:
            svcbrr = httpsrr.params
            return SVCBRecord(svcbrr).parsed
        except Exception as err:
            raise err

            
class HTTPSAnswer:
    """
    class to hold parsed https answers
    """
    def __init__(self, msg: dns.message.QueryMessage):
        self.answer = msg.answer
        self.parsed = None
    
    def parse_https_answer(self, msg:dns.message.QueryMessage) -> str(dict):
        res = {}
        
        #countmap = {"HTTPS": 0, "RRSIG": 0}
        for rr in msg.answer:
            if rr.rdtype == 5: # skip CNAME
                continue
            tmpres = self.__parse_by_type__(rr)
            res.update(tmpres)
        
        if res == {}:
            #print("empty")
            return None
        self.parsed = json.dumps(res)
        return json.dumps(res)
        

    def __parse_by_type__(self, rr) -> dict:
        try:
            rdata = rr.to_rdataset()
            res_l = []
            keystr = None
            
            for item in rdata:
                rdata_type = item.__class__
                
                match rdata_type:
                    case dns.rdtypes.IN.HTTPS.HTTPS: # https records
                        #count = countmap["HTTPS"]
                        #if count > 1:
                        #    raise ValueError("Multiple Entries for HTTPS records")
                        #countmap.update({"HTTPS": count+1})  # update count map

                        res = HTTPSRecord(item).parsed # parsing
                        res.update({"answer_ttl": rr.ttl})
                        res.update({"name": rr.name.to_text()})
                        res_l.append(res)
                        keystr = "HTTPS"
            
                    case dns.rdtypes.ANY.RRSIG.RRSIG: # rrsig records
                        #count = countmap["RRSIG"]
                        #countmap.update({"RRSIG": count+1}) # update count map

                        #if count > 1:
                        #    raise ValueError("Multiple Entries for RRSIG records")

                        res = RRSIGRecords(item).parsed # parsing
                        res.update({"answer_ttl": rr.ttl})
                        res.update({"name": rr.name.to_text()})
                        res_l.append(res)
                        keystr = "RRSIG"
                        #return {"RRSIG".format(count) : res}, countmap
        
                    case _:
                        raise ValueError("Record DataType Not Correct.")
                    
            return {keystr: res_l}
        except Exception as err:
            raise err
            
class IPAnswer:
    """
    class to hold parsed https answers
    """
    def __init__(self, msg: dns.message.QueryMessage):
        self.answer = msg.answer
        self.addresses = None
    
    def get_ip_addresses(self, msg:dns.message.QueryMessage) -> str(dict):
        addresses = []
        
        rdtypemap = {1:"A", 28:"AAAA"}
        
        if len(msg.answer) == 0:
            return None
        
        for rr in msg.answer:
            if rr.rdtype not in [1, 28]: # https://dnspython.readthedocs.io/en/latest/rdatatype-list.html
                continue
                
            rdata = rr.to_rdataset()
            for item in rdata:
                addresses.append(item.address)
            
            res = {rdtypemap[rr.rdtype]: ",".join([ip for ip in addresses])}
            
        self.addresses = json.dumps(res)
        return json.dumps(res) 