# path to store raw DNS queries
DATAROOT_DIR = "/data/raw"

# path to store tranco daily list
TRANCO_DIR = "/data/tranco"

# path th store parsed daily query
PARSE_DIR = "/data/parsed"

# path to store NS and SOA query for domain history measurement
HIST_DIR = "/data/history/raw"
HIST_PARSED = "/data/history/parsed"
HIST_DB = "/data/history/history.db"

ERRDF_DIR = "/data/errordom/raw"
ERRDF_PARSED = "/data/errordom/raw"

# path to store NS IP history measurement
NSIP_DIR = "/data/nsIP/raw"
NSIP_PARSED = "/data/nsIP/parsed"
NSIP_DB = "/data/nsIP/NameServerIP.db"

# path to store TLSconnection measurement
TLS_DIR = "/data/tlsconnect"

# default concurrency for query 
MAXCONCURRENCY = 30

# default DNS servers
RESOLVER_LIST = ['8.8.8.8', '1.1.1.1']