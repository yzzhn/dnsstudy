# dnsstudy
Query DNS HTTPS records for tranco lists and establishes TLS connections to domains with mismatch IP adresses in HTTPS RR.
----
### Script folder
`dailyquery.py`: pulls trancon1m records and call `code/multiproc_query.py` to query DNS HTTPS records.

`script.sh`: cronjob scripts for daily data pulling

`script_tlsconnection.sh`: script that send tls connection to server that has IP mismatch

### Code folder
Code folder contains the code used for query.

`config.py`: config data path, async semaphores, and DNS resolvers.

`utils.py`: class that store query messages.

`dnsrecord.py`: class for different DNS RRs (A, AAAA, HTTPS, SVCB, NS, SOA, RRSIG).

`multiproc_query.py`: main query script that uses #CPU cores to query tranco 1m records. Set up asyn semaphore in `config.py` to addjust query speed.

`scpt_*.py`: other query scripts. 

