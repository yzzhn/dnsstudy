# Change Log
All notable changes to this project will be documented in this file.

### 2023-08-15
- **Added:** 23-08-15, whenever we find a new domain that has an HTTPS records, we add it into database (history.db). 
- **Added:** 23-08-15, we query the NS and SOA records for domains in (history.db). 
- **Added:** 23-08-17, in the regular daily HTTPS query, whenever a domain has error DNS records, we query its NS and SOA records.

### 2023-07-18
-----
- **Added:** For domain that has HTTPS record, we also query its SOA records.
### 2023-07-06
----
- **Added:** For domains that has HTTPS records, we query the corresponding NS records
- **Changed:**
Removed the **authentication** flag (**AD flag**) in DNS query

### 2023-04-20
---
**Note:** Started daily query on AWS. 
 
### 2023-03-28
----
**Note:** Testing query process
- **Added:** Async query DNS HTTPS, A and AAAA records
- **Added:** DNS records parser
- **Added:** Pull tranco 1 million list daily. !uery HTTPS records for {apex} and {www} domain names. For domains that has HTTPS records, query the corresponding A and AAAA.