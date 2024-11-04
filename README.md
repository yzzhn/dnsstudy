# Async DNS query and log pipeline
[![License: CC BY 4.0](https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by/4.0/)

[Paper]() | 
[Website](https://keyinfra.cs.virginia.edu/dns_http/) | 
[Dataset](https://keyinfra.cs.virginia.edu/dns_http/artifact)


This repo is the query framework for our group's IMC 2024 publication: `Exploring the Ecosystem of DNS HTTPS Resource Records: An End-to-End Perspective.` 

The pipeline querys DNS HTTPS records from tranco lists and establishes TLS connections to domains with mismatch IP adresses in HTTPS RR.
---

### ✅Todos:
🔲 update code  
🔲 update installation guide  
🔲 update usage  
🔲 update links and docs  

---
### Table of Contents
1. [Dataset](#dataset)
1. [Installation](#installation)
2. [Folder Directory](#folder-directory)
3. [Usage](#usage)
5. [License](#license)
6. [Citation](#citation)

---

## Dataset

Our group plans to keep updating DNS HTTPS records for the Tranco 1 million domains on a monthly basis. For further information please visit our group's website: https://keyinfra.cs.virginia.edu/dns_http/

---
## Installation

To do.

---

### Folder Directory

```
imc2024dnshttps/
│
├── code/                    # Source Code
│   ├── ...                  
│   └── ...                  
│
├── script/                  # Cronjob scripts 
│   ├── ...                  
│   └── ...                  
│
├── README.md                # Project description and instructions
├── CHANGELOG.md             # Track newly added records
└── LICENSE                  # License for the project

```
---

### Usage

**Script folder**
`dailyquery.py`: pulls trancon1m records and call `code/multiproc_query.py` to query DNS HTTPS records.  
`script.sh`: cronjob scripts for daily data pulling.  
`script_tlsconnection.sh`: script that send tls connection to server that has IP mismatch.  

**Code folder**
Code folder contains the code used for query.
`config.py`: config data path, async semaphores, and DNS resolvers.  
`utils.py`: class that store query messages.  
`dnsrecord.py`: class for different DNS RRs (A, AAAA, HTTPS, SVCB, NS, SOA, RRSIG).  
`multiproc_query.py`: main query script that uses #CPU cores to query tranco 1m records. Set up asyn semaphore in `config.py` to addjust query speed.  
`scpt_*.py`: other query scripts. 

---
### License

This project is licensed under the [Creative Commons Attribution 4.0 International License (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).

[![License: CC BY 4.0](https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by/4.0/)

You are free to:
- **Share** — copy and redistribute the material in any medium or format
- **Adapt** — remix, transform, and build upon the material for any purpose, even commercially

**Under the following terms:**
- **Attribution** — You must give appropriate credit, provide a link to the license, and indicate if changes were made. You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.

For the full license, please visit [https://creativecommons.org/licenses/by/4.0/](https://creativecommons.org/licenses/by/4.0/).

### Citation
If you are using our data or code, please cite us:

```
todo: wait for IMC publication to update
```
