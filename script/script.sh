#!/bin/sh
PATH=/home/ubuntu/miniconda3/bin:/usr/local/bin:/usr/local/sbin:~/bin:/usr/bin:/bin:/usr/sbin:/sbin

python /home/ubuntu/dnsstudy/script/dailyquery.py >> /home/ubuntu/dnsstudy/script/cronpython.log 2>&1

python /home/ubuntu/dnsstudy/code/multi_parsing.py --dtype=www >> /home/ubuntu/dnsstudy/script/cronparsing.log 2>&1
python /home/ubuntu/dnsstudy/code/multi_parsing.py --dtype=apex >> /home/ubuntu/dnsstudy/script/cronparsing.log 2>&1

python /home/ubuntu/dnsstudy/code/scpt_hist_query.py 

python /home/ubuntu/dnsstudy/code/scpt_hist_parsing.py --dtype=apex
python /home/ubuntu/dnsstudy/code/scpt_hist_parsing.py --dtype=www

python /home/ubuntu/dnsstudy/code/scpt_errdom_query.py 

python /home/ubuntu/dnsstudy/code/scpt_nsIP_query.py 
python /home/ubuntu/dnsstudy/code/scpt_nsIP_parsing.py 
