import os
import sys
import datetime
import subprocess


TRANCO_DIR = "/data/tranco"
LOG_DIR = "/data/log"

if __name__ == "__main__":


    today = datetime.datetime.now()
    todaystr = today.strftime("%Y-%m-%d")

    trancodir = os.path.join(TRANCO_DIR, todaystr)
    if not os.path.exists(trancodir):
        os.mkdir(trancodir)

    logdir = os.path.join(LOG_DIR, todaystr)
    if not os.path.exists(logdir):
        os.mkdir(logdir)
    

    # for complex commands, with many args, use string + `shell=True`:
    cmd_str = "wget https://tranco-list.eu/top-1m.csv.zip -P {}".format(trancodir)
    subprocess.run(cmd_str, shell=True)


    myoutput = open(os.path.join(logdir,'query.log'), 'w')
    query_str = "python /home/ubuntu/dnsstudy/code/multiproc_query.py"
    subprocess.run(query_str, shell=True, stdout=myoutput)

