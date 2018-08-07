#!/usr/bin/env python3
"""wait for SIGTERM and when received, read text from a list of files and post the
retrieved data to an HTTP server"""

import sys
import os
import signal

import requests

# === constants ====================================
# format string for filenames from which to read
PATH_TEMPLATE = "/rsys/fs/cgroup/{group}{path}/{file}"

# list of files to read; note cgroup support for stats varies with kernel version!
# (the files here are known to be available on linux 4.4.0 and later, provided that
# 'memory' and 'cpu,cpuacct' are mounted into 'fs/cgroup')
FILES = (
# key-in-output, conv,  group        , file
("max_usage"   , int , "memory"     , "memory.max_usage_in_bytes"),
("cpu_time"    , int , "cpu,cpuacct", "cpuacct.usage")
)

POST_TIMEOUT = 2

# === post target ==================================
tgt="servo-k8s-multijob.default" # FOR TESTING ONLY, env var should be defined!
tgt=os.environ.get("REPORT_TARGET",tgt)
URI = "http://{}:8080/".format(tgt)

# === gather and send ==============================
def gather(path):
    data = {}
    for key,conv,grp,f in FILES:
        f = PATH_TEMPLATE.format(group=grp,path=path,file=f)
        d = open(f).read().strip()
        if not isinstance(d, str):
            d = d.decode("UTF-8")
        data[key] = conv(d)
    return data

def term_handler(n,frame):
    data = gather(PATH)
    # ignore failure
    try:
        print("DATA:", repr(data), file=sys.stderr) #DEBUG
        # use 'with' to ensure session is CLOSED before we terminate this process
        with requests.Session() as s:
            s.post(URI, json = { "id": ID, "stats": data }, timeout = POST_TIMEOUT)
    except Exception:
        pass
    sys.exit()

# globals, for testing as module, replaced when ran as executable:
PATH="/"
ID="x"

# === main =========================================
if __name__ == "__main__":
    PATH=os.environ.get("OPTUNE_CGROUP_PATH")
    ID=os.environ.get("OPTUNE_SELF_ID")
    if not PATH or not ID:
        raise Exception("Error: OPTUNE_CGROUP_PATH and OPTUNE_SELF_ID must be set.")
    signal.signal(signal.SIGTERM, term_handler)
    signal.pause() # go to sleep until a signal comes
    pass
