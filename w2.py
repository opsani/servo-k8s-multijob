#!/usr/bin/env python3
from __future__ import print_function

import sys
import os
import errno
import subprocess
import time
import calendar

# import re
import requests
import json
import select
import signal

# 'compact' format json encode (no spaces)
json_enc = json.JSONEncoder(separators=(",",":")).encode

# === config ==============
# defaults
cfg = {
    "url_pattern" : "https://us-central1-optune-saas-collect.cloudfunctions.net/metrics/{acct}/{app_id}/servo",
    "account" : None,
    "auth_token" : None,
    "send_timeout" : 10,
    "namespaces" : "default"
}

# update from env
cfg["url_pattern"] = os.environ.get("OPTUNE_URL",cfg["url_pattern"])
cfg["account"] = os.environ.get("OPTUNE_ACCOUNT",cfg["account"])
cfg["auth_token"] = os.environ.get("OPTUNE_AUTH_TOKEN",cfg["auth_token"])
cfg["namespaces"] = os.environ.get("POD_NAMESPACES",cfg["namespaces"])

# split into array for easy use
cfg["namespaces"] = cfg["namespaces"].split(",")

def k_get(namespace, qry):
    '''run kubectl get and return parsed json output'''

    # this will raise exception if it fails:
    r = subprocess.check_output(["kubectl", "--namespace="+namespace, "get", "--output=json", qry])
    r = r.decode('utf-8')
    r = json.loads(r)
    return r

def k_get_raw(namespace, qry, api="/api/v1"):
    '''use kubectl to make a direct API call'''

    if namespace:
        tgt = "/".join( (api, "namespaces", namespace, qry) )
    else:
        tgt = "/".join( (api, qry) )
    r = subprocess.check_output(["kubectl", "get", "--raw", tgt ] )
    r = r.decode('utf-8')
    r = json.loads(r)
    return r

def k_patch_json(namespace, typ, obj, patchstr):
    '''run kubectl 'patch --type=json' (list of patch ops) and return parsed json output'''

    # this will raise exception if it fails:
    cmd = ["kubectl", "--namespace="+namespace, "patch", "--output=json", "--type=json", typ, obj, "-p", patchstr]
    r = subprocess.check_output(cmd)
    r = r.decode('utf-8')
    r = json.loads(r)
    return r


def check_and_patch(obj, jobid):
    if "initializers" not in obj["metadata"]:
#        print("   no initializers", file=sys.stderr) # DEBUG
        return

    pending = obj["metadata"]["initializers"].get("pending",[])
    if not pending:
#        print("   initializers empty", file=sys.stderr) # DEBUG
        return # empty list

    if pending[0]["name"] != "initcfg.optune.io":
#        print("   not our turn, current list:", repr(pending), file=sys.stderr) # DEBUG
        return # not our

    # patch the object - ALWAYS apply this, even if not 'our' pod (the initializer config affects ALL namespaces, so we have to update, otherise pods just won't start)
    patch = [{"op": "remove", "path": "/metadata/initializers/pending/0"}]

    # if one of 'our' pods, apply other changes that we want
    if jobid is not None:
        print("starting {}".format(obj["metadata"]["name"]),file=sys.stderr)
        # patch.append(...) # TODO: append other changes
        pass # do nothing for now

    patch_str = json_enc(patch)
    k_patch_json(obj["metadata"]["namespace"], "pod", obj["metadata"]["name"], patch_str)

def getenv(envarray, key):
    """get a value from a k8s "env" object (array of {"name":x,"value":y}); return None if not found"""
    for e in envarray:
        if e["name"] == key:
            return e["value"]
    return None

def get_jobid(obj):
    """check if the k8s object is one of those we care about and return the configured JOBID env variable value if so, otherwise return None.
    the expected object type is 'pod' here, but this might change.
    """

    if obj["metadata"]["namespace"] not in cfg["namespaces"]:
        return None
    return getenv( obj["spec"]["containers"][0].get("env", []), "JOBID" )


iso_z_fmt = "%Y-%m-%dT%H:%M:%SZ"


def report(jobid, obj, m):
    """send a 'measure' event for a job completion"""


# d = either data from measure() or {status:failed, message:"..."}
    d = { "metrics" : m }
    d["annotations"] = { "resources": json_enc(obj["spec"]["containers"][0].get("resources",{})), "exitcode" : obj["status"]["containerStatuses"][0]["state"]["terminated"]["exitCode"] }
    post = {"event":"MEASUREMENT", "param" : d}
    # (TODO: this might need to be done in a separate thread, not to block the watch loop ; not critical if we use a short timeout here ; )
    print("POST", json_enc(post), file=sys.stderr)

# time curl -X POST -H 'Content-type: application/json'  -H 'Authorization: Bearer danuhBkOxjrexQuAksmRyQftRMkn2EyM' https://us-central1-optune-saas-collect.cloudfunctions.net/metrics/app1/servo -d @/tmp/payload
    args = {}
    if cfg["auth_token"]:
        args["headers"] = {"Authorization": "Bearer " + cfg["auth_token"] }
    try:
        url = cfg["url_pattern"].format(app_id=jobid, acct=cfg["account"])
        r = requests.post(url,json=post,timeout=cfg["send_timeout"], **args)
        if not r.ok: # http errors don't raise exception
            print("{} {} for url '{}', h={}".format(r.status_code, r.reason, r.url, repr(r.request.headers)))
    except Exception as x: # connection errors
        print( str(x), file = sys.stderr )


# track pods that we see entering "terminated" state (used to avoid sending a report more than once); entries are cleared from this map on a DELETED event
g_pods = {}

def watch1(ln):
    c = json.loads(ln)
    obj = c["object"]
    if obj["kind"] != "Pod":
        # warn, shouldnt happen
        return

    jobid = get_jobid(obj)

    print("watched obj {}: {}".format(c["type"], obj["metadata"]["name"]), file=sys.stderr)

    if c["type"] == "DELETED" and jobid is not None:
        g_pods.pop("{}/{}".format(obj["metadata"]["namespace"],obj["metadata"]["name"]),None)

    if not c["type"] in ("ADDED", "MODIFIED"):
        return # ignore delete and other changes

    check_and_patch(obj, jobid)

    # track job completion
    if jobid is not None:
        pid = "{}/{}".format(obj["metadata"]["namespace"],obj["metadata"]["name"])
        if pid not in g_pods and "containerStatuses" in obj["status"]:
            c0state = obj["status"]["containerStatuses"][0]["state"]
            if "terminated" in c0state:
                g_pods[pid] = True
                cc = calendar.timegm(time.strptime(obj["metadata"]["creationTimestamp"], iso_z_fmt))
                t = c0state["terminated"]
                cs = t["startedAt"]
                cs = calendar.timegm(time.strptime(cs, iso_z_fmt))
                ce = t["finishedAt"]
                ce = calendar.timegm(time.strptime(ce, iso_z_fmt))
                m = { "duration": {"value":ce - cs, "unit":"s"} }
                try: # "done-at" isn't mandatory
                    eta = int(obj["metadata"]["annotations"]["done-at"])
                    m["est_duration"] = {"value":eta - cc, "unit":"s"}
                except (KeyError,ValueError):
                    pass
                report(jobid, obj, m )


# global var storing the external 'kubectl' process obj, used to terminate it when
# we get INTR or TERM singal.
# TODO: temporary, not useable if we run more than one background process
g_p = None

def run_watch(v, p_line):

    api = "/api/v1" # FIXME
    qry = "pods?includeUninitialized=true&watch=1&resourceVersion="+str(v)
    tgt = "/".join( (api, qry) )
    cmd = ["kubectl", "get","--request-timeout=0", "--raw", tgt ]

    stderr = [] # collect all stderr here FIXME: don't collect stderr
    stdin = b''         # no stdin
    proc = subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    g_p = proc

    wi = [proc.stdin]
    ei = [proc.stdin, proc.stdout,proc.stderr]
    eof_stdout = False
    eof_stderr = False #
    while True:
        r,w,e = select.select([proc.stdout,proc.stderr], wi, ei )
        if eof_stdout and eof_stderr and proc.poll() is not None: # process exited and no more data
            break
        for h in r:
            if h is proc.stderr:
                l = h.read(4096)
                if not l:
                    eof_stderr = True
                    continue
                stderr.append(l)
            else: # h is proc.stdout
                l = h.readline()
                if not l:
                    eof_stdout = True
                    continue
                stdout_line = l.strip().decode("UTF-8") # there will always be a complete line, driver writes one line at a time
                print('STDOUT:', stdout_line) # DEBUG FIXME
                if not stdout_line:
                    continue # ignore blank lines (shouldn't be output, though)
                try:
                    stdout = json.loads(stdout_line)
                except Exception as x:
                    proc.terminate()
                    # TODO: handle exception in json.loads?
                    raise
                p_line(stdout_line)
        if w:
            l = min(getattr(select,'PIPE_BUF',512), len(stdin)) # write with select.PIPE_BUF bytes or less should not block
            if not l: # done sending stdin
                proc.stdin.close()
                wi = []
                ei = [proc.stdout,proc.stderr]
            else:
                proc.stdin.write(stdin[:l])
                stdin = stdin[l:]
        # if e:

    rc = proc.returncode
    g_p = None

    if rc == 1 and len(stderr) == 1 and "unexpected EOF" in stderr[0]:
        return 0 # this is OK, it times out after 5 minutes
    print("exited, code=", rc, file=sys.stderr) # DEBUG
    print("".join(stderr))
    return rc


def watch():
    pods = k_get_raw("", "pods?includeUninitialized=true")
    for p in pods.get("items", []): # should exist?
        jobid = get_jobid(p)
        if jobid is None:
            print("existing other obj: {}".format(p["metadata"]["name"]), file=sys.stderr) # DEBUG
        else:
            print("existing job obj: {}".format(p["metadata"]["name"]), file=sys.stderr) # DEBUG

        check_and_patch(p, jobid)

    while True:
        r = run_watch(pods["metadata"]["resourceVersion"], watch1)
        if r:
            return # exit (we'll be restarted from scratch - safer than re-running watch, in case of an error)
        print ("INFO: re-submitting watch request",file=sys.stderr)


def intr(sig_num, frame):
    # if we want to kill(0,sig) from the handler: 
    signal.signal(sig_num, signal.SIG_DFL)

    if g_p:
        g_p.terminate()
    else:
        os.kill(0, sig_num) # note this loses the frame where the original signal was caught
        # sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, intr)
    signal.signal(signal.SIGINT, intr)

    print(repr(cfg),file=sys.stderr) #DEBUG
    watch()
