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

# debug
def dprint(*args):
    print(*args, file=sys.stderr)
    sys.stderr.flush()

# === const
INITIALIZER_NAME="initcfg.optune.io"
MEM_STEP=4096 # minimal useful increment in mem limit/reserve
CPU_STEP=0.001 # 1 millicore, highest resolution supported by k8s
Gi=1024*1024*1024
MAX_MEM=1*Gi
MAX_CPU=3.5

# === config ==============
# defaults
cfg = {
    "url_pattern" : "https://us-central1-optune-saas-collect.cloudfunctions.net/metrics/{acct}/{app_id}/servo",
    "account" : None,
    "auth_token" : None,
    "send_timeout" : 10,
    "namespaces" : "default",
    "appid" : "spec/containers/0/env/[JOBID]"
}

# update from env
cfg["url_pattern"] = os.environ.get("OPTUNE_URL",cfg["url_pattern"])
cfg["account"] = os.environ.get("OPTUNE_ACCOUNT",cfg["account"])
cfg["auth_token"] = os.environ.get("OPTUNE_AUTH_TOKEN",cfg["auth_token"])
cfg["namespaces"] = os.environ.get("POD_NAMESPACES",cfg["namespaces"])
cfg["appid"] = os.environ.get("OPTUNE_APP_ID",cfg["appid"])

# split into array for easy use
cfg["namespaces"] = cfg["namespaces"].split(",")

try:
    debug = os.environ.get("OPTUNE_DEBUG",0)
    debug = int(debug)
except Exception:
    debug = 0

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


def update1(obj, path1, path2, val):
    """ find the earliest subpath in obj starting from path1 that exists and prepare a patch that would make obj contain path1/path2 with a value of 'val'"""
    # TODO: this works only for nested dicts for now; to add: arrays and arrays with key value (similar to k8s env array)
    # assert path1 exists
    tmp = qry(obj, path1)
    if tmp is None:
        dprint("ERR: no {} in {}".format(path1, repr(obj)))
        return # FIXME raise Hell
    p2 = path2.split("/")
    if p2[0] == "": p2 = p1[1:] # remove leading /
    left = p2[0:-1]
    right = p2[-1]
    o = val
    while left:
        t = qry(obj, path1 + "/" + "/".join(left))
        if t is not None: # this exists, add to it
            tmp = t
            break
        # not found, go back to higher level
        o = { right : o }
        right = left[-1]
        left  = left[0:-1]
    # make the update now on our copy of obj, so the next patch 'sees' the newly added elements
    tmp[right] = o # adds or replaces it
    path = path1 + "/" + "/".join(left+[right])

    return { "op" : "add", "path" : path, "value" : o }


def update(obj, adj):
    """prepare updates for a tracked k8s object 'obj', in the form of a patch.
    """

#FIXME: proper handling of settings that don't match the app (e.g., non-existent components, values out of range, etc.)
    patches = []
    if "state" in adj and "application" in adj["state"]: adj=adj["state"]
    containers = obj["spec"]["containers"] # should be present
    cmap = { c["name"]:n for n,c in enumerate(containers) }
    # patch = [{"op": "remove", "path": "/metadata/initializers/pending/0"}]
    comps = adj.get("application",{}).get("components",{})
    for cn,c in comps.items():
        idx = cmap[cn] #!!FIXME chk it is present!
        for sn,s in c.get("settings",{}).items():
            if sn in ("mem","cpu"): # update resources
                path1 = "/spec/containers/{}".format(idx) # this part should exist
                path2 = "resources/limits/"
                if sn == "mem": path2 += "memory"
                else: path2 += "cpu"
                patches.append(update1(obj, path1, path2, s["value"]))
            # else: FIXME not implemented - other settings

    return patches


def check_and_patch(obj, jobid):
    """Test if the monitored object 'obj' has a pending initializer that matches our initializer name.
    If it does, patch it. If the object is one of 'ours', we apply all scheduled changes to it.
    If it is not, only the pending initializers list is patched.
    This should be called for *every* creation or change of an object with the same type as the one
    for which our initializer was configured.
    """
    if "initializers" not in obj["metadata"]:
#        print("   no initializers", file=sys.stderr) # DEBUG
        return

    pending = obj["metadata"]["initializers"].get("pending",[])
    if not pending:
#        print("   initializers empty", file=sys.stderr) # DEBUG
        return # empty list

    if pending[0]["name"] != INITIALIZER_NAME:
#        print("   not our turn, current list:", repr(pending), file=sys.stderr) # DEBUG
        return # not our

    # patch the object - ALWAYS apply this, even if not 'our' pod (the initializer config affects ALL namespaces, so we have to update, otherise pods just won't start)
    patch = [{"op": "remove", "path": "/metadata/initializers/pending/0"}]

    # if one of 'our' pods, apply other changes that we want
    if jobid is not None:
        if debug>2: dprint("starting {}".format(obj["metadata"]["name"]))
        fake_sleep = False
        u = None
        while True:
            r = send("WHATS_NEXT", jobid, None)
            # if not r: error (=None) or no data (={}), assume we got 'measure' (do nothing now, run the job)
            if r:
                cmd = r["cmd"]
                if cmd == "DESCRIBE":
                    d = query(obj, None) # NOTE no config support for now
                    d["metrics"] = { "duration": { "unit":"s"}, "est_duration": { "unit":"s"} }
                    d["metrics"]["perf"] = {"unit":"1/s"} # FIXME - workaround, backend wants something named 'perf'
                    send("DESCRIPTION", jobid, d) # TODO post to a thread, not to block operation here
                    continue
                elif cmd == "ADJUST":
                    # prepare updates and add them to the patch
                    u = update(obj, r["param"])
                    send("ADJUSTMENT", jobid, { }) # expected by the backend
                    # NOTE FIXME: empty data sent, there's a problem with the backend otherwise
                    continue
                elif cmd == "MEASURE":
                    break # do nothing, we'll measure at the end. FIXME: no progress reports! (we *could* use the done-at estimate to predict progress and send messages while we wait for the job to run.
                elif cmd == "SLEEP": # pretend we did and re-send whats-next, hopefully will not repeat; we can't sleep here
                    if fake_sleep:
                        if debug>3: dprint("got 'sleep' twice, ignoring and running pod without changes")
                        break # did sleep already, move on
                    if debug>3: dprint("got 'sleep', ignoring")
                    fake_sleep = True
                    continue # re-send whats-next
                else:
                    if debug>0: dprint("remote server requested {}, not understood in the current state".format(cmd))
                    break
            else:
                if debug>3: dprint("whats_next req failed, assuming 'measure'") # DEBUG, FIXME remove
                break

        if u: # apply adjustment cmd from server:
            if debug>3: dprint("ADJ: "+json_enc(u)) #FIXME REMOVE
            patch.append(u)

    patch_str = json_enc(patch)
    k_patch_json(obj["metadata"]["namespace"], "pod", obj["metadata"]["name"], patch_str)


def getenv(envarray, key, keyname="name", valname="value"):
    """get a value from a k8s "env" object (array of {"name":x,"value":y}); return None if not found"""
    for e in envarray:
        if e[keyname] == key:
            return e[valname]
    return None

def qry(o,q):
    """pick a data item from a nested data structure, based on a filename-like query string
    (a simple replacement for tools like jq)"""
    try:
        for e in q.split("/"):
            if not e: continue # skip empty (e.g., leading or duplicate "/")
            if e[0] == "[": # special query [q,k,v]: find "k"==q in array of {"k":x,"v":y} items
                a = e[1:-1].split(",")
                if len(a) == 1: a += ["name","value"]
                k,kn,vn = a
                # assert a is list
                o = getenv(o, k, kn, vn)
            elif isinstance(o,dict):
                o = o[e]
            elif isinstance(o,list):
                o = o[int(e)]
            else:
                # print(e, type(o), o)
                raise ValueError
        return o
    except Exception:
        return None

def get_jobid(obj):
    """check if the k8s object is one of those we care about and return the configured JOBID
    env variable value if so, otherwise return None. The expected object type is 'pod' here,
    but this might change.
    """

    if obj["metadata"]["namespace"] not in cfg["namespaces"]:
        return None
    return qry(obj, cfg["appid"])


iso_z_fmt = "%Y-%m-%dT%H:%M:%SZ"



def report(jobid, obj, m):
    """send a 'measure' event for a job completion"""

# d = either data from measure() or {status:failed, message:"..."}
    d = { "metrics" : m }
    d["annotations"] = {
       "resources": json_enc(obj["spec"]["containers"][0].get("resources",{})),
       "exitcode" : obj["status"]["containerStatuses"][0]["state"]["terminated"]["exitCode"] }
    send("MEASUREMENT", jobid, d)

def send(event, app_id, d):
    post = {"event":event, "param" : d}
    # (TODO: this might need to be done in a separate thread, not to block the watch loop ; not critical if we use a short timeout here ; )
    if debug>1: dprint("POST", json_enc(post))

# time curl -X POST -H 'Content-type: application/json'  -H 'Authorization: Bearer <token>' https://us-central1-optune-saas-collect.cloudfunctions.net/metrics/app1/servo -d @/tmp/payload
    r = None
    args = {}
    if cfg["auth_token"]:
        args["headers"] = {"Authorization": "Bearer " + cfg["auth_token"] }
    try:
        url = cfg["url_pattern"].format(app_id=app_id, acct=cfg["account"])
        for retry in (1,2,3):
            r = requests.post(url,json=post,timeout=cfg["send_timeout"], **args)
            if r.ok: break
            if r.status_code != 502 and r.status_code != 503:
                break
            if debug>3: dprint("rq fail 50x, retry {}".format(retry))
            time.sleep(1) # FIXME: workaround for problem in back-end
        if not r.ok: # http errors don't raise exception
            if debug>0: dprint("{} {} for url '{}', h={}".format(r.status_code, r.reason, r.url, repr(r.request.headers)))
    except Exception as x: # connection errors - note these aren't retried for now
        if debug>0: dprint( "POST FAILED: " + str(x) )

    if not r:
        return None # error

    try:
        return r.json()
    except Exception as x: # no json data
        if debug>0: dprint( "JSON PARSE FAILED: " + str(x) + "\n   INPUT:" + r.text )
        return {}

# track pods that we see entering "terminated" state (used to avoid sending a report more than once); entries are cleared from this map on a DELETED event
g_pods = {}

def watch1(ln):
    c = json.loads(ln)
    obj = c["object"]
    if c["type"] == "ERROR":
        if debug>2: dprint("watch err: ", ln) # DEBUG REMOVE ME TODO
        return None # likely 'version too old' - trigger restart
        # {"type":"ERROR","object":{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"too old resource version: 1 (3473)","reason":"Gone","code":410}}
    v = obj["metadata"]["resourceVersion"]
    if obj["kind"] != "Pod":
        # warn, shouldnt happen
        return v

    jobid = get_jobid(obj)

    if debug>1: dprint("watched obj {}: {}".format(c["type"], obj["metadata"]["name"]))

    if c["type"] == "DELETED" and jobid is not None:
        g_pods.pop("{}/{}".format(obj["metadata"]["namespace"],obj["metadata"]["name"]),None)

    if not c["type"] in ("ADDED", "MODIFIED"):
        return v # ignore delete and other changes

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
                m["perf"] = {"value":1/float(m["duration"]["value"]), "unit":"1/s"} # FIXME - remove when backend stops requiring this
                try: # "done-at" isn't mandatory
                    eta = int(obj["metadata"]["annotations"]["done-at"])
                    m["est_duration"] = {"value":eta - cc, "unit":"s"}
                except (KeyError,ValueError):
                    pass
                report(jobid, obj, m )

    return v

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
                if debug>4: dprint('STDOUT:', stdout_line) # DEBUG FIXME
                if not stdout_line:
                    continue # ignore blank lines (shouldn't be output, though)
                try:
                    stdout = json.loads(stdout_line)
                except Exception as x:
                    proc.terminate()
                    # TODO: handle exception in json.loads?
                    raise
                v = p_line(stdout_line)
                if v is None: return 1, v # failure - return to trigger a new 'get' of all pods
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
        return 0, v # this is OK, it times out after 5 minutes
    if debug>0: dprint("kubectl exited, code=", rc) # DEBUG
    if debug>1:
        stderr = "".join(stderr)
        dprint(stderr)
        send("DIAG", "_global_", {"account":cfg["account"], "reason":"kubectl watch", "stderr": stderr[:2000]})
    return rc, v


def watch():
    pods = k_get_raw("", "pods?includeUninitialized=true")
    for p in pods.get("items", []): # should exist?
        jobid = get_jobid(p)
        if jobid is None:
            if debug>3: dprint("existing other obj: {}".format(p["metadata"]["name"])) # DEBUG
        else:
            if debug>3: dprint("existing job obj: {}".format(p["metadata"]["name"])) # DEBUG

        check_and_patch(p, jobid)

    v = pods["metadata"]["resourceVersion"]
    if debug>3: print("INFO: watch",v)
    while True:
        r, v = run_watch(v, watch1)
        if r:
            return # exit (we'll be restarted from scratch - safer than re-running watch, in case of an error)
        if debug>3: dprint ("INFO: resubmit watch", v)


def intr(sig_num, frame):
    # if we want to kill(0,sig) from the handler: 
    signal.signal(sig_num, signal.SIG_DFL)

    if g_p:
        g_p.terminate()
    send("GOODBYE", "_global_", {"account":cfg["account"], "reason":"signal {}".format(sig_num)})
    os.kill(0, sig_num) # note this loses the frame where the original signal was caught
    # or sys.exit(0)

# ===
# bits from servo-k8s
def numval(v,min,max,step=1):
    """shortcut for creating linear setting descriptions"""
    return {"value":v,"min":min,"max":max, "step":step, "type": "range"}


def cpuunits(s):
    '''convert a string for CPU resource (with optional unit suffix) into a number'''
    if s[-1] == "m": # there are no units other than 'm' (millicpu)
        return ( float(s[:-1])/1000.0 )
    else:
        return (float(s))

# valid mem units: E, P, T, G, M, K, Ei, Pi, Ti, Gi, Mi, Ki
mumap = {"E":1000**6,  "P":1000**5,  "T":1000**4,  "G":1000**3,  "M":1000**2,  "K":1000,
         "Ei":1024**6, "Pi":1024**5, "Ti":1024**4, "Gi":1024**3, "Mi":1024**2, "Ki":1024}
def memunits(s):
    '''convert a string for memory resource (with optional unit suffix) into a number'''
    for u,m in mumap.items():
        if s.endswith(u):
            return ( float(s[:-len(u)]) * m )
    return (float(s))

def query(obj,desc=None):
    """create a response to 'describe' cmd from k8s pod desc and optional custom properties desc """
    # this is a simplified version compared to what the k8s servo has (single container only); if we change it to multiple containers, they will be the app's components (here the app is a single pod, unlike servo-k8s where 'app = k8s deployment'
    if not desc:
        desc = {"application":{}}
    elif not desc.get("application"):
        desc["application"] = {}
    comps = desc["application"].setdefault("components", {})

    c = obj["spec"]["containers"][0]
    cn = c["name"]
    comp=comps.setdefault(cn, {})
    settings = comp.setdefault("settings", {})
    r = c.get("resources")
    if r:
        settings["mem"] = numval(memunits(r.get("limits",{}).get("memory","0")), 0, MAX_MEM, MEM_STEP) # (value,min,max,step)
        settings["cpu"] = numval(cpuunits(r.get("limits",{}).get("cpu","0")), 0, MAX_CPU, CPU_STEP) # (value,min,max,step)
    for ev in c.get("env",[]):
                # skip env vars that match the pre-defined setting names above
                if ev["name"] in ("mem","cpu","replicas"):
                    continue
                if ev["name"] in settings:
                    s = settings[ev["name"]]
                    if s.get("type", "linear") == "linear":
                        try:
                            s["value"] = float(ev["value"])
                        except ValueError:
                            raise ConfigError("invalid value found in environment {}={}, it was expected to be numeric".format(ev["name"],ev["value"]))
                    else:
                        s["value"] = ev["value"]
    return desc
# ===


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, intr)
    signal.signal(signal.SIGINT, intr)

#    dprint(repr(cfg)) #DEBUG
    send("HELLO", "_global_",{"account":cfg["account"]}) # NOTE: (here and in other msgs) acct not really needed, it is part of the URL, to be removed
    try:
        watch()
        send("GOODBYE", "_global_", {"account":cfg["account"], "reason":"exit" }) # happens if we missed too many events and need to re-read the pods list; TODO: handle this internally without exiting
    except Exception as x:
        send("GOODBYE", "_global_", {"account":cfg["account"], "reason":str(x) })

    # TODO send pod uuid (catch duplicates)
    # diag event (exceptional cases)
