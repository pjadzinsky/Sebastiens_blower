import re
import requests

def get_slaves(leader=None):
    return get_state()['slaves']

def get_state(leader=None):
    _leader = leader or "marathon.mousera-private-prod.com:5050"
    resp = requests.get("http://%s/master/state.json" % _leader)
    resp = resp.json()
    if (resp['hostname'] not in resp['leader']):
        print "Hostname:", resp['hostname'], "Leader:", resp['leader']
        leader = re.match(".*@(\d+\.\d+\.\d+\.\d+:\d+).*", resp['leader']).groups()[0]
        print "Switching to leader:", leader
        return get_state(leader=leader)
    return resp

