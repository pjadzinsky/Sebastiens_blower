import requests
import pprint
import json

# Chronos is running under Marathon, so we have to find it.
def find_chronos():
    r = requests.get("http://marathon.mousera-private-prod.com:8080/v2/apps/production/priority-2/infrastructure/chronos")
    # This can fail and kill the script if Chronos is missing, but that's fine.
    return r.json()['app']['tasks'][0]['host']

def send_job(job_spec):
    headers={"content-type":"application/json"}
    chronos_host = find_chronos()
    print " Job Spec:", job_spec
    r = requests.post("http://%s:31500/scheduler/iso8601" % chronos_host, data=json.dumps(job_spec), headers=headers)
    print "Chronos: send_job:"
    print "  Host:", chronos_host
    print "  Status:", r.status_code
    print "  Response:"
    try:
        print pprint.pprint(r.json())
        return r.json()
    except:
        print "None"
        return None

def get_jobs():
    c = find_chronos()
    r = requests.get("http://%s:31500/scheduler/jobs" % c)
    return r.json()
