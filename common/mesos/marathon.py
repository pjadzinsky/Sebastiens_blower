import json
import pprint
import requests
import re
from common import settings

def generate_app_id(environment, project, priority):
    id = "/%s/priority-%s/%s" % (environment, priority, project)
    return id.replace("_", '-')

def send_job(job_spec):
    # labels = job_spec.setdefault('labels', {})
    # Persist a record of the requested instances for later scaling
    # labels['requested_instances']= job_spec['instances']
    print "Marathon: Deployment config:"
    pprint.pprint(job_spec)

    return request(method="post", path="/apps", data=job_spec, status_code=204)

def end_job(id=None, environment=None, project=None):
    if not id and not environment and not project:
        raise RuntimeError("Marathon: end_job: must specify an id or an environment and project")
    if id and environment and project:
        raise RuntimeError("Marathon: end_job: must specify an id or an environment and project")
    if not len(filter(None, [environment, project])):
        raise RuntimeError("Marathon: end_job: must specify environment and project together.")
    if environment:
        app = find_existing_app_with_priority(environment, project)
        if not app:
            raise RuntimeError("Marathon: end_job: found no app with those requirements: %s, %s" % (environment, project))
        id = app['id']
    request(method="delete", path="/apps/%s"%id, status_code=204)


def send_app(config, deploy_config):
    app = find_existing_app_with_priority(config['environment'], config['project'])

    # if app and app['id'] != id:
    #     print "Marathon: send_app: found an existing app: %s" % app['id']
    #     print "   Ending it..."
    #     #end_job(id=app['id'])

    return send_job(deploy_config)

def find_existing_app_with_priority(environment, project):
    apps = filter(lambda a: a.endswith(project), get_app_ids())
    apps = filter(lambda a: a.startswith("/%s" % environment), apps)
    if len(apps) > 1:
        raise RuntimeError("Marathon: has_existing_app_id: found more than one app with those requirements: %s, %s" % (environment, project))
    if not len(apps): return None
    app = apps[0]
    match = re.match(".*priority-(\d*).*", app)
    if not match: return None
    return {
        'app'     : app,
        'priority': int(match.groups()[0])
    }

def has_existing_app_id(id):
    apps = filter(lambda a: a.endswith(id), get_app_ids())
    if len(apps) > 1:
        raise RuntimeError("Marathon: has_existing_app_id: found more than one app with that id : %s" % id)
    return 0 < len(apps)

def get_app_ids(prefix='/'):
    apps = get_apps()['apps']
    return [app['id'] for app in apps if app['id'].startswith(prefix)]

def get_apps(id="/"):
    r = request(method='get', path="/apps%s" % id, status_code=200)
    if not r: raise RuntimeError("Marathon: get_apps: failed")
    return r

def get_groups(group=""):
    r = request(method='get', path="/apps%s" % group, status_code=200)
    if not r: raise RuntimeError("Marathon: get_apps: failed")
    return r

def get_group(group):
    return get_groups(group=group)

def request(method="get", path='/', data=None, status_code=200):
    headers={"content-type":"application/json"}
    _data = data
    if dict == type(data): _data = json.dumps(data)
    r = getattr(requests, method)("%s%s" % (settings.MARATHON_API, path), data=_data, headers=headers)
    print "Marathon: request:"
    print "  Status:", r.status_code
    print "  Response:"
    try:
        print pprint.pprint(r.json())
    except:
        print "None"

    if not r.status_code == status_code:
        return None
    return r.json()
