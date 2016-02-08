#!/usr/bin/env python

from common import log
from common.mesos import marathon
from common.utils import subprocess as usubprocess
import collections
import gflags
import os
import sys
import yaml
from common.utils import decorators

gflags.DEFINE_string('config',
                     None,
                     'Path to a deploy.yml style config file, relative to mousera2 root')
gflags.MarkFlagAsRequired('config')


def _deep_merge(d1, d2):
    for k, v in d2.iteritems():
        if isinstance(v, collections.Mapping):
            r = _deep_merge(d1.get(k, {}), v)
            d1[k] = r
        else:
            d1[k] = d2[k]
    return d1


def load_deploy_config(filename):
    result = None
    deploy_base = "%s/deploy_marathon.yml" % (os.path.dirname(__file__))
    # Read and overlay base and project deployment files
    deploy_config = {}
    for yml in [deploy_base, filename]:
        d = yaml.load(open(yml, 'r').read())
        if d:
            deploy_config = _deep_merge(deploy_config, d)
    #deploy_config['env']['APP_ENV'] = 'staging'
    return deploy_config


@decorators.retry(RuntimeError)
def push_to_dockerhub(image_name, hub_image_name):
    cmd = "docker tag -f %s %s" % (image_name, hub_image_name)
    usubprocess.run_cmd_with_streaming_output(cmd)
    cmd = "docker push %s" % (hub_image_name)
    usubprocess.run_cmd_with_streaming_output(cmd)
    return


def check_docker_version():
    from subprocess import check_output
    out = check_output(['docker', '--version'])
    print out
    import re
    match = re.match( r'^Docker version ([0-9.]+).*$', out)
    version = match.group(1)
    from distutils.version import LooseVersion
    min_version = LooseVersion("1.8")
    if LooseVersion(version) < min_version:
        raise RuntimeError('Require docker > %s but have %s' % (min_version, version))
    return

def main(argv):
    check_docker_version()
    flags, argv = log.init_flags(argv)
    mousera2_root = os.path.dirname(
        os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    config_filename = "%s/%s" % (mousera2_root, flags.config)
    job_spec = load_deploy_config(config_filename)
    if 'image' not in job_spec:
        raise RuntimeError('Your job spec is missing the required field "image"')
    
    image_name = job_spec['image']
    
    tokens = image_name.split(':')
    if len(tokens) != 2:
        raise RuntimeError('You must specify image name with a tag like this imagename:tag - %s' % (image_name))
    if len(tokens[1]) <= 2:
        raise RuntimeError('You must specify image name with a tag like this imagename:tag - %s' % (image_name))
    
    
    image_name_for_docker_hub = "docker.io/mousera/%s" % (image_name)
    image_name_for_marathon = "mousera/%s" % (image_name)

    #TODO(heathkh): run tests before push so we don't waste our time  / dockerhub space
    push_to_dockerhub(image_name, image_name_for_docker_hub)
    
    # add missing fields to jobspec
    job_spec['env']['APP_ENV'] = 'production'
    job_spec['container']['docker']['image'] = image_name_for_marathon
    #job_spec['container']['docker']['forcePullImage'] = True
    # ask marathon to launch app
    result = marathon.send_job(job_spec)
    print result

if __name__ == '__main__':
    main(sys.argv)
