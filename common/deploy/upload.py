#!/usr/bin/env python
""" This utility tags and uploads an image to dockerhub. """

from common import log
from common.utils import subprocess as usubprocess
import gflags
import os
import sys
from common.utils import decorators
from common.utils import utime

gflags.DEFINE_string('image', None, 'docker image name')
gflags.MarkFlagAsRequired('image')


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
    FLAGS, argv = log.init_flags(argv)
    mousera2_root = os.path.dirname(
        os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
    image_name = FLAGS.image
    # TODO(heathkh): This should be the unique hash provided by docker hub
    # hopefully someday soon it can be computed BEFORE upload to dockerhub
    image_tag = int(utime.now()) 
    image_name_for_docker_hub = "docker.io/mousera/%s:%d" % (image_name, image_tag)
    push_to_dockerhub(image_name, image_name_for_docker_hub)
    return

if __name__ == '__main__':
    main(sys.argv)
