#!/usr/bin/env python

import cli  # from pip install pyCli
import cli.app
import collections
import ConfigParser
from datetime import datetime
import getpass
import json
import os
import pprint
import requests
import subprocess
import time
import sys
import yaml
from common.mesos import marathon, chronos
from common.utils import hipchat
from common.utils import subprocess as usubprocess

@cli.app.CommandLineApp
def main(app):
    """
    Command line utility to build and/or deploy Mousera projects.
    """
    config = process_options(app)
    print "Config:"
    print " Original config:"
    pprint.pprint(config)

    config = determine_tag(config)
    print " Modified config:"
    pprint.pprint(config)

    print config
    config["app_name"] = "/%s/%s" % (config['environment'], app.params.project)
    config["hub_name"] = "mousera/%s:%s" % (config["image_name"], config['tag'])

    print "\nApp name:   '%s'" % config['app_name']
    print "Image name: '%s'" % config['image_name']
    print "Hub name: '%s'" % config['hub_name']

    # Emit warnings or error if git has changes.
    if config['build'] and (config['dirty'] or config['new']):
        if app.params.ignore_git:
            print "\nWARNING: you have uncommitted git changes or new files."
            print "You asked us to ignore them..."
            print "You have 10 seconds to interrupt this script..."
            print "------------------------------------------------------------------"
            #time.sleep(10)
        else:
            print "You have uncommitted git or new files changes.  Exiting."
            print "----------------------------------------------------------------------------"
            app.argparser.print_help()
            exit()

    if config['build']:
        build_and_tag_docker_image(config)

    if config['undeploy']:
        print "ERROR & TODO: implement undeploy..."
        sys.exit(1)
    if config['deploy']:
        print "------------------------------------------------------------------"
        success = False
        if config['deploy_ansible_exists']:
            success = deploy_ansible(config)
        if config['deploy_yml_exists']:
            success = deploy_yml(config)
        if success:
            user = getpass.getuser()
            message = "%s deployed %s to %s" % (user, config['hub_name'], config['app_name'])
            # Below token is mike's, as robot tokens don't seem to work
            # with v2 api.
            token = "LwrLlOdVPu6bxG65fQAl1IXpDxU8T1fxaAsbQNPW"
            try:
                hipchat.send_room_message('Ops', message, hipchat_token=token)
            except Exception as e:
                print "Caught exception when posting hipchat deploy notification"
                print e

###########################################
# Figure out which tag to deploy
###########################################

def determine_tag(config):
    # This script deploys code under a variety of circumstances and
    # to a variety of environments, so determining what to build and/or
    # deploy is not straightfoward.

    # If the user has specified a tag, use that.
    if not config['tag']:
        # Only grab the last committish if we're not building.
        # The build process will re-gen this file/value.
        if not config['build']:
            _f = '%s/.last_committish' % config['project']
            if os.path.exists(_f):
                with open(_f, 'r') as f:
                    config['committish'] = f.read()
            config['committish'] = config['committish'].strip()
            config['tag']        = config['committish']
        else:
            # Use the current committish if we're building
            config['tag'] = config['committish']
    if not config['tag']:
        raise Exception("No tag determined.")
    return config

###########################################
# Test all the things
###########################################
def run_tests(config):
    cmd = "docker run -t --net=host --privileged=true --entrypoint /app/%s/run_tests.sh %s" % (config['project'], config['hub_name'])
    print "\n Running tests."
    print " Cmd:", cmd
    return usubprocess.run_cmd_with_streaming_output(cmd)


###########################################
# Deploy all the things
###########################################

def deploy_yml(config):
    result = None
    deploy_config = {}
    project_deploy_config_file = "%s/deploy.yml" % config['project']
    deploy_base = "./common/deploy/deploy_marathon.yml"

    # Check to see if we're deploying to Chronos
    print '  project_deploy_config_file:',project_deploy_config_file
    test_deploy_config = yaml.load(open(project_deploy_config_file,'r').read())
    print 'test_deploy_config:', test_deploy_config
    if test_deploy_config.get('schedule'):
        deploy_base = "./common/deploy/deploy_chronos.yml"
    print " Deploying with:", deploy_base

    # Read and overlay base and project deployment files
    for yml in [deploy_base, project_deploy_config_file]:
        d = yaml.load(open(yml,'r').read())
        if d:
            deploy_config = _deep_merge(deploy_config, d)

    if deploy_config.get('schedule'):
        if not deploy_config.get('command'):
            print "##### Chronos requires a 'command'.  Exiting."
            exit()
        if deploy_config.get('instances'):
            deploy_config.pop('instances', None)
            print "WARNING: your deploy.yml contains both 'instances' and 'schedule' parameters."
            print "  ###### IGNORING instances AND DEPLOYING TO CHRONOS."
        if deploy_config.pop('constraints', None):
            print "WARNING: Chronos does not accept 'constraints'.  Ignoring."
        if deploy_config.get('env'):
            print "WARNING: Chronos uses 'environmentVariables' for environment variables, but you're using 'env'."
            print "         Fixing."
            deploy_config['environmentVariables'] = deploy_config.pop('env')
        deploy_config.pop('id', None)
        deploy_config['container']['image'] = config['hub_name']
        deploy_config['name'] = config['app_name'].replace("_", "-").replace("/","-")[1:]
        deploy_config['environmentVariables'].append({"name": "APP_ENV", "value": config['environment']})
        print " Deployment config:"
        pprint.pprint(deploy_config)
        print "Sending to Chronos."
        result = chronos.send_job(deploy_config)
    else:
        deploy_config['env']['APP_ENV'] = config['environment']
        deploy_config['container']['docker']['image'] = config['hub_name']
        #p = deploy_config.get('labels')['priority']
        #if not p:
        #    raise RuntimeError("Marathon apps must have a priority specified in deploy.yml.")
        if not deploy_config.get('id'):
            deploy_config['id'] = marathon.generate_app_id(
                config['environment'], config['project'], 2) # p)
        print "Sending to Marathon: %s" % deploy_config['id']
        print " deploy_config: %s" % deploy_config

        result = marathon.send_app(config, deploy_config)
    return result

def deploy_ansible(config):
    print "\nDeploying using Ansible"
    #cmds = ['PWD_ROOT=$(pwd) PYTHONUNBUFFERED=1 ansible-playbook -i %s/ansible/inventory %s/ansible/launch.yml -e "env=%s tag=%s"' % (config['project'], config['project'], config['environment'], config['tag'])]
    cmds = ['PWD_ROOT=$(pwd) PYTHONUNBUFFERED=1 ansible-playbook -i %s/ansible/inventory %s/ansible/launch.yml -e "project_root=%s env=%s tag=%s"' % (config['project'], config['project'], '/mnt/hgfs/mousera2', config['environment'], config['tag'])]
    success = True
    with open('/tmp/ansible_deploy.log', 'w') as f:
        for cmd in cmds:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, bufsize=0)
            line = process.stdout.readline()
            while line:
                print line,
                line = process.stdout.readline()
            process.wait()
            if process.returncode != 0:
                success = False
    return success


###########################################
# Do docker stuff
#####################g######################
def build_and_tag_docker_image(config):
    print "\nBuilding:", config['image_name']
    if config['build']:
        print "------------------------------------------------------------------"
        usubprocess.run_cmd_with_streaming_output("docker build -f %s/Dockerfile -t %s ." % (config['project'], config['hub_name']))

    if config['test']:
        print "------------------------------------------------------------------"
        run_tests(config)

    if not config['no_push']:
        print "------------------------------------------------------------------"
        usubprocess.run_cmd_with_streaming_output("docker push %s" % (config['hub_name']))

    print "Writing %s to %s/.last_committish" % (config['committish'], config['project'])
    with open('%s/.last_committish' % config['project'], 'w') as f:
        f.write(config['committish'])
        config['committish'] = None


###########################################
# Process app's params and build the config
###########################################
def process_options(app):
    config = {}
    if not os.path.exists(".git"):
        print "ERROR: This script must be called from the root of the mousera2 repo."
        print "---------------------------------------------------------------------"
        app.argparser.print_help()
        exit(1)
    committish           = subprocess.check_output("git rev-parse HEAD", shell=True)
    config['committish'] = committish[:7]

    if not app.params.build and not app.params.deploy and not app.params.tag and not app.params.test:
        print "No build, deploy or tag requested.  Exiting."
        print "---------------------------------------"
        app.argparser.print_help()
        exit()
    config['tag']    = getattr(app.params, "tag",   None)
    config['test']   = getattr(app.params, "test",  False)
    config['no_push']= getattr(app.params, "no_push",  False)
    config['build']  = getattr(app.params, "build",  False)
    config['deploy'] = getattr(app.params, "deploy", False)
    config['undeploy'] = getattr(app.params, "undeploy", False)

    if config['deploy'] and not app.params.environment:
        print "You must specify an environment when deploying.  Exiting."
        print "----------------------------------------------------------------------------"
        app.argparser.print_help()
        exit()
    config['environment'] = app.params.environment

    # Pipe through cat to clean off any exit codes
    config['dirty'] = 0 < len(subprocess.check_output('git diff --shortstat 2> /dev/null | tail -n1 | cat',   shell=True))
    config['new']   = 0 < len(subprocess.check_output('git status --porcelain 2>/dev/null| grep "^??" | cat', shell=True))

    if not os.path.exists(app.params.project):
        print "Project '%s' does not exist.  Exiting." % app.params.project
        print "------------------------------------------------"
        app.argparser.print_help()
        exit()
    config['project'] = app.params.project

    if os.path.exists(app.params.project + "/.image_name"):
        config['image_name'] = open(app.params.project + "/.image_name").read()
        print "Found .image_name file.  Using its image name."
    if not config.get('image_name'):
        config['image_name'] = app.params.project.replace("/", "_")

    # Splat on some additional configs
    config['deploy_yml'] = "%s/deploy.yml" % config['project']
    config['deploy_yml_exists'] = os.path.exists(config['deploy_yml'])
    config['deploy_ansible'] = "%s/ansible/launch.yml" % config['project']
    config['deploy_ansible_exists'] = os.path.exists(config['deploy_ansible'])
    if config['deploy_ansible_exists'] and config['deploy_yml_exists']:
        print "ERROR: projects cannot contain both ansible and yml deployment artifacts."
        print "  This project contains both %s and %s." % (config['deploy_yml'], config['deploy_ansible'])
        exit(1)

    if getattr(app.params, "deploy", None) and not config['deploy_ansible_exists'] and not config['deploy_yml_exists']:
        print "ERROR: projects must contain either ansible or yml deployment artifacts."
        print "  This project contains neither %s and %s." % (config['deploy_yml'], config['deploy_ansible'])
        exit(1)
    return config

def _deep_merge(d1,d2):
    for k, v in d2.iteritems():
        if isinstance(v, collections.Mapping):
            r = _deep_merge(d1.get(k, {}), v)
            d1[k] = r
        else:
            d1[k] = d2[k]
    return d1

#########################################
# Config the app's params
#########################################
main.add_param('project',     help="The project to build and/or deploy.  e.g. queue_processors/mousera_slab_data")
main.add_param('environment', nargs="?", help="The environment to build and/or deploy.  e.g. staging")
main.add_param('-t', dest="test", action="store_true",
               help='Run the container\'s tests.')
main.add_param('--tag', nargs="?", dest="tag",
               help='Specify an additional tag to be applied or deploy.')
main.add_param('-np', '--no-push', dest='no_push', action='store_true', default=False,
                  help='Do not push the container to Docker Hub.')
main.add_param('-b', '--build', dest='build', action='store_true', default=False,
                  help='Build the project.')
main.add_param('-d', '--deploy', dest='deploy', action='store_true', default=False,
               help='Deploy the project.')
main.add_param('-g', '--ignore-git', dest='ignore_git', action='store_true', default=False,
               help='Ignore uncommitted changes in git.')
main.add_param('-u', '--undeploy', dest='undeploy', action='store_true', default=False,
                  help='Undeploy the existing application before deploying.')
main.run()
