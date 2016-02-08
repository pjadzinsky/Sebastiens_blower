# This script is run by CirclCI and drives the testing of all projects.
# Include projects to be tested in common/circleci/projects and
# make sure to include an /app/run_tests.sh in their root.  At the very
# least, the /app/run_tests.sh must be an "exit 0".

import os, re, subprocess
import json
import os
import urllib2
from common.utils import subprocess as usubprocess

def read_projects():
    projects = []
    with open("./common/circleci/projects") as f:
            projects = f.readlines()
    projects = filter(None, [p.strip() for p in projects])
    print "Projects to be checked:", projects
    return projects

def filter_for_changes(projects):
    prior_build = os.environ['CIRCLE_PREVIOUS_BUILD_NUM']
    print "Prior build:", prior_build

    opener = urllib2.build_opener()
    req = urllib2.Request('https://circleci.com/api/v1/project/Mousera/mousera2/%s?circle-token=af84a2eb2b90a60e71a30846a8c752150a62fe06' % prior_build,
          headers={'Content-Type': 'application/json', 'Accept': 'application/json'})
    response = opener.open(req)
    data = json.load(response)

    last_build_sha = data.get('vcs_revision')
    if last_build_sha:
        print "Prior SHA:", last_build_sha
        changed_files = subprocess.check_output("git diff --name-only  --oneline %s" % last_build_sha, shell=True).split("\n")
    else:
        changed_files = subprocess.check_output("git diff --name-only  --oneline HEAD^", shell=True).split("\n")

    print "Files changed since last build:", changed_files
    changed_dirs  = filter(None, [os.path.dirname(f) for f in changed_files])
    print "\n\nDirectories with files changed in the last commit:", changed_dirs
    def _is_a_project(d):
        # Find most specific match
        match = None
        for p in projects:
            if d.startswith(p):
                if not match or len(p) > len(match): match = p
        return match

    changed_dirs_we_care_about  = list(set(filter(None, [_is_a_project(d) for d in changed_dirs])))
    print "\n\n*Project* directories with files changed in the last commit:", changed_dirs_we_care_about
    return changed_dirs_we_care_about


def build_and_test(project):
    print "\n----------------------------------------------------------------------------"
    print "    Building:", project
    output = usubprocess.run_cmd_with_streaming_output("python -m common.build_and_deploy -t -b --tag $CIRCLE_BUILD_NUM %s circleci" % project)
    print "    Build output:\n", output

if __name__ == "__main__":
    projects = read_projects()
    projects = filter_for_changes(projects)

    for project in projects:
        build_and_test(project)


