# This script is run by CirclCI and drives the testing of all projects.
# Include projects to be tested in common/jenkins/projects and
# make sure to include an /app/run_tests.sh in their root.  At the very
# least, the /app/run_tests.sh must be an "exit 0".

import os
import subprocess
from common.utils import subprocess as usubprocess
from sys import argv

def read_projects():
    projects = []
    with open("./common/jenkins/projects") as f:
            projects = f.readlines()
    projects = filter(None, [p.strip() for p in projects])
    print "Projects to be checked:", projects
    return projects

def filter_for_changes(projects):
    last_build_sha = None
    if 'GIT_PREVIOUS_SUCCESSFUL_COMMIT' in os.environ:
        last_build_sha = os.environ['GIT_PREVIOUS_SUCCESSFUL_COMMIT']

    if not last_build_sha:
        # return all projects if GIT_PREVIOUS_SUCCESSFUL_COMMIT is not set (1st build)
        print 'GIT_PREVIOUS_SUCCESSFUL_COMMIT no set ... first build!'
        return projects

    print "Prior SHA:", last_build_sha

    changed_files = subprocess.check_output("git diff --name-only  --oneline %s" % last_build_sha, shell=True).split("\n")
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
    output = usubprocess.run_cmd_with_streaming_output("python -m common.build_and_deploy -t -b -g --no-push --tag $BUILD_NUMBER %s jenkins" % project)
    print "    Build output:\n", output

if __name__ == "__main__":
    if len(argv) > 1:
        projects = argv[1:]
    else:
        projects = read_projects()
        projects = filter_for_changes(projects)

    for project in projects:
        build_and_test(project)
