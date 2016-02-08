from __future__ import absolute_import
import subprocess
import sys
import os
import time
import signal
from common.utils import utime


def run_cmd_with_streaming_output(cmd):
    print "Running command:", cmd
    # Run the commands with streaming logging
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    with process.stdout:
        for line in iter(process.stdout.readline, ''):
            # We seem to get lots of blank lines, so strip
            # them and only print interesting ones.
            if len(line.strip()): print line.strip()
    process.wait()
    if process.returncode:
        raise RuntimeError("Command '%s' return a non-zero code." % cmd)


def timeout_command(command, timeout_sec):
    """call shell-command and either return its output or kill it 
    if it doesn't normally exit within timeout_sec seconds and return None"""
    start_sec = utime.now()
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, preexec_fn=os.setsid)
    while process.poll() is None:
        time.sleep(0.1)
        now = utime.now()
        if (now - start_sec) > timeout_sec:
            print 'timeout_command: killing process: %s' % (process.pid)
            os.kill(process.pid, signal.SIGKILL)
            print 'timeout_command: killed process'
            os.waitpid(-1, os.WNOHANG)
            print 'timeout_command: done waiting'
            return None
    return process.stdout.read()


def kill_subprocesses():
    """ Call to kill all child subprocesses recursviely. """
    import psutil
    proc = psutil.Process(os.getpid())
    for child in proc.children(recursive=True):
        child.kill()
    proc.kill()
    return
