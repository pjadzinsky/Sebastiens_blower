import importlib
import __builtin__
import os
import sys

app_env = os.environ.get('APP_ENV')
if not globals()['__package__'].startswith('common.'):
    print "##########################################################################"
    print "ERROR: 'settings' must be imported relative to project root (ie. mousera2)."
    print "You are probably not running your script as a module relative to mousera2."
    print "##########################################################################"
    sys.exit(1)

if not app_env:
    #print >> sys.stderr, "WARNING: no APP_ENV provided.  Defaulting to 'staging'."
    app_env = 'staging'

# This is a parameterized way of writing:
#   from common.settings.{{app_env}} import *
__builtin__.app_env = app_env   #Give imported settings modules access to app_env
_settings = importlib.import_module('common.settings.%s' % app_env)
for setting in filter(lambda s: s[0] != '_', _settings.__dict__):
    globals()[setting] = getattr(_settings, setting)
