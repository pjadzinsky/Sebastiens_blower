#!/usr/bin/env python
import sys
import json
import random

json_file_path = sys.argv[1]
my_ip = sys.argv[2]

app = json.loads(open(json_file_path).read())

# Randomize the list of Cassandra nodes
l = app['app']['tasks']
for task in random(l,len(l)):
    if task['host'] != my_ip:
        print task['host']
        sys.exit(0)

