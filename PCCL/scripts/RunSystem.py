#!/usr/bin/python
#
# Command line arguments:
# [1] -- Number of server nodes
# [2] -- Name of result file

import os
import sys
import time
import datetime
import re
import shlex
import subprocess
import itertools
from sys import argv
from hostnames import *
import socket

dashboard = None
result_dir = "./results/"

# Total nodes
nds=48

machines = hostip_machines

#	# check all rundb/runcl are killed
cmd = './scripts/vcloud_cmd.sh \"{}\" \"pkill -f \'rundb\'\"'.format(' '.join(machines))
print(cmd)
os.system(cmd)
time.sleep(0.5)
cmd = './scripts/vcloud_cmd.sh \"{}\" \"pkill -f \'runcl\'\"'.format(' '.join(machines))
print(cmd)
# cmd = './vcloud_cmd.sh \"{}\" \"mkdir -p \'resilientdb\'\"'.format(' '.join(machines))
# print(cmd)
os.system(cmd)
time.sleep(0.5)
cmd = './scripts/vcloud_cmd.sh \"{}\" \"rm ~/resdb/results/*\"'.format(' '.join(machines))
print(cmd)
os.system(cmd)
time.sleep(0.5)

# running the experiment
cmd = './scripts/vcloud_deploy.sh \"{}\" {} \"{}\"'.format(' '.join(machines), nds, result_dir)
print(cmd)
os.system(cmd)
