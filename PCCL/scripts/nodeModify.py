#!/usr/bin/python
#
import os,sys,datetime,re
import shlex
import subprocess
import itertools
from sys import argv
from hostnames import *

machines = hostip_machines

# running the experiment
cmd = './vcloud_modify.sh \"{}\" '.format(' '.join(machines))
print(cmd)
os.system(cmd)
