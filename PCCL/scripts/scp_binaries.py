#!/usr/bin/python
#
# Command line arguments:
# [1] -- Number of server nodes
# [2] -- Name of result file

import os
import sys
import datetime
import re
import shlex
import subprocess
import itertools
from sys import argv
from hostnames import *
import socket

machines = hostip_machines

cmd = './scripts/scp_binaries.sh \"{}\"'.format(' '.join(machines))
print(cmd)
os.system(cmd)