#!/usr/bin/env python

# Copyright (c) 2013-2014 Parallels, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from __future__ import print_function

import commands
import os
import subprocess
import sys

if len(sys.argv) != 2:
    print("Usage: %s <image-file> <extra glance args>" % sys.argv[0])
    sys.exit(1)

if not os.path.exists(sys.argv[1]):
    print("File '%s' is not found" % sys.argv[1])

cmd = "rpm -q --qf '%%{NAME},%%{VERSION},%%{RELEASE}' -p '%s'" % sys.argv[1]
ret, out = commands.getstatusoutput(cmd)
if ret:
    print("rpm returned %d" % ret)

name, ver, rel = out.split(',')
image_name = name[:-3]

cmd = ['glance', 'image-create',
        '--name', image_name,
        '--file', sys.argv[1],
        '--property', 'pcs_name=%s' % name,
        '--property', 'pcs_version=%s' % ver,
        '--property', 'pcs_release=%s' % rel,
        '--disk-format', 'ez-template',
        '--container-format', 'bare']

cmd += sys.argv[2:]

p = subprocess.Popen(cmd)
ret = p.wait()
sys.exit(p.wait())
