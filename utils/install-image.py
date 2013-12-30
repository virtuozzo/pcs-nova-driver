#!/usr/bin/env python

import os
import sys
import commands
import subprocess

if len(sys.argv) != 2:
    print "Usage: install-image.py <image-file> <extra glance args>"
    sys.exit(1)

if not os.path.exists(sys.argv[1]):
    print "File '%s' is not found" % sys.argv[1]

cmd = "rpm -q --qf '%%{NAME},%%{VERSION},%%{RELEASE}' -p '%s'" % sys.argv[1]
ret, out = commands.getstatusoutput(cmd)
if ret:
    print "rpm returned %d" % ret

name, ver, rel = out.split(',')
image_name = name[:-3]

cmd = ['glance', 'image-create',
        '--name', image_name,
        '--file', sys.argv[1],
        '--property', 'pcs_name=%s' % name,
        '--property', 'pcs_version=%s' % ver,
        '--property', 'pcs_release=%s' % rel,
        '--disk-format', 'raw',
        '--container-format', 'pcs-ez']

cmd += sys.argv[2:]

p = subprocess.Popen(cmd)
ret = p.wait()
sys.exit(p.wait())
