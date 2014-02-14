# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import subprocess
import shlex

def compress_ploop(src, dst):
    cmd1 = ['tar', 'cO', '-C', src, '.']
    cmd2 = ['prlcompress', '-p']

    dst_file = open(dst, 'w')
    try:
        p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE)
    except:
        dst_file.close()

    try:
        p2 = subprocess.Popen(cmd2, stdin=p1.stdout, stdout=dst_file)
    except:
        p1.kill()
        p1.wait()
        raise
    finally:
        dst_file.close()

    p1.stdout.close()

    ret1 = p1.wait()
    ret2 = p2.wait()

    msg = ""
    if ret1:
        msg = '%r returned %d' % (cmd1, ret1)
    if ret2:
        msg += ', %r returned %d' % (cmd2, ret2)
    if msg:
        raise Exception(msg)

def uncompress_ploop(src, dst, root_helper=""):
    cmd1 = ['prlcompress', '-u']
    cmd2 = shlex.split(root_helper) + ['tar', 'x', '-C', dst]

    src_file = open(src)
    try:
        p1 = subprocess.Popen(cmd1, stdin=src_file, stdout=subprocess.PIPE)
    finally:
        src_file.close()

    try:
        p2 = subprocess.Popen(cmd2, stdin=p1.stdout)
    except:
        p1.kill()
        p1.wait()
        raise

    p1.stdout.close()

    ret1 = p1.wait()
    ret2 = p2.wait()

    msg = ""
    if ret1:
        msg = '%r returned %d' % (cmd1, ret1)
    if ret2:
        msg += ', %r returned %d' % (cmd2, ret2)
    if msg:
        raise Exception(msg)

