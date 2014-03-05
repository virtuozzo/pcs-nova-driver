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


class Consts:
    VMS_COMPACTING = 0x0001
    VMS_CONTINUING = 0x0002
    VMS_DELETING_STATE = 0x0003
    VMS_MIGRATING = 0x0004
    VMS_PAUSED = 0x0005
    VMS_PAUSING = 0x0006
    VMS_RESETTING = 0x0007
    VMS_RESTORING = 0x0008
    VMS_RESUMING = 0x0009
    VMS_RUNNING = 0x000A
    VMS_SNAPSHOTING = 0x000B
    VMS_STARTING = 0x000C
    VMS_STOPPED = 0x000D
    VMS_STOPPING = 0x000E
    VMS_SUSPENDED = 0x000F
    VMS_SUSPENDING = 0x0010
    VMS_SUSPENDING_SYNC = 0x0011

    PVTF_VM = 0x0001
    PVTF_CT = 0x0002

    PGVC_SEARCH_BY_NAME = 0x0001
    PGVC_SEARCH_BY_UUID = 0x0002


class Errors(object):
    PRL_ERR_VM_UUID_NOT_FOUND = 0x0001

errors = Errors()


class PrlSdk(object):
    errors = errors

prlsdk = PrlSdk()


def conv_error(err):
    return err * 3 + 0x1000000


def init_server_sdk():
    pass


class PrlSDKError(Exception):
    def __init__(self, err):
        self.error_code = 3 * err + 0x1000000


class Result():
    def __init__(self, objects):
        self.objects = objects

    def __getitem__(self, n):
        return self.objects[n]


class Job(object):

    def __init__(self, objects=[]):
        self.objects = objects

    def wait(self):
        return Result(self.objects)


class Vm(object):
    def __init__(self, props):
        self.props = props

    def get_name(self):
        return self.props['name']

    def get_uuid(self):
        return self.props['uuid']


class Server(object):

    def __init__(self):
        self.vms = []

    def test_add_vm(self, props):
        self.vms.append(Vm(props))

    def test_add_vms(self, prop_list):
        for props in prop_list:
            self.test_add_vm(props)

    def login(self, host, login, password):
        return Job()

    def get_vm_list_ex(self, nFlags):
        return Job(self.vms)

    def get_vm_config(self, id, nFlags):
        for vm in self.vms:
            if nFlags == consts.PGVC_SEARCH_BY_NAME and id == vm.get_name():
                return Job([vm])
            elif nFlags == consts.PGVC_SEARCH_BY_UUID and id == vm.get_uuid():
                return Job([vm])
        raise PrlSDKError(errors.PRL_ERR_VM_UUID_NOT_FOUND)

consts = Consts()
