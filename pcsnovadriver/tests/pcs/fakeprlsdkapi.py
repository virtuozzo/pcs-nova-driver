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

    PSM_KILL = 0x0001
    PSM_ACPI = 0x0002

    PSF_FORCE = 0x0001


class Errors(object):
    PRL_ERR_VM_UUID_NOT_FOUND = 0x0001
    PRL_ERR_DISP_VM_IS_NOT_STOPPED = 0x0002
    PRL_ERR_DISP_VM_IS_NOT_STARTED = 0x0003

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

    def get_param(self):
        return self.objects[0]


class Job(object):

    def __init__(self, objects=[], error=None):
        self.objects = objects
        self.error = error

    def wait(self):
        if self.error:
            raise self.error
        return Result(self.objects)


class VmInfo(object):

    def __init__(self, props):
        self.props = props

    def get_state(self):
        return self.props['state']


class Vm(object):
    def __init__(self, props):
        self.props = props.copy()
        self.state = self.props.pop('state', consts.VMS_STOPPED)
        self.prev_state = None

    def get_name(self):
        return self.props['name']

    def get_uuid(self):
        return self.props['uuid']

    def start(self):
        if self.state in [consts.VMS_PAUSED,
                            consts.VMS_STOPPED]:
            self.prev_state = self.state
            self.state = consts.VMS_RUNNING
            return Job()
        elif self.state == consts.VMS_SUSPENDED:
            return self.resume()
        else:
            err = PrlSDKError(errors.PRL_ERR_DISP_VM_IS_NOT_STOPPED)
            return Job(error=err)

    def start_ex(self, flags1, flags2):
        return self.start()

    def stop(self):
        if self.state == consts.VMS_RUNNING:
            self.prev_state = self.state
            self.state = consts.VMS_STOPPED
            return Job()
        else:
            err = PrlSDKError(errors.PRL_ERR_DISP_VM_IS_NOT_STARTED)
            return Job(error=err)

    def stop_ex(self, flags1, flags2):
        return self.stop()

    def pause(self):
        if self.state == consts.VMS_RUNNING:
            self.prev_state = self.state
            self.state = consts.VMS_PAUSED
            return Job()
        else:
            err = PrlSDKError(errors.PRL_ERR_DISP_VM_IS_NOT_STARTED)
            return Job(error=err)

    def suspend(self):
        if self.state == consts.VMS_RUNNING:
            self.prev_state = self.state
            self.state = consts.VMS_SUSPENDED
            return Job()
        else:
            err = PrlSDKError(errors.PRL_ERR_DISP_VM_IS_NOT_STARTED)
            return Job(error=err)

    def resume(self):
        if self.state == consts.VMS_SUSPENDED:
            tmp = self.prev_state
            if not tmp:
                tmp = consts.VMS_RUNNING
            self.prev_state = self.state
            self.state = tmp
            return Job()
        else:
            err = PrlSDKError(errors.PRL_ERR_DISP_VM_IS_NOT_STOPPED)
            return Job(error=err)

    def get_state(self):
        return Job([VmInfo({'state': self.state})])

    def get_ram_size(self):
        return self.props['ram_size']

    def get_cpu_count(self):
        return self.props['cpu_count']


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
        return Job(error=PrlSDKError(errors.PRL_ERR_VM_UUID_NOT_FOUND))

consts = Consts()
