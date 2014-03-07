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

import threading


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

    PVT_VM = 0x0001
    PVT_CT = 0x0002

    PDE_HARD_DISK = 0x0001
    PDE_GENERIC_NETWORK_ADAPTER = 0x0002

    PMS_SATA_DEVICE = 0x0001

    PNA_BRIDGED_ETHERNET = 0x0001
    PNA_ROUTED = 0x0002

    PSM_VM_START = 0x0001

    PNSF_VM_START_WAIT = 0x0001

    PVS_GUEST_VER_LIN_REDHAT = 0x0001

    PDT_USE_REAL_HDD = 0x0002

consts = Consts()


class Errors(object):
    PRL_ERR_VM_UUID_NOT_FOUND = 0x0001
    PRL_ERR_DISP_VM_IS_NOT_STOPPED = 0x0002
    PRL_ERR_DISP_VM_IS_NOT_STARTED = 0x0003
    PRL_ERR_CONFIG_BEGIN_EDIT_NOT_FOUND_OBJECT_UUID = 0x0004
    PRL_ERR_OPERATION_FAILED = 0x0005

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


class FakePrlSDKError(Exception):
    "Custom errors from this module"
    pass


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


class ServerConfig(object):
    pass


class VmDevice(object):
    def __init__(self, vm, idx, props):
        self.vm = vm
        self.idx = idx
        self.props = props

    def _get_thread_config(self):
        tid = threading.currentThread().ident
        return self.vm.writers[tid]['props']['devs'][self.type][self.idx]

    def remove(self):
        tid = threading.currentThread().ident
        self.vm.writers[tid]['props']['devs'][self.type].pop(self.idx)

    def get_emulated_type(self):
        return self.props['emulated_type']

    def set_emulated_type(self, emu_type):
        dev = self._get_thread_config()
        dev['emulated_type'] = emu_type

    def get_friendly_name(self):
        return self.props['friendly_name']

    def set_friendly_name(self, name):
        dev = self._get_thread_config()
        dev['friendly_name'] = name

    def get_sys_name(self):
        return self.props['sys_name']

    def set_sys_name(self, name):
        dev = self._get_thread_config()
        dev['sys_name'] = name

    def get_index(self):
        return self.idx


class VmHardDisk(VmDevice):
    type = consts.PDE_HARD_DISK

    def resize_image(self, size, flags):
        return Job()


class VmNet(VmDevice):
    type = consts.PDE_GENERIC_NETWORK_ADAPTER

device_classes = {
    consts.PDE_HARD_DISK: VmHardDisk,
    consts.PDE_GENERIC_NETWORK_ADAPTER: VmNet,
}


class BootDevice(object):

    def __init__(self, vm, seq_idx=-1, props={}):
        self.vm = vm
        self.props = props
        self.seq_idx = seq_idx

    def _get_thread_config(self):
        tid = threading.currentThread().ident
        return self.vm.writers[tid]['props']['boot_order'][self.seq_idx]

    def set_type(self, dev_type):
        dev = self._get_thread_config()
        dev['type'] = dev_type

    def set_index(self, index):
        dev = self._get_thread_config()
        dev['index'] = index

    def set_sequence_index(self, index):
        tid = threading.currentThread().ident
        boot_order = self.vm.writers[tid]['props']['boot_order']
        dev = boot_order.pop(self.seq_idx)
        boot_order.insert(index, dev)

    def set_in_use(self, in_use):
        dev = self._get_thread_config()
        dev['in_use'] = in_use


class Vm(object):
    def __init__(self, srv, props):
        self.srv = srv
        self.props = props.copy()
        self.state = self.props.pop('state', consts.VMS_STOPPED)
        self.prev_state = None
        self.lock = threading.Lock()
        self.config_version = 0
        self.writers = {}

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
        if self.state in [consts.VMS_SUSPENDED, consts.VMS_PAUSED]:
            tmp = self.prev_state
            if not tmp:
                tmp = consts.VMS_RUNNING
            self.prev_state = self.state
            self.state = tmp
            return Job()
        else:
            err = PrlSDKError(errors.PRL_ERR_DISP_VM_IS_NOT_STOPPED)
            return Job(error=err)

    def restart(self):
        if self.state == consts.VMS_RUNNING:
            self.prev_state = self.state
            return Job()
        else:
            err = PrlSDKError(errors.PRL_ERR_DISP_VM_IS_NOT_STARTED)
            return Job(error=err)

    def get_state(self):
        return Job([VmInfo({'state': self.state})])

    def get_ram_size(self):
        return self.props['ram_size']

    def set_ram_size(self, ram_size):
        tid = threading.currentThread().ident
        self.writers[tid]['props']['ram_size'] = ram_size

    def get_cpu_count(self):
        return self.props['cpu_count']

    def set_cpu_count(self, cpu_count):
        tid = threading.currentThread().ident
        self.writers[tid]['props']['cpu_count'] = cpu_count

    def get_vm_type(self):
        return self.props['vm_type']

    def set_vm_type(self, vm_type):
        tid = threading.currentThread().ident
        self.writers[tid]['props']['vm_type'] = vm_type

    def begin_edit(self):
        tid = threading.currentThread().ident
        if tid in self.writers:
            err = FakePrlSDKError("Second call to begin_edit()")
            return Job(error=err)
        writer = {}
        writer['config_version'] = self.config_version
        writer['props'] = self.props
        self.writers[tid] = writer
        return Job()

    def commit(self):
        tid = threading.currentThread().ident
        if tid not in self.writers:
            err = FakePrlSDKError("Call to commit() not after begin_edit()")
            return Job(error=err)
        with self.lock:
            writer = self.writers.pop(tid)
            if writer['config_version'] != self.config_version:
                e = errors.PRL_ERR_CONFIG_BEGIN_EDIT_NOT_FOUND_OBJECT_UUID
                return Job(error=PrlSDKError(e))
            self.config_version = writer['config_version'] + 1
            self.props = writer['props']
        return Job()

    def get_devs_count_by_type(self, dev_type):
        return len(self.props['devs'][dev_type])

    def get_dev_by_type(self, dev_type, idx):
        dev_props = self.props['devs'][dev_type][idx]
        return device_classes[dev_type](self, idx, dev_props)

    def set_default_config(self, srv_cfg, os_ver, need_devs):
        pass

    def set_uuid(self, uuid):
        tid = threading.currentThread().ident
        self.writers[tid]['props']['uuid'] = uuid

    def set_name(self, name):
        tid = threading.currentThread().ident
        self.writers[tid]['props']['name'] = name

    def reg(self, path, non_int_mode):
        self.srv.vms.append(self)
        return self.commit()

    def add_default_device_ex(self, srv_cfg, dev_type):
        tid = threading.currentThread().ident
        i = 0
        while 1:
            if i not in self.writers[tid]['props']['devs']:
                break
            i += 1
        i += 1
        dev_props = {}
        if dev_type == consts.PDE_HARD_DISK:
            dev_props['emulated_type'] = consts.PMS_SATA_DEVICE
        elif dev_type == consts.PDE_GENERIC_NETWORK_ADAPTER:
            dev_props['emulated_type'] = consts.PNA_BRIDGED_ETHERNET
        else:
            raise PrlSDKError(errors.PRL_ERR_OPERATION_FAILED)

        self.writers[tid]['props']['devs'][dev_type][i] = dev_props
        return self.get_dev_by_type(dev_type, i)

    def create_boot_dev(self):
        tid = threading.currentThread().ident
        boot_dev = BootDevice(self)
        self.writers[tid]['props']['boot_order'].append({})
        return boot_dev

    def delete(self):
        self.srv.vms.remove(self)
        return Job()


class Server(object):

    def __init__(self):
        self.vms = []

    def test_add_vm(self, props):
        vm = Vm(self, props)
        self.vms.append(vm)
        return vm

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

    def get_srv_config(self):
        return Job([ServerConfig()])

    def create_vm(self):
        props = {
                'ram_size': 1024,
                'cpu_count': 1,
                'state': consts.VMS_STOPPED,
                'devs': {
                    consts.PDE_HARD_DISK: {
                        0: {
                            'emulated_type': consts.PDT_USE_REAL_HDD,
                        },
                    },
                    consts.PDE_GENERIC_NETWORK_ADAPTER: {
                        0: {
                            'emulated_type': consts.PNA_BRIDGED_ETHERNET,
                        },
                    },
                },
                'boot_order': [],
            }

        vm = Vm(self, props)
        vm.begin_edit().wait()
        return vm
