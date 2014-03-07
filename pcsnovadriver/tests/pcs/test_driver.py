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

import mock

from oslo.config import cfg

from nova.compute import flavors
from nova.compute import power_state
from nova import context
from nova import db
from nova.openstack.common import uuidutils
from nova import test
from nova.virt import fake

from pcsnovadriver.pcs import prlsdkapi_proxy
from pcsnovadriver.pcs import volume
from pcsnovadriver.tests.pcs import fakeprlsdkapi

prlsdkapi_proxy.prlsdkapi = fakeprlsdkapi
pc = fakeprlsdkapi.consts

from pcsnovadriver.pcs import driver

CONF = cfg.CONF
CONF.import_opt('compute_manager', 'nova.service')
CONF.import_opt('firewall_driver', 'nova.virt.firewall')
CONF.import_opt('host', 'nova.netconf')
CONF.import_opt('my_ip', 'nova.netconf')
CONF.import_opt('image_cache_subdirectory_name', 'nova.virt.imagecache')
CONF.import_opt('instances_path', 'nova.compute.manager')
CONF.import_opt('pcs_volume_drivers', 'pcsnovadriver.pcs.driver')

CONF.firewall_driver = "nova.virt.firewall.NoopFirewallDriver"
CONF.pcs_volume_drivers = \
    ['fake=pcsnovadriver.tests.pcs.test_driver.FakeVolumeDriver']

vm_stopped = {
            'name': 'instance001',
            'uuid': '{19be06cb-a6f2-47a7-a53e-11bc6d4c3b98}',
            'ram_size': 1024,
            'cpu_count': 1,
            'state': pc.VMS_STOPPED,
            'vm_type': pc.PVT_VM,
            'devs': {
                pc.PDE_HARD_DISK: {
                    0: {
                        'emulated_type': pc.PDT_USE_REAL_HDD,
                    },
                },
                pc.PDE_GENERIC_NETWORK_ADAPTER: {
                    0: {
                        'emulated_type': pc.PNA_BRIDGED_ETHERNET,
                    },
                },
            }
        }

vm_running = {
            'name': 'instance002',
            'uuid': '{d58fe074-ce99-46b7-8ce1-83620ba26426}',
            'ram_size': 2048,
            'cpu_count': 4,
            'state': pc.VMS_RUNNING,
        }

vms = [vm_stopped, vm_running]

instance_stopped = {
            'name': vm_stopped['name'],
            'power_state': power_state.SHUTDOWN
        }

instance_running = {
            'name': vm_running['name'],
            'power_state': power_state.RUNNING
        }

network_info_1vif = [
                {'id': uuidutils.generate_uuid()},
            ]

network_info_3vif = [
                {'id': uuidutils.generate_uuid()},
                {'id': uuidutils.generate_uuid()},
                {'id': uuidutils.generate_uuid()},
            ]

block_device_info1 = {
    'block_device_mapping': [
        {
            'connection_info': {
                'data': {
                    'device_path': '/qwe',
                    },
                'driver_volume_type': 'fake',
            },
            'mount_device': 'vda',
        },
    ],
    'ephemerals': [],
    'root_device_name': 'vda',
    'swap': None,
}

vm_with_disk = {
    'name': 'instance003',
    'uuid': '{19be06cb-a6f2-47a7-a53e-11bc6d4c3b99}',
    'ram_size': 1024,
    'cpu_count': 1,
    'state': pc.VMS_STOPPED,
    'vm_type': pc.PVT_VM,
    'devs': {
        pc.PDE_HARD_DISK: {
            0: {
                'emulated_type': pc.PDT_USE_REAL_HDD,
                'friendly_name': 'vda',
                'sys_name': '/qwe'
            },
        },
        pc.PDE_GENERIC_NETWORK_ADAPTER: {
            0: {
                'emulated_type': pc.PNA_BRIDGED_ETHERNET,
            },
        },
    }
}

OPENSTACK_STATES = {
    power_state.RUNNING: pc.VMS_RUNNING,
    power_state.PAUSED: pc.VMS_PAUSED,
    power_state.SHUTDOWN: pc.VMS_STOPPED,
    power_state.CRASHED: pc.VMS_STOPPED,
    power_state.SUSPENDED: pc.VMS_SUSPENDED,
}


class FakeVolumeDriver(volume.PCSBaseVolumeDriver):
    def connect_volume(self, connection_info, sdk_ve, disk_info):
        data = connection_info['data']
        return self._attach_blockdev(sdk_ve,
                    data['device_path'], disk_info['dev'])

    def disconnect_volume(self, connection_info, sdk_ve,
                    disk_info, ignore_errors):
        data = connection_info['data']
        self._detach_blockdev(sdk_ve, data['device_path'],
                    disk_info['dev'], ignore_errors)


class PCSDriverTestCase(test.TestCase):

    def setUp(self):
        super(PCSDriverTestCase, self).setUp()
        self.conn = driver.PCSDriver(fake.FakeVirtAPI(), True)
        self.conn.init_host(host='localhost')
        self.conn.psrv.test_add_vms(vms)

        self.user_id = 'fake'
        self.project_id = 'fake'
        self.context = context.get_admin_context()

    def _prep_instance(self, instance_ref, **kwargs):
        type_id = 5  # m1.small
        flavor = db.flavor_get(self.context, type_id)
        sys_meta = flavors.save_flavor_info({}, flavor)

        instance_ref.update(
            uuid='1e4fa700-a506-11e3-a1fc-7071bc7738b5',
            power_state=power_state.SHUTDOWN,
            user_id=self.user_id,
            project_id=self.project_id,
            instance_type_id=str(type_id),
            system_metadata=sys_meta,
            extra_specs={},
        )

        for key, val in kwargs.items():
            instance_ref[key] = val

        return db.instance_create(self.context, instance_ref)

    def _prep_instance_boot_image(self, **kwargs):
        instance_ref = {}
        instance_ref['image_ref'] = uuidutils.generate_uuid()
        return self._prep_instance(instance_ref, **kwargs)

    def _prep_instance_boot_volume(self, **kwargs):
        instance_ref = {}
        instance_ref['image_ref'] = None
        instance_ref['root_device_name'] = 'vda'
        return self._prep_instance(instance_ref, **kwargs)

    def test_list_instances(self):
        instances = self.conn.list_instances()
        self.assertEqual(instances, map(lambda x: x['name'], vms))

    def test_list_instance_uuids(self):
        instances = self.conn.list_instance_uuids()
        self.assertEqual(instances, map(lambda x: x['uuid'][1:-1], vms))

    def test_instance_exists_exists(self):
        self.assertTrue(self.conn.instance_exists(vms[0]['name']))

    def test_instance_exists_notexists(self):
        self.assertFalse(self.conn.instance_exists('x' + vms[0]['name']))

    def test_get_host_ip_addr(self):
        self.assertEqual(self.conn.get_host_ip_addr(), CONF.my_ip)

    def test_get_info(self):
        vm = vms[0]
        info = self.conn.get_info({'id': vm['uuid'], 'name': vm['name']})
        self.assertEqual(info['state'], power_state.SHUTDOWN)
        self.assertEqual(info['max_mem'], vm['ram_size'])
        self.assertEqual(info['mem'], vm['ram_size'])
        self.assertEqual(info['num_cpu'], vm['cpu_count'])
        self.assertEqual(info['cpu_time'], 1000)

    def _prep_plug_vifs(self, vm):
        self.conn._get_ve_by_name = mock.MagicMock()
        sdk_ve = self.conn.psrv.test_add_vm(vm)
        self.conn._get_ve_by_name.return_value = sdk_ve

        self.conn.vif_driver = mock.MagicMock()

        return sdk_ve

    def test_plug_vifs_running(self):
        vm = vm_running
        sdk_ve = self._prep_plug_vifs(vm)

        self.conn.plug_vifs(instance_running, network_info_1vif)

        args = (self.conn, instance_running, sdk_ve, network_info_1vif[0],)
        self.conn.vif_driver.plug.assert_called_once_with(*args)
        self.assertEqual(sdk_ve.state, pc.VMS_RUNNING)

    def test_plug_vifs_stopped(self):
        vm = vm_stopped
        sdk_ve = self._prep_plug_vifs(vm)

        self.conn.plug_vifs(instance_stopped, network_info_1vif)

        self.assertEqual(self.conn.vif_driver.plug.call_count, 0)
        self.assertEqual(sdk_ve.state, pc.VMS_STOPPED)

    def test_plug_vifs_unexistent(self):
        instance = {
                    'name': 'this_vm_does_not_exist',
                    'power_state': power_state.RUNNING
                }

        self.conn.vif_driver = mock.MagicMock()
        self.conn.plug_vifs(instance, network_info_1vif)

        self.assertEqual(self.conn.vif_driver.plug.call_count, 0)

    def test_plug_vifs_stopped_started(self):
        vm = vm_stopped
        sdk_ve = self._prep_plug_vifs(vm)

        instance = instance_stopped.copy()
        instance['power_state'] = power_state.RUNNING
        self.conn.plug_vifs(instance, network_info_1vif)

        args = (self.conn, instance, sdk_ve, network_info_1vif[0],)
        self.conn.vif_driver.plug.assert_called_once_with(*args)
        self.assertEqual(sdk_ve.state, pc.VMS_RUNNING)

    def test_plug_vifs_started_stopped(self):
        vm = vm_running
        sdk_ve = self._prep_plug_vifs(vm)

        instance = instance_running.copy()
        instance['power_state'] = power_state.SHUTDOWN
        self.conn.plug_vifs(instance, network_info_1vif)

        self.assertEqual(self.conn.vif_driver.plug.call_count, 0)
        self.assertEqual(sdk_ve.state, pc.VMS_STOPPED)

    def test__sync_ve_state(self):
        sdk_states = {
                pc.VMS_STOPPED: 'VMS_STOPPED',
                pc.VMS_RUNNING: 'VMS_RUNNING',
                pc.VMS_PAUSED: 'VMS_PAUSED',
                pc.VMS_SUSPENDED: 'VMS_SUSPENDED',
            }

        vm = {
                'name': 'instance123',
                'uuid': '{d58fe074-ce99-46b7-8ce1-83620ba26426}',
                'ram_size': 2048,
                'cpu_count': 4,
            }

        instance = {
                'name': vm['name'],
            }

        for cur in sdk_states:
            for req in OPENSTACK_STATES:
                vm['state'] = cur
                sdk_ve = self.conn.psrv.test_add_vm(vm)
                instance['power_state'] = req
                self.conn._sync_ve_state(sdk_ve, instance)
                msg = "%s->%s" % (sdk_states[cur], power_state.STATE_MAP[req])
                self.assertEqual(sdk_ve.state, OPENSTACK_STATES[req], msg)

    def test_unplug_vifs(self):
        vm = vm_running
        sdk_ve = self._prep_plug_vifs(vm)

        self.conn.unplug_vifs(instance_running, network_info_1vif)

        args = (self.conn, instance_running, sdk_ve, network_info_1vif[0],)
        self.conn.vif_driver.unplug.assert_called_once_with(*args)
        self.assertEqual(sdk_ve.state, pc.VMS_RUNNING)

    def test_spawn_vm_from_image(self):
        func_name = 'pcsnovadriver.pcs.template.get_template'
        with mock.patch(func_name) as get_template_mock:
            template = get_template_mock.return_value
            sdk_ve = self.conn.psrv.test_add_vm(vm_stopped)
            template.create_instance.return_value = sdk_ve
            instance = self._prep_instance_boot_image()
            admin_pw = 'fon234mc9pd1'
            self.conn.vif_driver = mock.MagicMock()
            self.conn.spawn(self.context, instance, None, [],
                        admin_pw, network_info_1vif, [])

    def test_spawn_vm_from_volume(self):
        instance = self._prep_instance_boot_volume()
        admin_pw = 'fon234mc9pd1'
        self.conn.vif_driver = mock.MagicMock()
        self.conn.volume_driver = FakeVolumeDriver(self.conn)

        self.conn.spawn(self.context, instance, None, [],
                    admin_pw, network_info_1vif, block_device_info1)

    def _prep_instance_and_vm(self, **kwargs):
        instance = self._prep_instance_boot_volume(**kwargs)
        vm = vm_with_disk.copy()
        vm['uuid'] = '{%s}' % instance['uuid']
        vm['name'] = instance['name']
        vm['state'] = OPENSTACK_STATES[instance['power_state']]
        sdk_ve = self.conn.psrv.test_add_vm(vm)
        return instance, sdk_ve

    def test_destroy(self):
        srv = self.conn.psrv
        instance, sdk_ve = self._prep_instance_and_vm()

        # check Vm exists
        srv.get_vm_config(instance['name'], pc.PGVC_SEARCH_BY_NAME).wait()

        self.conn.vif_driver = mock.MagicMock()
        self.conn.get_disk_dev_path = mock.MagicMock()
        self.conn.volume_driver = FakeVolumeDriver(self.conn)

        self.conn.destroy(instance, network_info_1vif, block_device_info1)

        job = srv.get_vm_config(instance['name'], pc.PGVC_SEARCH_BY_NAME)
        self.assertRaises(fakeprlsdkapi.PrlSDKError, job.wait)

    def test_destroy_unexistent(self):
        instance = self._prep_instance_boot_image()
        self.conn.destroy(instance, network_info_1vif, None)

    def test_reboot_soft(self):
        instance, sdk_ve = self._prep_instance_and_vm(
                            power_state=power_state.RUNNING)
        self.conn.vif_driver = mock.MagicMock()

        self.conn.reboot(self.context, instance,
                         network_info_1vif, reboot_type='SOFT')

        self.assertEqual(sdk_ve.state, pc.VMS_RUNNING)

    def test_reboot_hard(self):
        for state in OPENSTACK_STATES:
            instance, sdk_ve = self._prep_instance_and_vm(
                                uuid=uuidutils.generate_uuid(),
                                power_state=state)
            self.conn.vif_driver = mock.MagicMock()

            self.conn.reboot(self.context, instance,
                             network_info_1vif, reboot_type='HARD')

            msg = "state=%s" % (power_state.STATE_MAP[state])
            self.assertEqual(sdk_ve.state, pc.VMS_RUNNING, msg)

    def test_suspend(self):
        instance, sdk_ve = self._prep_instance_and_vm(
                                power_state=power_state.RUNNING)
        self.conn.vif_driver = mock.MagicMock()

        self.conn.suspend(instance)

        self.assertEqual(sdk_ve.state, pc.VMS_SUSPENDED)

    def test_resume(self):
        instance, sdk_ve = self._prep_instance_and_vm(
                                power_state=power_state.SUSPENDED)
        self.conn.vif_driver = mock.MagicMock()

        self.conn.resume(instance, network_info_1vif)

        self.assertEqual(sdk_ve.state, pc.VMS_RUNNING)

    def test_pause(self):
        instance, sdk_ve = self._prep_instance_and_vm(
                                power_state=power_state.RUNNING)
        self.conn.vif_driver = mock.MagicMock()

        self.conn.pause(instance)

        self.assertEqual(sdk_ve.state, pc.VMS_PAUSED)

    def test_unpause(self):
        instance, sdk_ve = self._prep_instance_and_vm(
                                power_state=power_state.PAUSED)
        self.conn.vif_driver = mock.MagicMock()

        self.conn.unpause(instance, network_info_1vif)

        self.assertEqual(sdk_ve.state, pc.VMS_RUNNING)

    def test_power_off(self):
        instance, sdk_ve = self._prep_instance_and_vm(
                                power_state=power_state.RUNNING)
        self.conn.vif_driver = mock.MagicMock()

        self.conn.power_off(instance)

        self.assertEqual(sdk_ve.state, pc.VMS_STOPPED)

    def test_power_on(self):
        instance, sdk_ve = self._prep_instance_and_vm(
                                power_state=power_state.SHUTDOWN)
        self.conn.vif_driver = mock.MagicMock()

        self.conn.power_on(self.context, instance, network_info_1vif)

        self.assertEqual(sdk_ve.state, pc.VMS_RUNNING)
