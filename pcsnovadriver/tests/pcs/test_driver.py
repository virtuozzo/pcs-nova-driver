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

from nova.compute import power_state
from nova.openstack.common import uuidutils
from nova import test
from nova.virt import fake

from pcsnovadriver.pcs import prlsdkapi_proxy
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

CONF.firewall_driver = "nova.virt.firewall.NoopFirewallDriver"

vm_stopped = {
            'name': 'instance001',
            'uuid': '{19be06cb-a6f2-47a7-a53e-11bc6d4c3b98}',
            'ram_size': 1024,
            'cpu_count': 1,
            'state': pc.VMS_STOPPED,
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


class PCSDriverTestCase(test.TestCase):

    def setUp(self):
        super(PCSDriverTestCase, self).setUp()
        self.conn = driver.PCSDriver(fake.FakeVirtAPI(), True)
        self.conn.init_host(host='localhost')
        self.conn.psrv.test_add_vms(vms)

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
        sdk_ve = fakeprlsdkapi.Vm(vm)
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

        openstack_states = {
                power_state.RUNNING: pc.VMS_RUNNING,
                power_state.PAUSED: pc.VMS_PAUSED,
                power_state.SHUTDOWN: pc.VMS_STOPPED,
                power_state.CRASHED: pc.VMS_STOPPED,
                power_state.SUSPENDED: pc.VMS_SUSPENDED,
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
            for req in openstack_states:
                vm['state'] = cur
                sdk_ve = fakeprlsdkapi.Vm(vm)
                instance['power_state'] = req
                self.conn._sync_ve_state(sdk_ve, instance)
                msg = "%s->%s" % (sdk_states[cur], power_state.STATE_MAP[req])
                self.assertEqual(sdk_ve.state, openstack_states[req], msg)

    def test_unplug_vifs(self):
        vm = vm_running
        sdk_ve = self._prep_plug_vifs(vm)

        self.conn.unplug_vifs(instance_running, network_info_1vif)

        args = (self.conn, instance_running, sdk_ve, network_info_1vif[0],)
        self.conn.vif_driver.unplug.assert_called_once_with(*args)
        self.assertEqual(sdk_ve.state, pc.VMS_RUNNING)
