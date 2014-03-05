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

from oslo.config import cfg

from nova.compute import power_state
from nova import test
from nova.virt import fake

from pcsnovadriver.pcs import prlsdkapi_proxy
from pcsnovadriver.tests.pcs import fakeprlsdkapi

prlsdkapi_proxy.prlsdkapi = fakeprlsdkapi

from pcsnovadriver.pcs import driver

CONF = cfg.CONF
CONF.import_opt('compute_manager', 'nova.service')
CONF.import_opt('firewall_driver', 'nova.virt.firewall')
CONF.import_opt('host', 'nova.netconf')
CONF.import_opt('my_ip', 'nova.netconf')
CONF.import_opt('image_cache_subdirectory_name', 'nova.virt.imagecache')
CONF.import_opt('instances_path', 'nova.compute.manager')

CONF.firewall_driver = "nova.virt.firewall.NoopFirewallDriver"

vms = [
        {
            'name': 'instance001',
            'uuid': '{19be06cb-a6f2-47a7-a53e-11bc6d4c3b98}',
            'ram_size': 1024,
            'cpu_count': 1,
            'state': fakeprlsdkapi.consts.VMS_STOPPED,
        },
        {
            'name': 'instance002',
            'uuid': '{d58fe074-ce99-46b7-8ce1-83620ba26426}',
            'ram_size': 2048,
            'cpu_count': 4,
            'state': fakeprlsdkapi.consts.VMS_RUNNING,
        },
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

