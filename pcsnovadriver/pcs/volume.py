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

from oslo.config import cfg

from nova.openstack.common import log as logging

import prlsdkapi
from prlsdkapi import consts as pc

LOG = logging.getLogger(__name__)

volume_opts = [
    ]

CONF = cfg.CONF
CONF.register_opts(volume_opts)

class PCSBaseVolumeDriver(object):
    """Base class for volume drivers."""
    def __init__(self, driver):
        LOG.info("%s.__init__" % self.__class__.__name__)
        self.driver = driver

    def _attach_blockdev(self, sdk_ve, device):
        #TODO: handle QOS specifications
        #TODO: handle RW mode
        #TODO: handle device name inside VE
        srv_cfg = self.driver.psrv.get_srv_config().wait().get_param()
        sdk_ve.begin_edit().wait()
        hdd = sdk_ve.add_default_device_ex(srv_cfg, pc.PDE_HARD_DISK)
        hdd.set_emulated_type(pc.PDT_USE_REAL_HDD)
        hdd.set_friendly_name(device.replace('/', '_'))
        hdd.set_sys_name(device)
        sdk_ve.commit().wait()

    def connect_volume(self, connection_info, sdk_ve, disk_info):
        raise NotImplementedError()

class PCSLocalVolumeDriver(PCSBaseVolumeDriver):

    def connect_volume(self, connection_info, sdk_ve, disk_info):
        data = connection_info['data']
        self._attach_blockdev(sdk_ve, data['device_path'])
