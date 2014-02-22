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

import socket
import os
import subprocess
import shlex
import time
import tempfile

import prlsdkapi
from prlsdkapi import consts as pc

from oslo.config import cfg

from nova.image import glance
from nova import exception
from nova.compute import power_state
from nova.compute import task_states
from nova.openstack.common.gettextutils import _
from nova.openstack.common import jsonutils
from nova.openstack.common import log as logging
from nova.virt import driver
from nova import utils

from pcsnovadriver.pcs import imagecache
from pcsnovadriver.pcs import template
from pcsnovadriver.pcs import utils as pcsutils
from pcsnovadriver.pcs.vif import PCSVIFDriver

LOG = logging.getLogger(__name__)

pcs_opts = [
    cfg.StrOpt('pcs_login',
                help = 'PCS SDK login'),

    cfg.StrOpt('pcs_password',
                help = 'PCS SDK password'),

    cfg.StrOpt('pcs_template_dir',
                default = '/vz/openstack-templates',
                help = 'Directory for storing image cache.'),

    cfg.StrOpt('pcs_snapshot_disk_format',
                default = 'cploop',
                help = 'Disk format for snapshots.'),

    cfg.StrOpt('pcs_snapshot_dir',
                default = '/vz/openstack-snapshots',
                help = 'Directory for snapshot operation.'),

    cfg.StrOpt('pcs_volume_drivers',
                default = [
                    'local=pcsnovadriver.pcs.volume.PCSLocalVolumeDriver',
                    'iscsi=pcsnovadriver.pcs.volume.PCSISCSIVolumeDriver'
                ],
                help = 'PCS handlers for remote volumes.'),
    ]

CONF = cfg.CONF
CONF.register_opts(pcs_opts)

# FIXME: add this constant to prlsdkapi
PRL_PRIVILEGED_GUEST_OS_SESSION = "531582ac-3dce-446f-8c26-dd7e3384dcf4"

PCS_POWER_STATE = {
    pc.VMS_COMPACTING:           power_state.NOSTATE,
    pc.VMS_CONTINUING:           power_state.NOSTATE,
    pc.VMS_DELETING_STATE:       power_state.NOSTATE,
    pc.VMS_MIGRATING:            power_state.NOSTATE,
    pc.VMS_PAUSED:               power_state.PAUSED,
    pc.VMS_PAUSING:              power_state.RUNNING,
    pc.VMS_RESETTING:            power_state.RUNNING,
    pc.VMS_RESTORING:            power_state.NOSTATE,
    pc.VMS_RESUMING:             power_state.NOSTATE,
    pc.VMS_RUNNING:              power_state.RUNNING,
    pc.VMS_SNAPSHOTING:          power_state.RUNNING,
    pc.VMS_STARTING:             power_state.RUNNING,
    pc.VMS_STOPPED:              power_state.SHUTDOWN,
    pc.VMS_STOPPING:             power_state.RUNNING,
    pc.VMS_SUSPENDED:            power_state.SUSPENDED,
    pc.VMS_SUSPENDING:           power_state.RUNNING,
    pc.VMS_SUSPENDING_SYNC:      power_state.NOSTATE,
}

PCS_STATE_NAMES = {
    pc.VMS_COMPACTING:           'COMPACTING',
    pc.VMS_CONTINUING:           'CONTINUING',
    pc.VMS_DELETING_STATE:       'DELETING_STATE',
    pc.VMS_MIGRATING:            'MIGRATING',
    pc.VMS_PAUSED:               'PAUSED',
    pc.VMS_PAUSING:              'PAUSING',
    pc.VMS_RESETTING:            'RESETTING',
    pc.VMS_RESTORING:            'RESTORING',
    pc.VMS_RESUMING:             'RESUMING',
    pc.VMS_RUNNING:              'RUNNING',
    pc.VMS_SNAPSHOTING:          'SNAPSHOTING',
    pc.VMS_STARTING:             'STARTING',
    pc.VMS_STOPPED:              'STOPPED',
    pc.VMS_STOPPING:             'STOPPING',
    pc.VMS_SUSPENDED:            'SUSPENDED',
    pc.VMS_SUSPENDING:           'SUSPENDING',
    pc.VMS_SUSPENDING_SYNC:      'SUSPENDING_SYNC',
}

def get_sdk_errcode(strerr):
    lib_err = getattr(prlsdkapi.prlsdk.errors, strerr)
    return prlsdkapi.conv_error(lib_err)

firewall_msg = """nova's firewall deprecated, please 
set it to nova.virt.firewall.NoopFirewallDriver and 
use neutron's firewall. Edit /etc/neutron/plugin.conf
and set
firewall_driver=pcsnovadriver.neutron.pcs_firewall.PCSIptablesFirewallDriver
in [SECURITYGROUP] section.
"""

class PCSDriver(driver.ComputeDriver):

    capabilities = {
        "has_imagecache": True,
        "supports_recreate": False,
        }

    def __init__(self, virtapi, read_only=False):
        super(PCSDriver, self).__init__(virtapi)
        LOG.info("__init__")

        self.host = None
        self._host_state = None

        if CONF.firewall_driver != "nova.virt.firewall.NoopFirewallDriver":
            raise NotImplementedError(firewall_msg)
        self.vif_driver = PCSVIFDriver()
        self.image_cache_manager = imagecache.ImageCacheManager()
        self.volume_drivers = driver.driver_dict_from_config(
                                CONF.pcs_volume_drivers, self)

    @property
    def host_state(self):
        if not self._host_state:
            self._host_state = HostState(self)
        return self._host_state

    def init_host(self, host=socket.gethostname()):
        LOG.info("init_host")
        if not self.host:
            self.host = host

        prlsdkapi.init_server_sdk()
        self.psrv = prlsdkapi.Server()
        self.psrv.login('localhost', CONF.pcs_login, CONF.pcs_password).wait()

    def list_instances(self):
        LOG.info("list_instances")
        flags = pc.PVTF_CT|pc.PVTF_VM
        ves = self.psrv.get_vm_list_ex(nFlags=flags).wait()
        return map(lambda x: x.get_name(), ves)

    def list_instance_uuids(self):
        LOG.info("list_instance_uuids")
        flags = pc.PVTF_CT|pc.PVTF_VM
        ves = self.psrv.get_vm_list_ex(nFlags = flags).wait()
        return map(lambda x: x.get_uuid()[1:-1], ves)

    def instance_exists(self, instance_id):
        LOG.info("instance_exists: %s" % instance_id)
        try:
            self._get_ve_by_name(instance_id)
            return True
        except exception.InstanceNotFound:
            return False

    def _get_ve_by_name(self, name):
        try:
            ve = self.psrv.get_vm_config(name,
                        prlsdkapi.consts.PGVC_SEARCH_BY_NAME).wait()[0]
        except prlsdkapi.PrlSDKError, e:
            if e.error_code == get_sdk_errcode('PRL_ERR_VM_UUID_NOT_FOUND'):
                raise exception.InstanceNotFound(instance_id=name)
            raise
        return ve

    def _start(self, sdk_ve):
        sdk_ve.start().wait()

    def _stop(self, sdk_ve, kill=False):
        if kill:
            sdk_ve.stop_ex(pc.PSM_KILL, pc.PSF_FORCE).wait()
        else:
            sdk_ve.stop_ex(pc.PSM_ACPI, pc.PSF_FORCE).wait()

    def _suspend(self, sdk_ve):
        sdk_ve.suspend().wait()

    def _resume(self, sdk_ve):
        sdk_ve.resume().wait()

    def _pause(self, sdk_ve):
        sdk_ve.pause().wait()

    def _unpause(self, sdk_ve):
        sdk_ve.start().wait()

    def _get_state(self, sdk_ve):
        vm_info = sdk_ve.get_state().wait().get_param()
        return vm_info.get_state()

    def _wait_intermediate_state(self, sdk_ve):
        intermediate_states = [
            pc.VMS_COMPACTING,
            pc.VMS_CONTINUING,
            pc.VMS_DELETING_STATE,
            pc.VMS_MIGRATING,
            pc.VMS_PAUSING,
            pc.VMS_RESETTING,
            pc.VMS_RESTORING,
            pc.VMS_RESUMING,
            pc.VMS_SNAPSHOTING,
            pc.VMS_STARTING,
            pc.VMS_STOPPING,
            pc.VMS_SUSPENDING,
            pc.VMS_SUSPENDING_SYNC,
            ]

        while True:
            state = self._get_state(sdk_ve)
            if state not in intermediate_states:
                break
            LOG.info('VE "%s" is in %s state, waiting' % \
                     (sdk_ve.get_name(), PCS_STATE_NAMES[state]))
            time.sleep(1)

    def _set_started_state(self, sdk_ve):
        self._wait_intermediate_state(sdk_ve)
        state = self._get_state(sdk_ve)
        LOG.info("Switch VE to RUNNING state, current is %s" % \
                                        PCS_STATE_NAMES[state])
        if state == pc.VMS_STOPPED:
            self._start(sdk_ve)
        elif state == pc.VMS_SUSPENDED:
            self._resume(sdk_ve)
        elif state == pc.VMS_PAUSED:
            self._unpause(sdk_ve)

    def _set_stopped_state(self, sdk_ve, kill):
        self._wait_intermediate_state(sdk_ve)
        state = self._get_state(sdk_ve)
        LOG.info("Switch VE to STOPPED state, current is %s" % \
                                        PCS_STATE_NAMES[state])
        if state == pc.VMS_RUNNING:
            self._stop(sdk_ve, kill)
        elif state == pc.VMS_SUSPENDED:
            self._resume(sdk_ve)
            self._stop(sdk_ve, kill)
        elif state == pc.VMS_PAUSED:
            self._unpause(sdk_ve)
            self._stop(sdk_ve, kill)

    def _set_paused_state(self, sdk_ve):
        self._wait_intermediate_state(sdk_ve)
        state = self._get_state(sdk_ve)
        LOG.info("Switch VE to PAUSED state, current is %s" % \
                                        PCS_STATE_NAMES[state])
        if state == pc.VMS_RUNNING:
            self._pause(sdk_ve)
        elif state == pc.VMS_SUSPENDED:
            self._resume(sdk_ve)
            self._pause(sdk_ve)
        elif state == pc.VMS_STOPPED:
            self._start(sdk_ve)
            self._pause(sdk_ve)

    def _set_suspended_state(self, sdk_ve):
        self._wait_intermediate_state(sdk_ve)
        state = self._get_state(sdk_ve)
        LOG.info("Switch VE to SUSPENDED state, current is %s" % \
                                        PCS_STATE_NAMES[state])
        if state == pc.VMS_RUNNING:
            self._suspend(sdk_ve)
        elif state == pc.VMS_PAUSED:
            self._unpause(sdk_ve)
            self._suspend(sdk_ve)
        elif state == pc.VMS_STOPPED:
            self._stop(sdk_ve)
            self._suspend(sdk_ve)

    def _sync_ve_state(self, sdk_ve, instance):
        req_state = instance['power_state']
        if req_state == power_state.NOSTATE:
            return
        if req_state == power_state.RUNNING:
            self._set_started_state(sdk_ve)
        elif req_state == power_state.PAUSED:
            self._set_paused_state(sdk_ve)
        elif req_state == power_state.SHUTDOWN:
            self._set_stopped_state(sdk_ve)
        elif req_state == power_state.CRASHED:
            self._set_stopped_state(sdk_ve)
        elif req_state == power_state.SUSPENDED:
            self._set_suspended_state(sdk_ve)

    def _plug_vifs(self, instance, sdk_ve, network_info):
        for vif in network_info:
            self.vif_driver.plug(self, instance, sdk_ve, vif)

    def plug_vifs(self, instance, network_info):
        stopped_states = [power_state.NOSTATE,
                          power_state.SHUTDOWN,
                          power_state.CRASHED,
                          power_state.SUSPENDED,
                          ]
        LOG.info("plug_vifs: %s" % instance['name'])
        if instance['power_state'] in stopped_states:
            return
        if not self.instance_exists(instance['name']):
            return
        sdk_ve = self._get_ve_by_name(instance['name'])
        self._sync_ve_state(sdk_ve, instance)
        self._plug_vifs(instance, sdk_ve, network_info)

    def _unplug_vifs(self, instance, sdk_ve, network_info):
        for vif in network_info:
            self.vif_driver.unplug(self, instance, sdk_ve, vif)

    def unplug_vifs(self, instance, network_info):
        LOG.info("unplug_vifs: %s" % instance['name'])
        sdk_ve = self._get_ve_by_name(instance['name'])
        self._unplug_vifs(instance, sdk_ve, network_info)

    def _format_system_metadata(self, instance):
        res = {}
        for d in instance['system_metadata']:
            res[d['key']] = d['value']
        return res

    def _apply_flavor(self, instance, sdk_ve):
        metadata = self._format_system_metadata(instance)
        sdk_ve.begin_edit().wait()

        sdk_ve.set_cpu_count(int(metadata['instance_type_vcpus']))

        sdk_ve.set_ram_size(int(metadata['instance_type_memory_mb']))
        if sdk_ve.get_vm_type() == pc.PVT_CT:
            # Can't tune physpages and swappages for VMs
            physpages = int(metadata['instance_type_memory_mb']) << 8
            sdk_ve.set_resource(pc.PCR_PHYSPAGES, physpages, physpages)

            swappages = int(metadata['instance_type_swap']) << 8
            sdk_ve.set_resource(pc.PCR_SWAPPAGES, swappages, swappages)
        sdk_ve.commit().wait()

        # TODO: tune swap size in VMs

        ndisks = sdk_ve.get_devs_count_by_type(pc.PDE_HARD_DISK)
        if ndisks != 1:
            raise Exception("More than one disk in container")
        disk = sdk_ve.get_dev_by_type(pc.PDE_HARD_DISK, 0)
        disk_size = int(metadata['instance_type_root_gb']) << 10
        disk.resize_image(disk_size, 0).wait()

    def _set_admin_password(self, sdk_ve, admin_password):
        if sdk_ve.get_vm_type() == pc.PVT_VM:
            # FIXME: waiting for system boot is broken for VMs
            LOG.info("Skip setting admin password")
            return
        session = sdk_ve.login_in_guest(
                PRL_PRIVILEGED_GUEST_OS_SESSION, '', 0).wait()[0]
        session.set_user_passwd('root', admin_password, 0).wait()
        session.logout(0)

    def spawn(self, context, instance, image_meta, injected_files,
            admin_password, network_info=None, block_device_info=None):
        LOG.info("spawn: %s" % instance['name'])

        tmpl = template.get_template(self, context, instance['image_ref'],
                        instance['user_id'], instance['project_id'])
        sdk_ve = tmpl.create_instance(self.psrv, instance)

        self._apply_flavor(instance, sdk_ve)
        self._reset_network(sdk_ve)
        for vif in network_info:
            self.vif_driver.setup_dev(self, instance, sdk_ve, vif)

        block_device_mapping = driver.block_device_info_get_mapping(
            block_device_info)

        for vol in block_device_mapping:
            connection_info = vol['connection_info']
            disk_info = {
                'dev': vol['mount_device'],
                'mount_device': vol['mount_device']}
            self.volume_driver_method('connect_volume',
                                connection_info, sdk_ve, disk_info)

        sdk_ve.start_ex(pc.PSM_VM_START, pc.PNSF_VM_START_WAIT).wait()

        self._plug_vifs(instance, sdk_ve, network_info)
        self._set_admin_password(sdk_ve, admin_password)

    def destroy(self, instance, network_info, block_device_info=None,
                destroy_disks=True, context=None):
        LOG.info("destroy: %s" % instance['name'])
        try:
            sdk_ve = self._get_ve_by_name(instance['name'])
        except exception.InstanceNotFound:
            return

        vm_info = sdk_ve.get_state().wait().get_param()
        state = vm_info.get_state()

        self._unplug_vifs(instance, sdk_ve, network_info)

        # TODO: handle all possible states
        if state == pc.VMS_RUNNING:
            sdk_ve.stop_ex(pc.PSM_KILL, pc.PSF_FORCE).wait()

        block_device_mapping = driver.block_device_info_get_mapping(
            block_device_info)

        for vol in block_device_mapping:
            connection_info = vol['connection_info']
            disk_info = {
                'dev': vol['mount_device'],
                'mount_device': vol['mount_device']}
            self.volume_driver_method('disconnect_volume',
                                connection_info, sdk_ve, disk_info)

        sdk_ve.delete().wait()

    def get_info(self, instance):
        LOG.info("get_info: %s %s" % (instance['id'], instance['name']))
        sdk_ve = self._get_ve_by_name(instance['name'])
        vm_info = sdk_ve.get_state().wait().get_param()

        data = {}
        data['state'] = PCS_POWER_STATE[vm_info.get_state()]
        data['max_mem'] = sdk_ve.get_ram_size()
        data['mem'] = data['max_mem']
        data['num_cpu'] = sdk_ve.get_cpu_count()
        data['cpu_time'] = 1000
        return data

    def get_host_stats(self, refresh=False):
        LOG.info("get_host_stats")
        return self.host_state.get_host_stats(refresh=refresh)

    def get_available_resource(self, nodename):
        LOG.info("get_available_resource")
        return self.host_state.get_host_stats(refresh=True)

    def reboot(self, context, instance, network_info, reboot_type='SOFT',
            block_device_info=None, bad_volumes_callback=None):
        LOG.info("reboot %s" % instance['name'])
        sdk_ve = self._get_ve_by_name(instance['name'])

        if self._get_state(sdk_ve) == pc.VMS_RUNNING \
                            and reboot_type == 'SOFT':
            sdk_ve.restart().wait()
        else:
            kill = not (reboot_type == 'SOFT')
            self._set_stopped_state(sdk_ve, kill)
            self._set_started_state(sdk_ve)
        self._plug_vifs(instance, sdk_ve, network_info)

    def suspend(self, instance):
        LOG.info("suspend %s" % instance['name'])
        sdk_ve = self._get_ve_by_name(instance['name'])
        sdk_ve.suspend().wait()

    def resume(self, instance, network_info, block_device_info=None):
        LOG.info("resume %s" % instance['name'])
        sdk_ve = self._get_ve_by_name(instance['name'])
        sdk_ve.resume().wait()
        self._plug_vifs(instance, sdk_ve, network_info)

    def pause(self, instance):
        LOG.info("suspend %s" % instance['name'])
        sdk_ve = self._get_ve_by_name(instance['name'])
        if sdk_ve.get_vm_type() != pc.PVT_VM:
            raise NotImplementedError()
        sdk_ve.pause().wait()

    def unpause(self, instance, network_info, block_device_info=None):
        LOG.info("resume %s" % instance['name'])
        sdk_ve = self._get_ve_by_name(instance['name'])
        if sdk_ve.get_vm_type() != pc.PVT_VM:
            raise NotImplementedError()
        sdk_ve.resume().wait()

    def power_off(self, instance):
        LOG.info("power_off %s" % instance['name'])
        sdk_ve = self._get_ve_by_name(instance['name'])
        sdk_ve.stop_ex(pc.PSM_ACPI, pc.PSF_FORCE).wait()

    def power_on(self, context, instance, network_info,
                    block_device_info=None):
        LOG.info("power_on %s" % instance['name'])
        sdk_ve = self._get_ve_by_name(instance['name'])
        sdk_ve.start().wait()
        self._unplug_vifs(instance, sdk_ve, network_info)
        self._plug_vifs(instance, sdk_ve, network_info)

    def get_vnc_console(self, instance):
        LOG.info("get_vnc_console %s" % instance['name'])
        sdk_ve = self._get_ve_by_name(instance['name'])

        if sdk_ve.get_vncmode() != pc.PRD_AUTO:
            sdk_ve.begin_edit().wait()
            sdk_ve.set_vncmode(pc.PRD_AUTO)
            sdk_ve.commit().wait()
            sdk_ve.refresh_config()

        sleep_time = 0.5
        for attempt in xrange(5):
            #FIXME: it's possible a bug in dispatcher: sometimes when
            # you setup VNC, port still 0 for some short time.
            port = sdk_ve.get_vncport()
            if port:
                break
            time.sleep(sleep_time)
            sleep_time = sleep_time * 2
            sdk_ve.refresh_config()

        return {'host': self.host, 'port': port, 'internal_access_path': None}

    def _reset_network(self, sdk_ve):
        """
        Remove all network adapters (except for venet, which
        can't be removed).
        """
        ndevs = sdk_ve.get_devs_count_by_type(
                    pc.PDE_GENERIC_NETWORK_ADAPTER)
        sdk_ve.begin_edit().wait()
        for i in xrange(ndevs):
            dev = sdk_ve.get_dev_by_type(pc.PDE_GENERIC_NETWORK_ADAPTER, i)
            if dev.get_emulated_type() != pc.PNA_ROUTED:
                dev.remove()
        sdk_ve.commit().wait()

    def _snapshot_ve(self, context, instance, image_id, update_task_state, ve):
        def upload(context, image_service, image_id, metadata, f):
            LOG.info("Start uploading image %s ..." % image_id)
            image_service.update(context, image_id, metadata, f)
            LOG.info("Image %s uploading complete." % image_id)

        _image_service = glance.get_remote_image_service(context, image_id)
        snapshot_image_service, snapshot_image_id = _image_service
        snapshot = snapshot_image_service.show(context, snapshot_image_id)
        disk_format = CONF.pcs_snapshot_disk_format

        metadata = {'is_public': False,
                    'status': 'active',
                    'name': snapshot['name'],
                    'container_format': 'bare',
                    'disk_format': disk_format,
        }

        props = {}
        metadata['properties'] = props

        if ve.get_vm_type() == pc.PVT_VM:
            props['vm_mode'] = 'hvm'
        else:
            props['vm_mode'] = 'exe'

        update_task_state(task_state=task_states.IMAGE_UPLOADING,
                    expected_state=task_states.IMAGE_PENDING_UPLOAD)

        hdd = pcsutils.get_boot_disk(ve)
        hdd_path = hdd.get_image_path()

        props['pcs_ostemplate'] = ve.get_os_template()
        if disk_format == 'ploop':
            xml_path = os.path.join(hdd_path, "DiskDescriptor.xml")
            cmd = ['ploop', 'snapshot-list', '-H', '-o', 'fname', xml_path]
            out, err = utils.execute(*cmd, run_as_root=True)
            image_path = out.strip()

            with open(xml_path) as f:
                props['pcs_disk_descriptor'] = f.read().replace('\n', '')

            with open(image_path) as f:
                upload(context, snapshot_image_service, image_id, metadata, f)
        elif disk_format == 'cploop':
            uploader = pcsutils.CPloopUploader(hdd_path)
            f = uploader.start()
            try:
                upload(context, snapshot_image_service, image_id, metadata, f)
            finally:
                uploader.wait()
        else:
            dst = tempfile.mktemp(dir=os.path.dirname(hdd_path))
            LOG.info("Convert image %s to %s format ..." % \
                     (image_id, disk_format))
            pcsutils.convert_image(hdd_path, dst, disk_format,
                                   root_helper=utils.get_root_helper())
            with open(dst) as f:
                upload(context, snapshot_image_service, image_id, metadata, f)
            os.unlink(dst)

    def snapshot(self, context, instance, image_id, update_task_state):
        LOG.info("snapshot %s" % instance['name'])
        sdk_ve = self._get_ve_by_name(instance['name'])

        update_task_state(task_state=task_states.IMAGE_PENDING_UPLOAD)

        tmpl_ve_name = "tmpl-" + image_id
        tmpl_ve = sdk_ve.clone_ex(tmpl_ve_name, CONF.pcs_snapshot_dir,
                pc.PCVF_CLONE_TO_TEMPLATE).wait().get_param()

        ve_dir = tmpl_ve.get_home_path()
        if tmpl_ve.get_vm_type() == pc.PVT_VM:
            # for containers get_home_path returns path
            # to private area, but for VMs - path to VM
            # config file.
            ve_dir = os.path.dirname(ve_dir)

        utils.execute('chown', '-R', 'nova:nova', ve_dir, run_as_root=True)
        try:
            self._snapshot_ve(context, instance, image_id,
                              update_task_state, tmpl_ve)
        finally:
            tmpl_ve.delete().wait()

    def set_admin_password(self, context, instance_id, new_pass=None):
        LOG.info("set_admin_password %s %s" % (instance_id, new_pass))
        sdk_ve = self._get_ve_by_name(instance_id)
        self._set_admin_password(sdk_ve, new_pass)

    def manage_image_cache(self, context, all_instances):
        LOG.info("manage_image_cache")
        self.image_cache_manager.update(context, all_instances)

    def volume_driver_method(self, method_name, connection_info,
                             *args, **kwargs):
        driver_type = connection_info.get('driver_volume_type')
        if driver_type not in self.volume_drivers:
            raise exception.VolumeDriverNotFound(driver_type=driver_type)
        driver = self.volume_drivers[driver_type]
        method = getattr(driver, method_name)
        return method(connection_info, *args, **kwargs)

    def get_volume_connector(self, instance):
        connector = {'ip': CONF.my_ip,
                     'host': CONF.host,
                     'initiator': '_not_implemented'}
        return connector

    def get_disk_dev_path(self, hdd):
        #FIXME: add snapshots support
        xml_path = os.path.join(hdd.get_sys_name(), "DiskDescriptor.xml")
        cmd = ['ploop', 'snapshot-list', '-H', '-o', 'fname', xml_path]
        out, err = utils.execute(*cmd, run_as_root=True)
        return out.strip()

    def get_used_block_devices(self):
        ves = self.psrv.get_vm_list_ex(nFlags=pc.PVTF_VM).wait()
        devices = []
        for sdk_ve in ves:
            n = sdk_ve.get_devs_count_by_type(pc.PDE_HARD_DISK)
            for i in xrange(n):
                dev = sdk_ve.get_dev_by_type(pc.PDE_HARD_DISK, i)
                if dev.get_emulated_type() == pc.PDT_USE_REAL_HDD:
                    devices.append(self.get_disk_dev_path(dev))
        return devices

class HostState(object):
    def __init__(self, driver):
        super(HostState, self).__init__()
        self._stats = {}
        self.driver = driver
        self.update_status()

    def get_host_stats(self, refresh=False):
        if refresh or not self._stats:
            self.update_status()
        return self._stats

    def _format_ver(self, pver):
        pver = pver.split('.')
        return int(pver[0]) * 10000 + int(pver[1]) * 100

    def update_status(self):
        stat = self.driver.psrv.get_statistics().wait()[0]
        cfg = self.driver.psrv.get_srv_config().wait()[0]
        info = self.driver.psrv.get_server_info()
        data = {}

        data = dict()
        data['vcpus'] = cfg.get_cpu_count()
        data['vcpus_used'] = 0 # TODO: think, how we can provide used CPUs
        data['cpu_info'] = 0
        data['memory_mb'] = stat.get_total_ram_size() >> 20
        data['memory_mb_used'] = stat.get_usage_ram_size() >> 20
        data['local_gb'] = 101 # TODO
        data['local_gb_used'] = 44 # TODO
        data['hypervisor_type'] = 'PCS'
        data['hypervisor_version'] = self._format_ver(info.get_product_version())
        data['hypervisor_hostname'] = self.driver.host
        data["supported_instances"] = jsonutils.dumps([('i686', 'pcs', 'hvm'),
                                       ('x86_64', 'pcs', 'hvm'),
                                       ('i686', 'pcs', 'exe'),
                                       ('x86_64', 'pcs', 'exe')])

        self._stats = data

        return data

