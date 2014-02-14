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
import tempfile
import subprocess
import shlex
import shutil
import time
import re
from xml.dom import minidom

from oslo.config import cfg

from nova.image import glance
from nova.network import linux_net
from nova import exception
from nova.compute import utils as compute_utils
from nova.compute import power_state
from nova.compute import task_states
from nova.openstack.common.gettextutils import _
from nova.openstack.common import jsonutils
from nova.openstack.common.processutils import ProcessExecutionError
from nova.openstack.common import log as logging
from nova.virt import driver
from nova.virt import images
from nova.virt import firewall
from nova import utils

from pcsnovadriver.pcs import imagecache
from pcsnovadriver.pcs.vif import PCSVIFDriver

prlsdkapi = None

LOG = logging.getLogger(__name__)

pcs_opts = [
    cfg.StrOpt('pcs_login',
                help = 'PCS SDK login'),

    cfg.StrOpt('pcs_password',
                help = 'PCS SDK password'),

    cfg.StrOpt('pcs_template_dir',
                default = '/vz/openstack-templates',
                help = 'Directory for storing image cache.'),
    ]

CONF = cfg.CONF
CONF.register_opts(pcs_opts)

# FIXME: add this constant to prlsdkapi
PRL_PRIVILEGED_GUEST_OS_SESSION = "531582ac-3dce-446f-8c26-dd7e3384dcf4"

def pcs_init_state_map():
    global PCS_POWER_STATE
    PCS_POWER_STATE = {
        prlconsts.VMS_COMPACTING:           power_state.NOSTATE,
        prlconsts.VMS_CONTINUING:           power_state.NOSTATE,
        prlconsts.VMS_DELETING_STATE:       power_state.NOSTATE,
        prlconsts.VMS_MIGRATING:            power_state.NOSTATE,
        prlconsts.VMS_PAUSED:               power_state.PAUSED,
        prlconsts.VMS_PAUSING:              power_state.RUNNING,
        prlconsts.VMS_RESETTING:            power_state.RUNNING,
        prlconsts.VMS_RESTORING:            power_state.NOSTATE,
        prlconsts.VMS_RESUMING:             power_state.NOSTATE,
        prlconsts.VMS_RUNNING:              power_state.RUNNING,
        prlconsts.VMS_SNAPSHOTING:          power_state.RUNNING,
        prlconsts.VMS_STARTING:             power_state.RUNNING,
        prlconsts.VMS_STOPPED:              power_state.SHUTDOWN,
        prlconsts.VMS_STOPPING:             power_state.RUNNING,
        prlconsts.VMS_SUSPENDED:            power_state.SUSPENDED,
        prlconsts.VMS_SUSPENDING:           power_state.RUNNING,
        prlconsts.VMS_SUSPENDING_SYNC:      power_state.NOSTATE,
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

        global prlsdkapi
        global prlconsts
        if prlsdkapi is None:
            prlsdkapi = __import__('prlsdkapi')
            prlconsts = prlsdkapi.consts
            pcs_init_state_map()

        self.host = None
        self._host_state = None

        if CONF.firewall_driver != "nova.virt.firewall.NoopFirewallDriver":
            raise NotImplementedError(firewall_msg)
        self.firewall_driver = firewall.load_driver(None, self.virtapi)
        self.vif_driver = PCSVIFDriver()
        self.image_cache_manager = imagecache.ImageCacheManager()

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
        flags = prlconsts.PVTF_CT|prlconsts.PVTF_VM
        ves = self.psrv.get_vm_list_ex(nFlags=flags).wait()
        return map(lambda x: x.get_name(), ves)

    def list_instance_uuids(self):
        LOG.info("list_instance_uuids")
        flags = prlconsts.PVTF_CT|prlconsts.PVTF_VM
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

    def _plug_vifs(self, instance, sdk_ve, network_info):
        for vif in network_info:
            self.vif_driver.plug(self, instance, sdk_ve, vif)

    def plug_vifs(self, instance, network_info):
        LOG.info("plug_vifs: %s" % instance['name'])
        if not self.instance_exists(instance['name']):
            return
        sdk_ve = self._get_ve_by_name(instance['name'])
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
        if sdk_ve.get_vm_type() == prlconsts.PVT_CT:
            # Can't tune physpages and swappages for VMs
            physpages = int(metadata['instance_type_memory_mb']) << 8
            sdk_ve.set_resource(prlconsts.PCR_PHYSPAGES, physpages, physpages)

            swappages = int(metadata['instance_type_swap']) << 8
            sdk_ve.set_resource(prlconsts.PCR_SWAPPAGES, swappages, swappages)
        sdk_ve.commit().wait()

        # TODO: tune swap size in VMs

        ndisks = sdk_ve.get_devs_count_by_type(prlconsts.PDE_HARD_DISK)
        if ndisks != 1:
            raise Exception("More than one disk in container")
        disk = sdk_ve.get_dev_by_type(prlconsts.PDE_HARD_DISK, 0)
        disk_size = int(metadata['instance_type_root_gb']) << 10
        disk.resize_image(disk_size, 0).wait()

    def _set_admin_password(self, sdk_ve, admin_password):
        if sdk_ve.get_vm_type() == prlconsts.PVT_VM:
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

        tmpl = get_template(self, context, instance['image_ref'],
                        instance['user_id'], instance['project_id'])
        sdk_ve = tmpl.create_instance(self.psrv, instance)

        self._apply_flavor(instance, sdk_ve)
        self._reset_network(sdk_ve)
        for vif in network_info:
            self.vif_driver.setup_dev(self, instance, sdk_ve, vif)
        sdk_ve.start_ex(prlconsts.PSM_VM_START,
                    prlconsts.PNSF_VM_START_WAIT).wait()

        self._plug_vifs(instance, sdk_ve, network_info)
        self._set_admin_password(sdk_ve, admin_password)

        self.firewall_driver.setup_basic_filtering(instance, network_info)
        self.firewall_driver.prepare_instance_filter(instance, network_info)

        self.firewall_driver.apply_instance_filter(instance, network_info)

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
        if state == prlconsts.VMS_RUNNING:
            sdk_ve.stop_ex(prlconsts.PSM_KILL, prlconsts.PSF_FORCE).wait()
        sdk_ve.delete().wait()

        self.firewall_driver.unfilter_instance(instance,
                                network_info=network_info)

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

        if reboot_type == 'SOFT':
            sdk_ve.restart().wait()
        else:
            sdk_ve.stop_ex(prlconsts.PSM_KILL, 0).wait()
            sdk_ve.start().wait()
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

    def power_off(self, instance):
        LOG.info("power_off %s" % instance['name'])
        sdk_ve = self._get_ve_by_name(instance['name'])
        sdk_ve.stop_ex(prlconsts.PSM_ACPI, prlconsts.PSF_FORCE).wait()

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

        if sdk_ve.get_vncmode() != prlconsts.PRD_AUTO:
            sdk_ve.begin_edit().wait()
            sdk_ve.set_vncmode(prlconsts.PRD_AUTO)
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
                    prlconsts.PDE_GENERIC_NETWORK_ADAPTER)
        sdk_ve.begin_edit().wait()
        for i in xrange(ndevs):
            dev = sdk_ve.get_dev_by_type(
                        prlconsts.PDE_GENERIC_NETWORK_ADAPTER, i)
            if dev.get_emulated_type() != prlconsts.PNA_ROUTED:
                dev.remove()
        sdk_ve.commit().wait()

    def snapshot(self, context, instance, image_id, update_task_state):
        LOG.info("snapshot %s" % instance['name'])

        sdk_ve = self._get_ve_by_name(instance['name'])

        update_task_state(task_state=task_states.IMAGE_PENDING_UPLOAD)

        tmpl_ve = sdk_ve.clone_ex("tmpl-" + image_id, '',
                prlconsts.PCVF_CLONE_TO_TEMPLATE).wait().get_param()

        self._reset_network(tmpl_ve)

        try:
            _image_service = glance.get_remote_image_service(context, image_id)
            snapshot_image_service, snapshot_image_id = _image_service
            snapshot = snapshot_image_service.show(context, snapshot_image_id)
            LOG.info("snapshot=%r" % snapshot)

            metadata = {'is_public': False,
                        'status': 'active',
                        'name': snapshot['name'],
                        'container_format': 'bare',
            }

            if tmpl_ve.get_vm_type() == prlconsts.PVT_VM:
                metadata['disk_format'] = 'ploop-vm'
            else:
                metadata['disk_format'] = 'ploop-container'

            update_task_state(task_state=task_states.IMAGE_UPLOADING,
                        expected_state=task_states.IMAGE_PENDING_UPLOAD)

            ve_dir = tmpl_ve.get_home_path()
            if tmpl_ve.get_vm_type() == prlconsts.PVT_VM:
                # for containers get_home_path returns path
                # to private area, but for VMs - path to VM
                # config file.
                ve_dir = os.path.dirname(ve_dir)

            args = shlex.split(utils.get_root_helper()) + \
                    ['tar', 'cO', '-C', ve_dir, '.']
            LOG.info("Running tar: %r" % args)
            p = subprocess.Popen(args, stdout = subprocess.PIPE)
            snapshot_image_service.update(context, image_id, metadata, p.stdout)
            ret = p.wait()
            if ret:
                raise Exception("tar returned %d" % ret)
            LOG.info(_("Snapshot image upload complete"), instance=instance)
        finally:
            tmpl_ve.delete().wait()

    def refresh_security_group_rules(self, security_group_id):
        LOG.info("refresh_security_group_rules %s" % instance['name'])
        self.firewall_driver.refresh_security_group_rules(security_group_id)

    def refresh_security_group_members(self, security_group_id):
        LOG.info("refresh_security_group_members %s" % instance['name'])
        self.firewall_driver.refresh_security_group_members(security_group_id)

    def refresh_instance_security_rules(self, instance):
        LOG.info("refresh_instance_security_rules %s" % instance['name'])
        self.firewall_driver.refresh_instance_security_rules(instance)

    def refresh_provider_fw_rules(self):
        LOG.info("refresh_provider_fw_rules %s" % instance['name'])
        self.firewall_driver.refresh_provider_fw_rules()

    def filter_defer_apply_on(self):
        LOG.info("filter_defer_apply_on %s" % instance['name'])
        self.firewall_driver.filter_defer_apply_on()

    def filter_defer_apply_off(self):
        LOG.info("filter_defer_apply_off %s" % instance['name'])
        self.firewall_driver.filter_defer_apply_off()

    def unfilter_instance(self, instance, network_info):
        """See comments of same method in firewall_driver."""
        LOG.info("unfilter_instance %s" % instance['name'])
        self.firewall_driver.unfilter_instance(instance,
                                               network_info=network_info)

    def inject_network_info(self, instance, nw_info):
        LOG.info("inject_network_info %s" % instance['name'])
        self.firewall_driver.setup_basic_filtering(instance, nw_info)

    def ensure_filtering_rules_for_instance(self, instance, network_info,
                                            time_module=None):
        LOG.info("ensure_filtering_rules_for_instance %s" % instance['name'])
        self.firewall_driver.setup_basic_filtering(instance, network_info)
        self.firewall_driver.prepare_instance_filter(instance, network_info)

    def set_admin_password(self, context, instance_id, new_pass=None):
        LOG.info("set_admin_password %s %s" % (instance_id, new_pass))
        sdk_ve = self._get_ve_by_name(instance_id)
        self._set_admin_password(sdk_ve, new_pass)

    def manage_image_cache(self, context, all_instances):
        LOG.info("manage_image_cache")
        self.image_cache_manager.update(context, all_instances)

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

def get_template(driver, context, image_ref, user_id, project_id):
        (image_service, image_id) = \
            glance.get_remote_image_service(context, image_ref)
        image_info = image_service.show(context, image_ref)

        if image_info['disk_format'] == 'ez-template':
            return EzTemplate(driver, context, image_ref, user_id, project_id)
        elif image_info['disk_format'] == 'ploop':
            return PloopTemplate(driver, context, image_ref, user_id, project_id)
        elif image_info['disk_format'] == 'cploop':
            return LZRWTemplate(driver, context, image_ref, user_id, project_id)
        else:
            return QemuTemplate(driver, context, image_ref, user_id, project_id)

class PCSTemplate(object):
    def __init__(self, context, image_ref, user_id, project_id):
        pass

    def create_instance(self, instance):
        raise NotImplementedError()

class EzTemplate:
    def __init__(self, driver, context, image_ref, user_id, project_id):
        LOG.info("PCSTemplate.__init__")
        self.user_id = user_id
        self.project_id = project_id
        self.rpm_path = None

        # get image information from glance
        (image_service, image_id) = \
            glance.get_remote_image_service(context, image_ref)
        image_info = image_service.show(context, image_ref)

        name, version, release = self._get_remote_info(context,
                                            image_ref, image_info)
        lname, lversion, lrelease = self._get_rpm_info(pkg = name)
        LOG.info("Glance template: %s-%s-%s, local rpm: %s-%s-%s" % \
                (name, version, release, lname, lversion, lrelease))
        self.name = name[:-3]

        if not lname:
            self._download_rpm(context, image_ref, image_info)
            LOG.info("installing rpm for template %s" % name)
            utils.execute('rpm', '-i', self.rpm_path, run_as_root = True)
        else:
            x = self._cmp_version_release(version, release, lversion, lrelease)
            if x == 0:
                return
            elif x < 0:
                self._download_rpm(context, image_ref, image_info)
                LOG.info("updating rpm for template %s" % name)
                utils.execute('rpm', '-U', file, run_as_root = True)
            else:
                LOG.warn("local rpm is newer than remote one!")

    def _download_rpm(self, context, image_ref, image_info):
        LOG.info("_download_rpm")
        if self.rpm_path:
            return

        if image_info['name']:
            name = image_info['name']
        else:
            name = image_info['id']

        if CONF.tempdir:
            tempdir = CONF.tempdir
        else:
            tempdir = tempfile.gettempdir()
        rpm_path = os.path.join(tempdir, name)
        images.fetch(context, image_ref, rpm_path,
                self.user_id, self.project_id)
        self.rpm_path = rpm_path

    def _get_remote_info(self, context, image_ref, image_info):
        LOG.info("_get_remote_info")
        for prop in 'pcs_name', 'pcs_version', 'pcs_release':
            if not image_info['properties'].has_key(prop):
                self._download_rpm(context, image_ref, image_info)
                name, ver, rel = self._get_rpm_info(file = self.rpm_path)
                if not name:
                    raise Exception("Invalid rpm file: %s" % self.rpm_path)
        return (image_info['properties']['pcs_name'],
                image_info['properties']['pcs_version'],
                image_info['properties']['pcs_release'])

    def _get_rpm_info(self, file = None, pkg = None):
        LOG.info("_get_rpm_info")
        cmd = ['rpm', '-q', '--qf', '%{NAME},%{VERSION},%{RELEASE}']
        if file:
            cmd += ['-p', file]
        else:
            cmd.append(pkg)

        try:
            out, err = utils.execute(*cmd)
        except ProcessExecutionError:
            return None, None, None
        LOG.info("out: %r" % out)
        return tuple(out.split(','))

    def _cmp_version(self, ver1, ver2):
        ver1_list = ver1.split('.')
        ver2_list = ver2.split('.')
        if len(ver1_list) > len(ver2_list):
            return -1
        elif len(ver1_list) < len(ver2_list):
            return 1
        else:
            i = 0
            for i in range(len(ver1_list)):
                if int(ver1_list[i]) > int(ver2_list[i]):
                    return -1
                elif int(ver1_list[i]) < int(ver2_list[i]):
                    return 1
        return 0

    def _cmp_version_release(self, ver1, rel1, ver2, rel2):
        x = self._cmp_version(ver1, ver2)
        if x:
            return x
        else:
            return self._cmp_version(rel1, rel2)

    def create_instance(self, psrv, instance):
        sdk_ve = psrv.get_default_vm_config(
                        prlsdkapi.consts.PVT_CT, 'vswap.1024MB', 0, 0).wait()[0]
        sdk_ve.set_uuid(instance['uuid'])
        sdk_ve.set_name(instance['name'])
        sdk_ve.set_vm_type(prlsdkapi.consts.PVT_CT)
        sdk_ve.set_os_template(self.name)
        sdk_ve.reg('', True).wait()
        return sdk_ve

class DiskCacheTemplate(PCSTemplate):
    """
    This class is for templates, based on disk images, stored
    in glance.
    """
    def __init__(self, driver, context, image_ref, user_id, project_id):
        LOG.info("%s.__init__" % self.__class__.__name__)
        self.user_id = user_id
        self.project_id = project_id
        self.driver = driver

        (image_service, image_id) = \
            glance.get_remote_image_service(context, image_ref)
        self.image_info = image_service.show(context, image_ref)
        self.image_id = image_id

        if not self._is_image_cached():
            self._cache_image(context, image_service)

    def _is_image_cached(self):
        """
        Returns True, if image with given id cached.
        """
        raise NotImplementedError()

    def _cache_image(self, context, image_service):
        """
        Cache image from glance to local FS.
        """
        raise NotImplementedError()

    def _put_image(self, dst):
        """
        Copy ploop image to the specified destination.
        """
        raise NotImplementedError()

    def _create_ct(self, psrv, instance):
        sdk_ve = psrv.get_default_vm_config(
                        prlsdkapi.consts.PVT_CT, 'vswap.1024MB', 0, 0).wait()[0]
        sdk_ve.set_uuid(instance['uuid'])
        sdk_ve.set_name(instance['name'])
        sdk_ve.set_vm_type(prlsdkapi.consts.PVT_CT)
        sdk_ve.set_os_template(self.image_info['properties']['pcs_ostemplate'])
        LOG.info("Creating container from eztemplate ...")
        sdk_ve.reg('', True).wait()

        disk_path = sdk_ve.get_home_path()
        disk_path = os.path.join(disk_path, 'root.hdd')
        LOG.info("Removing original disk ...")
        utils.execute('rm', '-rf', disk_path, run_as_root = True)
        self._put_image(disk_path)
        LOG.info("Done")
        return sdk_ve

    def _create_vm(self, psrv, instance):
        # create an empty VM
        sdk_ve = psrv.create_vm()
        srv_cfg = psrv.get_srv_config().wait().get_param()
        os_ver = getattr(prlconsts, "PVS_GUEST_VER_LIN_REDHAT")
        sdk_ve.set_default_config(srv_cfg, os_ver, True)
        sdk_ve.set_uuid('{%s}' % instance['uuid'])
        sdk_ve.set_name(instance['name'])
        sdk_ve.set_vm_type(prlsdkapi.consts.PVT_VM)

        # remove unneded devices
        n = sdk_ve.get_devs_count_by_type(prlconsts.PDE_HARD_DISK)
        for i in xrange(n):
            dev = sdk_ve.get_dev_by_type(prlconsts.PDE_HARD_DISK, i)
            dev.remove()

        n = sdk_ve.get_devs_count_by_type(
                    prlconsts.PDE_GENERIC_NETWORK_ADAPTER)
        for i in xrange(n):
            dev = sdk_ve.get_dev_by_type(
                        prlconsts.PDE_GENERIC_NETWORK_ADAPTER, i)
            dev.remove()

        sdk_ve.reg('', True).wait()

        # copy hard disk to VM directory
        ve_path = os.path.dirname(sdk_ve.get_home_path())
        disk_path = os.path.join(ve_path, "harddisk.hdd")
        self._put_image(disk_path)

        # add hard disk to VM config and set is as boot device
        sdk_ve.begin_edit().wait()

        hdd = sdk_ve.add_default_device_ex(srv_cfg, prlconsts.PDE_HARD_DISK)
        hdd.set_image_path(disk_path)

        b = sdk_ve.create_boot_dev()
        b.set_type(prlconsts.PDE_HARD_DISK)
        b.set_index(hdd.get_index())
        b.set_sequence_index(0)
        b.set_in_use(1)

        sdk_ve.commit().wait()

        return sdk_ve

    def create_instance(self, psrv, instance):
        props = self.image_info['properties']
        if not 'vm_mode' in props or props['vm_mode'] == 'hvm':
            return self._create_vm(psrv, instance)
        elif props['vm_mode'] == 'exe':
            return self._create_ct(psrv, instance)
        else:
            raise Exception("Unsupported VM mode '%s'" % props['vm_mode'])

class LZRWCacheTemplate(DiskCacheTemplate):
    """
    Class for templates, cached in form of ploop images,
    compressed with LZRW.
    """
    def _get_cached_file(self):
        return os.path.join(CONF.pcs_template_dir,
                            self.image_id + '.tar.lzrw')

    def _is_image_cached(self):
        return os.path.exists(self._get_cached_file())

    def _put_image(self, dst):
        cmd1 = ['prlcompress', '-u']
        cmd2 = shlex.split(utils.get_root_helper()) + ['tar', 'x', '-C', dst]

        utils.execute('mkdir', dst, run_as_root = True)

        LOG.info("Unpacking image %s to %s" % (self._get_cached_file(), dst))
        src_file = open(self._get_cached_file())
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

class PloopTemplate(LZRWCacheTemplate):

    def _compress_ploop(self, src, dst):
        cmd1 = ['tar', 'cO', '-C', src, '.']
        cmd2 = ['prlcompress', '-p']

        LOG.info("Packing image to %s" % dst)
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

    def _get_image_name(self, disk_descriptor):
        doc = minidom.parseString(disk_descriptor)
        disk_image = doc.firstChild

        items = disk_image.getElementsByTagName('StorageData')
        if len(items) != 1:
            raise Exception('Invalid DiskDescriptor.xml')
        storage_data = items[0]

        items = storage_data.getElementsByTagName('Storage')
        if len(items) != 1:
            raise Exception('Invalid DiskDescriptor.xml')
        storage = items[0]

        images = storage.getElementsByTagName('Image')
        if len(images) != 1:
            raise Exception('Ploop contains spapshots')
        image = images[0]

        files = image.getElementsByTagName('File')
        if len(files) != 1:
            raise Exception('Invalid DiskDescriptor.xml')
        file = files[0]

        text = file.firstChild
        if text.nodeType != text.TEXT_NODE:
            raise Exception('Invalid DiskDescriptor.xml')

        return text.nodeValue

    def _download_ploop(self, context, image_service, dst):
        LOG.info("Downloading image to %s ..." % dst)
        dd = self.image_info['properties']['pcs_disk_descriptor']
        image_name = self._get_image_name(dd)
        with open(os.path.join(dst, image_name), 'w') as f:
            image_service.download(context, self.image_id, f)
        with open(os.path.join(dst, 'DiskDescriptor.xml'), 'w') as f:
            f.write(self.image_info['properties']['pcs_disk_descriptor'])

    def _cache_image(self, context, image_service):
        tmpl_dir = os.path.join(CONF.pcs_template_dir, self.image_id)
        tmpl_file = self._get_cached_file()

        if os.path.exists(tmpl_dir):
            shutil.rmtree(tmpl_dir)
        os.mkdir(tmpl_dir)

        self._download_ploop(context, image_service, tmpl_dir)
        self._compress_ploop(tmpl_dir, tmpl_file)
        shutil.rmtree(tmpl_dir)

class QemuTemplate(PloopTemplate):
    """
    This class creates instances from images in formats, which
    qemu-img supports.
    """
    def _download_ploop(self, context, image_service, dst):
        glance_img = 'glance.img'
        glance_path = os.path.join(dst, glance_img)
        LOG.info("Download image from glance ...")
        with open(glance_path, 'w') as f:
            image_service.download(context, self.image_id, f)

        out, err = utils.execute('qemu-img', 'info',
                                 '--output=json', glance_path)
        img_info = jsonutils.loads(out)
        size = int(img_info['virtual-size'])

        utils.execute('ploop', 'init', '-s',
                      '%dK' % (size >> 10), os.path.join(dst, 'root.hds'))

        dd_path = os.path.join(dst, 'DiskDescriptor.xml')
        out, err = utils.execute('ploop', 'mount', dd_path, run_as_root=True)

        ro = re.search('dev=(\S+)', out)
        if not ro:
            utils.execute('ploop', 'umount', dd_path, run_as_root=True)
        ploop_dev = ro.group(1)

        try:
            LOG.info("Convert to ploop format ...")
            utils.execute('qemu-img', 'convert', '-O', 'raw',
                          glance_path, ploop_dev, run_as_root=True)
        finally:
            utils.execute('ploop', 'umount', dd_path, run_as_root=True)
            utils.execute('rm', '-f', dd_path + '.lck')
            os.unlink(glance_path)

class LZRWTemplate(LZRWCacheTemplate):
    """
    Class for images stored in cploop format.
    """
    def _cache_image(self, context, image_service):
        LOG.info("Download image from glance ...")
        with open(self._get_cached_file(), 'w') as f:
            image_service.download(context, self.image_id, f)
