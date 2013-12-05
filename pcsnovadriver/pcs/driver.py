# vim: tabstop=4 shiftwidth=4 softtabstop=4

import socket

from oslo.config import cfg

from nova.network import linux_net
from nova import exception
from nova.compute import power_state
from nova.openstack.common.gettextutils import _
from nova.openstack.common import log as logging
from nova.virt import driver
from nova.virt import firewall
from nova import utils

from nova.virt.libvirt import imagecache
from pcsnovadriver.pcs.vif import PCSVIFDriver

prlsdkapi = None

LOG = logging.getLogger(__name__)

pcs_opts = [
    cfg.StrOpt('pcs_login',
                help = 'PCS SDK login'),

    cfg.StrOpt('pcs_password',
                help = 'PCS SDK password'),

    cfg.StrOpt('pcs_ostemplate',
                help = 'Temporary option to specify ostemplate'),
    ]

CONF = cfg.CONF
CONF.register_opts(pcs_opts)

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

class PCSDriver(driver.ComputeDriver):

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

        self.firewall_driver = firewall.load_driver(None, self.virtapi)
        self.vif_driver = PCSVIFDriver()

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
        self._psrv = prlsdkapi.Server()
        self._psrv.login('localhost', CONF.pcs_login, CONF.pcs_password).wait()

    def list_instances(self):
        LOG.info("list_instances")
        ves = self._psrv.get_vm_list_ex(nFlags = prlconsts.PVTF_CT).wait()
        return map(lambda x: x.get_name(), ves)

    def list_instance_uuids(self):
        LOG.info("list_instance_uuids")
        ves = self._psrv.get_vm_list_ex(nFlags = prlconsts.PVTF_CT).wait()
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
            ve = self._psrv.get_vm_config(name,
                        prlsdkapi.consts.PGVC_SEARCH_BY_NAME).wait()[0]
        except prlsdkapi.PrlSDKError, e:
            lib_err = prlsdkapi.prlsdk.errors.PRL_ERR_VM_UUID_NOT_FOUND
            if e.error_code == prlsdkapi.conv_error(lib_err):
                raise exception.InstanceNotFound(instance_id=name)
            raise
        return ve

    def plug_vifs(self, instance, network_info):
        LOG.info("plug_vifs: %s" % instance['name'])
        if not self.instance_exists(instance['name']):
            return
        for vif in network_info:
            self.vif_driver.plug(instance, vif)

    def unplug_vifs(self, instance, network_info):
        LOG.info("unplug_vifs: %s" % instance['name'])
        for vif in network_info:
            self.vif_driver.unplug(instance, vif)

    def spawn(self, context, instance, image_meta, injected_files,
            admin_password, network_info=None, block_device_info=None):
        LOG.info("spawn: %s" % instance['name'])
        LOG.info("context=%r" % context)
        LOG.info("instance=%r" % instance)
        LOG.info("image_meta=%r" % image_meta)
        LOG.info("admin_password=%r" % admin_password)
        LOG.info("network_info=%r" % network_info)
        LOG.info("block_device_info=%r" % block_device_info)

        sdk_ve = self._psrv.get_default_vm_config(
                        prlsdkapi.consts.PVT_CT, '', 0, 0).wait()[0]
        sdk_ve.set_uuid(instance['uuid'])
        sdk_ve.set_name(instance['name'])
        sdk_ve.set_vm_type(prlsdkapi.consts.PVT_CT)
        sdk_ve.set_os_template(CONF.pcs_ostemplate)
        sdk_ve.reg('', True).wait()

        sdk_ve.start_ex(prlconsts.PSM_VM_START,
                    prlconsts.PNSF_VM_START_WAIT).wait()

        self.plug_vifs(instance, network_info)
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

        # TODO: handle all possible states
        if state == prlconsts.VMS_RUNNING:
            sdk_ve.stop_ex(prlconsts.PSM_ACPI, prlconsts.PSF_FORCE).wait()
        sdk_ve.delete().wait()

        self.unplug_vifs(instance, network_info)
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
        stat = self.driver._psrv.get_statistics().wait()[0]
        cfg = self.driver._psrv.get_srv_config().wait()[0]
        info = self.driver._psrv.get_server_info()
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

        self._stats = data

        return data
