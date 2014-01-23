# vim: tabstop=4 shiftwidth=4 softtabstop=4

import os
import re

from oslo.config import cfg

from nova import exception
from nova.openstack.common.gettextutils import _
from nova.network import linux_net
from nova.network import model as network_model
from nova.openstack.common import log as logging
from nova import utils

LOG = logging.getLogger(__name__)
CONF = cfg.CONF
prlsdkapi = None

def get_bridge_ifaces(bridge):
    return os.listdir(os.path.join('/sys', 'class', 'net', bridge, 'brif'))

def format_mac(raw_mac):
	"""
	convert mac to format XX:XX:XX:XX:XX:XX
	(with upper-case symbols)
	"""

	if not raw_mac:
		return None

	if re.match("(?:[0-9a-fA-F]{2}-){5}[0-9a-fA-F]{2}", raw_mac):
		# Windows-like XX-XX-XX-XX-XX-XX
		return raw_mac.replace("-", ":").upper()
	elif re.match("(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}", raw_mac):
		# *nix XX:XX:XX:XX:XX:XX
		return raw_mac.upper()
	elif re.match("[0-9a-fA-F]{12}", raw_mac):
		# parallels-like XXXXXXXXXXXX
		mac = ""
		for byte_num in range(6):
			mac += raw_mac[byte_num * 2:byte_num * 2 + 2] + ':'
		mac = mac[:-1]
		return mac.upper()
	else:
		raise Exception("'%s' is not a mac-address" % raw_mac)

def pcs_create_ovs_vif_port(bridge, dev, iface_id, iface_name,
                            mac, instance_id):
    utils.execute('ovs-vsctl', '--', '--may-exist', 'add-port',
                  bridge, dev,
                  '--', 'set', 'Interface', dev,
                  'external-ids:iface-id=%s' % iface_id,
                  'external-ids:iface-name=%s' % iface_name,
                  'external-ids:iface-status=active',
                  'external-ids:attached-mac=%s' % mac,
                  'external-ids:vm-uuid=%s' % instance_id,
                  run_as_root=True)

class PCSVIFDriver(object):

    def __init__(self):
        global prlsdkapi
        global prlconsts
        if prlsdkapi is None:
            prlsdkapi = __import__('prlsdkapi')
            prlconsts = prlsdkapi.consts

    def get_firewall_required(self):
        """Nova's firewall is deprecated, let's assume, that we
        always use neutron's firewall and connect instances to
        integration bridge via intermediate linux bridge.
        """
        return True

    def _get_vif_class(self, instance, vif):
        if vif['type'] is None:
            raise exception.NovaException(
                _("vif_type parameter must be present "
                "for this vif_driver implementation"))
        elif vif['type'] == network_model.VIF_TYPE_OVS:
            if self.get_firewall_required():
                return VifOvsHybrid()
            else:
                return VifOvsEthernet()
        else:
            raise exception.NovaException(
                _("Unexpected vif_type=%s") % vif_type)

    def plug(self, driver, instance, sdk_ve, vif):
        LOG.info("plug: %s:%s" % (instance['name'], vif['devname']))
        vif_class = self._get_vif_class(instance, vif)
        vif_class.plug(driver, instance, sdk_ve, vif)

    def unplug(self, driver, instance, sdk_ve, vif):
        LOG.info("unplug: %s:%s" % (instance['name'], vif['devname']))
        vif_class = self._get_vif_class(instance, vif)
        vif_class.unplug(driver, instance, sdk_ve, vif)

class BaseVif:
    def get_ovs_interfaceid(self, vif):
        return vif.get('ovs_interfaceid') or vif['id']

    def get_br_name(self, iface_id):
        return ('pcsbr-' + iface_id)[:network_model.NIC_NAME_LEN]

    def get_veth_pair_names(self, iface_id):
        return (('pcsvb-' + iface_id)[:network_model.NIC_NAME_LEN],
                ('pcsvo-' + iface_id)[:network_model.NIC_NAME_LEN])

    def get_bridge_name(self, vif):
            return vif['network']['bridge']

    def get_prl_name(self, sdk_ve, netdev):
        if sdk_ve.get_vm_type() == prlconsts.PVT_VM:
            return "vme%08x.%d" % (sdk_ve.get_env_id(), netdev.get_index())
        else:
            return "veth%d.%d" % (sdk_ve.get_env_id(), netdev.get_index())

    def get_prl_dev(self, driver, sdk_ve, mac):
        """Return first network device with given MAC address
        or None, if it's not found.
        """
        mac = format_mac(mac)
        ndevs = sdk_ve.get_devs_count_by_type(
                    prlconsts.PDE_GENERIC_NETWORK_ADAPTER)
        for i in xrange(ndevs):
            netdev = sdk_ve.get_dev_by_type(
                            prlconsts.PDE_GENERIC_NETWORK_ADAPTER, i)
            if netdev.get_emulated_type() == prlconsts.PNA_ROUTED:
                continue
            if format_mac(netdev.get_mac_address()) == mac:
                return netdev
        else:
            return None

    def create_prl_dev(self, driver, sdk_ve, vif):
        """Add network device to VE and set MAC address.
        Set virtual network to some unexistent value, so that
        device will not be plugged into any bridged and we can
        do it by ourselves.
        """
        srv_config = driver.psrv.get_srv_config().wait()[0]
        sdk_ve.begin_edit().wait()
        netdev = sdk_ve.add_default_device_ex(srv_config,
                                prlconsts.PDE_GENERIC_NETWORK_ADAPTER)
        netdev.set_mac_address(vif['address'])
        netdev.set_virtual_network_id('_fake_unexistent')
        sdk_ve.commit().wait()
        return netdev

    def setup_prl_dev(self, driver, sdk_ve, vif):
        """Sets up device in VE, so that one end will be inside
        VE with given MAC. Another end - in host with specified
        device name.
        """
        if_name = vif['devname']

        netdev = self.get_prl_dev(driver, sdk_ve, vif['address'])
        if not netdev:
            netdev = self.create_prl_dev(driver, sdk_ve, vif)
        prl_name = self.get_prl_name(sdk_ve, netdev)

        if not linux_net.device_exists(if_name):
            utils.execute('ip', 'link', 'set', prl_name,
                          'up', run_as_root=True)
        return netdev

class VifOvsHybrid(BaseVif):

    def plug(self, driver, instance, sdk_ve, vif):
        iface_id = self.get_ovs_interfaceid(vif)
        if_name = vif['devname']
        br_name = self.get_br_name(vif['id'])
        v1_name, v2_name = self.get_veth_pair_names(vif['id'])

        if not linux_net.device_exists(br_name):
            utils.execute('brctl', 'addbr', br_name, run_as_root=True)
            utils.execute('brctl', 'setfd', br_name, 0, run_as_root=True)
            utils.execute('brctl', 'stp', br_name, 'off', run_as_root=True)

        netdev = self.setup_prl_dev(driver, sdk_ve, vif)
        prl_name = self.get_prl_name(sdk_ve, netdev)
        if_name = prl_name

        if not linux_net.device_exists(v2_name):
            linux_net._create_veth_pair(v1_name, v2_name)
            utils.execute('ip', 'link', 'set', br_name, 'up', run_as_root=True)
            utils.execute('brctl', 'addif', br_name, v1_name, run_as_root=True)
            pcs_create_ovs_vif_port(self.get_bridge_name(vif), v2_name,
                                    iface_id, prl_name, vif['address'],
                                    instance['uuid'])

        if if_name not in get_bridge_ifaces(br_name):
            utils.execute('brctl', 'addif', br_name, if_name, run_as_root=True)

        sdk_ve.begin_edit().wait()
        netdev.set_configure_with_dhcp(1)
        netdev.set_auto_apply(1)
        sdk_ve.commit().wait()

    def unplug(self, driver, instance, sdk_ve, vif):
        iface_id = self.get_ovs_interfaceid(vif)
        br_name = self.get_br_name(vif['id'])
        v1_name, v2_name = self.get_veth_pair_names(vif['id'])

        netdev = self.get_prl_dev(driver, sdk_ve, vif['address'])
        prl_name = self.get_prl_name(sdk_ve, netdev)

        linux_net.delete_ovs_vif_port(self.get_bridge_name(vif), v2_name)
        utils.execute('ip', 'link', 'set', br_name, 'down', run_as_root=True)
        utils.execute('brctl', 'delbr', br_name, run_as_root=True)

class VifOvsEthernet(BaseVif):

    def plug(self, driver, instance, sdk_ve, vif):
        iface_id = self.get_ovs_interfaceid(vif)
        if_name = vif['devname']

        netdev = self.setup_prl_dev(driver, sdk_ve, vif)

        prl_name = self.get_prl_name(sdk_ve, netdev)
        linux_net.create_ovs_vif_port(self.get_bridge_name(vif),
                                        prl_name, iface_id, vif['address'],
                                        instance['uuid'])
        sdk_ve.begin_edit().wait()
        netdev.set_configure_with_dhcp(1)
        netdev.set_auto_apply(1)
        sdk_ve.commit().wait()

    def unplug(self, driver, instance, sdk_ve, vif):
        netdev = self.get_prl_dev(driver, sdk_ve, vif['address'])
        prl_name = self.get_prl_name(sdk_ve, netdev)
        linux_net.delete_ovs_vif_port(self.get_bridge_name(vif), prl_name)
