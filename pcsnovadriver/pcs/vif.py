# vim: tabstop=4 shiftwidth=4 softtabstop=4

import os

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

class PCSVIFDriver(object):

    def __init__(self):
        global prlsdkapi
        global prlconsts
        if prlsdkapi is None:
            prlsdkapi = __import__('prlsdkapi')
            prlconsts = prlsdkapi.consts

    def get_firewall_required(self):
        if CONF.firewall_driver != "nova.virt.firewall.NoopFirewallDriver":
            return True
        return False

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

        if not linux_net.device_exists(v2_name):
            linux_net._create_veth_pair(v1_name, v2_name)
            utils.execute('ip', 'link', 'set', br_name, 'up', run_as_root=True)
            utils.execute('brctl', 'addif', br_name, v1_name, run_as_root=True)
            linux_net.create_ovs_vif_port(self.get_bridge_name(vif),
                                        v2_name, iface_id, vif['address'],
                                        instance['uuid'])

        netif = '%s,%s,%s' % (if_name, vif['address'], if_name)
        out, err = utils.execute('vzctl', 'set', instance['name'], '--save',
            '--netif_add', netif, run_as_root=True)
        utils.execute('ip', 'link', 'set', if_name, 'up', run_as_root=True)

        if if_name not in get_bridge_ifaces(br_name):
            utils.execute('brctl', 'addif', br_name, if_name, run_as_root=True)
        out, err = utils.execute('vzctl', 'set', instance['name'], '--save',
            '--ifname', if_name, '--host_ifname', if_name,
            '--dhcp', 'yes', run_as_root=True)

    def unplug(self, driver, instance, sdk_ve, vif):
        iface_id = self.get_ovs_interfaceid(vif)
        br_name = self.get_br_name(vif['id'])
        v1_name, v2_name = self.get_veth_pair_names(vif['id'])

        linux_net.delete_ovs_vif_port(self.get_bridge_name(vif), v2_name)
        utils.execute('ip', 'link', 'set', br_name, 'down', run_as_root=True)
        utils.execute('brctl', 'delbr', br_name, run_as_root=True)

class VifOvsEthernet(BaseVif):

    def plug(self, driver, instance, sdk_ve, vif):
        iface_id = self.get_ovs_interfaceid(vif)
        if_name = vif['devname']

        netif = '%s,%s,%s' % (if_name, vif['address'], if_name)
        out, err = utils.execute('vzctl', 'set', instance['name'], '--save',
                            '--netif_add', netif, run_as_root=True)
        utils.execute('ip', 'link', 'set', if_name, 'up', run_as_root=True)

        linux_net.create_ovs_vif_port(self.get_bridge_name(vif),
                                        if_name, iface_id, vif['address'],
                                        instance['uuid'])
        out, err = utils.execute('vzctl', 'set', instance['name'], '--save',
            '--ifname', if_name, '--host_ifname', if_name,
            '--dhcp', 'yes', run_as_root=True)

    def unplug(self, driver, instance, sdk_ve, vif):
        linux_net.delete_ovs_vif_port(self.get_bridge_name(vif), vif['devname'])
