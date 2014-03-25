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

import json
from oslo.config import cfg
import types

from neutron.agent.linux.iptables_firewall import IptablesFirewallDriver
from neutron.agent.linux import utils
from neutron.openstack.common import log as logging

LOG = logging.getLogger(__name__)


def get_ovs_vif_ports(bridge, search_str):
    """Find ports in the bridge and return python-like structure
    of then ports information. Format of the structure:
    [{u'_uuid': u'd9a7c869-72f3-436e-98fd-a58a13c41aa1',
      u'admin_state': u'up',
      u'external_ids': {u'attached-mac': u'fa:16:3e:a2:03:1a',
                        u'iface-id': u'7575dfb9-9f5c-47ff-9b31-749466ec0081',
                        ....
    }, ... ]
    """
    args = ['ovs-vsctl', '-f', 'json', '--timeout=2',
            '--', 'find', 'Interface', search_str]
    try:
        out = utils.execute(args, root_helper=cfg.CONF.AGENT.root_helper)
    except Exception as e:
        LOG.error(_("Unable to execute %(cmd)s. Exception: %(exception)s"),
                    {'cmd': args, 'exception': e})

    o = json.loads(out)
    ifaces = []
    for if_row in o['data']:
        iface = {}
        for i in xrange(len(o['headings'])):
            if isinstance(if_row[i], types.ListType):
                if if_row[i][0] == 'set':
                    iface[o['headings'][i]] = if_row[i][1]
                elif if_row[i][0] == 'map':
                    iface[o['headings'][i]] = dict(if_row[i][1])
                elif if_row[i][0] == 'uuid':
                    iface[o['headings'][i]] = if_row[i][1]
            else:
                iface[o['headings'][i]] = if_row[i]
        ifaces.append(iface)
    return ifaces


def get_ovs_vif_port_by_id(bridge, port_id):
    """Find OVS port by given port_id (value in exterenal_ids)
    and return port info.
    """
    ret = get_ovs_vif_ports(bridge, 'external_ids:iface-id="%s"' % port_id)
    if len(ret) == 0:
        raise Exception("Port with id %s is not found" % port_id)
    elif len(ret) > 1:
        raise Exception("More than one port with id %r" % ret)
    else:
        return ret[0]


class PCSIptablesFirewallDriver(IptablesFirewallDriver):
    """Here is how neutron with OVS works:
    br-int <pcsvoXXX>|--|<pcsvbXXX> pcsbrXXX <veth100.0>|---<eth0 in CT>

    First, we have integration bridge br-int and veth pair, one
    end of which is inside container, another one (veth100.0) -
    in host. We have to connect veth100.0 to bridge br-int.

    Also neutron need to setup security group rules between
    container and br-int. IptablesFirewallDriver class does it
    with help of iptables' match physdev:
    PHYSDEV match --physdev-out veth100.0 --physdev-is-bridged
    PHYSDEV match --physdev-in veth100.0 --physdev-is-bridged

    The problem is that this match works only for devices,
    connected to a linux bridge, not OVS. So we connect
    veth100.0 to br-int through intermediate linux bridge
    pcsbrXXX.

    We can use base class IptablesFirewallDriver, but we
    need to provide _get_device_name method, which returns
    a device name for physdev match. We use external_ids in
    openvswitch port to pass device name from pcs nova driver
    to this class. So here we just find OVS port and return
    external_ids.iface-name from it.
    """
    def _get_device_name(self, port):
        br = cfg.CONF.OVS.integration_bridge
        try:
            xport = get_ovs_vif_port_by_id(br, port['device'])
        except Exception:
            LOG.warn('Skipping firewall setup for port %s' % port['device'])
        return xport['external_ids']['iface-name']
