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

import os
import tempfile
import time

from oslo.config import cfg

from nova import exception
from nova.openstack.common.gettextutils import _
from nova.openstack.common import log as logging
from nova.openstack.common import processutils
from nova import utils

from pcsnovadriver.pcs import prlsdkapi_proxy

pc = prlsdkapi_proxy.consts

LOG = logging.getLogger(__name__)

volume_opts = [
    cfg.IntOpt('num_iscsi_scan_tries',
                default=3,
                help='Number of times to rescan iSCSI target to find volume'),
    cfg.BoolOpt('pcs_iscsi_use_multipath',
                default=False,
                help='Use multipath connection of the iSCSI volume'),

   cfg.StrOpt('pstorage_mount_point_base',
               default='/pstorage/nova-compute',
               help='Base directory for PStorage mounts.'),
    ]

CONF = cfg.CONF
CONF.register_opts(volume_opts)


class PCSBaseVolumeDriver(object):
    """Base class for volume drivers."""
    def __init__(self, driver):
        LOG.info("%s.__init__" % self.__class__.__name__)
        self.driver = driver

    def _attach_blockdev(self, sdk_ve, host_device, guest_device):
        #TODO(dguryanov): handle QOS specifications
        #TODO(dguryanov): handle RW mode
        #TODO(dguryanov): handle device name inside VE
        srv_cfg = self.driver.psrv.get_srv_config().wait().get_param()
        sdk_ve.begin_edit().wait()
        hdd = sdk_ve.add_default_device_ex(srv_cfg, pc.PDE_HARD_DISK)
        hdd.set_emulated_type(pc.PDT_USE_REAL_HDD)
        hdd.set_friendly_name(guest_device)
        hdd.set_sys_name(host_device)
        sdk_ve.commit().wait()
        return hdd

    def _detach_blockdev(self, sdk_ve, host_device,
                         guest_device, ignore_errors):
        sdk_ve.begin_edit().wait()
        n = sdk_ve.get_devs_count_by_type(pc.PDE_HARD_DISK)
        for i in xrange(n):
            dev = sdk_ve.get_dev_by_type(pc.PDE_HARD_DISK, i)
            if dev.get_emulated_type() != pc.PDT_USE_REAL_HDD:
                continue
            if self.driver.get_disk_dev_path(dev) == host_device:
                LOG.info("Removing device %s" % dev.get_friendly_name())
                dev.remove()
                break
        else:
            msg = "Can't find device %s" % guest_device
            if ignore_errors:
                LOG.error(msg)
            else:
                raise Exception(msg)
        sdk_ve.commit().wait()

    def _attach_image(self, sdk_ve, image):
        #TODO(dguryanov): handle QOS specifications
        #TODO(dguryanov): handle RW mode
        #TODO(dguryanov): handle device name inside VE
        srv_cfg = self.driver.psrv.get_srv_config().wait().get_param()
        sdk_ve.begin_edit().wait()
        hdd = sdk_ve.add_default_device_ex(srv_cfg, pc.PDE_HARD_DISK)
        hdd.set_emulated_type(pc.PDT_USE_IMAGE_FILE)
        hdd.set_image_path(image)
        sdk_ve.commit().wait()
        return hdd

    def _detach_image(self, sdk_ve, image, ignore_errors):
        sdk_ve.begin_edit().wait()
        n = sdk_ve.get_devs_count_by_type(pc.PDE_HARD_DISK)
        for i in xrange(n):
            dev = sdk_ve.get_dev_by_type(pc.PDE_HARD_DISK, i)
            if dev.get_emulated_type() != pc.PDT_USE_IMAGE_FILE:
                continue
            if dev.get_image_path() == image:
                LOG.info("Removing device %s" % dev.get_friendly_name())
                dev.remove()
                break
        else:
            msg = "Can't find device with image %s" % image
            if ignore_errors:
                LOG.error(msg)
            else:
                raise Exception(msg)
        sdk_ve.commit().wait()

    def connect_volume(self, connection_info, sdk_ve, disk_info):
        raise NotImplementedError()

    def disconnect_volume(self, connection_info, sdk_ve, disk_info):
        raise NotImplementedError()


class PCSLocalVolumeDriver(PCSBaseVolumeDriver):

    def connect_volume(self, connection_info, sdk_ve, disk_info):
        data = connection_info['data']
        return self._attach_blockdev(sdk_ve,
                data['device_path'], disk_info['dev'])

    def disconnect_volume(self, connection_info, sdk_ve,
                          disk_info, ignore_errors):
        data = connection_info['data']
        self._detach_blockdev(sdk_ve, data['device_path'],
                              disk_info['dev'], ignore_errors)


class PCSISCSIVolumeDriver(PCSBaseVolumeDriver):

    def _run_iscsiadm(self, iscsi_properties, iscsi_command, **kwargs):
        check_exit_code = kwargs.pop('check_exit_code', 0)
        (out, err) = utils.execute('iscsiadm', '-m', 'node', '-T',
                                   iscsi_properties['target_iqn'],
                                   '-p', iscsi_properties['target_portal'],
                                   *iscsi_command, run_as_root=True,
                                   check_exit_code=check_exit_code)
        LOG.debug("iscsiadm %s: stdout=%s stderr=%s" %
                  (iscsi_command, out, err))
        return (out, err)

    def _iscsiadm_update(self, iscsi_properties, property_key, property_value,
                         **kwargs):
        iscsi_command = ('--op', 'update', '-n', property_key,
                         '-v', property_value)
        return self._run_iscsiadm(iscsi_properties, iscsi_command, **kwargs)

    def _get_target_portals_from_iscsiadm_output(self, output):
        return [line.split()[0] for line in output.splitlines()]

    @utils.synchronized('connect_volume')
    def connect_volume(self, connection_info, sdk_ve, disk_info):
        """Attach the volume to instance_name."""
        iscsi_properties = connection_info['data']

        pcs_iscsi_use_multipath = CONF.pcs_iscsi_use_multipath

        if pcs_iscsi_use_multipath:
            #multipath installed, discovering other targets if available
            #multipath should be configured on the nova-compute node,
            #in order to fit storage vendor
            out = self._run_iscsiadm_bare(['-m',
                                          'discovery',
                                          '-t',
                                          'sendtargets',
                                          '-p',
                                          iscsi_properties['target_portal']],
                                          check_exit_code=[0, 255])[0] \
                or ""

            for ip in self._get_target_portals_from_iscsiadm_output(out):
                props = iscsi_properties.copy()
                props['target_portal'] = ip
                self._connect_to_iscsi_portal(props)

            self._rescan_iscsi()
        else:
            self._connect_to_iscsi_portal(iscsi_properties)

        host_device = ("/dev/disk/by-path/ip-%s-iscsi-%s-lun-%s" %
                       (iscsi_properties['target_portal'],
                        iscsi_properties['target_iqn'],
                        iscsi_properties.get('target_lun', 0)))

        # The /dev/disk/by-path/... node is not always present immediately
        # TODO(justinsb): This retry-with-delay is a pattern, move to utils?
        tries = 0
        disk_dev = disk_info['dev']
        while not os.path.exists(host_device):
            if tries >= CONF.num_iscsi_scan_tries:
                raise exception.NovaException(_("iSCSI device not found at %s")
                                              % (host_device))

            LOG.warn(_("ISCSI volume not yet found at: %(disk_dev)s. "
                       "Will rescan & retry.  Try number: %(tries)s"),
                     {'disk_dev': disk_dev,
                      'tries': tries})

            # The rescan isn't documented as being necessary(?), but it helps
            self._run_iscsiadm(iscsi_properties, ("--rescan",))

            tries = tries + 1
            if not os.path.exists(host_device):
                time.sleep(tries ** 2)

        if tries != 0:
            LOG.debug(_("Found iSCSI node %(disk_dev)s "
                        "(after %(tries)s rescans)"),
                      {'disk_dev': disk_dev,
                       'tries': tries})

        if pcs_iscsi_use_multipath:
            #we use the multipath device instead of the single path device
            self._rescan_multipath()
            multipath_device = self._get_multipath_device_name(host_device)
            if multipath_device is not None:
                host_device = multipath_device

        return self._attach_blockdev(sdk_ve, host_device, disk_info['dev'])

    @utils.synchronized('connect_volume')
    def disconnect_volume(self, connection_info, sdk_ve,
                          disk_info, ignore_errors):
        """Detach the volume from instance_name."""
        iscsi_properties = connection_info['data']
        multipath_device = None
        host_device = ("/dev/disk/by-path/ip-%s-iscsi-%s-lun-%s" %
                       (iscsi_properties['target_portal'],
                        iscsi_properties['target_iqn'],
                        iscsi_properties.get('target_lun', 0)))

        if CONF.pcs_iscsi_use_multipath:
            multipath_device = self._get_multipath_device_name(host_device)

        self._detach_blockdev(sdk_ve, host_device,
                              disk_info['dev'], ignore_errors)

        if CONF.pcs_iscsi_use_multipath and multipath_device:
            return self._disconnect_volume_multipath_iscsi(iscsi_properties)

        # NOTE(vish): Only disconnect from the target if no luns from the
        #             target are in use.
        device_prefix = ("/dev/disk/by-path/ip-%s-iscsi-%s-lun-" %
                         (iscsi_properties['target_portal'],
                          iscsi_properties['target_iqn']))
        devices = self.driver.get_used_block_devices()
        devices = [dev for dev in devices if dev.startswith(device_prefix)]
        if not devices:
            self._disconnect_from_iscsi_portal(iscsi_properties)

    def _disconnect_volume_multipath_iscsi(self, iscsi_properties):
        self._rescan_iscsi()
        self._rescan_multipath()
        block_devices = self.driver.get_used_block_devices()
        devices = []
        for dev in block_devices:
            if "/mapper/" in dev:
                devices.append(dev)
            else:
                mpdev = self._get_multipath_device_name(dev)
                if mpdev:
                    devices.append(mpdev)

        if not devices:
            # disconnect if no other multipath devices
            self._disconnect_mpath(iscsi_properties)
            return

        other_iqns = [self._get_multipath_iqn(device)
                      for device in devices]

        if iscsi_properties['target_iqn'] not in other_iqns:
            # disconnect if no other multipath devices with same iqn
            self._disconnect_mpath(iscsi_properties)
            return

        # else do not disconnect iscsi portals,
        # as they are used for other luns
        return

    def _connect_to_iscsi_portal(self, iscsi_properties):
        # NOTE(vish): If we are on the same host as nova volume, the
        #             discovery makes the target so we don't need to
        #             run --op new. Therefore, we check to see if the
        #             target exists, and if we get 255 (Not Found), then
        #             we run --op new. This will also happen if another
        #             volume is using the same target.
        try:
            self._run_iscsiadm(iscsi_properties, ())
        except processutils.ProcessExecutionError as exc:
            # iscsiadm returns 21 for "No records found" after version 2.0-871
            if exc.exit_code in [21, 255]:
                self._run_iscsiadm(iscsi_properties, ('--op', 'new'))
            else:
                raise

        if iscsi_properties.get('auth_method'):
            self._iscsiadm_update(iscsi_properties,
                                  "node.session.auth.authmethod",
                                  iscsi_properties['auth_method'])
            self._iscsiadm_update(iscsi_properties,
                                  "node.session.auth.username",
                                  iscsi_properties['auth_username'])
            self._iscsiadm_update(iscsi_properties,
                                  "node.session.auth.password",
                                  iscsi_properties['auth_password'])

        #duplicate logins crash iscsiadm after load,
        #so we scan active sessions to see if the node is logged in.
        out = self._run_iscsiadm_bare(["-m", "session"],
                                      run_as_root=True,
                                      check_exit_code=[0, 1, 21])[0] or ""

        portals = [{'portal': p.split(" ")[2], 'iqn': p.split(" ")[3]}
                   for p in out.splitlines() if p.startswith("tcp:")]

        stripped_portal = iscsi_properties['target_portal'].split(",")[0]
        if len(portals) == 0 or len([s for s in portals
                                     if stripped_portal ==
                                     s['portal'].split(",")[0]
                                     and
                                     s['iqn'] ==
                                     iscsi_properties['target_iqn']]
                                    ) == 0:
            try:
                self._run_iscsiadm(iscsi_properties,
                                   ("--login",),
                                   check_exit_code=[0, 255])
            except processutils.ProcessExecutionError as err:
                #as this might be one of many paths,
                #only set successfull logins to startup automatically
                if err.exit_code in [15]:
                    self._iscsiadm_update(iscsi_properties,
                                          "node.startup",
                                          "automatic")
                    return

            self._iscsiadm_update(iscsi_properties,
                                  "node.startup",
                                  "automatic")

    def _disconnect_from_iscsi_portal(self, iscsi_properties):
        self._iscsiadm_update(iscsi_properties, "node.startup", "manual",
                              check_exit_code=[0, 21, 255])
        self._run_iscsiadm(iscsi_properties, ("--logout",),
                           check_exit_code=[0, 21, 255])
        self._run_iscsiadm(iscsi_properties, ('--op', 'delete'),
                           check_exit_code=[0, 21, 255])

    def _get_multipath_device_name(self, single_path_device):
        device = os.path.realpath(single_path_device)
        out = self._run_multipath(['-ll',
                                  device],
                                  check_exit_code=[0, 1])[0]
        mpath_line = [line for line in out.splitlines()
                      if "scsi_id" not in line]  # ignore udev errors
        if len(mpath_line) > 0 and len(mpath_line[0]) > 0:
            return "/dev/mapper/%s" % mpath_line[0].split(" ")[0]

        return None

    def _get_iscsi_devices(self):
        try:
            devices = list(os.walk('/dev/disk/by-path'))[0][-1]
        except IndexError:
            return []
        return [entry for entry in devices if entry.startswith("ip-")]

    def _disconnect_mpath(self, iscsi_properties):
        entries = self._get_iscsi_devices()
        ips = [ip.split("-")[1] for ip in entries
               if iscsi_properties['target_iqn'] in ip]
        for ip in ips:
            props = iscsi_properties.copy()
            props['target_portal'] = ip
            self._disconnect_from_iscsi_portal(props)

        self._rescan_multipath()

    def _get_multipath_iqn(self, multipath_device):
        entries = self._get_iscsi_devices()
        for entry in entries:
            entry_real_path = os.path.realpath("/dev/disk/by-path/%s" % entry)
            entry_multipath = self._get_multipath_device_name(entry_real_path)
            if entry_multipath == multipath_device:
                return entry.split("iscsi-")[1].split("-lun")[0]
        return None

    def _run_iscsiadm_bare(self, iscsi_command, **kwargs):
        check_exit_code = kwargs.pop('check_exit_code', 0)
        (out, err) = utils.execute('iscsiadm',
                                   *iscsi_command,
                                   run_as_root=True,
                                   check_exit_code=check_exit_code)
        LOG.debug("iscsiadm %s: stdout=%s stderr=%s" %
                  (iscsi_command, out, err))
        return (out, err)

    def _run_multipath(self, multipath_command, **kwargs):
        check_exit_code = kwargs.pop('check_exit_code', 0)
        (out, err) = utils.execute('multipath',
                                   *multipath_command,
                                   run_as_root=True,
                                   check_exit_code=check_exit_code)
        LOG.debug("multipath %s: stdout=%s stderr=%s" %
                  (multipath_command, out, err))
        return (out, err)

    def _rescan_iscsi(self):
        self._run_iscsiadm_bare(('-m', 'node', '--rescan'),
                                check_exit_code=[0, 1, 21, 255])
        self._run_iscsiadm_bare(('-m', 'session', '--rescan'),
                                check_exit_code=[0, 1, 21, 255])

    def _rescan_multipath(self):
        self._run_multipath('-r', check_exit_code=[0, 1, 21])


class PCSPStorageVolumeDriver(PCSBaseVolumeDriver):

    def _read_mounts(self):
        (out, err) = utils.execute('mount', run_as_root=True)
        lines = out.splitlines()
        mounts = {}
        for line in lines:
            tokens = line.split()
            if len(tokens) > 2:
                device = tokens[0]
                mnt_point = tokens[2]
                mounts[mnt_point] = device
        return mounts

    def _get_mount_point(self, data):
        return os.path.join(CONF.pstorage_mount_point_base,
                            data['cluster_name'])

    def _update_mds_list(self, data):
        mds_list_path = os.path.join('/etc/pstorage/clusters',
                            data['cluster_name'], 'bs.list')
        mds_list = utils.read_file_as_root(mds_list_path).splitlines()
        if set(mds_list) != set(data['mds_list']):
            LOG.info("Updating MDS list ...")
            fd, name = tempfile.mkstemp()
            f = os.fdopen(fd)
            f.write('\n'.join(data['mds_list']))
            f.close()
            utils.execute('cp', '-f', name, mds_list_path)
            os.unlink(name)

    def _mount_pstorage(self, data):
        self._update_mds_list(data)

        mp = self._get_mount_point(data)
        try:
            utils.execute('stat', mp, run_as_root=True)
        except Exception:
            utils.execute('mkdir', '-p', mp, run_as_root=True)
        utils.execute('pstorage-mount', '-c', data['cluster_name'],
                        mp, run_as_root=True)

    def _ensure_mounted(self, data):
        dev = "pstorage://%s" % data['cluster_name']
        mp = self._get_mount_point(data)
        mounts = self._read_mounts()
        if mp not in mounts:
            self._mount_pstorage(data)
            return
        if mounts[mp] != dev:
            raise Exception("%s already mounted to %s" % (mounts[mp], mp))

    def connect_volume(self, connection_info, sdk_ve, disk_info):
        data = connection_info['data']
        self._ensure_mounted(data)
        mp = self._get_mount_point(data)
        return self._attach_image(sdk_ve,
                os.path.join(mp, data['volume_name']))

    def disconnect_volume(self, connection_info, sdk_ve,
                          disk_info, ignore_errors):
        data = connection_info['data']
        mp = self._get_mount_point(data)
        vol_path = os.path.join(mp, data['volume_name'])
        self._detach_image(sdk_ve, vol_path, ignore_errors)
