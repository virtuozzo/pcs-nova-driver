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
import re
import shutil
import tempfile
from xml.dom import minidom

from oslo.config import cfg

from nova.image import glance
from nova.openstack.common import jsonutils
from nova.openstack.common import lockutils
from nova.openstack.common import log as logging
from nova.openstack.common import processutils
from nova import utils
from nova.virt import images

from pcsnovadriver.pcs import prlsdkapi_proxy
from pcsnovadriver.pcs import utils as pcsutils

pc = prlsdkapi_proxy.consts

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


def get_template(driver, context, instance, image_meta):
        if image_meta['disk_format'] == 'ez-template':
            return EzTemplate(driver, context, instance, image_meta)
        else:
            return DiskTemplate(driver, context, instance, image_meta)


class PCSTemplate(object):
    def __init__(self, driver, context, instance, image_meta):
        LOG.info("%s.__init__" % self.__class__.__name__)

    def create_instance(self, instance):
        raise NotImplementedError()


class EzTemplate(PCSTemplate):
    def __init__(self, driver, context, instance, image_meta):
        PCSTemplate.__init__(self, driver, context, instance, image_meta)
        self.driver = driver
        self.rpm_path = None
        self.instance = instance

        name, version, release = self._get_remote_info(context, image_meta)
        lname, lversion, lrelease = self._get_rpm_info(pkg=name)
        LOG.info("Glance template: %s-%s-%s, local rpm: %s-%s-%s" %
                (name, version, release, lname, lversion, lrelease))
        self.name = name[:-3]

        if not lname:
            self._download_rpm(context, image_meta)
            LOG.info("installing rpm for template %s" % name)
            utils.execute('rpm', '-i', self.rpm_path, run_as_root=True)
        else:
            x = self._cmp_version_release(version, release, lversion, lrelease)
            if x == 0:
                return
            elif x < 0:
                self._download_rpm(context, image_meta)
                LOG.info("updating rpm for template %s" % name)
                utils.execute('rpm', '-U', file, run_as_root=True)
            else:
                LOG.warn("local rpm is newer than remote one!")

    def _download_rpm(self, context, image_meta):
        LOG.info("_download_rpm")
        if self.rpm_path:
            return

        if image_meta['name']:
            name = image_meta['name']
        else:
            name = image_meta['id']

        if CONF.tempdir:
            tempdir = CONF.tempdir
        else:
            tempdir = tempfile.gettempdir()
        rpm_path = os.path.join(tempdir, name)
        images.fetch(context, self.instance['image_ref'], rpm_path,
                self.instance['user_id'], self.instance['project_id'])
        self.rpm_path = rpm_path

    def _get_remote_info(self, context, image_meta):
        LOG.info("_get_remote_info")
        for prop in 'pcs_name', 'pcs_version', 'pcs_release':
            if prop not in image_meta['properties']:
                self._download_rpm(context, image_meta)
                name, ver, rel = self._get_rpm_info(file=self.rpm_path)
                if not name:
                    raise Exception("Invalid rpm file: %s" % self.rpm_path)
        return (image_meta['properties']['pcs_name'],
                image_meta['properties']['pcs_version'],
                image_meta['properties']['pcs_release'])

    def _get_rpm_info(self, file=None, pkg=None):
        LOG.info("_get_rpm_info")
        cmd = ['rpm', '-q', '--qf', '%{NAME},%{VERSION},%{RELEASE}']
        if file:
            cmd += ['-p', file]
        else:
            cmd.append(pkg)

        try:
            out, err = utils.execute(*cmd)
        except processutils.ProcessExecutionError:
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
        sdk_ve = self.driver.psrv.get_default_vm_config(pc.PVT_CT,
                                            'vswap.1024MB', 0, 0).wait()[0]
        sdk_ve.set_uuid(self.instance['uuid'])
        sdk_ve.set_name(self.instance['name'])
        sdk_ve.set_vm_type(pc.PVT_CT)
        sdk_ve.set_os_template(self.name)
        sdk_ve.reg('', True).wait()
        return sdk_ve


class DiskTemplate(PCSTemplate):
    """This class is for templates, based on disk images,
    stored in glance.
    """
    def __init__(self, driver, context, instance, image_meta):
        PCSTemplate.__init__(self, driver, context, instance, image_meta)
        self.driver = driver
        self.context = context
        self.instance = instance
        self.image_meta = image_meta

    def _is_image_cached(self):
        "Returns True, if image with given id cached."

        raise NotImplementedError()

    def _cache_image(self, context):
        "Cache image from glance to local FS."

        raise NotImplementedError()

    def _put_image(self, dst):
        "Copy ploop image to the specified destination."

        raise NotImplementedError()

    def _create_ct(self):
        sdk_ve = self.driver.psrv.get_default_vm_config(pc.PVT_CT,
                                            'vswap.1024MB', 0, 0).wait()[0]
        sdk_ve.set_uuid(self.instance['uuid'])
        sdk_ve.set_name(self.instance['name'])
        sdk_ve.set_vm_type(pc.PVT_CT)
        sdk_ve.set_os_template(self.image_meta['properties']['pcs_ostemplate'])
        LOG.info("Creating container from eztemplate ...")
        sdk_ve.reg('', True).wait()

        disk_path = sdk_ve.get_home_path()
        disk_path = os.path.join(disk_path, 'root.hdd')
        LOG.info("Removing original disk ...")
        utils.execute('rm', '-rf', disk_path, run_as_root=True)

        image = LZRWCachedImage()
        image.put_image(self.context, self.instance['image_ref'],
                        self.image_meta, disk_path)

        LOG.info("Done")
        return sdk_ve

    def _create_vm(self):
        sdk_ve = self.driver._create_blank_vm(self.instance)

        # copy hard disk to VM directory
        ve_path = os.path.dirname(sdk_ve.get_home_path())
        disk_path = os.path.join(ve_path, "harddisk.hdd")

        image = LZRWCachedImage()
        image.put_image(self.context, self.instance['image_ref'],
                        self.image_meta, disk_path)

        # add hard disk to VM config and set is as boot device
        srv_cfg = self.driver.psrv.get_srv_config().wait().get_param()
        sdk_ve.begin_edit().wait()

        hdd = sdk_ve.add_default_device_ex(srv_cfg, pc.PDE_HARD_DISK)
        hdd.set_image_path(disk_path)

        b = sdk_ve.create_boot_dev()
        b.set_type(pc.PDE_HARD_DISK)
        b.set_index(hdd.get_index())
        b.set_sequence_index(0)
        b.set_in_use(1)

        sdk_ve.commit().wait()

        return sdk_ve

    def create_instance(self):
        props = self.image_meta['properties']
        if 'vm_mode' in props and props['vm_mode'] not in ['hvm', 'exe']:
            raise Exception("Unsupported VM mode '%s'" % props['vm_mode'])

        if not 'vm_mode' in props or props['vm_mode'] == 'hvm':
            return self._create_vm()
        elif props['vm_mode'] == 'exe':
            return self._create_ct()


class CachedImage(object):
    """Base class for image cache handlers. There is only one
    operation: put image to the specified destination. If image
    is not in cache - it should be downloaded.
    """
    def put_image(self, context, image_ref, image_meta, dst):
        raise NotImplementedError()


class LZRWCachedImage(CachedImage):
    """Class for retrieving from cache of LZRW images.

    There are 3 actions on cached image:
    1. cache
    2. unpack
    3. remove (will be done in ImageCacheManager later)

    So we need to synchronize these places. All these actions
    can be executed from separate threads.

    The idea is that since cache images are regular files, we
    can open file and unpack using that fd. So removing cached
    image while unpacking it will not fail.

    Several unpacks can work simultaneously, because they just
    read file contents.

    To forbid unpacking image while caching it it's cached to the
    temporary file and then first opened and then renamed. So
    if cached file exists - it can be unpacked. If doesn't exists
    it has to be cached.

    Several caching operation protected by lock. So if we got lock
    then ether file is not cached and nobody will try to cache it
    until we release lock or someone already cached file before us.
    In this case we just open a file and release lock.

    Several remove operations can be a problem. We need to check if
    manage_image_cache can be called from several threads
    simultaneously.
    """
    def _get_cached_file(self, image_id):
        return os.path.join(CONF.pcs_template_dir,
                            image_id + '.tar.lzrw')

    def _cache_image(self, context, image_ref, image_meta, dst):
        downloader = get_downloader(image_meta['disk_format'])
        LOG.info('Downloading image %s (%s) from glance' %
                 (image_meta['name'], image_ref))
        downloader.fetch_to_lzrw(context, image_ref, image_meta, dst)

    def _open(self, path):
        try:
            f = open(path)
            return f
        except IOError as e:
            if e.errno != os.errno.ENOENT:
                raise
            return None

    def _open_cached_file(self, context, image_ref, image_meta, dst):
        image_id = image_meta['id']
        fpath = self._get_cached_file(image_id)

        f = self._open(fpath)
        if f:
            return f

        with lockutils.lock('lock' + image_id, external=True,
                    lock_path=CONF.pcs_template_dir):
            f = self._open(fpath)
            if f:
                return f

            tmp = tempfile.mktemp(dir=CONF.pcs_template_dir)
            self._cache_image(context, image_ref, image_meta, tmp)
            f = open(tmp)
            os.rename(tmp, fpath)
            return f

    def put_image(self, context, image_ref, image_meta, dst):
        utils.execute('mkdir', dst, run_as_root=True)

        f = self._open_cached_file(context, image_ref, image_meta, dst)
        try:
            LOG.info("Unpacking image %s to %s" %
                    (self._get_cached_file(image_meta['id']), dst))
            pcsutils.uncompress_ploop(None, dst, src_file=f,
                                  root_helper=utils.get_root_helper())
        finally:
            f.close()


class ImageDownloader(object):
    """Subclasses of this class download images from glance
    to local image cache with all needed conversions.
    """

    def fetch_to_lzrw(self, context, image_ref, image_meta, dst):
        raise NotImplementedError()


class PloopDownloader(ImageDownloader):
    "Dowload images in ploop format."

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

    def _download_ploop(self, context, image_ref,
                        image_meta, image_service, dst):
        dd = image_meta['properties']['pcs_disk_descriptor']
        image_name = self._get_image_name(dd)
        with open(os.path.join(dst, image_name), 'w') as f:
            image_service.download(context, image_ref, f)
        with open(os.path.join(dst, 'DiskDescriptor.xml'), 'w') as f:
            f.write(image_meta['properties']['pcs_disk_descriptor'])

    def fetch_to_lzrw(self, context, image_ref, image_meta, dst):
        tmpl_dir = os.path.join(CONF.pcs_template_dir, image_meta['id'])

        if os.path.exists(tmpl_dir):
            shutil.rmtree(tmpl_dir)
        os.mkdir(tmpl_dir)

        image_service = glance.get_remote_image_service(context, image_ref)[0]
        self._download_ploop(context, image_ref, image_meta,
                             image_service, tmpl_dir)
        LOG.info("Packing image to %s" % dst)
        pcsutils.compress_ploop(tmpl_dir, dst)
        shutil.rmtree(tmpl_dir)


class QemuDownloader(PloopDownloader):
    """This class downloads images in formats, which
    qemu-img supports.
    """
    def _download_ploop(self, context, image_ref,
                        image_meta, image_service, dst):
        glance_img = 'glance.img'
        glance_path = os.path.join(dst, glance_img)
        with open(glance_path, 'w') as f:
            image_service.download(context, image_ref, f)

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


class LZRWDownloader(ImageDownloader):
    "Class for images stored in cploop format."

    def fetch_to_lzrw(self, context, image_ref, image_meta, dst):
        image_service = glance.get_remote_image_service(context, image_ref)[0]
        with open(dst, 'w') as f:
            image_service.download(context, image_ref, f)


def get_downloader(disk_format):
    if disk_format == 'ploop':
        return PloopDownloader()
    elif disk_format == 'cploop':
        return LZRWDownloader()
    else:
        return QemuDownloader()
