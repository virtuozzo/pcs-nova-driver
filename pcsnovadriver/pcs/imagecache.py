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

from nova.openstack.common import log as logging

LOG = logging.getLogger(__name__)


class ImageCacheManager(object):
    def __init__(self, driver):
        self.driver = driver

    def update(self, context, all_instances):
        LOG.info("ImageCacheManager.update")
        used_images = map(lambda x: x['image_ref'], all_instances)
        used_images = set(used_images)

        cached_images = self.driver.image_cache.list_images()
        for image in cached_images:
            if image not in used_images:
                LOG.info("ImageCacheManager: removing image %s" % image)
                self.driver.image_cache.delete_image(image)
