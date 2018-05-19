###############################################################################
#
# Model publish library
#
###############################################################################

import os.path
import re
import shutil

from boto.s3.connection import S3Connection
from boto.s3.key import Key


def publish(local_file, publish_url):
    is_snapshot = re.search('.*/[1-9][0-9]\.[0-9]{3}.[0-9]{4}-SNAPSHOT/.*', publish_url) is not None
    if publish_url.startswith('s3a://'):
        s3_bucket_name = re.search('s3a://([0-9a-z\-]*).*', publish_url).group(1)
        s3_key_name = re.search('s3a://[0-9a-z\-]*/(.*)', publish_url).group(1)
        s3_connection = S3Connection()
        s3_bucket = s3_connection.get_bucket(s3_bucket_name)
        if is_snapshot or len(list(s3_bucket.list(prefix=s3_key_name))) == 0:
            s3_key = Key(s3_bucket, s3_key_name)
            s3_key.set_contents_from_filename(local_file)
    elif publish_url.startswith('file://'):
        publish_file = publish_url.replace('file://', '')
        if is_snapshot or not os.path.isfile(publish_file):
            if os.path.exists(os.path.dirname(publish_file)):
                shutil.rmtree(os.path.dirname(publish_file))
            os.makedirs(os.path.dirname(publish_file))
            shutil.copyfile(local_file, publish_file)


def nearest(publish_url):
    if publish_url.startswith('s3a://'):
        s3_bucket_name = re.search('s3a://([0-9a-z\-]*).*', publish_url).group(1)
        s3_key_name = re.search('s3a://[0-9a-z\-]*/(.*)', publish_url).group(1) + "/_SUCCESS"
        s3_connection = S3Connection()
        s3_bucket = s3_connection.get_bucket(s3_bucket_name)
        s3_list = list(s3_bucket.list(prefix=s3_key_name))
        if len(s3_list) > 0 and s3_list[0].key == s3_key_name:
            return publish_url
    else:
        return publish_url
    publish_matcher = \
        re.search('.*/amodel_version=([1-9][0-9]\.[0-9]{3}.[0-9]{4})(.*)/.*', publish_url)
    if publish_matcher is None: raise Exception('Eroneous URL [{}]'.format(publish_url))
    publish_version_base = str(int(re.sub('[^0-9]', '', publish_matcher.group(1))) - 1)
    if publish_version_base < 100000000: raise Exception('Cannot find base version for URL [{}]'.format(publish_url))
    return nearest(publish_url.replace(
        publish_matcher.group(1) + publish_matcher.group(2),
        '{}.{}.{}'.format(publish_version_base[:2], publish_version_base[2:5], publish_version_base[5:])))
