"""
Repository utility library
"""

import os.path
import os.path
import re
import shutil
import tempfile

from boto.s3.connection import S3Connection
from boto.s3.key import Key


def publish(local_file, publish_url):
    """
    Publish a file stored locally to an s3a:// or file:// URL
    """
    is_snapshot = re.search(
        '.*/amodel_version=[1-9][0-9]\.[0-9]{3}.[0-9]{4}-SNAPSHOT/.*',
        publish_url) is not None
    if publish_url.startswith('s3a://'):
        publish_re = re.search('s3a://([0-9a-z\-]*)/(.*)', publish_url)
        s3_bucket_name = publish_re.group(1)
        s3_key_name = publish_re.group(2)
        s3_connection = S3Connection()
        s3_bucket = s3_connection.get_bucket(s3_bucket_name)
        if is_snapshot or \
                len(list(s3_bucket.list(prefix=s3_key_name))) == 0:
            s3_key = Key(s3_bucket, s3_key_name)
            s3_key.set_contents_from_filename(local_file)
    elif publish_url.startswith('file://'):
        publish_file = publish_url.replace('file://', '')
        if is_snapshot or not os.path.isfile(publish_file):
            if os.path.exists(os.path.dirname(publish_file)):
                shutil.rmtree(os.path.dirname(publish_file))
            os.makedirs(os.path.dirname(publish_file))
            shutil.copyfile(local_file, publish_file)


def nearest(publish_url, publish_file="_SUCCESS", single_level=False):
    """
    Determine the nearest URL by version numbers encoded within it,
    falling back to previous versions until a match is found
    """
    if publish_url.startswith('s3a://'):
        publish_re = re.search('s3a://([0-9a-z\-]*)/(.*)', publish_url)
        s3_bucket_name = publish_re.group(1)
        s3_key_name = publish_re.group(2) + "/" + publish_file
        s3_connection = S3Connection()
        s3_bucket = s3_connection.get_bucket(s3_bucket_name)
        s3_list = list(s3_bucket.list(prefix=s3_key_name))
        if len(s3_list) > 0 and s3_list[0].key == s3_key_name:
            return publish_url
    else:
        return publish_url
    publish_matcher = re.search(
        '.*/amodel_version=([1-9][0-9]\.[0-9]{3}.[0-9]{4})(.*)/.*',
        publish_url)
    if publish_matcher is None: raise Exception('Eroneous URL [{}]'
                                                .format(publish_url))
    publish_version_base = str(int(re.sub('[^0-9]', '',
                                          publish_matcher.group(1))) - 1)
    if publish_version_base < 100000000:
        raise Exception("Cannot find base version for URL [{}]"
                        .format(publish_url))
    publish_url_next = publish_url.replace(
        publish_matcher.group(1) + publish_matcher.group(2),
        "{}.{}.{}".format(publish_version_base[:2],
                          publish_version_base[2:5],
                          publish_version_base[5:]))
    return None if single_level else nearest(publish_url_next, publish_file)


def get(publish_url):
    """
    Get the resource at an s3a:// or file:// URL endpoint as a local file path
    """
    local_dir = tempfile.mkdtemp(prefix='repo_util-')
    local_file = os.path.join(local_dir,
                              publish_url.split('/')[-1])
    if publish_url.startswith('s3a://'):
        publish_re = re.search('s3a://([0-9a-z\-]*)/(.*)', publish_url)
        s3_bucket_name = publish_re.group(1)
        s3_key_name = publish_re.group(2)
        s3_connection = S3Connection()
        s3_bucket = s3_connection.get_bucket(s3_bucket_name)
        s3_list = list(s3_bucket.list(prefix=s3_key_name))
        if len(s3_list) > 0 and s3_list[0].key == s3_key_name:
            s3_key = Key(s3_bucket, s3_key_name)
            s3_key.get_contents_to_filename(local_file)
            return local_file
    else:
        if os.path.isfile(publish_url):
            shutil.copyfile(publish_url, local_file)
            return local_file
    raise Exception("Could not get URL [{}]".format(publish_url))


def paths(prefix_glob, partitions_globs, suffix_glob):
    """
    List the paths in a form that is most efficient for the consumption
    by Spark (eg as globs for HDFS, specific files for S3)
    """
    paths = []
    if prefix_glob.startswith('s3a://'):
        publish_re = re.search('s3a://([0-9a-z\-]*)/\[0-9\](/.*)', prefix_glob)
        s3_bucket_name = publish_re.group(1)
        s3_key_prefix = publish_re.group(2) + "/"
        s3_key_partitions = []
        for partitions_glob in partitions_globs:
            s3_key_partitions.extend(['/{0}/'.format(i) for i in
                                      filter(lambda j: '*' not in j,
                                             filter(None, partitions_glob.split("/")))])
        s3_key_suffix = re.search('/\*(.*)', suffix_glob).group(1)
        s3_connection = S3Connection()
        s3_bucket = s3_connection.get_bucket(s3_bucket_name)
        for i in range(10):
            for path in list(s3_bucket.list(prefix=str(i) + s3_key_prefix)):
                if path.key.endswith(s3_key_suffix) and \
                        any(partition in path.key for partition in s3_key_partitions):
                    paths.append(os.path.join("s3a://" + s3_bucket_name, path.key))
    else:
        for partitions_glob in partitions_globs:
            paths.append(prefix_glob + partitions_glob + suffix_glob)
    return paths
