#!/usr/local/bin/python -u
"""
Provide a release gateway script
Usage: %s [options]
Options:
-h --help                                Show help
--connection_jar=<path_to_jar>           The connection jar
--transaction_id=<alpha-numeric-string>  The transaction ID
"""

import getopt
import inspect
import logging
import os
import textwrap

import sys

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/../manager/python')

import metadata
from metadata import METADATA_NAMESPACE


def assert_metadata(metadatas, job, key=None, custom=True, compare=lambda x: x == "0"):
    asserted = True
    if job not in metadatas:
        print("Required job [{}] metadata not found".format(job))
        asserted = False
    if key is not None:
        actual = "NOT_DEFINED"
        if asserted:
            metadata = metadatas[job]
            if custom:
                if 'customProperties' in metadata and metadata['customProperties'] is not None and \
                        METADATA_NAMESPACE in metadata['customProperties'] and \
                        metadata['customProperties'][METADATA_NAMESPACE] is not None \
                        and key in metadata['customProperties'][METADATA_NAMESPACE]:
                    actual = metadata['customProperties'][METADATA_NAMESPACE][key]
            else:
                if 'properties' in metadata and metadata['properties'] is not None \
                        and key in metadata['properties']:
                    actual = metadata['properties'][key]
        if actual == "NOT_DEFINED" or not compare(str(actual)):
            print("Required job [{}] metadata {}property [{}] was [{}] when expected [{}]"
                  .format(job, "custom " if custom else "", key, actual,
                          inspect.getsource(compare).split('\n', 1)[0]
                          .split(": ", 1)[-1].rsplit(")", 1)[0]
                          .split(" ", 1)[-1].replace("\"", "")))
            asserted = False
    return asserted


def do_call(connection_jar, transaction_id):
    metadata_bodies = dict([(metadata_body['originalName']
                             if 'originalName' in metadata_body and
                                metadata_body['originalName'] is not None else '', metadata_body)
                            for metadata_body in metadata.get(connection_jar, transaction_id)])
    print("Found job metadata:")
    for name, metadata_body in metadata_bodies.iteritems():
        print("\t{}: {}".format(name, metadata_body['navigatorUrl']))
    success = \
        assert_metadata(metadata_bodies, 'asystem-astore-process-repair') and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-repair', 'System_Exit') and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-repair', 'STAGED_FILES_PURE', False, lambda x: x > "0") and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-repair', 'STAGED_FILES_FAIL', False) and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-repair', 'PROCESSED_FILES_FAIL', False) and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-repair', 'PROCESSED_FILES_PURE', False, lambda x: x >= "0") and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-batch') and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-batch', 'System_Exit') and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-batch', 'STAGED_FILES_PURE', False, lambda x: x > "0") and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-batch', 'STAGED_FILES_FAIL', False) and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-batch', 'STAGED_FILES_TEMP', False) and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-batch', 'STAGED_PARTITIONS_TEMP', False) and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-batch', 'PROCESSED_FILES_FAIL', False) and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-batch', 'PROCESSED_FILES_PURE', False, lambda x: x >= "0") and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-stats') and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-stats', 'System_Exit') and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-stats', 'STAGED_FILES_PURE', False, lambda x: x > "0") and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-stats', 'STAGED_FILES_FAIL', False) and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-stats', 'STAGED_FILES_TEMP', False) and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-stats', 'STAGED_PARTITIONS_TEMP', False) and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-stats', 'STAGED_PARTITIONS_DONE', False, lambda x: x >= "0") and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-stats', 'STAGED_PARTITIONS_REDO', False) and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-stats', 'PROCESSED_FILES_FAIL', False) and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-stats', 'PROCESSED_PARTITIONS_DONE', False, lambda x: x >= "0") and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-stats', 'PROCESSED_PARTITIONS_REDO', False) and \
        assert_metadata(metadata_bodies, 'asystem-astore-process-stats', 'PROCESSED_FILES_PURE', False, lambda x: x > "0")
    return 0 if success else 1


def usage():
    doc = inspect.getmodule(usage).__doc__
    print >> sys.stderr, textwrap.dedent(doc % (sys.argv[0],))


def setup_logging(level):
    logging.basicConfig()
    logging.getLogger().setLevel(level)
    logging.getLogger("requests").setLevel(logging.WARNING)


def main():
    setup_logging(logging.INFO)
    connection_jar = None
    transaction_id = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h', ['help', 'connection_jar='
            , 'transaction_id='])
    except getopt.GetoptError, err:
        print >> sys.stderr, err
        usage()
        return -1
    for option, value in opts:
        if option in ('-h', '--help'):
            usage()
            return -1
        elif option in '--connection_jar':
            connection_jar = value
        elif option in '--transaction_id':
            transaction_id = value
        else:
            print >> sys.stderr, 'Unknown option or flag: ' + option
            usage()
            return -1
    if not all([connection_jar, transaction_id]):
        print >> sys.stderr, \
            'Required parameters [connection_jar, transaction_id] not passed on command line'
        usage()
        return -1
    return do_call(connection_jar, transaction_id)


if __name__ == '__main__':
    sys.exit(main())
