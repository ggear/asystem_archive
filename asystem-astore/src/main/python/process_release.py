#!/usr/local/bin/python -u
"""
Provide a release gateway script
Usage: %s [options]
Options:
-h --help                                Show help
--connection_jar=<path_to_jar>           The connection jar
--transaction_id=<alpha-numeric-string>  The transaction ID
"""

import os

import sys

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/../manager/python')

import getopt
import inspect
import logging
import sys
import textwrap

from metadata import get
from metadata import METADATA_NAMESPACE


def assert_metadata(metadatas, job, key=None, custom=True, expected="0", compare=lambda (actual): str(actual) == "0"):
    if job not in metadatas:
        print("Not releasing: Required job [{}] metadata not found".format(job))
        return True
    if key is not None:
        actual = "NOT_DEFINED"
        metadata = metadatas[job]
        if custom:
            if 'customProperties' in metadata and metadata['customProperties'] is not None and \
                    METADATA_NAMESPACE in metadata['customProperties'] and metadata['customProperties'][METADATA_NAMESPACE] is not None \
                    and \
                    key in metadata['customProperties'][METADATA_NAMESPACE]:
                actual = metadata['customProperties'][METADATA_NAMESPACE][key]
        else:
            if 'properties' in metadata and metadata['properties'] is not None and key in metadata['properties']:
                actual = metadata['properties'][key]
        if not compare(actual):
            print("Not releasing: Required job [{}] metadata {}property [{}] was actual [{}] when expected [{}]"
                  .format(job, "custom " if custom else "", key, actual, expected))
            return True
    return False


def do_call(connection_jar, transaction_id):
    metadatas = dict([(metadata['originalName'] if 'originalName' in metadata and metadata['originalName'] is not None else '', metadata)
                      for metadata in get(connection_jar, transaction_id)])
    print("Found job metadata:")
    for name, metadata in metadatas.iteritems(): print("\t{}: {}".format(name, metadata['navigatorUrl']))
    failure = \
        assert_metadata(metadatas, 'asystem-astore-process-repair') or \
        assert_metadata(metadatas, 'asystem-astore-process-repair', 'Exit') or \
        assert_metadata(metadatas, 'asystem-astore-process-repair', 'STAGED_FILES_PURE', False, ">0", lambda (actual): actual > 0) or \
        assert_metadata(metadatas, 'asystem-astore-process-repair', 'STAGED_FILES_FAIL', False) or \
        assert_metadata(metadatas, 'asystem-astore-process-repair', 'PROCESSED_FILES_FAIL', False) or \
        assert_metadata(metadatas, 'asystem-astore-process-repair', 'PROCESSED_FILES_PURE', False, ">0", lambda (actual): actual > 0) or \
        assert_metadata(metadatas, 'asystem-astore-process-batch') or \
        assert_metadata(metadatas, 'asystem-astore-process-batch', 'Exit') or \
        assert_metadata(metadatas, 'asystem-astore-process-batch', 'STAGED_FILES_PURE', False, ">0", lambda (actual): actual > 0) or \
        assert_metadata(metadatas, 'asystem-astore-process-batch', 'STAGED_FILES_FAIL', False) or \
        assert_metadata(metadatas, 'asystem-astore-process-batch', 'STAGED_FILES_TEMP', False) or \
        assert_metadata(metadatas, 'asystem-astore-process-batch', 'STAGED_PARTITIONS_TEMP', False) or \
        assert_metadata(metadatas, 'asystem-astore-process-batch', 'PROCESSED_FILES_FAIL', False) or \
        assert_metadata(metadatas, 'asystem-astore-process-batch', 'PROCESSED_FILES_PURE', False, ">0", lambda (actual): actual > 0) or \
        assert_metadata(metadatas, 'asystem-astore-process-stats') or \
        assert_metadata(metadatas, 'asystem-astore-process-stats', 'Exit') or \
        assert_metadata(metadatas, 'asystem-astore-process-stats', 'STAGED_FILES_PURE', False, ">0", lambda (actual): actual > 0) or \
        assert_metadata(metadatas, 'asystem-astore-process-stats', 'STAGED_FILES_FAIL', False) or \
        assert_metadata(metadatas, 'asystem-astore-process-stats', 'STAGED_FILES_TEMP', False) or \
        assert_metadata(metadatas, 'asystem-astore-process-stats', 'STAGED_PARTITIONS_TEMP', False) or \
        assert_metadata(metadatas, 'asystem-astore-process-stats', 'STAGED_PARTITIONS_DONE', False) or \
        assert_metadata(metadatas, 'asystem-astore-process-stats', 'STAGED_PARTITIONS_REDO', False) or \
        assert_metadata(metadatas, 'asystem-astore-process-stats', 'PROCESSED_FILES_FAIL', False) or \
        assert_metadata(metadatas, 'asystem-astore-process-stats', 'PROCESSED_PARTITIONS_DONE', False) or \
        assert_metadata(metadatas, 'asystem-astore-process-stats', 'PROCESSED_PARTITIONS_REDO', False) or \
        assert_metadata(metadatas, 'asystem-astore-process-stats', 'PROCESSED_FILES_PURE', False, ">0", lambda (actual): actual > 0)
    return 1 if failure else 0


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
        opts, args = getopt.getopt(sys.argv[1:], 'h', ['help', 'connection_jar=', 'transaction_id='])
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
    if connection_jar is None or transaction_id is None:
        print >> sys.stderr, \
            'Required parameters [connection_jar, transaction_id] not passed on command line'
        usage()
        return -1
    return do_call(connection_jar, transaction_id)


if __name__ == '__main__':
    sys.exit(main())
