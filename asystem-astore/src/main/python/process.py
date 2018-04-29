#!/usr/local/bin/python -u
'''
Provide a quality gatweay on the Process workload
'''

import os

import sys

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/../manager/python')

import getopt
import inspect
import logging
import sys
import textwrap

from metadata import getMetaData
from metadata import METADATA_NAMESPACE


def do_call(connection_jar, transaction_id):
    metadatas = dict([(metadata['originalName'] if 'originalName' in metadata and metadata['originalName'] is not None else '', metadata)
                      for metadata in getMetaData(connection_jar, transaction_id)])
    print("Found job metadata:")
    for name, metadata in metadatas.iteritems(): print("\t{}: {}".format(name, metadata['navigatorUrl']))

    # TODO: Implement more sophisticated quality checks

    return sum(int(metadata['customProperties'][METADATA_NAMESPACE]['Exit'])
               if 'customProperties' in metadata and metadata['customProperties'] is not None and
                  METADATA_NAMESPACE in metadata['customProperties'] and metadata['customProperties'][METADATA_NAMESPACE] is not None and
                  'Exit' in metadata['customProperties'][METADATA_NAMESPACE] else 0 for name, metadata in metadatas.iteritems())


def usage():
    doc = inspect.getmodule(usage).__doc__
    print >> sys.stderr, textwrap.dedent(doc % (sys.argv[0],))


def setup_logging(level):
    logging.basicConfig()
    logging.getLogger().setLevel(level)
    logging.getLogger("requests").setLevel(logging.WARNING)


def main(argv):
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
        elif option in ('--connection_jar'):
            connection_jar = value
        elif option in ('--transaction_id'):
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
    sys.exit(main(sys.argv))
