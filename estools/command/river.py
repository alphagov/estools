from __future__ import print_function, unicode_literals
import json
import logging
import sys

import argh
from argh import arg
import pyes

log = logging.getLogger(__name__)


class RiverManager(object):

    def __init__(self, hosts):
        self.es = pyes.ES(hosts)

    def get(self, name):
        log.info('Fetching river %s', name)
        try:
            res = self.es._send_request('GET', '/_river/{0}/_meta'.format(name))
        except pyes.exceptions.NotFoundException:
            log.warn("River %s doesn't exist", name)
            return None
        if not res.exists:
            log.warn("River %s doesn't exist", name)
            return None
        return res._source

    def create(self, name, data):
        log.info('Creating river %s from passed data', name)
        self.es.create_river(data, river_name=name)

    def delete(self, name):
        log.info('Deleting river %s', name)
        try:
            self.es.delete_river({}, river_name=name)
        except pyes.exceptions.TypeMissingException:
            log.warn("River %s doesn't exist to delete", name)

    def compare(self, name, data):
        log.info('Comparing river %s to passed data', name)
        try:
            res = self.es._send_request('GET', '/_river/{0}/_meta'.format(name))
        except pyes.exceptions.NotFoundException:
            log.warn("River %s doesn't exist for comparison", name)
            return False
        comparison = (res._source == data)
        if comparison:
            log.info('River %s and passed data are the same', name)
        else:
            log.info('River %s and passed data are different', name)
        return comparison


@arg('name', help='River name')
def get(args):
    """
    Get a river by name.
    """
    m = RiverManager(args.hosts)
    r = m.get(args.name)
    if r:
        print(json.dumps(r, indent=2))
    else:
        sys.exit(1)


@arg('name', help='River name')
def create(args):
    """
    Create a river. This command expects to be fed a JSON document on STDIN.
    """
    data = json.load(sys.stdin)
    m = RiverManager(args.hosts)
    m.create(args.name, data)


@arg('name', help='River name')
def delete(args):
    """
    Delete a river by name
    """
    m = RiverManager(args.hosts)
    m.delete(args.name)


@arg('name', help='River name')
def compare(args):
    """
    Compare the extant river with the given name to the passed JSON. The
    command will exit with a return code of 0 if the named river is configured
    as specified, and 1 otherwise.
    """
    data = json.load(sys.stdin)
    m = RiverManager(args.hosts)
    if m.compare(args.name, data):
        sys.exit(0)
    else:
        sys.exit(1)


parser = argh.ArghParser()
parser.add_argument('-H', '--hosts',
                    nargs='+',
                    default=['localhost:9200'],
                    help='elasticsearch hosts to connect to')
parser.add_commands([get, create, delete, compare])


def main():
    parser.dispatch()


if __name__ == '__main__':
    main()
