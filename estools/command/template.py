from __future__ import print_function, unicode_literals
import json
import logging
import sys

import argh
from argh import arg
import pyes

log = logging.getLogger(__name__)


class TemplateManager(object):

    def __init__(self, hosts):
        self.es = pyes.ES(hosts)

    def get(self, name):
        log.info('Fetching template %s', name)
        res = self.es._send_request('GET', '/_template/{0}'.format(name))
        if res == {}:
            log.warn("Template %s doesn't exist", name)
            return
        return res

    def create(self, name, data):
        log.info('Creating template %s from passed data', name)
        self.es._send_request('PUT', '/_template/{0}'.format(name), data)

    def delete(self, name):
        log.info('Deleting template %s', name)
        try:
            self.es._send_request('DELETE', '/_template/{0}'.format(name))
        except pyes.exceptions.TypeMissingException:
            log.warn("Template %s doesn't exist to delete", name)

    def compare(self, name, data):
        log.info('Comparing template %s to passed data', name)
        try:
            res = self.es._send_request('GET', '/_template/{0}'.format(name))
        except pyes.exceptions.NotFoundException:
            log.warn("Template %s doesn't exist for comparison", name)
            return False
        comparison = (res[name] == data)
        if comparison:
            log.info('Template %s and passed data are the same', name)
        else:
            log.info('Template %s and passed data are different', name)
        return comparison


@arg('name', help='Template name')
def get(args):
    """
    Get a template by name.
    """
    m = TemplateManager(args.hosts)
    t = m.get(args.name)
    if t:
        print(json.dumps(t, indent=2))
    else:
        sys.exit(1)


@arg('name', help='Template name')
def create(args):
    """
    Create a template. This command expects to be fed a JSON document on STDIN.
    """
    data = json.load(sys.stdin)
    m = TemplateManager(args.hosts)
    m.create(args.name, data)


@arg('name', help='Template name')
def delete(args):
    """
    Delete a template by name
    """
    m = TemplateManager(args.hosts)
    m.delete(args.name)

@arg('name', help='Template name')
def compare(args):
    """
    Compare the extant template with the given name to the passed JSON. The
    command will exit with a return code of 0 if the named template is configured
    as specified, and 1 otherwise.
    """
    data = json.load(sys.stdin)
    m = TemplateManager(args.hosts)
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
