from __future__ import print_function, unicode_literals
from datetime import datetime
import logging

from argh import arg, dispatch_command
import pyes

log = logging.getLogger(__name__)


DEFAULT_DATE_FORMAT = '%Y.%m.%d'
DEFAULT_SEPARATOR = '-'
DEFAULT_CURRENT_ALIAS_NAME = 'current'


class Rotator(object):

    def __init__(self, hosts, date_format=None, separator=None, current_alias_name=None):
        self.es = pyes.ES(hosts)
        self.date_format = date_format or DEFAULT_DATE_FORMAT
        self.separator = separator or DEFAULT_SEPARATOR
        self.current_alias_name = current_alias_name or DEFAULT_CURRENT_ALIAS_NAME

    def rotate(self, prefix):
        now = datetime.utcnow()
        now_str = now.strftime(self.date_format)

        cur_alias = ''.join([prefix, self.separator, self.current_alias_name])
        now_index = ''.join([prefix, self.separator, now_str])

        log.info('Creating index %s if needed', now_index)
        self.es.indices.create_index_if_missing(now_index)

        log.info('Setting alias %s to point at index %s', cur_alias, now_index)
        self.es.indices.set_alias(cur_alias, now_index)


@arg('prefixes',
     nargs='+',
     help='prefixes of indices to rotate')
@arg('-H', '--hosts',
     nargs='+',
     default=['localhost:9200'],
     help='elasticsearch hosts to connect to')
@arg('-f', '--date-format',
     metavar='%Y.%m.%d',
     help='postfixed date format for daily indices')
@arg('-s', '--separator',
     metavar='SEP',
     help='separator between index prefix and date postfix')
@arg('-c', '--current-alias-name',
     metavar='NAME',
     help='postfix for current index alias')
def rotate(prefixes, hosts, **kwargs):
    """
    Rotates a set of daily indices by updating a "-current" alias to point to
    a (newly-created) index for today. This should probably be called from a
    daily cronjob just after midnight.
    """

    rotator = Rotator(hosts, **kwargs)

    for prefix in prefixes:
        rotator.rotate(prefix)


def main():
    dispatch_command(rotate)


if __name__ == '__main__':
    main()
