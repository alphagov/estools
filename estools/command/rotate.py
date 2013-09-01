from __future__ import print_function, unicode_literals
from datetime import datetime, timedelta
import logging

from argh import arg, dispatch_command
import pyes

log = logging.getLogger(__name__)


DEFAULT_DATE_FORMAT = '%Y.%m.%d'
DEFAULT_SEPARATOR = '-'
DEFAULT_CURRENT_ALIAS_NAME = 'current'
DEFAULT_DELETE_MAXAGE = timedelta(days=31)
DEFAULT_COMPRESS_MAXAGE = timedelta(days=1)
DEFAULT_OPTIMIZE_MAXAGE = timedelta(days=1)


class Rotator(object):

    def __init__(self,
                 es,
                 date_format=None,
                 separator=None,
                 current_alias_name=None,
                 delete_old=False,
                 delete_maxage=None,
                 compress_old=False,
                 compress_maxage=None,
                 optimize_old=False,
                 optimize_maxage=None):

        self.es = es

        self.date_format = date_format or DEFAULT_DATE_FORMAT
        self.separator = separator or DEFAULT_SEPARATOR
        self.current_alias_name = current_alias_name or DEFAULT_CURRENT_ALIAS_NAME
        self.delete_old = delete_old
        self.delete_maxage = delete_maxage or DEFAULT_DELETE_MAXAGE
        self.compress_old = compress_old
        self.compress_maxage = compress_maxage or DEFAULT_COMPRESS_MAXAGE
        self.optimize_old = optimize_old
        self.optimize_maxage = optimize_maxage or DEFAULT_OPTIMIZE_MAXAGE

    def rotate(self, prefix):
        now = datetime.utcnow()
        now_str = now.strftime(self.date_format)

        cur_alias = ''.join([prefix, self.separator, self.current_alias_name])
        now_index = ''.join([prefix, self.separator, now_str])

        log.info('Creating index %s if needed', now_index)
        self.es.indices.create_index_if_missing(now_index)

        log.info('Setting alias %s to point at index %s', cur_alias, now_index)
        self.es.indices.set_alias(cur_alias, now_index)

        if self.optimize_old:
          log.info("Optimizing old indices")
          self.for_old_indices(prefix, timedelta(days=1), self.optimize_index)

        if self.compress_old:
            log.info("Compressing old indices")
            self.for_old_indices(prefix, self.compress_maxage, self.compress_index)

        if self.delete_old:
            log.info("Deleting old indices")
            self.for_old_indices(prefix, self.delete_maxage, self.delete_index)

    def optimize_index(self, name):
        log.info("Index %s is being optimized", name)
        self.es.optimize([name], max_num_segments=1 )

    def compress_index(self, name):
        log.info("Index %s is being compressed", name)
        self.es.indices.update_settings(name, {
           "index": { "store.compress.stored": True }
        })

    def delete_index(self, name):
        log.info("Index %s is being deleted", name)
        self.es.indices.delete_index(name)

    def for_old_indices(self, prefix, maxage, fn):
        now = datetime.utcnow()
        cutoff = now - maxage

        index_names = self.es.indices.get_indices().keys()

        for name in sorted(index_names):

            # Don't pay any attention to indexes with different prefixes
            if not name.startswith(prefix + self.separator):
                log.debug("Index %s doesn't match prefix '%s': skipping", name, prefix)
                continue

            log.debug('Considering index %s for deletion', name)
            index_date_str = name[len(prefix + self.separator):]
            try:
                index_date = datetime.strptime(index_date_str, self.date_format)
            except ValueError:
                log.warn("Couldn't parse date in index name %s: skipping", name)
                continue

            if index_date < cutoff:
                log.info("Index %s older than cutoff %s", name, cutoff.strftime(self.date_format))
                fn(name)
            else:
                log.debug('Index %s younger than cutoff %s', name, cutoff.strftime(self.date_format))




def _timedelta(days):
    if days is not None:
        return timedelta(days=int(days))


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
@arg('--delete-old',
     default=False,
     help='delete old indices')
@arg('--delete-maxage',
     metavar='DAYS',
     type=_timedelta,
     help='maximum age of an index in days before it will be deleted by --delete-old')
@arg('--compress-old',
     default=False,
     help='compress old indices')
@arg('--compress-maxage',
     metavar='DAYS',
     type=_timedelta,
     help='maximum age of an index in days before it will be compressed by --compress-old')
@arg('--optimize-old',
     default=False,
     help='optimize old indices')
@arg('--optimize-maxage',
     metavar='DAYS',
     type=_timedelta,
     help='maximum age of an index in days before it will be optimizeed by --optimize-old')
def rotate(prefixes, hosts, **kwargs):
    """
    Rotates a set of daily indices by updating a "-current" alias to point to
    a (newly-created) index for today. This should probably be called from a
    daily cronjob just after midnight.
    """

    rotator = Rotator(pyes.ES(hosts), **kwargs)

    for prefix in prefixes:
        rotator.rotate(prefix)


def main():
    dispatch_command(rotate)


if __name__ == '__main__':
    main()
