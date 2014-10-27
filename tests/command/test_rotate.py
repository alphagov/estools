import unittest
from datetime import timedelta
from freezegun import freeze_time
from mock import Mock, call

from estools.command.rotate import Rotator

COMPRESS_MAP = { "index": { "store.compress.stored": True } }
OPTIMIZE_ARGS_SET = { "max_num_segments": 1 }

def to_map(m, i):
    m[i] = {}
    return m

def mock_es(indices=[]):
    index_map = reduce(to_map, indices, {})

    es = Mock()
    es.indices.get_indices.return_value = index_map

    return es


class TestRotateCommand(unittest.TestCase):

    @freeze_time("2013-08-28")
    def test_rotate(self):
        es = mock_es()

        rotator = Rotator(es)

        rotator.rotate('logs')

        print es.optimize.call_count
        expected_index_name = 'logs-2013.08.28'
        self.assertEqual(
            es.indices.create_index_if_missing.call_args_list,
            [ call(expected_index_name) ]
        )
        self.assertEqual(
            es.indices.set_alias.call_args_list,
            [ call('logs-current', expected_index_name) ]
        )

    @freeze_time("2013-08-28")
    def test_no_opts_run_by_default(self):
        es = mock_es(['logs-2013.08.28', 'logs-2013.07.28', 'logs-2013.07.27'])

        rotator = Rotator(es)

        rotator.rotate('logs')

        # ensure that no options are run when they are not flagged
        self.assertEqual(es.indices.delete_index.call_count, 0)
        self.assertEqual(es.indices.update_settings.call_count, 0)
        self.assertEqual(es.optimize.call_count, 0)

    @freeze_time("2013-08-28")
    def test_old_indices(self):
        es = mock_es(['logs-current', 'logs-2013.08.27', 'logs-2013.07.01'])

        rotator = Rotator(es)

        fn_mock = Mock()
        rotator.for_old_indices('logs', timedelta(days=31), fn_mock)

        args_expected = [ call('logs-2013.07.01') ]
        self.assertEqual(fn_mock.call_count, 1)
        self.assertEqual(fn_mock.call_args_list, args_expected)

    @freeze_time("2013-08-28")
    def test_rotate_delete(self):
        es = mock_es(['logs-2013.08.28', 'logs-2013.07.28', 'logs-2013.07.27'])

        rotator = Rotator(es, delete_old = True)

        rotator.rotate('logs')

        self.assertEqual(
            es.indices.delete_index.call_args_list,
            [ call('logs-2013.07.27') ]
        )

    @freeze_time("2013-08-28")
    def test_rotate_compress(self):
        es = mock_es(['logs-2013.08.28', 'logs-2013.08.26'])

        rotator = Rotator(es, compress_old = True)

        rotator.rotate('logs')

        self.assertEqual(
            es.indices.update_settings.call_args_list,
            [ call('logs-2013.08.26', COMPRESS_MAP) ]
        )

    @freeze_time("2013-08-28")
    def test_rotate_optimize(self):
        es = mock_es(['logs-2013.08.28', 'logs-2013.08.26'])

        rotator = Rotator(es, optimize_old = True)

        rotator.rotate('logs')

        self.assertEqual(
            es.optimize.call_args_list,
            [ call(['logs-2013.08.26'], **OPTIMIZE_ARGS_SET) ]
        )

    @freeze_time("2013-08-28")
    def test_all_opts(self):
        es = mock_es(['logs-2013.08.28', 'logs-2013.07.28', 'logs-2013.07.27'])

        rotator = Rotator(es, delete_old=True, compress_old=True, optimize_old=True)

        rotator.rotate('logs')

        self.assertEqual(
            es.indices.delete_index.call_args_list,
            [ call('logs-2013.07.27') ]
        )
        self.assertEqual(
            es.indices.update_settings.call_args_list,
            [ call('logs-2013.07.27', COMPRESS_MAP), call('logs-2013.07.28', COMPRESS_MAP) ]
        )
        self.assertEqual(
            es.optimize.call_args_list, 
            [ 
              call(['logs-2013.07.27'], **OPTIMIZE_ARGS_SET), 
              call(['logs-2013.07.28'], **OPTIMIZE_ARGS_SET) 
            ]
        )


