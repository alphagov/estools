import unittest
from datetime import timedelta
from freezegun import freeze_time
from mock import Mock, call

from estools.command.rotate import Rotator

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
        [ call('logs-2013.08.26', { "index": { "store.compress.stored": True } }) ]
    )



