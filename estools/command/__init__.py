import logging
from os import environ as env

__all__ = []

_log_level_default = logging.INFO
_log_level = getattr(logging, env.get('ESTOOLS_LOGLEVEL', '').upper(), _log_level_default)

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=_log_level)
