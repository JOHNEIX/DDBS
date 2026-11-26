"""Configuration, validation and shared domain errors."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


class Error(Exception):
    def __init__(self, code, message=''):
        self.code, self.message = code, message or code
        super().__init__(self.message)


def require(condition, code='INVALID_ARGUMENT', message=''):
    if not condition:
        raise Error(code, message)


def positive(value):
    require(type(value) is int and 0 < value <= 2**63 - 1, message='ID must be a positive SQLite integer')
    return value


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')


def load_config(path):
    path = Path(path).resolve()
    cfg = json.loads(path.read_text(encoding='utf-8'))
    require(cfg['node_count'] == 3 and len(cfg['nodes']) == 3)
    require([n['id'] for n in cfg['nodes']] == [0, 1, 2])
    cfg['root'] = path.parent
    cfg['path'] = path
    cfg['data'] = path.parent / cfg.get('data_dir', 'data')
    return cfg


def db_path(cfg, node):
    return cfg['data'] / f'node{node}' / 'node.db'


def response(rpc_id, data=None, error=None):
    return dict(version=1, rpc_id=rpc_id, ok=error is None,
                code=error.code if error else 'OK',
                message=error.message if error else 'success', data=data)
