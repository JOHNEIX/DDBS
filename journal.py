"""Exclusive coordinator ownership and durable, validated JSONL decisions."""
import json
import os
from common import Error, canonical, now, require


class Journal:
    def __init__(self, directory):
        require(directory.is_dir(), message='run init first')
        self.path = directory / 'global.jsonl'
        self.lock = (directory / 'coordinator.lock').open('a+b')
        try:
            if os.fstat(self.lock.fileno()).st_size == 0:
                self.lock.write(b'0')
                self.lock.flush()
            self.lock.seek(0)
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(self.lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.lock.close()
            raise Error('COORDINATOR_BUSY', 'another coordinator owns this directory') from exc
        self.records, self.transactions, self.requests = [], {}, {}
        self.failure = None
        try:
            self.load()
        except (Error, OSError, ValueError, KeyError, TypeError) as exc:
            self.failure = str(exc)

    def apply(self, event):
        require(type(event['seq']) is int and event['seq'] == len(self.records) + 1, 'LOG_CORRUPT')
        kind, tx, request = event['event'], event['tx_id'], event['request_id']
        require(isinstance(tx, str) and isinstance(request, str) and isinstance(event['timestamp'], str), 'LOG_CORRUPT')
        if kind == 'STARTED':
            require(tx not in self.transactions and request not in self.requests, 'LOG_CORRUPT')
            require(event['action'] in ('enroll', 'drop') and type(event['student_id']) is int and event['student_id'] > 0
                    and type(event['course_id']) is int and event['course_id'] > 0, 'LOG_CORRUPT')
            require(set(event['participants']) == set(event['operations']) and event['participants'], 'LOG_CORRUPT')
            require(all(n in ('0', '1', '2') for n in event['participants']), 'LOG_CORRUPT')
            self.transactions[tx] = dict(event)
            self.requests[request] = tx
        else:
            require(tx in self.transactions and self.requests.get(request) == tx, 'LOG_CORRUPT')
            old = self.transactions[tx]['event']
            allowed = {'STARTED': ('DECIDED_COMMIT', 'DECIDED_ABORT'),
                       'DECIDED_COMMIT': ('DONE_COMMIT',), 'DECIDED_ABORT': ('DONE_ABORT',)}
            require(kind in allowed.get(old, ()), 'LOG_CORRUPT', 'invalid transaction transition')
            self.transactions[tx].update(event)
        self.records.append(event)

    def load(self):
        raw = self.path.read_bytes()
        lines = raw.splitlines(keepends=True)
        offset = 0
        for index, line in enumerate(lines):
            if not line.endswith(b'\n'):
                require(index == len(lines) - 1, 'LOG_CORRUPT')
                try:
                    json.loads(line)
                except (ValueError, UnicodeError):
                    with self.path.open('r+b') as stream:
                        stream.truncate(offset)
                        stream.flush()
                        os.fsync(stream.fileno())
                    break
                with self.path.open('ab') as stream:
                    stream.write(b'\n')
                    stream.flush()
                    os.fsync(stream.fileno())
            self.apply(json.loads(line))
            offset += len(line)

    def append(self, event, tx_id, request_id, **fields):
        require(self.failure is None, 'BLOCKED', self.failure or '')
        entry = dict(seq=len(self.records) + 1, event=event, tx_id=tx_id,
                     request_id=request_id, timestamp=now(), **fields)
        try:
            with self.path.open('ab') as stream:
                stream.write((canonical(entry) + '\n').encode('utf-8'))
                stream.flush()
                os.fsync(stream.fileno())
            self.apply(entry)
        except (OSError, Error) as exc:
            self.failure = str(exc)
            raise Error('BLOCKED', 'journal write failed; restart recovery') from exc
        return self.transactions[tx_id]

    def close(self):
        self.lock.close()
