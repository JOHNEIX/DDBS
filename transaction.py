"""Single-owner coordinator: serial access and durable two-phase decisions."""
import json
import os
import threading
import uuid
from common import Error, digest, now, positive, require
from journal import Journal
from protocol import Client
from recovery import finish, recover
from router import operations, query
from tracing import Trace


class Coordinator:
    def __init__(self, cfg, display=False, fault=None):
        self.cfg, self.state, self.fault = cfg, 'STARTING', fault
        self.last_error, self.checked = None, set()
        self.gate = threading.RLock()
        directory = cfg['data'] / 'coordinator'
        self.journal = Journal(directory)
        self.trace = Trace(directory / 'trace.jsonl', display)
        self.client = Client(cfg, self.trace)
        try:
            self.manifest = json.loads((directory / 'manifest.json').read_text(encoding='utf-8'))
            require(all(self.manifest[k] == cfg[k] for k in ('cluster_id', 'coordinator_id', 'dataset_version')),
                    'VERSION_MISMATCH')
            recover(self)
        except BaseException:
            self.journal.close()
            raise

    def close(self):
        self.journal.close()

    def crash(self, point):
        if self.fault == point:
            self.trace.emit('FAULT', point=point)
            os._exit(77)

    def check_node(self, node):
        info = self.client.call(node, 'PING')
        require(info['node_id'] == node and all(info[k] == self.manifest[k]
                for k in ('dataset_version', 'catalog_digest')), 'VERSION_MISMATCH')
        self.checked.add(node)
        return info

    def call(self, node, op, payload=None):
        if node not in self.checked:
            self.check_node(node)
        return self.client.call(node, op, payload)

    def record(self, event, tx, **extra):
        updated = self.journal.append(event, tx['tx_id'], tx['request_id'], **extra)
        self.trace.emit(event, tx_id=tx['tx_id'], request_id=tx['request_id'], **extra)
        return updated

    def status(self):
        nodes = []
        for node in range(3):
            try:
                nodes.append(dict(reachable=True, **self.check_node(node)))
            except Error as exc:
                nodes.append(dict(node_id=node, reachable=False, code=exc.code, message=exc.message))
        pending = [self.result(tx) for tx in self.journal.transactions.values() if not tx['event'].startswith('DONE_')]
        return dict(system_state=self.state, last_error=self.last_error, nodes=nodes, pending=pending)

    def result(self, tx):
        state = tx['event']
        return dict(request_id=tx['request_id'], tx_id=tx['tx_id'], state=state,
                    outcome='success' if state == 'DONE_COMMIT' else 'aborted' if state == 'DONE_ABORT' else 'processing',
                    reason=tx.get('reason'))

    def request_status(self, request):
        tx_id = self.journal.requests.get(request)
        require(tx_id, 'NOT_FOUND', 'request not found')
        return self.result(self.journal.transactions[tx_id])

    def read(self, op, payload):
        with self.gate:
            require(self.state == 'READY', self.state)
            self.state = 'BUSY'
            try:
                return query(self, self.trace, op, payload)
            finally:
                self.state = 'READY'

    def write(self, action, sid, cid, request, actor='cli'):
        positive(sid)
        positive(cid)
        require(isinstance(request, str) and 0 < len(request) <= 128)
        with self.gate:
            if request in self.journal.requests:
                tx = self.journal.transactions[self.journal.requests[request]]
                require((tx['action'], tx['student_id'], tx['course_id']) == (action, sid, cid), 'REQUEST_CONFLICT')
                return self.result(tx)
            require(self.state == 'READY', self.state)
            self.state = 'BUSY'
            tx = None
            try:
                ops = operations(action, sid, cid, now())
                for node in ops:
                    self.check_node(int(node))
                tx_id = uuid.uuid4().hex
                tx = self.journal.append('STARTED', tx_id, request, action=action, student_id=sid,
                                         course_id=cid, participants=list(ops), operations=ops, actor=actor)
                self.trace.emit('STARTED', tx_id=tx_id, request_id=request, participants=list(ops))
                try:
                    for node, items in ops.items():
                        local = self.client.call(int(node), 'PREPARE', dict(request_id=request,
                                                 operations=items, payload_digest=digest(items)), tx_id)
                        require(local['state'] == 'PREPARED', 'TX_CONFLICT')
                        self.trace.emit('PREPARED', tx_id=tx_id, node=int(node))
                    self.crash('after_prepare')
                except Error as exc:
                    self.record('DECIDED_ABORT', tx, reason=exc.code)
                else:
                    self.record('DECIDED_COMMIT', tx)
                    self.crash('after_decision')
                finish(self, tx)
                self.state = 'READY'
                return self.result(tx)
            except (Error, OSError) as exc:
                self.state = 'BLOCKED' if tx is not None or self.journal.failure else 'READY'
                self.last_error = str(exc)
                self.trace.emit('WRITE_ERROR', state=self.state, reason=str(exc))
                if self.state == 'BLOCKED' and tx:
                    return dict(self.result(tx), system_state='BLOCKED', error=str(exc))
                raise

    def recover(self):
        with self.gate:
            self.checked.clear()
            return recover(self)
