"""Real TCP/process tests for routing, idempotency and crash recovery."""
import json
import os
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import time
import threading
import urllib.request
import urllib.error
import unittest
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from common import Error, canonical, db_path, digest, load_config
from init_db import initialize
from protocol import Client, receive
from router import operations
from tracing import Trace
from transaction import Coordinator


class SystemTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='minishard-test-')
        self.directory = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        cfg = json.loads((ROOT / 'config.json').read_text(encoding='utf-8'))
        cfg.update(connect_timeout=.2, request_timeout=.6, retries=1, retry_delay=.02)
        ports = []
        sockets = []
        for _ in range(3):
            sock = socket.socket()
            sock.bind(('127.0.0.1', 0))
            sockets.append(sock)
            ports.append(sock.getsockname()[1])
        for sock in sockets:
            sock.close()
        for node, port in zip(cfg['nodes'], ports):
            node['port'] = port
        self.config = self.directory / 'config.json'
        self.config.write_text(json.dumps(cfg), encoding='utf-8')
        self.cfg = load_config(self.config)
        initialize(self.cfg, 30, 12, 5)
        self.workers, self.streams = {}, []
        self.addCleanup(self.shutdown)
        self.client = Client(self.cfg, Trace())
        self.coordinator = None
        for node in range(3):
            self.start(node)

    def start(self, node, fault=None):
        log = (self.directory / f'node{node}.log').open('ab')
        self.streams.append(log)
        command = [sys.executable, str(ROOT / 'node_server.py'), '--config', str(self.config), '--node', str(node)]
        if fault:
            command += ['--fault', fault]
        self.workers[node] = subprocess.Popen(command, stdout=log, stderr=log,
                            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        deadline = time.monotonic() + 10
        while True:
            if self.workers[node].poll() is not None:
                self.fail((self.directory / f'node{node}.log').read_text(encoding='utf-8', errors='replace'))
            try:
                self.client.call(node, 'PING')
                return
            except Error:
                if time.monotonic() > deadline:
                    raise
                time.sleep(.02)

    def stop(self, node):
        process = self.workers.pop(node)
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=5)

    def shutdown(self):
        if self.coordinator:
            self.coordinator.close()
            self.coordinator = None
        for node in list(self.workers):
            self.stop(node)
        for stream in self.streams:
            stream.close()

    def connect(self):
        self.coordinator = Coordinator(self.cfg)
        return self.coordinator

    def error(self, code, fn, *args):
        with self.assertRaises(Error) as caught:
            fn(*args)
        self.assertEqual(caught.exception.code, code)

    def scalar(self, node, sql, args=()):
        with closing(sqlite3.connect(db_path(self.cfg, node))) as db:
            return db.execute(sql, args).fetchone()[0]

    def invariant(self):
        counts, stocks = {}, {}
        for node in range(3):
            with closing(sqlite3.connect(db_path(self.cfg, node))) as db:
                for cid, count in db.execute('SELECT course_id,count(*) FROM Enrollment GROUP BY course_id'):
                    counts[cid] = counts.get(cid, 0) + count
                for cid, capacity, remaining in db.execute('SELECT * FROM CourseStock'):
                    self.assertTrue(0 <= remaining <= capacity)
                    stocks[cid] = capacity - remaining
                self.assertEqual(db.execute('SELECT count(*) FROM ResourceLock').fetchone()[0], 0)
                self.assertEqual(db.execute('SELECT count(*) FROM PendingOperation').fetchone()[0], 0)
        self.assertEqual(stocks, {c: counts.get(c, 0) for c in stocks})

    def cli(self, *args, fault=None):
        command = [sys.executable, str(ROOT / 'main.py'), '--config', str(self.config), '--json']
        if fault:
            command += ['--fault', fault]
        env = dict(os.environ, PYTHONIOENCODING='utf-8')
        return subprocess.run(command + list(args), capture_output=True, encoding='utf-8', env=env, timeout=20)

    def test_initialization_and_local_query_trace(self):
        c = self.connect()
        self.assertEqual(c.state, 'READY')
        result = c.read('GET_SCHEDULE', {'student_id': 2})
        self.assertEqual(len(result['courses']), 5)
        self.assertTrue(result['query_plan'])
        self.assertTrue(any(e['kind'] == 'LOCAL_JOIN' and e['node'] == 2 for e in c.trace.events))
        self.assertEqual(c.status()['nodes'][0]['dataset_version'], 'v1')
        self.invariant()

    def test_cross_node_enroll_drop_and_idempotency(self):
        c = self.connect()
        before = c.read('GET_STOCK', {'course_id': 1})['remaining']
        first = c.write('enroll', 2, 1, 'add-1')
        self.assertEqual(first['state'], 'DONE_COMMIT')
        self.assertEqual(c.write('enroll', 2, 1, 'add-1'), first)
        self.error('REQUEST_CONFLICT', c.write, 'enroll', 2, 2, 'add-1')
        self.assertEqual(c.read('GET_STOCK', {'course_id': 1})['remaining'], before - 1)
        self.assertEqual(c.write('drop', 2, 1, 'drop-1')['state'], 'DONE_COMMIT')
        self.assertEqual(c.read('GET_STOCK', {'course_id': 1})['remaining'], before)
        self.invariant()

    def test_same_node_atomic_operations(self):
        result = self.connect().write('enroll', 1, 4, 'same-node')
        self.assertEqual(result['state'], 'DONE_COMMIT')
        tx = self.coordinator.journal.transactions[result['tx_id']]
        self.assertEqual(tx['participants'], ['1'])
        self.invariant()

    def test_business_rejections_abort_cleanly(self):
        c = self.connect()
        self.assertEqual(c.write('enroll', 2, 3, 'duplicate')['reason'], 'ALREADY_ENROLLED')
        self.assertEqual(c.write('drop', 2, 1, 'missing')['reason'], 'NOT_ENROLLED')
        self.assertEqual(c.write('enroll', 32, 1, 'unknown-student')['reason'], 'NOT_FOUND')
        self.assertEqual(c.state, 'READY')
        self.invariant()

    def test_no_seats_and_constraints(self):
        for node in range(3):
            with closing(sqlite3.connect(db_path(self.cfg, node))) as db:
                db.execute('DELETE FROM Enrollment WHERE course_id=1')
                db.execute('UPDATE CourseStock SET capacity=0,remaining=0 WHERE course_id=1')
                db.commit()
        c = self.connect()
        self.assertEqual(c.write('enroll', 2, 1, 'full')['reason'], 'NO_SEATS')
        self.invariant()

    def test_offline_read_and_catalog_failover(self):
        c = self.connect()
        self.stop(2)
        self.error('NODE_UNAVAILABLE', c.read, 'GET_SCHEDULE', {'student_id': 2})
        self.assertEqual(c.state, 'READY')
        self.assertEqual(c.read('GET_STUDENT', {'student_id': 1})['student_id'], 1)
        self.stop(0)
        self.assertEqual(c.read('GET_CATALOG', {'course_id': 1})['course_id'], 1)
        self.assertEqual(c.status()['system_state'], 'READY')

    def test_exclusive_coordinator_lock(self):
        self.connect()
        self.error('COORDINATOR_BUSY', Coordinator, self.cfg)

    def test_coordinator_crash_after_prepare_aborts(self):
        result = self.cli('enroll', '2', '1', '--request', 'crash-prepared', fault='after_prepare')
        self.assertEqual(result.returncode, 77, result.stdout + result.stderr)
        self.assertEqual(self.client.call(2, 'TX_STATUS', {'list_unfinished': True})[0]['state'], 'PREPARED')
        c = self.connect()
        self.assertEqual(c.state, 'READY')
        self.assertEqual(c.request_status('crash-prepared')['state'], 'DONE_ABORT')
        self.invariant()

    def test_coordinator_crash_after_decision_commits(self):
        result = self.cli('enroll', '2', '1', '--request', 'crash-decision', fault='after_decision')
        self.assertEqual(result.returncode, 77, result.stdout + result.stderr)
        c = self.connect()
        self.assertEqual(c.request_status('crash-decision')['state'], 'DONE_COMMIT')
        self.invariant()

    def test_partial_commit_is_blocked_then_recovers(self):
        result = self.cli('enroll', '2', '1', '--request', 'partial', fault='after_first_commit')
        self.assertEqual(result.returncode, 77, result.stdout + result.stderr)
        self.stop(1)
        c = self.connect()
        self.assertEqual(c.state, 'BLOCKED')
        self.error('BLOCKED', c.read, 'GET_SCHEDULE', {'student_id': 2})
        self.assertEqual(c.request_status('partial')['outcome'], 'processing')
        self.start(1)
        self.assertEqual(c.recover(), 'READY')
        self.assertEqual(c.request_status('partial')['state'], 'DONE_COMMIT')
        self.invariant()

    def test_node_crash_after_prepare_preserves_intents(self):
        self.stop(1)
        self.start(1, 'after_prepare')
        c = self.connect()
        self.assertEqual(c.write('enroll', 2, 1, 'node-prepare')['system_state'], 'BLOCKED')
        self.assertEqual(self.scalar(1, "SELECT count(*) FROM LocalTransaction WHERE state='PREPARED'"), 1)
        self.stop(1)
        self.start(1)
        self.assertEqual(c.recover(), 'READY')
        self.assertEqual(c.request_status('node-prepare')['state'], 'DONE_ABORT')
        self.invariant()

    def test_node_commit_response_lost_is_idempotent(self):
        self.stop(1)
        self.start(1, 'after_commit')
        c = self.connect()
        before = c.read('GET_STOCK', {'course_id': 1})['remaining']
        self.assertEqual(c.write('enroll', 2, 1, 'node-commit')['system_state'], 'BLOCKED')
        self.assertEqual(self.scalar(1, 'SELECT remaining FROM CourseStock WHERE course_id=1'), before - 1)
        self.stop(1)
        self.start(1)
        self.assertEqual(c.recover(), 'READY')
        self.assertEqual(c.write('enroll', 2, 1, 'node-commit')['state'], 'DONE_COMMIT')
        self.assertEqual(c.read('GET_STOCK', {'course_id': 1})['remaining'], before - 1)
        self.invariant()

    def test_truncated_log_tail_recovery(self):
        self.assertEqual(self.cli('enroll', '2', '1', '--request', 'tail').returncode, 0)
        path = self.cfg['data'] / 'coordinator/global.jsonl'
        with path.open('ab') as stream:
            stream.write(b'{"seq":')
        self.assertEqual(self.connect().state, 'READY')
        self.assertTrue(path.read_bytes().endswith(b'\n'))
        self.invariant()

    def test_corrupt_complete_log_blocks_access(self):
        path = self.cfg['data'] / 'coordinator/global.jsonl'
        path.write_bytes(b'bad-json\n')
        c = self.connect()
        self.assertEqual(c.state, 'BLOCKED')
        self.error('BLOCKED', c.write, 'enroll', 2, 1, 'bad-log')

    def test_orphan_prepared_blocks_recovery(self):
        items = operations('enroll', 2, 1, '2026-11-26T00:00:00Z')['2']
        self.client.call(2, 'PREPARE', dict(request_id='orphan', operations=items, payload_digest=digest(items)), 'orphan-tx')
        self.assertEqual(self.connect().state, 'BLOCKED')

    def test_rpc_validation_and_terminal_conflicts(self):
        self.error('WRONG_NODE', self.client.call, 0, 'GET_STUDENT', {'student_id': 2})
        self.error('INVALID_ARGUMENT', self.client.call, 2, 'GET_STUDENT', {'student_id': '2 OR 1=1'})
        self.error('INVALID_ARGUMENT', self.client.call, 2, 'GET_STUDENT', {'student_id': 2**80})
        self.error('INVALID_ARGUMENT', self.client.call, 0, 'UNRECOGNIZED')
        self.client.call(1, 'ABORT', tx_id='aborted-first')
        self.error('TX_CONFLICT', self.client.call, 1, 'COMMIT', None, 'aborted-first')
        self.error('TX_UNKNOWN', self.client.call, 1, 'COMMIT', None, 'unknown')

    def test_catalog_version_mismatch_blocks_startup(self):
        self.stop(0)
        with closing(sqlite3.connect(db_path(self.cfg, 0))) as db:
            db.execute("UPDATE CourseCatalog SET course_name='changed' WHERE course_id=1")
            actual = db.execute('SELECT * FROM CourseCatalog ORDER BY course_id').fetchall()
            db.execute("UPDATE Metadata SET value=? WHERE key='catalog_digest'", (digest(actual),))
            db.commit()
        self.start(0)
        self.assertEqual(self.connect().state, 'BLOCKED')

    def test_conflicting_durable_decision_blocks(self):
        self.assertEqual(self.cli('enroll', '2', '1', '--request', 'conflict-log').returncode, 0)
        path = self.cfg['data'] / 'coordinator/global.jsonl'
        entries = [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines()]
        bad = dict(entries[-1], seq=len(entries) + 1, event='DECIDED_ABORT')
        with path.open('a', encoding='utf-8') as stream:
            stream.write(canonical(bad) + '\n')
        self.assertEqual(self.connect().state, 'BLOCKED')

    def test_split_packet_and_bad_length(self):
        request = dict(version=1, cluster_id=self.cfg['cluster_id'], coordinator_id=self.cfg['coordinator_id'],
                       rpc_id='split', op='PING', tx_id=None, payload={})
        body = canonical(request).encode('utf-8')
        packet = struct.pack('!I', len(body)) + body
        port = self.cfg['nodes'][0]['port']
        with socket.create_connection(('127.0.0.1', port)) as sock:
            for part in (packet[:2], packet[2:7], packet[7:]):
                sock.sendall(part)
            self.assertTrue(receive(sock, time.monotonic() + 2)[0]['ok'])
        with socket.create_connection(('127.0.0.1', port)) as sock:
            sock.sendall(struct.pack('!I', 1048577))
            self.assertEqual(receive(sock, time.monotonic() + 2)[0]['code'], 'PROTOCOL_ERROR')

    def test_cli_and_persistent_history(self):
        result = self.cli('enroll', '2', '1', '--request', 'cli-sample')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        history = self.cli('history', 'cli-sample')
        data = json.loads(history.stdout)
        self.assertEqual([e['event'] for e in data['timeline']], ['STARTED', 'DECIDED_COMMIT', 'DONE_COMMIT'])
        self.assertTrue((self.cfg['data'] / 'coordinator/trace.jsonl').is_file())
        self.invariant()

    def start_web(self):
        from web_server import WebServer
        self.web = WebServer(('127.0.0.1', 0), self.connect())
        thread = threading.Thread(target=self.web.serve_forever)
        thread.start()
        def stop():
            self.web.shutdown()
            self.web.server_close()
            thread.join(5)
        self.addCleanup(stop)
        self.url = f'http://127.0.0.1:{self.web.server_port}'
        self.web.accounts.add('2', 'student2', 'student', 2)
        self.cookie = ''
        self.http('/api/login', dict(username='2', password='student2'))

    def http(self, path, body=None, token=True):
        headers = {'X-Session-Token': self.web.token} if token else {}
        headers['Cookie'] = getattr(self, 'cookie', '')
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(self.url + path, data=data, headers=headers)
        try:
            reply = urllib.request.urlopen(request, timeout=10)
        except urllib.error.HTTPError as exc:
            reply = exc
        with reply:
            if reply.headers.get('Set-Cookie'):
                self.cookie = reply.headers['Set-Cookie'].split(';')[0]
            return json.loads(reply.read())

    def test_web_read_write_and_request_identity(self):
        self.start_web()
        self.assertTrue(self.http('/api/session')['data']['token'])
        schedule = self.http('/api/schedule?student_id=2')
        self.assertNotIn('trace', schedule)
        self.assertEqual(len(schedule['data']['courses']), 5)
        catalog = self.http('/api/catalog')['data']
        self.assertEqual(len(catalog), 10)
        self.assertTrue(all('remaining' in c for c in catalog))
        p = dict(student_id=2, course_id=1, request_id='web-add')
        added = self.http('/api/enroll', p)
        self.assertEqual(added['data']['state'], 'DONE_COMMIT')
        self.assertEqual(self.http('/api/enroll', p)['data'], added['data'])
        self.assertEqual(self.http('/api/request?request_id=web-add')['data']['outcome'], 'success')
        p['request_id'] = 'web-drop'
        self.assertEqual(self.http('/api/drop', p)['data']['outcome'], 'success')
        self.invariant()

    def test_web_rejects_bad_input_and_cross_site_writes(self):
        self.start_web()
        p = dict(student_id=2, course_id=1, request_id='bad-web')
        self.assertEqual(self.http('/api/enroll', p, False)['code'], 'INVALID_SESSION')
        self.assertFalse(self.http('/api/student?student_id=invalid')['ok'])
        p['student_id'] = '2 OR 1=1'
        self.assertEqual(self.http('/api/enroll', p)['code'], 'FORBIDDEN')
        self.assertFalse(self.http('/api/missing')['ok'])
        self.invariant()

    def test_web_blocked_state_recovers_partial_commit(self):
        result = self.cli('enroll', '2', '1', '--request', 'web-recover', fault='after_first_commit')
        self.assertEqual(result.returncode, 77)
        self.stop(1)
        self.start_web()
        self.http('/api/login', dict(username='admin', password='admin123'))
        self.assertEqual(self.http('/api/status')['data']['system_state'], 'BLOCKED')
        self.assertEqual(self.http('/api/schedule?student_id=2')['code'], 'BLOCKED')
        self.start(1)
        self.assertEqual(self.http('/api/recover', {})['data']['system_state'], 'READY')
        self.assertEqual(self.http('/api/request?request_id=web-recover')['data']['outcome'], 'success')
        self.invariant()

    def test_web_launcher_reuses_existing_stations(self):
        process = subprocess.Popen([sys.executable, str(ROOT / 'web_server.py'), '--config', str(self.config),
                                   '--local', '--port', '0'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, encoding='utf-8', creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        try:
            line = process.stdout.readline().strip()
            self.assertTrue(line.startswith('Browser client: http://'), line)
            with urllib.request.urlopen(line.split('Browser client: ')[1] + '/api/session', timeout=5) as reply:
                self.assertTrue(json.loads(reply.read())['ok'])
        finally:
            process.terminate()
            process.wait(timeout=5)
            process.stdout.close()
        for node in range(3):
            self.assertEqual(self.client.call(node, 'PING')['node_id'], node)

    def test_web_static_assets_have_browser_executable_types(self):
        self.start_web()
        for path, content_type in [('/', 'text/html'), ('/app.js', 'application/javascript'), ('/style.css', 'text/css')]:
            with urllib.request.urlopen(self.url + path, timeout=5) as reply:
                self.assertEqual(reply.headers.get_content_type(), content_type)
                self.assertEqual(reply.headers['X-Content-Type-Options'], 'nosniff')
                self.assertGreater(len(reply.read()), 100)

    def test_roles_bind_student_and_authorize_admin(self):
        self.start_web()
        self.assertEqual(self.http('/api/schedule?student_id=1')['code'], 'FORBIDDEN')
        self.assertEqual(self.http('/api/status')['code'], 'FORBIDDEN')
        self.assertEqual(self.http('/api/recover', {})['code'], 'FORBIDDEN')
        self.assertEqual(self.http('/api/login',dict(username='2',password='wrong'))['code'], 'LOGIN_FAILED')
        result=self.http('/api/enroll',dict(course_id=1,request_id='bound-student'))
        self.assertEqual(result['data']['state'],'DONE_COMMIT')
        self.http('/api/login',dict(username='admin',password='admin123'))
        records=self.http('/api/records')['data']
        self.assertEqual(records[0]['actor'],'2')
        roster=self.http('/api/roster?course_id=1')
        self.assertTrue(any(r['student_id']==2 for r in roster['data']))
        self.assertEqual({e['node'] for e in roster['trace'] if e['kind']=='ROUTE'}, {0,1,2})
        self.assertEqual(self.http('/api/schedule?student_id=1')['data']['student']['student_id'],1)
        self.http('/api/logout',{})
        self.assertEqual(self.http('/api/catalog')['code'],'LOGIN_REQUIRED')
        self.invariant()


if __name__ == '__main__':
    unittest.main(verbosity=2)
