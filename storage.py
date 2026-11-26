"""SQLite participant: durable intents, logical locks, idempotent decisions."""
import sqlite3
from contextlib import contextmanager
from common import Error, db_path, digest, now, positive, require

SCHEDULE = '''SELECT s.student_id,s.name,c.course_id,c.course_name,c.credits,e.enrolled_at
 FROM Student s JOIN Enrollment e ON s.student_id=e.student_id
 JOIN CourseCatalog c ON e.course_id=c.course_id
 WHERE s.student_id=? ORDER BY c.course_id'''


class Storage:
    def __init__(self, cfg, node):
        require(db_path(cfg, node).is_file(), message='initialize database first')
        self.db = sqlite3.connect(db_path(cfg, node), isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.execute('PRAGMA journal_mode=DELETE')
        self.db.execute('PRAGMA synchronous=FULL')
        self.node, self.cfg = node, cfg
        self.meta = dict(self.db.execute('SELECT key,value FROM Metadata'))
        require(self.meta['node_id'] == str(node) and
                all(self.meta[k] == cfg[k] for k in ('cluster_id', 'dataset_version', 'coordinator_id')),
                'VERSION_MISMATCH')
        actual = [list(r) for r in self.db.execute('SELECT * FROM CourseCatalog ORDER BY course_id')]
        require(digest(actual) == self.meta['catalog_digest'], 'VERSION_MISMATCH', 'catalog content changed')

    @contextmanager
    def atomic(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def rows(self, sql, params=()):
        return [dict(r) for r in self.db.execute(sql, params)]

    def owned(self, key):
        require(positive(key) % 3 == self.node, 'WRONG_NODE')

    def query(self, op, p):
        if op == 'PING':
            return dict(node_id=self.node, **{k: self.meta[k] for k in
                        ('dataset_version', 'catalog_digest')})
        if op in ('GET_STUDENT', 'GET_SCHEDULE'):
            sid = p.get('student_id')
            self.owned(sid)
            student = self.rows('SELECT * FROM Student WHERE student_id=?', (sid,))
            require(student, 'NOT_FOUND', 'student not found')
            if op == 'GET_STUDENT':
                return student[0]
            return dict(student=student[0], courses=self.rows(SCHEDULE, (sid,)),
                        query_plan=self.rows('EXPLAIN QUERY PLAN ' + SCHEDULE, (sid,)))
        if op == 'GET_STOCK':
            cid = p.get('course_id')
            self.owned(cid)
            rows = self.rows('SELECT * FROM CourseStock WHERE course_id=?', (cid,))
            require(rows, 'NOT_FOUND', 'course not found')
            return rows[0]
        if op == 'GET_ROSTER':
            cid, after = positive(p.get('course_id')), p.get('after_id', 0)
            require(type(after) is int and after >= 0)
            return self.rows('SELECT s.student_id,s.name,s.department,e.enrolled_at FROM Student s '
                             'JOIN Enrollment e ON s.student_id=e.student_id '
                             'WHERE e.course_id=? AND s.student_id>? ORDER BY s.student_id LIMIT 100', (cid, after))
        if op == 'GET_CATALOG':
            if 'course_id' in p:
                rows = self.rows('SELECT * FROM CourseCatalog WHERE course_id=?',
                                 (positive(p['course_id']),))
                require(rows, 'NOT_FOUND', 'course not found')
                return rows[0]
            after, limit = p.get('after_id', 0), p.get('limit', 100)
            require(type(after) is int and after >= 0 and type(limit) is int and 1 <= limit <= 100)
            return self.rows('SELECT * FROM CourseCatalog WHERE course_id>? ORDER BY course_id LIMIT ?',
                             (after, limit))
        raise Error('INVALID_ARGUMENT', 'unknown query')

    def status(self, tx):
        row = self.db.execute('SELECT * FROM LocalTransaction WHERE tx_id=?', (tx,)).fetchone()
        return dict(row) if row else dict(tx_id=tx, state='UNKNOWN')

    def prepare(self, tx, p):
        operations = p.get('operations')
        require(isinstance(operations, list) and 1 <= len(operations) <= 2)
        require(p.get('payload_digest') == digest(operations), 'TX_CONFLICT')
        require(isinstance(p.get('request_id'), str) and 0 < len(p['request_id']) <= 128)
        with self.atomic():
            old = self.status(tx)
            if old['state'] != 'UNKNOWN':
                if old.get('payload_digest') is not None:
                    require(old['payload_digest'] == p['payload_digest'] and
                            old['request_id'] == p['request_id'], 'TX_CONFLICT')
                return old
            self.db.execute('INSERT INTO LocalTransaction VALUES (?,?,?,?,?)',
                            (tx, p['request_id'], p['payload_digest'], 'PREPARED', now()))
            seen = set()
            for seq, item in enumerate(operations):
                require(isinstance(item, dict) and type(item.get('seq')) is int and item['seq'] == seq)
                kind, cid, sid = item.get('op_type'), positive(item.get('course_id')), item.get('student_id')
                require(kind in ('ENROLL_ADD', 'ENROLL_REMOVE', 'STOCK_DEC', 'STOCK_INC'))
                if kind.startswith('ENROLL'):
                    self.owned(sid)
                    require(self.rows('SELECT 1 FROM Student WHERE student_id=?', (sid,)), 'NOT_FOUND')
                    require(self.rows('SELECT 1 FROM CourseCatalog WHERE course_id=?', (cid,)), 'NOT_FOUND')
                    exists = bool(self.rows('SELECT 1 FROM Enrollment WHERE student_id=? AND course_id=?', (sid, cid)))
                    require(not exists if kind == 'ENROLL_ADD' else exists,
                            'ALREADY_ENROLLED' if kind == 'ENROLL_ADD' else 'NOT_ENROLLED')
                    require(kind != 'ENROLL_ADD' or isinstance(item.get('enrolled_at'), str))
                    resource = f'enrollment:{sid}:{cid}'
                else:
                    self.owned(cid)
                    stock = self.query('GET_STOCK', dict(course_id=cid))
                    require(stock['remaining'] > 0 if kind == 'STOCK_DEC' else stock['remaining'] < stock['capacity'],
                            'NO_SEATS' if kind == 'STOCK_DEC' else 'TX_CONFLICT')
                    resource = f'stock:{cid}'
                require(resource not in seen, 'INVALID_ARGUMENT', 'duplicate resource')
                seen.add(resource)
                require(not self.rows('SELECT 1 FROM ResourceLock WHERE resource_key=?', (resource,)), 'LOCK_CONFLICT')
                self.db.execute('INSERT INTO ResourceLock VALUES (?,?)', (resource, tx))
                self.db.execute('INSERT INTO PendingOperation VALUES (?,?,?,?,?,?)',
                                (tx, seq, kind, sid, cid, item.get('enrolled_at')))
        return self.status(tx)

    def decide(self, tx, commit):
        target = 'COMMITTED' if commit else 'ABORTED'
        with self.atomic():
            state = self.status(tx)['state']
            if state == target:
                return self.status(tx)
            require(state in ('UNKNOWN', 'PREPARED'), 'TX_CONFLICT')
            if state == 'UNKNOWN':
                require(not commit, 'TX_UNKNOWN')
                self.db.execute('INSERT INTO LocalTransaction VALUES (?,NULL,NULL,?,?)', (tx, target, now()))
            else:
                if commit:
                    for item in self.rows('SELECT * FROM PendingOperation WHERE tx_id=? ORDER BY seq', (tx,)):
                        kind, sid, cid = item['op_type'], item['student_id'], item['course_id']
                        if kind == 'ENROLL_ADD':
                            cursor = self.db.execute('INSERT INTO Enrollment VALUES (?,?,?)', (sid, cid, item['enrolled_at']))
                        elif kind == 'ENROLL_REMOVE':
                            cursor = self.db.execute('DELETE FROM Enrollment WHERE student_id=? AND course_id=?', (sid, cid))
                        elif kind == 'STOCK_DEC':
                            cursor = self.db.execute('UPDATE CourseStock SET remaining=remaining-1 WHERE course_id=? AND remaining>0', (cid,))
                        else:
                            cursor = self.db.execute('UPDATE CourseStock SET remaining=remaining+1 WHERE course_id=? AND remaining<capacity', (cid,))
                        require(cursor.rowcount == 1, 'TX_CONFLICT', 'prepared operation invariant failed')
                self.db.execute('UPDATE LocalTransaction SET state=?,updated_at=? WHERE tx_id=?', (target, now(), tx))
            self.db.execute('DELETE FROM PendingOperation WHERE tx_id=?', (tx,))
            self.db.execute('DELETE FROM ResourceLock WHERE tx_id=?', (tx,))
        return self.status(tx)

    def dispatch(self, op, p, tx):
        if op == 'TX_STATUS':
            return self.rows("SELECT * FROM LocalTransaction WHERE state='PREPARED'") if p.get('list_unfinished') is True else self.status(tx)
        if op == 'PREPARE':
            return self.prepare(tx, p)
        if op in ('COMMIT', 'ABORT'):
            return self.decide(tx, op == 'COMMIT')
        return self.query(op, p)
