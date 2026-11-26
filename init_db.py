"""Build a consistent 10,000-student, 1,000-course dataset."""
import sqlite3
from collections import Counter
from contextlib import closing
from common import canonical, db_path, digest, now, require

SCHEMA = '''
CREATE TABLE Metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE Student(student_id INTEGER PRIMARY KEY CHECK(student_id>0),
 name TEXT NOT NULL, department TEXT NOT NULL);
CREATE TABLE CourseCatalog(course_id INTEGER PRIMARY KEY CHECK(course_id>0),
 course_name TEXT NOT NULL, credits INTEGER NOT NULL CHECK(credits>0));
CREATE TABLE Enrollment(student_id INTEGER NOT NULL REFERENCES Student,
 course_id INTEGER NOT NULL REFERENCES CourseCatalog, enrolled_at TEXT NOT NULL,
 PRIMARY KEY(student_id,course_id));
CREATE INDEX enrollment_course ON Enrollment(course_id);
CREATE TABLE CourseStock(course_id INTEGER PRIMARY KEY REFERENCES CourseCatalog,
 capacity INTEGER NOT NULL CHECK(capacity>=0), remaining INTEGER NOT NULL,
 CHECK(remaining>=0 AND remaining<=capacity));
CREATE TABLE LocalTransaction(tx_id TEXT PRIMARY KEY, request_id TEXT,
 payload_digest TEXT, state TEXT NOT NULL CHECK(state IN ('PREPARED','COMMITTED','ABORTED')),
 updated_at TEXT NOT NULL);
CREATE TABLE PendingOperation(tx_id TEXT REFERENCES LocalTransaction, seq INTEGER,
 op_type TEXT NOT NULL, student_id INTEGER, course_id INTEGER NOT NULL,
 enrolled_at TEXT, PRIMARY KEY(tx_id,seq));
CREATE TABLE ResourceLock(resource_key TEXT PRIMARY KEY,
 tx_id TEXT NOT NULL REFERENCES LocalTransaction);
'''


def initialize(cfg, students=10000, courses=1000, per_student=5):
    require(students > 0 and courses > 0 and 0 <= per_student <= courses)
    require(not any(db_path(cfg, n).exists() for n in range(3)), message='database already exists')
    coord = cfg['data'] / 'coordinator'
    require(not coord.exists(), message='coordinator data already exists')
    catalog = [(c, f'课程{c:04}', c % 4 + 1) for c in range(1, courses + 1)]
    stamp = now()
    enrollments = [(s, (s * 7 + j) % courses + 1, stamp)
                   for s in range(1, students + 1) for j in range(per_student)]
    counts = Counter(c for _, c, _ in enrollments)
    capacity = max(100, max(counts.values(), default=0) + 10)
    meta = dict(cluster_id=cfg['cluster_id'], dataset_version=cfg['dataset_version'],
                catalog_digest=digest(catalog), coordinator_id=cfg['coordinator_id'])
    for n in range(3):
        path = db_path(cfg, n)
        path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(path)) as db:
            db.execute('PRAGMA foreign_keys=ON')
            db.executescript(SCHEMA)
            db.executemany('INSERT INTO Metadata VALUES (?,?)', {**meta, 'node_id': str(n)}.items())
            db.executemany('INSERT INTO CourseCatalog VALUES (?,?,?)', catalog)
            db.executemany('INSERT INTO Student VALUES (?,?,?)',
                           [(s, f'学生{s:05}', f'院系{s % 8 + 1}')
                            for s in range(1, students + 1) if s % 3 == n])
            db.executemany('INSERT INTO Enrollment VALUES (?,?,?)',
                           [e for e in enrollments if e[0] % 3 == n])
            db.executemany('INSERT INTO CourseStock VALUES (?,?,?)',
                           [(c, capacity, capacity - counts[c])
                            for c in range(1, courses + 1) if c % 3 == n])
            db.commit()
    coord.mkdir()
    (coord / 'manifest.json').write_text(canonical(meta), encoding='utf-8')
    (coord / 'global.jsonl').touch()
    return dict(students=students, courses=courses, enrollments=len(enrollments))
