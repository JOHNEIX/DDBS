"""Distributed student course-selection system CLI and local launcher."""
import argparse
import json
import os
import shlex
import subprocess
import sys
import time
import uuid
from pathlib import Path
from common import Error, db_path, load_config, require
from init_db import initialize
from protocol import Client
from tracing import Trace
from transaction import Coordinator


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', default=str(Path(__file__).with_name('config.json')))
    p.add_argument('--trace', action='store_true', help='show query and transaction timeline')
    p.add_argument('--json', action='store_true', help='machine-readable output')
    p.add_argument('--fault', choices=['after_prepare', 'after_decision', 'after_first_commit'])
    sub = p.add_subparsers(dest='command', required=True)
    init = sub.add_parser('init', help='initialize a new cluster')
    init.add_argument('--students', type=int, default=10000)
    init.add_argument('--courses', type=int, default=1000)
    init.add_argument('--per-student', type=int, default=5)
    for name in ('shell', 'local', 'status', 'recover'):
        sub.add_parser(name)
    for name in ('student', 'schedule', 'stock'):
        sub.add_parser(name).add_argument('id', type=int)
    catalog = sub.add_parser('catalog')
    catalog.add_argument('id', type=int, nargs='?')
    catalog.add_argument('--after', type=int, default=0)
    catalog.add_argument('--limit', type=int, default=100)
    for name in ('enroll', 'drop'):
        w = sub.add_parser(name)
        w.add_argument('student_id', type=int)
        w.add_argument('course_id', type=int)
        w.add_argument('--request', default=None)
    sub.add_parser('request').add_argument('request_id')
    sub.add_parser('history').add_argument('request_id')
    return p


def execute(coordinator, args):
    command = args.command
    if command == 'status':
        return coordinator.status()
    if command == 'recover':
        coordinator.recover()
        return coordinator.status()
    if command in ('request', 'history'):
        result = coordinator.request_status(args.request_id)
        if command == 'history':
            result['timeline'] = [e for e in coordinator.journal.records if e['request_id'] == args.request_id]
        return result
    if command in ('enroll', 'drop'):
        request = args.request or uuid.uuid4().hex
        print(f'request_id={request}', file=sys.stderr, flush=True)
        return coordinator.write(command, args.student_id, args.course_id, request)
    mapping = {'student': ('GET_STUDENT', 'student_id'), 'schedule': ('GET_SCHEDULE', 'student_id'),
               'stock': ('GET_STOCK', 'course_id')}
    if command in mapping:
        op, key = mapping[command]
        return coordinator.read(op, {key: args.id})
    if command == 'catalog':
        return coordinator.read('GET_CATALOG', {'course_id': args.id} if args.id is not None
                                else dict(after_id=args.after, limit=args.limit))
    raise Error('INVALID_ARGUMENT', 'unknown command')


def output(value):
    print(json.dumps(value, ensure_ascii=False, indent=2), flush=True)


def shell(coordinator, cfg_path):
    print('分布式学生选课系统: student/schedule/stock ID | catalog [ID] | enroll/drop SID CID --request ID')
    print('status | request/history ID | recover | trace on/off | exit')
    while True:
        try:
            line = input('选课系统> ').strip()
            if line in ('exit', 'quit'):
                return
            if line in ('trace on', 'trace off'):
                coordinator.trace.display = line.endswith('on')
                continue
            if not line:
                continue
            args = parser().parse_args(['--config', str(cfg_path)] + shlex.split(line))
            output(execute(coordinator, args))
        except Error as exc:
            output(dict(ok=False, code=exc.code, message=exc.message))
        except SystemExit:
            continue
        except (EOFError, KeyboardInterrupt):
            print()
            return


def launch_nodes(cfg, nodes=range(3)):
    workers, streams = [], []
    try:
        for node in nodes:
            log = (cfg['data'] / f'node{node}' / 'server.log').open('ab')
            streams.append(log)
            command = [sys.executable, str(Path(__file__).with_name('node_server.py')),
                       '--config', str(cfg['path']), '--node', str(node)]
            workers.append(subprocess.Popen(command, stdout=log, stderr=log,
                           creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0))
        probe = Client(cfg, Trace())
        for node, worker in zip(nodes, workers):
            deadline = time.monotonic() + 15
            while True:
                if worker.poll() is not None:
                    raise Error('NODE_UNAVAILABLE', f'node {node} startup failed; see server.log')
                try:
                    probe.call(node, 'PING')
                    break
                except Error:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(.1)
        return workers, streams
    except BaseException:
        stop_nodes(workers, streams)
        raise


def stop_nodes(workers, streams):
    for worker in workers:
        if worker.poll() is None:
            worker.terminate()
        worker.wait(timeout=10)
    for stream in streams:
        stream.close()


def main():
    args = parser().parse_args()
    coordinator, workers, streams = None, [], []
    try:
        cfg = load_config(args.config)
        if args.command == 'init':
            output(initialize(cfg, args.students, args.courses, args.per_student))
            return 0
        if args.command == 'local':
            require(args.fault is None, message='use independent node terminals for coordinator crash demonstrations')
            if not any(db_path(cfg, n).exists() for n in range(3)):
                output(initialize(cfg))
            workers, streams = launch_nodes(cfg)
        coordinator = Coordinator(cfg, args.trace and not args.json, args.fault)
        if args.command in ('shell', 'local'):
            shell(coordinator, cfg['path'])
        else:
            result = execute(coordinator, args)
            output(result)
            if isinstance(result, dict) and (result.get('outcome') in ('aborted', 'processing')
                                             or result.get('system_state') == 'BLOCKED'):
                return 2
        return 0
    except (Error, OSError, ValueError) as exc:
        output(dict(ok=False, code=getattr(exc, 'code', 'IO_ERROR'), message=str(exc)))
        return 2
    finally:
        if coordinator:
            coordinator.close()
        stop_nodes(workers, streams)


if __name__ == '__main__':
    sys.exit(main())
