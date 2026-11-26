"""Authenticated browser gateway for students and administrators."""
import argparse
import json
import secrets
import threading
import webbrowser
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from auth import Accounts
from common import Error, db_path, load_config, require
from init_db import initialize
from transaction import Coordinator
from protocol import Client
from tracing import Trace

ASSETS = Path(__file__).with_name('web')


class WebServer(ThreadingHTTPServer):
    daemon_threads = False

    def __init__(self, address, coordinator):
        self.coordinator, self.token = coordinator, secrets.token_urlsafe(32)
        self.accounts = Accounts(coordinator.cfg['data'] / 'coordinator')
        super().__init__(address, Handler)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def reply(self, value, status=200, content_type='application/json; charset=utf-8'):
        body = value if isinstance(value, bytes) else json.dumps(value, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        for key, item in {'Content-Type':content_type, 'Content-Length':str(len(body)),
                         'Cache-Control':'no-store', 'X-Content-Type-Options':'nosniff',
                         'Content-Security-Policy':"default-src 'self'; frame-ancestors 'none'"}.items():
            self.send_header(key, item)
        if getattr(self, 'cookie', None):
            self.send_header('Set-Cookie', self.cookie)
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        self.handle_api(False)

    def do_POST(self):
        self.handle_api(True)

    def handle_api(self, writing):
        path = urlsplit(self.path).path
        try:
            cookies = SimpleCookie(self.headers.get('Cookie', ''))
            session = cookies.get('session')
            self.session = session.value if session else ''
            self.user = self.server.accounts.user(self.session)
            if not writing and path in ('/', '/app.js', '/style.css'):
                asset = ASSETS / ('index.html' if path == '/' else path[1:])
                mime = {'.html':'text/html','.js':'application/javascript','.css':'text/css'}[asset.suffix]
                return self.reply(asset.read_bytes(), content_type=mime+'; charset=utf-8')
            if not writing and path == '/api/session':
                return self.reply(dict(ok=True, data=dict(token=self.server.token, user=self.user)))
            if writing:
                require(self.headers.get('X-Session-Token') == self.server.token, 'INVALID_SESSION')
                require(self.headers.get('Origin', 'http://'+self.headers.get('Host','')) == 'http://'+self.headers.get('Host',''), 'INVALID_SESSION')
                length = int(self.headers.get('Content-Length', '0'))
                require(0 < length <= 8192)
                self.connection.settimeout(10)
                p = json.loads(self.rfile.read(length).decode('utf-8'))
                require(isinstance(p, dict))
            else:
                p = {k:v[0] for k,v in parse_qs(urlsplit(self.path).query).items()}
            c = self.server.coordinator
            with c.gate:
                start = len(c.trace.events)
                try:
                    data = self.operation(path, p, writing)
                    result = dict(ok=True, data=data)
                except Error as exc:
                    result = dict(ok=False, code=exc.code, message=exc.message)
                result['system_state'] = c.state
                if self.user.get('role') == 'admin':
                    result['trace'] = c.trace.events[start:][-200:]
            self.reply(result)
        except (Error, ValueError, TypeError, KeyError, UnicodeError, OSError) as exc:
            self.reply(dict(ok=False, code=getattr(exc,'code','INVALID_ARGUMENT'), message=str(exc)), 400)

    def operation(self, path, p, writing):
        c, accounts = self.server.coordinator, self.server.accounts
        if writing and path == '/api/login':
            token = accounts.login(p.get('username'), p.get('password'))
            self.cookie = f'session={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=28800'
            self.user = accounts.user(token)
            return self.user
        require(self.user, 'LOGIN_REQUIRED')
        admin = self.user['role'] == 'admin'
        if writing:
            if path == '/api/logout':
                accounts.sessions.pop(self.session, None)
                self.cookie = 'session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0'
                return None
            if path == '/api/recover':
                require(admin, 'FORBIDDEN')
                c.recover()
                return c.status()
            require(path in ('/api/enroll','/api/drop'), 'NOT_FOUND')
            require(not admin and p.get('student_id',self.user['student_id']) == self.user['student_id'], 'FORBIDDEN')
            return c.write(path.rsplit('/',1)[1],self.user['student_id'],p['course_id'],p['request_id'],actor=self.user['username'])
        if path in ('/api/status','/api/records','/api/roster'):
            require(admin, 'FORBIDDEN')
            if path == '/api/status':
                return c.status()
            if path == '/api/records':
                return list(reversed(list(c.journal.transactions.values())))[:100]
            return c.read('GET_ROSTER', {'course_id':int(p['course_id']), 'after_id':int(p.get('after_id',0))})
        if path == '/api/request':
            tx_id = c.journal.requests.get(p['request_id'])
            require(tx_id, 'NOT_FOUND')
            require(admin or c.journal.transactions[tx_id]['student_id'] == self.user['student_id'], 'FORBIDDEN')
            return c.request_status(p['request_id'])
        if path in ('/api/student','/api/schedule'):
            sid = int(p['student_id']) if admin else self.user['student_id']
            require(admin or 'student_id' not in p or int(p['student_id']) == sid, 'FORBIDDEN')
            return c.read('GET_STUDENT' if path.endswith('student') else 'GET_SCHEDULE', {'student_id':sid})
        if path == '/api/stock':
            return c.read('GET_STOCK', {'course_id':int(p['course_id'])})
        require(path == '/api/catalog', 'NOT_FOUND')
        params = {'course_id':int(p['course_id'])} if p.get('course_id') else dict(after_id=int(p.get('after_id',0)),limit=10)
        rows = c.read('GET_CATALOG',params)
        rows = rows if isinstance(rows,list) else [rows]
        failed = set()
        for row in rows:
            node = row['course_id'] % 3
            if node not in failed:
                try:
                    row.update(c.read('GET_STOCK', {'course_id':row['course_id']}))
                    continue
                except Error as exc:
                    if exc.code != 'NODE_UNAVAILABLE':
                        raise
                    failed.add(node)
            row['remaining'] = None
        return rows


def main():
    from main import launch_nodes, stop_nodes
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(Path(__file__).with_name('config.json')))
    parser.add_argument('--local', action='store_true', help='initialize and launch three local stations')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8081)
    parser.add_argument('--open', action='store_true', help='open browser automatically')
    args = parser.parse_args()
    workers, streams, coordinator, server = [], [], None, None
    try:
        cfg = load_config(args.config)
        if args.local:
            if not any(db_path(cfg, n).exists() for n in range(3)):
                print(initialize(cfg), flush=True)
            missing = []
            probe = Client(dict(cfg, retries=0, connect_timeout=.3, request_timeout=1), Trace())
            for node in range(3):
                try:
                    probe.call(node, 'PING')
                except Error as exc:
                    if exc.code != 'NODE_UNAVAILABLE':
                        raise
                    missing.append(node)
            workers, streams = launch_nodes(cfg, missing)
        coordinator = Coordinator(cfg)
        server = WebServer((args.host, args.port), coordinator)
        url = f'http://{args.host if args.host != "0.0.0.0" else "127.0.0.1"}:{server.server_port}'
        print(f'Browser client: {url}', flush=True)
        if args.open:
            threading.Timer(.5, lambda: webbrowser.open(url)).start()
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    except (Error, OSError, ValueError) as exc:
        print(f'{getattr(exc, "code", "STARTUP_ERROR")}: {exc}', flush=True)
        return 2
    finally:
        if server:
            server.server_close()
        if coordinator:
            coordinator.close()
        stop_nodes(workers, streams)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
