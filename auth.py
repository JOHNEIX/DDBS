"""Local accounts and expiring browser sessions."""
import hashlib
import hmac
import json
import secrets
import time
from common import require


class Accounts:
    def __init__(self, directory):
        self.path = directory / 'accounts.json'
        self.users = json.loads(self.path.read_text(encoding='utf-8')) if self.path.exists() else {}
        self.sessions = {}
        if not self.users:
            for name, password, role, sid in [('admin', 'admin123', 'admin', None),
                    ('1001', 'student1001', 'student', 1001), ('1002', 'student1002', 'student', 1002)]:
                self.add(name, password, role, sid)

    def add(self, name, password, role, sid=None):
        salt = secrets.token_hex(16)
        self.users[name] = dict(username=name, role=role, student_id=sid, salt=salt,
                               password=hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex())
        temporary = self.path.with_suffix('.tmp')
        temporary.write_text(json.dumps(self.users, ensure_ascii=False), encoding='utf-8')
        temporary.replace(self.path)

    def login(self, name, password):
        user = self.users.get(name)
        require(user and isinstance(password, str), 'LOGIN_FAILED')
        value = hashlib.pbkdf2_hmac('sha256', password.encode(), user['salt'].encode(), 100000).hex()
        require(hmac.compare_digest(value, user['password']), 'LOGIN_FAILED')
        token = secrets.token_urlsafe(32)
        self.sessions[token] = (user, time.monotonic() + 8 * 3600)
        return token

    def user(self, token):
        user, expires = self.sessions.get(token, ({}, 0))
        if expires < time.monotonic():
            self.sessions.pop(token, None)
            return {}
        return {k: user[k] for k in ('username', 'role', 'student_id')}
