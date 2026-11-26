"""Length-prefixed UTF-8 JSON RPC with absolute deadlines."""
import json
import socket
import struct
import time
import uuid
from common import Error, canonical, require

MAX_BODY = 1048576


def recv_exact(sock, size, deadline):
    data = bytearray()
    while len(data) < size:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('message deadline exceeded')
        sock.settimeout(remaining)
        part = sock.recv(size - len(data))
        if not part:
            raise ConnectionError('connection closed before full message')
        data.extend(part)
    return bytes(data)


def receive(sock, deadline):
    size = struct.unpack('!I', recv_exact(sock, 4, deadline))[0]
    require(0 < size <= MAX_BODY, 'PROTOCOL_ERROR', 'invalid body length')
    try:
        value = json.loads(recv_exact(sock, size, deadline).decode('utf-8'))
    except (ValueError, UnicodeError) as exc:
        raise Error('PROTOCOL_ERROR', 'invalid UTF-8 JSON') from exc
    require(isinstance(value, dict), 'PROTOCOL_ERROR', 'object required')
    return value, size + 4


def send(sock, value, deadline):
    body = canonical(value).encode('utf-8')
    require(0 < len(body) <= MAX_BODY, 'PROTOCOL_ERROR', 'message too large')
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('message deadline exceeded')
    sock.settimeout(remaining)
    sock.sendall(struct.pack('!I', len(body)) + body)
    return len(body) + 4


class Client:
    def __init__(self, cfg, trace):
        self.cfg, self.trace = cfg, trace

    def call(self, node, op, payload=None, tx_id=None):
        address = self.cfg['nodes'][node]
        request = dict(version=1, cluster_id=self.cfg['cluster_id'],
                       coordinator_id=self.cfg['coordinator_id'], rpc_id=uuid.uuid4().hex,
                       op=op, tx_id=tx_id, payload=payload or {})
        for attempt in range(self.cfg['retries'] + 1):
            try:
                deadline = time.monotonic() + self.cfg['request_timeout']
                with socket.create_connection((address['host'], address['port']),
                                              self.cfg['connect_timeout']) as sock:
                    sent = send(sock, request, deadline)
                    reply, received = receive(sock, deadline)
                require(reply.get('version') == 1 and reply.get('rpc_id') == request['rpc_id']
                        and type(reply.get('ok')) is bool, 'PROTOCOL_ERROR', 'invalid response')
                require(all(k in reply for k in ('code', 'message', 'data')) and
                        isinstance(reply['code'], str) and isinstance(reply['message'], str), 'PROTOCOL_ERROR')
                self.trace.emit('RPC', node=node, op=op, tx_id=tx_id, sent=sent,
                                received=received, code=reply.get('code'))
                if not reply['ok']:
                    raise Error(reply['code'], reply.get('message', ''))
                return reply['data']
            except OSError as exc:
                self.trace.emit('RETRY', node=node, op=op, attempt=attempt + 1, reason=str(exc))
                if attempt == self.cfg['retries']:
                    raise Error('NODE_UNAVAILABLE', f'node {node}: {exc}') from exc
                time.sleep(self.cfg['retry_delay'])
