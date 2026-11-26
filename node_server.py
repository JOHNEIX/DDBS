"""One sequential TCP server per independent SQLite station."""
import argparse
import os
import socket
import sqlite3
import time
from common import Error, load_config, require, response
from protocol import receive, send
from storage import Storage

OPS = {'PING', 'GET_STUDENT', 'GET_CATALOG', 'GET_SCHEDULE', 'GET_STOCK', 'GET_ROSTER',
       'PREPARE', 'COMMIT', 'ABORT', 'TX_STATUS'}


def serve(cfg, node, fault=None):
    store = Storage(cfg, node)
    address = cfg['nodes'][node]
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((address.get('bind_host', address['host']), address['port']))
        server.listen(16)
        print(f'node {node} listening on {address["port"]}', flush=True)
        while True:
            conn, _ = server.accept()
            with conn:
                deadline, rpc_id = time.monotonic() + cfg['request_timeout'], None
                try:
                    r, _ = receive(conn, deadline)
                    rpc_id = r.get('rpc_id')
                    require(set(r) == {'version', 'cluster_id', 'coordinator_id', 'rpc_id', 'op', 'tx_id', 'payload'}, 'PROTOCOL_ERROR')
                    require(type(r['version']) is int and r['version'] == 1 and r['cluster_id'] == cfg['cluster_id'] and
                            r['coordinator_id'] == cfg['coordinator_id'], 'VERSION_MISMATCH')
                    require(isinstance(rpc_id, str) and 0 < len(rpc_id) <= 128, 'PROTOCOL_ERROR')
                    require(isinstance(r['op'], str) and r['op'] in OPS and isinstance(r['payload'], dict))
                    tx = r['tx_id']
                    require(tx is None or isinstance(tx, str) and 0 < len(tx) <= 128)
                    require(r['op'] not in ('PREPARE', 'COMMIT', 'ABORT') or tx is not None)
                    require(r['op'] != 'TX_STATUS' or tx is not None or r['payload'].get('list_unfinished') is True)
                    data = store.dispatch(r['op'], r['payload'], tx)
                    if fault == 'after_prepare' and r['op'] == 'PREPARE' or fault == 'after_commit' and r['op'] == 'COMMIT':
                        os._exit(78)
                    answer = response(rpc_id, data)
                except Error as exc:
                    answer = response(rpc_id, error=exc)
                except (sqlite3.Error, TypeError, KeyError, ValueError) as exc:
                    answer = response(rpc_id, error=Error('TX_CONFLICT', str(exc)))
                except OSError:
                    continue
                try:
                    send(conn, answer, deadline)
                except (OSError, Error):
                    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='config.json')
    parser.add_argument('--node', type=int, choices=range(3), required=True)
    parser.add_argument('--fault', choices=['after_prepare', 'after_commit'])
    args = parser.parse_args()
    serve(load_config(args.config), args.node, args.fault)
