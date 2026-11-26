"""Recovery follows durable decisions and keeps unfinished work observable."""
from common import Error, require


def finish(coordinator, tx):
    commit = tx['event'] == 'DECIDED_COMMIT'
    target = 'COMMITTED' if commit else 'ABORTED'
    for index, node in enumerate(tx['participants']):
        current = coordinator.client.call(int(node), 'TX_STATUS', tx_id=tx['tx_id'])
        coordinator.trace.emit('PARTICIPANT', tx_id=tx['tx_id'], node=int(node), state=current['state'])
        if current['state'] != target:
            result = coordinator.client.call(int(node), 'COMMIT' if commit else 'ABORT', tx_id=tx['tx_id'])
            require(result['state'] == target, 'TX_CONFLICT')
        coordinator.trace.emit(target, tx_id=tx['tx_id'], node=int(node))
        if commit and index == 0:
            coordinator.crash('after_first_commit')
    coordinator.record('DONE_COMMIT' if commit else 'DONE_ABORT', tx)


def recover(coordinator):
    coordinator.state = 'RECOVERING'
    try:
        require(coordinator.journal.failure is None, 'LOG_CORRUPT', coordinator.journal.failure or '')
        for node in range(3):
            try:
                coordinator.check_node(node)
                pending = coordinator.client.call(node, 'TX_STATUS', {'list_unfinished': True})
                for local in pending:
                    tx = coordinator.journal.transactions.get(local['tx_id'])
                    require(tx and str(node) in tx['participants'], 'LOG_CORRUPT', 'orphan PREPARED')
            except Error as exc:
                if exc.code != 'NODE_UNAVAILABLE':
                    raise
        for tx in list(coordinator.journal.transactions.values()):
            if tx['event'].startswith('DONE_'):
                for node in tx['participants']:
                    try:
                        state = coordinator.client.call(int(node), 'TX_STATUS', tx_id=tx['tx_id'])['state']
                        require(state == ('COMMITTED' if tx['event'] == 'DONE_COMMIT' else 'ABORTED'), 'LOG_CORRUPT')
                    except Error as exc:
                        if exc.code != 'NODE_UNAVAILABLE':
                            raise
                continue
            coordinator.trace.emit('RECOVER', tx_id=tx['tx_id'], state=tx['event'])
            if tx['event'] == 'STARTED':
                coordinator.record('DECIDED_ABORT', tx, reason='interrupted before decision')
            finish(coordinator, tx)
        coordinator.state, coordinator.last_error = 'READY', None
    except (Error, OSError, KeyError, TypeError) as exc:
        coordinator.state, coordinator.last_error = 'BLOCKED', str(exc)
        coordinator.trace.emit('BLOCKED', reason=str(exc))
    return coordinator.state
