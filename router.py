"""Fixed operation routing and co-located schedule queries."""
from common import Error, positive, require


def node_for(value):
    return positive(value) % 3


def query(client, trace, op, payload):
    if op == 'GET_ROSTER':
        positive(payload.get('course_id'))
        rows = []
        for node in range(3):
            trace.emit('ROUTE', node=node, rule='course roster across student fragments', op=op)
            rows.extend(client.call(node, op, payload))
        return sorted(rows, key=lambda r: r['student_id'])[:100]
    if op in ('GET_STUDENT', 'GET_SCHEDULE'):
        node = node_for(payload.get('student_id'))
        trace.emit('ROUTE', node=node, rule='student_id % 3', op=op)
    elif op == 'GET_STOCK':
        node = node_for(payload.get('course_id'))
        trace.emit('ROUTE', node=node, rule='course_id % 3', op=op)
    elif op == 'GET_CATALOG':
        for node in range(3):
            try:
                trace.emit('REPLICA', node=node, op=op)
                return client.call(node, op, payload)
            except Error as exc:
                if exc.code != 'NODE_UNAVAILABLE':
                    raise
        raise Error('NODE_UNAVAILABLE', 'all catalog replicas unavailable')
    else:
        raise Error('INVALID_ARGUMENT', 'unknown operation')
    data = client.call(node, op, payload)
    if op == 'GET_SCHEDULE':
        trace.emit('LOCAL_JOIN', node=node, tables=['Student', 'Enrollment', 'CourseCatalog'],
                   rows=len(data['courses']), query_plan=data['query_plan'])
    return data


def operations(action, sid, cid, stamp):
    require(action in ('enroll', 'drop'))
    result = {}
    pairs = [(node_for(sid), dict(op_type='ENROLL_ADD' if action == 'enroll' else 'ENROLL_REMOVE',
                                 student_id=sid, course_id=cid, enrolled_at=stamp)),
             (node_for(cid), dict(op_type='STOCK_DEC' if action == 'enroll' else 'STOCK_INC', course_id=cid))]
    for node, item in pairs:
        group = result.setdefault(str(node), [])
        group.append(dict(seq=len(group), **item))
    return result
