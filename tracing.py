"""Human-readable live tracing plus a persistent diagnostic timeline."""
import json
from common import now


class Trace:
    def __init__(self, path=None, display=False):
        self.path, self.display, self.events = path, display, []

    def emit(self, kind, **fields):
        event = dict(time=now(), kind=kind, **fields)
        self.events.append(event)
        if self.display:
            print(f'[{kind}] ' + json.dumps(fields, ensure_ascii=False), flush=True)
        if self.path:
            try:
                with self.path.open('a', encoding='utf-8') as stream:
                    stream.write(json.dumps(event, ensure_ascii=False) + '\n')
            except OSError:
                pass  # Diagnostic output does not participate in commit decisions.
