from pathlib import Path
import os
import socket
import sqlite3
import tempfile

assert os.environ.get('STUDIO_TEST_ONLY') == '1', 'test-only marker required'
root = Path(os.environ['STUDIO_TEST_ROOT'])
assert root.is_absolute() and root == root.resolve(strict=True) and root != Path(root.anchor)
assert Path(os.environ['HERMES_STUDIO_NOTIFICATION_DB']).resolve(strict=True).is_relative_to(root)
os.environ['HOME'] = str(root / 'home')
os.environ['TMPDIR'] = str(root)
home = Path(tempfile.mkdtemp(prefix='worker-config-', dir=root))
os.environ['HERMES_HOME'] = str(home)
os.environ['HERMES_AGENT_BRIDGE_BASE_HOME'] = str(home)
from bridge_transport import WorkerProcess
from bridge_studio_notifications import StudioNotificationStore
with socket.socket() as listener:
    listener.bind(('127.0.0.1', 0))
    port = listener.getsockname()[1]
import hermes_cli
worker = WorkerProcess('default', 'default', 'tcp://127.0.0.1:' + str(port),
    str(Path(hermes_cli.__file__).resolve().parent.parent), str(home))
try:
    worker.start()
    process = worker.process
    result = worker.request({'action': 'ping'}, allow_start=False)
    assert result['notification_receiver'] == worker._studio_notifications.configuration()
    foreign = home / 'foreign.db'
    with sqlite3.connect(foreign) as db:
        db.execute('CREATE TABLE synthetic(x)')
    os.environ['HERMES_STUDIO_NOTIFICATION_DB'] = str(foreign)
    worker._studio_notifications = StudioNotificationStore()
    try:
        worker.request({'action': 'list'}, allow_start=False)
    except RuntimeError as exc:
        assert 'receiver configuration mismatch' in str(exc), exc
    else:
        raise AssertionError('reused worker accepted a foreign receiver')
    assert worker.process is process and process.poll() is None
finally:
    worker.stop()
assert process.poll() is not None
print('actual worker matched; reused receiver mismatch refused; owned child exited')
