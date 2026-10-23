"""Real facade processes and Unix socket negative controls; temporary endpoints only."""
from pathlib import Path
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

BRIDGE = Path('packages/server/src/modules/hermes/services/bridge/python/hermes_bridge.py').resolve()


def identity(path):
    info = path.lstat()
    return info.st_dev, info.st_ino, info.st_mode


def request(path):
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(2)
        client.connect(str(path))
        client.sendall(b'{"action":"ping"}\n')
        return json.loads(client.makefile('rb').readline())


with tempfile.TemporaryDirectory(prefix='bridge-recovery-') as temporary:
    root = Path(temporary)
    root.chmod(0o700)
    (root / 'home').mkdir()
    children = []
    handles = []
    env = {'PATH': os.defpath, 'HOME': str(root / 'home'), 'HERMES_HOME': str(root / 'home'),
           'TMPDIR': str(root), 'PYTHONDONTWRITEBYTECODE': '1'}

    def start(path, label, ready=True):
        out = (root / f'{label}.out').open('w+')
        err = (root / f'{label}.err').open('w+')
        handles.extend([out, err])
        proc = subprocess.Popen([sys.executable, '-B', str(BRIDGE), '--endpoint', 'ipc://' + str(path),
                                 '--hermes-home', str(root / 'home')], env=env, cwd=root, stdout=out, stderr=err)
        children.append(proc)
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            out.seek(0)
            if '"event": "ready"' in out.read():
                assert ready, 'unexpected endpoint takeover'
                pong = request(path)
                assert pong['pong'] and pong['broker']['pid'] == proc.pid, pong
                return proc
            if proc.poll() is not None:
                err.seek(0)
                assert not ready, err.read()
                assert proc.returncode != 0
                return proc
            time.sleep(.02)
        raise AssertionError('bounded startup deadline')

    def stop(proc, sig=signal.SIGTERM):
        proc.send_signal(sig)
        proc.wait(timeout=6)

    try:
        path = root / 'main.sock'
        a = start(path, 'first')
        stop(a)
        b = start(path, 'graceful')
        stop(b, signal.SIGKILL)
        c = start(path, 'abnormal')
        assert c.pid != b.pid
        before = identity(path)
        start(path, 'contender', ready=False)
        assert identity(path) == before and request(path)['broker']['pid'] == c.pid
        path.rename(root / 'moved.sock')
        path.write_text('exit replacement')
        before = identity(path)
        stop(c)
        assert identity(path) == before and path.read_text() == 'exit replacement'
        start(path, 'replacement', ready=False)
        assert identity(path) == before
        for kind in ('file', 'symlink', 'socket', 'live', 'directory'):
            unknown = root / f'{kind}.sock'
            server = None
            if kind == 'file': unknown.write_text('not owned')
            elif kind == 'symlink': unknown.symlink_to('absent')
            elif kind == 'directory': unknown.mkdir()
            else:
                server = socket.socket(socket.AF_UNIX)
                server.bind(str(unknown))
                if kind == 'live': server.listen(2)
                else: server.close()
            before = identity(unknown)
            try:
                start(unknown, kind, ready=False)
                assert identity(unknown) == before
                if kind == 'live':
                    with socket.socket(socket.AF_UNIX) as client: client.connect(str(unknown))
            finally:
                if server: server.close()
        print('normal/crash recovery, contention, exit replacement and five unknown/live controls passed')
    finally:
        for proc in children:
            if proc.poll() is None: proc.kill(); proc.wait(timeout=6)
        for handle in handles: handle.close()
