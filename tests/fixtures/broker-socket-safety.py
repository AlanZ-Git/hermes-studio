"""Lease rejection and deterministic syscall races, using only temporary sockets."""
from pathlib import Path
import os
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path('packages/server/src/modules/hermes/services/bridge/python').resolve()))
import bridge_broker_socket as lease


def identity(path):
    info = path.lstat()
    return info.st_dev, info.st_ino, info.st_mode


class Safety(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='broker-lease-')
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.path = self.root / 'main.sock'
        self.endpoint = 'ipc://' + str(self.path)

    def tearDown(self):
        self.temporary.cleanup()

    def stale(self):
        lease.make_broker_socket(self.endpoint).close()

    def test_unknown_objects_preserved(self):
        for kind in ('file', 'symlink', 'socket', 'directory'):
            with self.subTest(kind=kind):
                if kind == 'file': self.path.write_text('unknown')
                elif kind == 'symlink': self.path.symlink_to('absent')
                elif kind == 'directory': self.path.mkdir()
                else:
                    with socket.socket(socket.AF_UNIX) as server: server.bind(str(self.path))
                before = identity(self.path)
                with self.assertRaises(RuntimeError): lease.make_broker_socket(self.endpoint)
                self.assertEqual(identity(self.path), before)
                if kind == 'directory': self.path.rmdir()
                else: self.path.unlink()

    def test_unknown_live_listener(self):
        with socket.socket(socket.AF_UNIX) as server:
            server.bind(str(self.path)); server.listen(2)
            before = identity(self.path)
            with self.assertRaises(RuntimeError): lease.make_broker_socket(self.endpoint)
            self.assertEqual(identity(self.path), before)
            with socket.socket(socket.AF_UNIX) as client: client.connect(str(self.path))

    def test_lease_contention(self):
        with lease.make_broker_socket(self.endpoint):
            before = identity(self.path)
            with self.assertRaises(BlockingIOError): lease.make_broker_socket(self.endpoint)
            self.assertEqual(identity(self.path), before)
        self.stale()

    def test_owned_live_without_lock(self):
        with lease.make_broker_socket(self.endpoint) as server:
            server.listen(2); server._broker_lease.close()
            before = identity(self.path)
            with self.assertRaisesRegex(RuntimeError, 'live'): lease.make_broker_socket(self.endpoint)
            self.assertEqual(identity(self.path), before)

    def test_before_capture_replacement_restored(self):
        self.stale(); rename = os.rename; captured = {}
        def race(src, dst, *args, **kwargs):
            if src == self.path.name:
                self.path.rename(self.root / 'old.sock'); self.path.write_text('unknown')
                captured['id'] = identity(self.path)
            return rename(src, dst, *args, **kwargs)
        with patch.object(lease.os, 'rename', side_effect=race):
            with self.assertRaisesRegex(RuntimeError, 'replacement restored'): lease.make_broker_socket(self.endpoint)
        self.assertEqual(identity(self.path), captured['id'])
        self.assertEqual(self.path.read_text(), 'unknown')

    def test_after_capture_new_public_not_overwritten(self):
        self.stale(); rename = os.rename; captured = {}
        def race(src, dst, *args, **kwargs):
            if src == self.path.name:
                self.path.rename(self.root / 'old.sock'); self.path.write_text('captured')
                captured['unknown'] = identity(self.path)
                result = rename(src, dst, *args, **kwargs)
                self.path.write_text('new public'); captured['public'] = identity(self.path)
                return result
            return rename(src, dst, *args, **kwargs)
        with patch.object(lease.os, 'rename', side_effect=race):
            with self.assertRaisesRegex(RuntimeError, 'preserved'): lease.make_broker_socket(self.endpoint)
        self.assertEqual(identity(self.path), captured['public'])
        preserved = list(self.root.glob('.hermes-broker-*/capture-*'))
        self.assertEqual(len(preserved), 1)
        self.assertEqual(identity(preserved[0]), captured['unknown'])
        with self.assertRaisesRegex(RuntimeError, 'preserved/unknown'): lease.make_broker_socket(self.endpoint)

    def test_after_owned_capture_replacement_preserved(self):
        self.stale(); rename = os.rename
        def race(src, dst, *args, **kwargs):
            result = rename(src, dst, *args, **kwargs)
            if src == self.path.name: self.path.write_text('late replacement')
            return result
        with patch.object(lease.os, 'rename', side_effect=race):
            with self.assertRaises(FileExistsError): lease.make_broker_socket(self.endpoint)
        self.assertEqual(self.path.read_text(), 'late replacement')

    def test_failed_bind_releases(self):
        with patch.object(lease._BrokerSocket, 'bind', side_effect=OSError('injected')):
            with self.assertRaises(OSError): lease.make_broker_socket(self.endpoint)
        self.stale()

    def test_failed_constructor_releases(self):
        with patch.object(lease, '_BrokerSocket', side_effect=OSError('injected')):
            with self.assertRaises(OSError): lease.make_broker_socket(self.endpoint)
        self.stale()

    def test_failed_publish_preserves_and_releases(self):
        link = os.link
        def race(src, dst, *args, **kwargs):
            if dst == self.path.name: self.path.write_text('publish replacement')
            return link(src, dst, *args, **kwargs)
        with patch.object(lease.os, 'link', side_effect=race):
            with self.assertRaises(FileExistsError): lease.make_broker_socket(self.endpoint)
        before = identity(self.path)
        with self.assertRaisesRegex(RuntimeError, 'replaced'): lease.make_broker_socket(self.endpoint)
        self.assertEqual(identity(self.path), before)
        self.path.unlink(); self.stale()

    def test_no_fd_leak(self):
        self.path.write_text('unknown'); before = len(os.listdir('/proc/self/fd'))
        for _ in range(25):
            with self.assertRaises(RuntimeError): lease.make_broker_socket(self.endpoint)
        self.assertEqual(len(os.listdir('/proc/self/fd')), before)

    def test_unsafe_state_permissions(self):
        self.stale(); state = next(self.root.glob('.hermes-broker-*')); state.chmod(0o755)
        before = identity(self.path)
        with self.assertRaises(PermissionError): lease.make_broker_socket(self.endpoint)
        self.assertEqual(identity(self.path), before)

    def test_state_symlink(self):
        self.stale(); state = next(self.root.glob('.hermes-broker-*'))
        state.rename(self.root / 'saved'); state.symlink_to(self.root / 'saved', target_is_directory=True)
        with self.assertRaises(OSError): lease.make_broker_socket(self.endpoint)

    def test_lock_and_anchor_permissions(self):
        self.stale(); state = next(self.root.glob('.hermes-broker-*')); lock = state / 'lock'
        lock.chmod(0o644)
        with self.assertRaises(PermissionError): lease.make_broker_socket(self.endpoint)
        lock.chmod(0o600); (state / 'listener').chmod(0o666)
        with self.assertRaises(PermissionError): lease.make_broker_socket(self.endpoint)

    def test_parent_symlink(self):
        alias = self.root / 'alias'; alias.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(OSError): lease.make_broker_socket('ipc://' + str(alias / 'another.sock'))
        self.assertFalse((self.root / 'another.sock').exists())

    def test_exit_replacement_untouched(self):
        server = lease.make_broker_socket(self.endpoint)
        self.path.rename(self.root / 'moved.sock'); self.path.write_text('replacement')
        before = identity(self.path); server.close()
        self.assertEqual(identity(self.path), before)
        with self.assertRaisesRegex(RuntimeError, 'replaced'): lease.make_broker_socket(self.endpoint)


if __name__ == '__main__': unittest.main(verbosity=2)
