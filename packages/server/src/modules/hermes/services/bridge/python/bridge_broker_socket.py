"""Main-broker Unix listener lease; never infer ownership from connect failure.

A private hardlink anchors the published inode across process death. flock is held
until close; it serializes participating brokers, not arbitrary same-UID writers.
The public name is captured with rename, then checked, never check/unlinked.
Unknown captured replacements are restored without overwrite or preserved with
an explicit error. Normal close leaves an owned stale inode for the next lease.
"""
from __future__ import annotations

import hashlib
import os
import socket
import stat
import uuid
from pathlib import Path


def _identity(info: os.stat_result) -> tuple[int, int]:
    return info.st_dev, info.st_ino


def _at(directory: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=directory, follow_symlinks=False)
    except FileNotFoundError:
        return None


def _refused(path: str) -> None:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.2)
        try:
            probe.connect(path)
        except ConnectionRefusedError:
            return
        # Missing path, timeout, EACCES, etc. are uncertainty, not stale proof.
        raise RuntimeError('broker socket is live; refusing recovery')


class _BrokerSocket(socket.socket):
    _broker_lease: _Lease | None = None

    def close(self) -> None:
        try:
            super().close()
        finally:
            lease = getattr(self, '_broker_lease', None)
            if lease is not None:
                self._broker_lease = None
                lease.close()


class _Lease:
    def __init__(self, path: Path) -> None:
        import fcntl

        self.parent = -1
        self.state = -1
        self.lock = -1
        self.path = path
        if not path.is_absolute() or path.name in {'', '.', '..'}:
            raise ValueError('broker IPC path must be absolute')
        # Pin every ancestor, rejecting symlink traversal and untrusted writers.
        fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
        try:
            for component in path.parent.parts[1:]:
                next_fd = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = next_fd
                info = os.fstat(fd)
                if info.st_uid not in {0, os.getuid()} or (info.st_mode & 0o022 and not info.st_mode & stat.S_ISVTX):
                    raise PermissionError('unsafe broker socket ancestor')
            self.parent = fd
            fd = -1
            digest = hashlib.sha256(os.fsencode(str(path))).hexdigest()[:20]
            self.state_name = f'.hermes-broker-{os.getuid()}-{digest}'
            try:
                os.mkdir(self.state_name, 0o700, dir_fd=self.parent)
            except FileExistsError:
                pass
            self.state = os.open(self.state_name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self.parent)
            info = os.fstat(self.state)
            if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
                raise PermissionError('broker lease directory must be owned mode 0700')
            self.lock = os.open('lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=self.state)
            info = os.fstat(self.lock)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1:
                raise PermissionError('unsafe broker lease lock')
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if set(os.listdir(self.state)) - {'lock', 'listener'}:
                raise RuntimeError('broker lease has preserved/unknown objects; inspect before recovery')
            self.check_directory()
        except BaseException:
            self.close()
            raise
        finally:
            if fd >= 0:
                os.close(fd)

    def check_directory(self) -> None:
        # Also reject a renamed/replaced private directory. Never follow it.
        if _identity(os.fstat(self.parent)) != _identity(self.path.parent.stat()):
            raise RuntimeError('broker socket parent replaced')
        info = _at(self.parent, self.state_name)
        if info is None or _identity(info) != _identity(os.fstat(self.state)):
            raise RuntimeError('broker lease directory replaced')

    def private_path(self, name: str) -> str:
        # Linux fd-relative bind/connect: pinned even if an ancestor is renamed.
        return f'/proc/self/fd/{self.state}/{name}'

    def public_identity(self) -> tuple[int, int]:
        self.check_directory()
        anchor = _at(self.state, 'listener')
        public = _at(self.parent, self.path.name)
        if anchor is None or public is None or _identity(anchor) != _identity(public):
            raise RuntimeError('broker listener replaced before ready')
        return _identity(anchor)

    def recover(self) -> None:
        self.check_directory()
        anchor = _at(self.state, 'listener')
        public = _at(self.parent, self.path.name)
        if anchor is None:
            if public is not None:
                raise RuntimeError('unknown broker socket object; no ownership anchor')
            return
        if not stat.S_ISSOCK(anchor.st_mode) or anchor.st_uid != os.getuid() or stat.S_IMODE(anchor.st_mode) != 0o600:
            raise PermissionError('invalid broker ownership anchor')
        if public is not None and (not stat.S_ISSOCK(public.st_mode) or _identity(public) != _identity(anchor)):
            raise RuntimeError('broker socket replaced; refusing recovery')
        _refused(self.private_path('listener'))
        if public is not None:
            captured = 'capture-' + uuid.uuid4().hex
            os.rename(self.path.name, captured, src_dir_fd=self.parent, dst_dir_fd=self.state)
            info = _at(self.state, captured)
            if info is None or _identity(info) != _identity(anchor):
                # link is create-only. Never clobber a later public replacement.
                try:
                    os.link(captured, self.path.name, src_dir_fd=self.state, dst_dir_fd=self.parent, follow_symlinks=False)
                except OSError as exc:
                    raise RuntimeError(f'concurrent replacement preserved in {self.state_name}/{captured}; public name not overwritten') from exc
                else:
                    os.unlink(captured, dir_fd=self.state)
                raise RuntimeError('concurrent broker socket replacement restored; refusing recovery')
            try:
                _refused(self.private_path(captured))
            except BaseException:
                try:
                    os.link(captured, self.path.name, src_dir_fd=self.state, dst_dir_fd=self.parent, follow_symlinks=False)
                except OSError:
                    pass  # Captured live inode remains preserved in private state.
                raise
            os.unlink(captured, dir_fd=self.state)
        os.unlink('listener', dir_fd=self.state)

    def close(self) -> None:
        # No shared-path mutation on exit. SIGKILL and graceful close converge.
        for name in ('lock', 'state', 'parent'):
            fd = getattr(self, name, -1)
            if fd >= 0:
                setattr(self, name, -1)
                os.close(fd)


def make_broker_socket(endpoint: str) -> socket.socket:
    if not sys_platform_supported():
        raise RuntimeError('safe broker IPC recovery currently requires Linux /proc; use TCP on other platforms')
    path = Path(endpoint.removeprefix('ipc://'))
    lease = _Lease(path)
    server = None
    try:
        server = _BrokerSocket(socket.AF_UNIX, socket.SOCK_STREAM)
        server._broker_lease = lease
        lease.recover()
        server.bind(lease.private_path('listener'))
        os.chmod('listener', 0o600, dir_fd=lease.state, follow_symlinks=False)
        lease.check_directory()
        os.link('listener', path.name, src_dir_fd=lease.state, dst_dir_fd=lease.parent, follow_symlinks=False)
        return server
    except BaseException:
        if server is not None:
            server.close()
        else:
            lease.close()
        raise


def sys_platform_supported() -> bool:
    import sys
    return sys.platform.startswith('linux') and Path('/proc/self/fd').is_dir()
