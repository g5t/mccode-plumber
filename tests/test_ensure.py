import os
import stat

import pytest

from mccode_plumber.manage.ensure import ensure_executable


def test_same_named_directory_does_not_shadow_path(tmp_path, monkeypatch):
    bindir = tmp_path / 'bin'
    bindir.mkdir()
    exe = bindir / ('bifrost.exe' if os.name == 'nt' else 'bifrost')
    exe.write_text('#!/bin/sh\n')
    exe.chmod(exe.stat().st_mode | stat.S_IXUSR)
    work = tmp_path / 'work'
    (work / 'bifrost').mkdir(parents=True)
    monkeypatch.chdir(work)
    monkeypatch.setenv('PATH', str(bindir))
    assert ensure_executable('bifrost') == exe


@pytest.mark.skipif(os.name == 'nt', reason='X_OK is not meaningful on Windows')
def test_non_executable_file_is_rejected(tmp_path, monkeypatch):
    f = tmp_path / 'plain'
    f.write_text('')
    monkeypatch.setenv('PATH', '')
    with pytest.raises(FileNotFoundError):
        ensure_executable(f)
