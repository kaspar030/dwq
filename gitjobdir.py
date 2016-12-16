import os
from threading import Lock
import hashlib
import shutil
import subprocess
import sched, time
import threading
from collections import deque

class GitJobDir(object):
    def __init__(s, basedir=None, maxdirs=4):
        s.basedir = basedir
        s.maxdirs = maxdirs
        s.lock = Lock()
        s.use_counts = {}
        s.unused = deque()

        try:
            os.mkdir(basedir)
        except FileExistsError:
            pass

    def dirkey(repo, commit, extra=None):
        _str = "%s::%s::%s" % (repo, commit, extra or "")
        return hashlib.md5(_str.encode("utf-8")).hexdigest()

    def path(s, dirkey):
        return os.path.join(s.basedir, dirkey)

    def get(s, repo, commit, extra=None):
        with s.lock:

            _dir = s.path(GitJobDir.dirkey(repo, commit, extra))

            _users = s.use_counts.get(_dir)
            if _users == None:
                if s.maxdirs == 0:
                    if not s.clean_unused():
                        print("gitjobdir: could not get free jobdir slot")
                        return None
                try:
                    s.checkout(repo, commit, extra)
                    _users = 0
                    s.maxdirs -= 1
                except subprocess.CalledProcessError:
                    return None

            _users += 1
            s.use_counts[_dir] = _users

            return _dir

    def clean_unused(s):
        while True:
            try:
                _dir = s.unused.popleft()
            except IndexError:
                break

            if s.use_counts.get(_dir):
                continue

            s.use_counts.pop(_dir, None)

            GitJobDir.clean_dir(_dir)
            s.maxdirs += 1
            return True

    def release(s, _dir):
        with s.lock:
            use_count = s.use_counts.get(_dir)
            if use_count == 0 or use_count == None:
                print("GitJobDir: warning: release() on unused job dir!")
            else:
                use_count -= 1
                if use_count == 0:
                    print("GitJobDir: last user of %s gone." % _dir)
                    s.unused.append(_dir)

                s.use_counts[_dir] = use_count

    def clean_dir(_dir):
        print("gitjobdir: cleaning directory", _dir)
        shutil.rmtree(_dir)

    def checkout(s, repo, commit, extra):
        target_path = s.path(GitJobDir.dirkey(repo, commit, extra))
        subprocess.check_call(["git", "cache", "clone", repo, commit, target_path])

    def cleanup(s):
        for _dir, v in s.use_counts.items():
            GitJobDir.clean_dir(_dir)

def print_sth():
    print("sth")

if __name__=="__main__":
    gjd = GitJobDir("/tmp/gitjobdir", maxdirs=1)

    _dira = gjd.get("http://github.com/RIOT-OS/RIOT", "c879154d144a349f890ab74ca8e0c70ded359de8")
    print("got", _dira)
    _dirb = gjd.get("http://github.com/RIOT-OS/RIOT", "96ef13dc9801595f4aec8763bd9c36278b5789e9")
    print("got", _dirb)
    print("releasing first")
    gjd.release(_dira)
    if not _dirb:
        print("trying second again", _dirb)
        _dirb = gjd.get("http://github.com/RIOT-OS/RIOT", "96ef13dc9801595f4aec8763bd9c36278b5789e9")
        print("got", _dirb)

    print("releasing 2nd")
    gjd.release(_dirb)

    _exclusive = gjd.get("http://github.com/RIOT-OS/RIOT", "96ef13dc9801595f4aec8763bd9c36278b5789e9", "TEST")
    gjd.release(_exclusive)

    gjd.cleanup()
