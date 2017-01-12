import os
from threading import Lock
import hashlib
import random
import shutil
import subprocess
import sched, time
import threading

def dictadd(_dict, key, val=1, ret_post=True):
    pre = _dict.get(key)
    post = (pre or 0) + val
    _dict[key] = post
    if ret_post:
        return post
    else:
        return pre

class GitJobDir(object):
    def __init__(s, basedir=None, maxdirs=4):
        s.basedir = basedir
        s.maxdirs = maxdirs
        s.dirs_left = maxdirs
        s.lock = Lock()
        s.use_counts = {}
        s.unused = {}
        s.deferred_clean_delay = 30

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

            if dictadd(s.use_counts, _dir, ret_post=False) == None:
                if s.dirs_left == 0:
                    if not s.clean_unused():
                        s.use_counts.pop(_dir)
                        print("gitjobdir: could not get free jobdir slot")
                        return None
                try:
                    s.dirs_left -= 1
                    s.lock.release()
                    s.checkout(repo, commit, extra)
                    s.lock.acquire()
                except subprocess.CalledProcessError:
                    return None
            else:
                lock = s.unused.pop(_dir, None)
                if lock:
                    lock.release()

            return _dir

    def clean_unused(s):
        for _dir, lock in s.unused.items():
            if s.clean_dir(_dir):
                print("gitjobdir: randomly cleaned", _dir)
                lock.release()
                return True
        return False

    def release(s, _dir):
        with s.lock:
            use_count = s.use_counts.get(_dir)
            if use_count == 0 or use_count == None:
                print("GitJobDir: warning: release() on unused job dir!")
            else:
                if dictadd(s.use_counts, _dir, -1) == 0:
                    print("GitJobDir: last user of %s gone." % _dir)
                    s.clean_deferred(_dir)

    def clean_dir(s, _dir, delete_only=False):
        print("gitjobdir: cleaning directory", _dir)
        if not delete_only:
            s.unused.pop(_dir, None)
            s.use_counts.pop(_dir, None)
            s.dirs_left += 1
        shutil.rmtree(_dir)
        return True

    def clean_deferred(s, _dir):
        lock = Lock()
        lock.acquire()
        s.unused[_dir] = lock
        threading.Thread(target=GitJobDir.clean_deferred_handler, args=(s, _dir, lock)).start()

    def clean_deferred_handler(s, _dir, lock):
        print("clean_deferred_handler() waiting for", _dir)
        lock.acquire(timeout=s.deferred_clean_delay)
        with s.lock:
            if s.unused.get(_dir) is lock:
                print("clean_deferred_handler() triggered for", _dir)
                s.clean_dir(_dir)

    def checkout(s, repo, commit, extra):
        target_path = s.path(GitJobDir.dirkey(repo, commit, extra))
        subprocess.check_call(["git", "cache", "clone", repo, commit, target_path])

    def cleanup(s):
        with s.lock:
            for _dir, v in s.use_counts.items():
                s.clean_dir(_dir, True)

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
    time.sleep(1)

    _dirb = gjd.get("http://github.com/RIOT-OS/RIOT", "96ef13dc9801595f4aec8763bd9c36278b5789e9")
    print("got", _dirb)

    time.sleep(5)
    gjd.cleanup()
