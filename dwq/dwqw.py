#!/usr/bin/env python3

import argparse
import os
import threading
import random
import time
import signal
import socket
import traceback
import multiprocessing
import shutil
import subprocess

import redis
from redis.exceptions import ConnectionError, RedisError

from dwq import Job, Disque

import dwq.cmdserver as cmdserver
from dwq.gitjobdir import GitJobDir

import dwq.util as util

from dwq.version import __version__


def sigterm_handler(signal, stack_frame):
    raise SystemExit()


def parse_args():
    parser = argparse.ArgumentParser(
        prog="dwqw", description="dwq: disque-based work queue (worker)"
    )

    parser.add_argument(
        "--version", action="version", version="%(prog)s " + __version__
    )

    parser.add_argument(
        "-q",
        "--queues",
        type=str,
        help='queues to wait for jobs (default: "default")',
        nargs="*",
        default=["default"],
    )
    parser.add_argument(
        "-j",
        "--jobs",
        help="number of workers to start",
        type=int,
        default=multiprocessing.cpu_count(),
    )
    parser.add_argument(
        "-n",
        "--name",
        type=str,
        help="name of this worker (default: hostname)",
        default=socket.gethostname(),
    )
    parser.add_argument(
        "-v", "--verbose", help="be more verbose", action="count", default=1
    )
    parser.add_argument(
        "-Q", "--quiet", help="be less verbose", action="count", default=0
    )

    return parser.parse_args()


shutdown = False

active_event = threading.Event()


def worker(n, cmd_server_pool, gitjobdir, args, working_set):
    global active_event
    global shutdown
    print("worker %2i: started" % n)
    buildnum = 0
    while not shutdown:
        try:
            if not shutdown and not Disque.connected():
                time.sleep(1)
                continue
            while not shutdown:
                active_event.wait()
                jobs = Job.get(args.queues)
                for job in jobs:
                    if shutdown:
                        job.nack()
                        continue

                    if job.additional_deliveries > 2:
                        error = "too many deliveries (usual reason: timeout)"
                        vprint(2, "worker %2i: %s" % (n, error))
                        job.done(
                            {
                                "status": "error",
                                "output": "dwqw: %s\n" % error,
                                "worker": args.name,
                                "runtime": 0,
                                "body": job.body,
                            }
                        )
                        continue

                    buildnum += 1
                    working_set.add(job.job_id)
                    before = time.time()
                    vprint(
                        2,
                        "worker %2i: got job %s from queue %s"
                        % (n, job.job_id, job.queue_name),
                    )

                    try:
                        repo = job.body["repo"]
                        commit = job.body["commit"]
                        command = job.body["command"]
                    except KeyError:
                        vprint(2, "worker %2i: invalid job json body" % n)
                        job.done(
                            {
                                "status": "error",
                                "output": "worker.py: invalid job description",
                            }
                        )
                        continue

                    vprint(2, 'worker %2i: command="%s"' % (n, command))

                    exclusive = None
                    try:
                        options = job.body.get("options") or {}
                        if options.get("jobdir") or "" == "exclusive":
                            exclusive = str(random.random())
                    except KeyError:
                        pass

                    unique = random.random()

                    _env = os.environ.copy()

                    try:
                        _env.update(job.body["env"])
                    except KeyError:
                        pass

                    _env.update(
                        {
                            "DWQ_REPO": repo,
                            "DWQ_COMMIT": commit,
                            "DWQ_QUEUE": job.queue_name,
                            "DWQ_WORKER": args.name,
                            "DWQ_WORKER_BUILDNUM": str(buildnum),
                            "DWQ_WORKER_THREAD": str(n),
                            "DWQ_JOBID": job.job_id,
                            "DWQ_JOB_UNIQUE": str(unique),
                            "DWQ_CONTROL_QUEUE": job.body.get("control_queues")[0],
                        }
                    )

                    workdir = None
                    workdir_error = None
                    try:
                        try:
                            workdir = gitjobdir.get(
                                repo, commit, exclusive=exclusive or str(n)
                            )
                        except subprocess.CalledProcessError as e:
                            workdir_error = (
                                "dwqw: error getting jobdir. output: \n"
                                + e.output.decode("utf-8")
                            )

                        if not workdir:
                            if job.nacks < options.get("max_retries", 2):
                                job.nack()
                                vprint(
                                    1,
                                    "worker %2i: error getting job dir, requeueing job"
                                    % n,
                                )
                            else:
                                job.done(
                                    {
                                        "status": "error",
                                        "output": workdir_error
                                        or "dwqw: error getting jobdir\n",
                                        "worker": args.name,
                                        "runtime": 0,
                                        "body": job.body,
                                    }
                                )
                                vprint(
                                    1,
                                    "worker %2i: cannot get job dir, erroring job" % n,
                                )
                            working_set.discard(job.job_id)
                            continue

                        util.write_files(options.get("files"), workdir)

                        # assets
                        asset_dir = os.path.join(
                            workdir, "assets", "%s:%s" % (hash(job.job_id), str(unique))
                        )
                        _env.update({"DWQ_ASSETS": asset_dir})

                        timeout = options.get("timeout", 300)

                        if timeout >= 8:
                            # send explit nack before disque times us out
                            # but only if original timeout is not too small
                            timeout -= 2

                        handle = cmd_server_pool.runcmd(
                            command,
                            cwd=workdir,
                            shell=True,
                            env=_env,
                            start_new_session=True,
                        )
                        output, result = handle.wait(timeout=timeout)
                        if handle.timeout:
                            result = "timeout"
                            output = "dwqw: command timed out\n"

                        if (result not in {0, "0", "pass"}) and job.nacks < options.get(
                            "max_retries", 2
                        ):
                            vprint(
                                2,
                                "worker %2i: command:" % n,
                                command,
                                "result:",
                                result,
                                "nacks:",
                                job.nacks,
                                "re-queueing.",
                            )
                            job.nack()
                        else:
                            runtime = time.time() - before

                            options = job.body.get("options")
                            if options:
                                options.pop("files", None)

                            # remove options from body if it is now empty
                            if not options:
                                job.body.pop("options", None)

                            _result = {
                                "status": result,
                                "output": output,
                                "worker": args.name,
                                "runtime": runtime,
                                "body": job.body,
                                "unique": str(unique),
                            }

                            # pack assets
                            try:
                                asset_files = os.listdir(asset_dir)
                                if asset_files:
                                    _result.update(
                                        {
                                            "assets": util.gen_file_data(
                                                asset_files, asset_dir
                                            )
                                        }
                                    )
                                    shutil.rmtree(asset_dir, ignore_errors=True)
                            except FileNotFoundError:
                                pass

                            job.done(_result)

                            vprint(
                                2,
                                "worker %2i: command:" % n,
                                command,
                                "result:",
                                result,
                                "runtime: %.1fs" % runtime,
                            )
                            working_set.discard(job.job_id)
                    except Exception as e:
                        if workdir:
                            gitjobdir.release(workdir)
                        raise e

                    gitjobdir.release(workdir)

        except Exception as e:
            vprint(1, "worker %2i: uncaught exception" % n)
            traceback.print_exc()
            time.sleep(10)
            vprint(1, "worker %2i: restarting worker" % n)


class SyncSet(object):
    def __init__(s):
        s.set = set()
        s.lock = threading.Lock()

    def add(s, obj):
        with s.lock:
            s.set.add(obj)

    def discard(s, obj):
        with s.lock:
            s.set.discard(obj)

    def empty(s):
        with s.lock:
            oldset = s.set
            s.set = set()
            return oldset


verbose = 0


def vprint(n, *args, **kwargs):
    global verbose
    if n <= verbose:
        print(*args, **kwargs)


def handle_control_job(args, job):
    global active_event
    global shutdown
    body = job.body
    status = 0
    result = ""

    try:
        control = body["control"]
        cmd = control["cmd"]
        if cmd == "shutdown":
            vprint(1, "dwqw: shutdown command received")
            result = "shutting down"
            shutdown = 1

        elif cmd == "pause":
            vprint(1, "dwqw: pause command received. pausing ...")
            active_event.clear()
            result = "paused"
        elif cmd == "resume":
            vprint(1, "dwqw: resume command received. resuming ...")
            active_event.set()
            result = "resumed"
        elif cmd == "ping":
            vprint(1, "dwqw: ping received")
            result = "pong"
        else:
            vprint(1, 'dwqw: unknown control command "%s" received' % cmd)

    except KeyError:
        vprint(1, "dwqw: error: invalid control job")

    control_reply(args, job, result, status)

    if shutdown:
        raise SystemExit()


def control_reply(args, job, reply, status=0):
    job.done({"status": status, "output": reply, "worker": args.name, "body": job.body})


def main():
    global shutdown
    global verbose
    global active_event

    args = parse_args()
    verbose = args.verbose - args.quiet

    cmd_server_pool = cmdserver.CmdServerPool(args.jobs)

    signal.signal(signal.SIGTERM, sigterm_handler)

    _dir = "/tmp/dwq.%s" % str(random.random())
    gitjobdir = GitJobDir(_dir, args.jobs)

    servers = ["localhost:7711"]
    try:
        Disque.connect(servers)
        vprint(1, "dwqw: connected.")
    except:
        pass

    working_set = SyncSet()

    for n in range(1, args.jobs + 1):
        threading.Thread(
            target=worker,
            args=(n, cmd_server_pool, gitjobdir, args, working_set),
            daemon=True,
        ).start()

    active_event.set()

    try:
        while True:
            if not Disque.connected():
                try:
                    vprint(1, "dwqw: connecting...")
                    Disque.connect(servers)
                    vprint(1, "dwqw: connected.")
                except RedisError:
                    time.sleep(1)
                    continue

            try:
                control_jobs = Job.get(["control::worker::%s" % args.name])
                for job in control_jobs or []:
                    handle_control_job(args, job)
            except RedisError:
                pass

    except (KeyboardInterrupt, SystemExit):
        vprint(1, "dwqw: shutting down")
        shutdown = True
        cmd_server_pool.destroy()
        vprint(1, "dwqw: nack'ing jobs")
        jobs = working_set.empty()
        d = Disque.get()
        d.nack_job(*jobs)
        vprint(1, "dwqw: cleaning up job directories")
        gitjobdir.cleanup()
