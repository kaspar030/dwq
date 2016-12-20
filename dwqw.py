#!/usr/bin/env python3

import argparse
import json
import sys
import threading
import random
import time
import socket
import traceback
import multiprocessing

import cmdserver
from dwq import Job, Disque

from gitjobdir import GitJobDir

def parse_args():
    parser = argparse.ArgumentParser(prog='dwqw', description='dwq: disque-based work queue (worker)')

    parser.add_argument('--version', action='version', version='%(prog)s 0.1')

    parser.add_argument('-q', '--queues', type=str,
            help='queues to wait for jobs (default: \"default\")', nargs='*', default=["default"])
    parser.add_argument('-j', "--jobs",
            help='number of workers to start', type=int, default=multiprocessing.cpu_count())
    parser.add_argument('-n', '--name', type=str,
            help='name of this worker (default: hostname)', default=socket.gethostname())

    return parser.parse_args()

def worker(n, cmd_server_pool, gitjobdir, args):
    print("worker %2i: started" % n)

    try:
        while True:
            jobs = Job.get(args.queues)
            for job in jobs:
                before = time.time()
                print("worker %2i: got job %s from queue %s" % (n, job.job_id, job.queue_name))

                try:
                    repo = job.body["repo"]
                    commit = job.body["commit"]
                    command = job.body["command"]
                except KeyError:
                    print("worker %2i: invalid job json body" % n)
                    job.done({ "status" : "error", "output" : "worker.py: invalid job description" })
                    continue

                exclusive = None
                try:
                    options = job.body.get("options") or {}
                    if options.get("jobdir") or "" == "exclusive":
                        exclusive = str(random.random())
                except KeyError:
                    pass

                workdir = gitjobdir.get(repo, commit, exclusive)
                if not workdir:
                    job.nack()
                    print("worker %2i: cannot get job dir, requeueing job" % n)
                    continue

                handle = cmd_server_pool.runcmd(command, cwd=workdir, shell=True)
                output, result = handle.wait()

                if (result not in { 0, "0", "pass" }) and job.nacks < (options.get("max_retries") or 2):
                    print("worker %2i: result:", result, "nacks:", job.nacks, "re-queueing.")
                    job.nack()
                else:
                    runtime = time.time() - before
                    job.done({ "status" : result, "output" : output, "worker" : args.name, "runtime" : runtime, "body" : job.body })
                    print("worker %2i: result:" % n, result, "runtime:", runtime)
                gitjobdir.release(workdir)

    except Exception as e:
        print("worker %2i: uncaught exception")
        traceback.print_exc()
        time.sleep(10)

def main():
    args = parse_args()

    cmd_server_pool = cmdserver.CmdServerPool(args.jobs)

    _dir = "/tmp/dwq.%s" % str(random.random())
    gitjobdir = GitJobDir(_dir, args.jobs)

    Disque.connect(["localhost:7711"])

    for n in range(1, args.jobs + 1):
        threading.Thread(target=worker, args=(n, cmd_server_pool, gitjobdir, args), daemon=True).start()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        gitjobdir.cleanup()
