#!/usr/bin/env python3
import json
import sys
import threading
import random
import time
import socket

import cmdserver
from dwq import Job, Disque

from gitjobdir import GitJobDir

def worker(n, cmd_server_pool, gitjobdir):
    print("worker %2i: started" % n)

    my_queues = sys.argv[1:] or ["default"]

    while True:
        jobs = Job.get(my_queues)
        for job in jobs:
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
                options = job.body["options"]
                if options.get("jobdir") or "" == "exclusive":
                    exclusive = str(random.random())
            except KeyError:
                pass

            workdir = gitjobdir.get(repo, commit, exclusive)
            if not workdir:
                job.nack()

            handle = cmd_server_pool.runcmd(command, cwd=workdir, shell=True)
            output, result = handle.wait()

            print("worker %2i: result:" % n, result)
            job.done({ "status" : result, "output" : output, "worker" : hostname })
            gitjobdir.release(workdir)

workers = 4
hostname = "unknown"

def main():
    cmd_server_pool = cmdserver.CmdServerPool(workers)
    gitjobdir = GitJobDir("/tmp/wd", workers)

    hostname = socket.gethostname()

    Disque.connect(["localhost:7711"])

    for n in range(1, workers + 1):
        threading.Thread(target=worker, args=(n, cmd_server_pool, gitjobdir), daemon=True).start()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        gitjobdir.cleanup()
