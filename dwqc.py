#!/usr/bin/env python3

import json
import random
import sys
import time
import argparse

from dwq import Job, Disque

def parse_args():
    parser = argparse.ArgumentParser(prog='dwqc', description='dwq: disque-based work queue')

    parser.add_argument('-q', '--queue', type=str,
            help='queue name for jobs (default: \"default\")', default="default")
    parser.add_argument('--version', action='version', version='%(prog)s 0.1')

    parser.add_argument('-r', "--repo", help='git repository to work on', type=str, required=True)
    parser.add_argument('-c', "--commit", help='git commit to work on', type=str, required=True)
    parser.add_argument('-v', "--verbose", help='enable status output', action="store_true" )
    parser.add_argument('-s', "--stdin", help='read from stdin', action="store_true" )
    parser.add_argument('command', type=str, nargs='?')

    return parser.parse_args()

def create_body(repo, commit, command):
    return { "repo" : repo, "commit" : commit, "command" : command }

def queue_job(jobs_set, queue, body, status_queues):
    job_id = Job.add(job_queue, body, status_queues)
    jobs_set.add(job_id)

def vprint(*args, **kwargs):
    global verbose
    if verbose:
        print(*args, **kwargs)

verbose = False
if __name__=="__main__":
    args = parse_args()

    job_queue = args.queue

    Disque.connect(["localhost:7711"])

    status_queue = "status_%s" % random.random()
    verbose = args.verbose

    jobs = set()
    if args.command and not args.stdin:
        queue_job(jobs, job_queue, create_body(args.repo, args.commit, args.command), [status_queue])
        result = Job.wait(status_queue)
        print(result["result"]["output"], end="")
        sys.exit(result["result"]["status"])
    else:
        for line in sys.stdin:
            line = line.rstrip()
            if args.stdin:
                command = args.command.replace("${1}", line)
            else:
                command = line
            job_id = queue_job(jobs, job_queue, create_body(args.repo, args.commit, command), [status_queue])
            vprint("client: job %s command=\"%s\" sent." % (job_id, command))

    total = len(jobs)
    done = 0
    failed = 0
    passed = 0
    while jobs:
       job = Job.wait(status_queue)
       try:
           jobs.remove(job["job_id"])
           done += 1
           vprint("\033[F\033[K", end="")
           vprint("client: job %s done. result=%s" % (job["job_id"], job["result"]["status"]), end="\n\n")
           print(job["result"]["output"], end="")
           if job["result"]["status"] in { "0", "pass" }:
               passed += 1
           else:
               failed += 1
       except KeyError:
           vprint("client: ignoring unknown job result from id=%s" % job["job_id"])

       vprint("\033[F\033[Kclient: %s of %s jobs done (%s passed, %s failed.) " % (done, total, passed, failed))

    if failed > 0:
        sys.exit(1)
