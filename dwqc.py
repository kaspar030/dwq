#!/usr/bin/env python3

import json
import random
import os
import sys
import time
import argparse

from dwq import Job, Disque

def nicetime(time):
    secs = round(time)
    minutes = secs/60
    hrs = minutes/60
    days = int(hrs/24)
    secs = int(secs % 60)
    minutes = int(minutes % 60)
    hrs = int(hrs % 24)
    res = ""
    if days:
        res += "%id:" % days
    if hrs:
        res += "%ih:" % hrs
    if minutes:
        if hrs and minutes < 10:
            res += "0"
        res += "%im:" % minutes
    if minutes and secs < 10:
            res += "0"
    res += "%is" % secs
    return res

def parse_args():
    parser = argparse.ArgumentParser(prog='dwqc', description='dwq: disque-based work queue')

    parser.add_argument('-q', '--queue', type=str,
            help='queue name for jobs (default: \"default\")',
            default=os.environ.get("DWQ_QUEUE") or "default")

    parser.add_argument('--version', action='version', version='%(prog)s 0.1')

    parser.add_argument('-r', "--repo", help='git repository to work on', type=str,
            required="DWQ_REPO" not in os.environ, default=os.environ.get("DWQ_REPO"))

    parser.add_argument('-c', "--commit", help='git commit to work on', type=str,
            required="DWQ_COMMIT" not in os.environ, default=os.environ.get("DWQ_COMMIT"))

    parser.add_argument('-v', "--verbose", help='enable status output', action="store_true" )
    parser.add_argument('-P', "--progress", help='enable progress output', action="store_true" )
    parser.add_argument('-s', "--stdin", help='read from stdin', action="store_true" )
    parser.add_argument('-Q', "--quiet", help='don\'t print command output', action="store_true" )
    parser.add_argument('-o', "--outfile", help='write job results to file', type=argparse.FileType('w'))
    parser.add_argument('-b', "--batch", help='send all jobs together', action="store_true")
    parser.add_argument('-e', "--exclusive-jobdir", help='don\'t share jobdirs between jobs', action="store_true")
    parser.add_argument('-E', "--env", help='export environment variable to client', type=str, action="append")

    parser.add_argument('command', type=str, nargs='?')

    return parser.parse_args()

def get_env(env):
    result = {}
    for var in env:
        var = var.split("=", maxsplit=1)
        if len(var) == 1:
            val = os.environ.get(var[0])
            if val:
                var.append(val)
            else:
                continue
        result[var[0]] = var[1]

    return result

def create_body(args, command, options=None):
    body = { "repo" : args.repo, "commit" : args.commit, "command" : command }
    if options:
        body["options"] = options

    if args.env:
        body["env"] = get_env(args.env)

    return body

def queue_job(jobs_set, queue, body, status_queues):
    job_id = Job.add(queue, body, status_queues)
    jobs_set.add(job_id)
    return job_id

def vprint(*args, **kwargs):
    global verbose
    if verbose:
        print(*args, **kwargs)

verbose = False

def main():
    global verbose
    args = parse_args()

    job_queue = args.queue

    Disque.connect(["localhost:7711"])

    status_queue = "status_%s" % random.random()
    verbose = args.verbose

    if args.progress:
        start_time = time.time()

    result_list = []
    try:
        jobs = set()
        batch = []
        if args.command and not args.stdin:
            queue_job(jobs, job_queue, create_body(args, args.command), [status_queue])
            result = Job.wait(status_queue)[0]
            print(result["result"]["output"], end="")
            sys.exit(result["result"]["status"])
        else:
            jobs_read = 0
            vprint("dwqc: reading jobs from stdin")
            for line in sys.stdin:
                line = line.rstrip()
                if args.stdin:
                    cmdargs = line.split(" ")
                    command = args.command
                    for i in range(0, len(cmdargs)):
                        command = command.replace("${%i}" % (i+1), cmdargs[i])
                else:
                    command = line

                tmp = command.split("###")
                command = tmp[0]
                options = {}
                if len(tmp) > 1:
                    options = json.loads(tmp[1])

                if args.exclusive_jobdir:
                    options.update({ "jobdir" : "exclusive" })

                if args.batch:
                    batch.append((job_queue, create_body(args, command, options), [status_queue]))
                else:
                    job_id = queue_job(jobs, job_queue, create_body(args, command, options), [status_queue])
                    vprint("dwqc: job %s command=\"%s\" sent." % (job_id, command))
                    if args.progress:
                        print("")

                if args.progress:
                    jobs_read += 1
                    elapsed = time.time() - start_time
                    print("\033[F\033[K[%s] %s jobs read" \
                            % (nicetime(elapsed), jobs_read), end="\r")

        _time = ""
        if args.batch:
            before = time.time()
            vprint("dwqc: sending jobs")
            for _tuple in batch:
                queue_job(jobs, *_tuple)
            _time = "(took %s)" % nicetime(time.time() - before)
        vprint("dwqc: all jobs sent.", _time)

        if args.progress:
            vprint("")

        total = len(jobs)
        done = 0
        failed = 0
        passed = 0
        while jobs:
            for job in Job.wait(status_queue, count=16):
                try:
                    jobs.remove(job["job_id"])
                    done += 1
                    #if args.progress:
                    #    vprint("\033[F\033[K", end="")
                    #vprint("dwqc: job %s done. result=%s" % (job["job_id"], job["result"]["status"]))
                    if not args.quiet:
                        print(job["result"]["output"], end="")
                    if job["result"]["status"] in { 0, "0", "pass" }:
                        passed += 1
                    else:
                        failed += 1
                    if args.outfile:
                        result_list.append(job)
                except KeyError:
                    vprint("dwqc: ignoring unknown job result from id=%s" % job["job_id"])
                finally:
                    if args.progress:
                        print("")

                if args.progress:
                    elapsed = time.time() - start_time
                    per_job = elapsed / done
                    eta = (total - done) * per_job

                    print("\033[F\033[K[%s] %s/%s jobs done (%s passed, %s failed.) " \
                            "ETA:" % (nicetime(elapsed), done, total, passed, failed), nicetime(eta), end="\r")

        if args.outfile:
            args.outfile.write(json.dumps(result_list))

        if failed > 0:
            sys.exit(1)
    except KeyboardInterrupt:
        print("dwqc: cancelling...")
        Job.cancel_all(jobs)
        sys.exit(1)
